"""图片专用协作运行时：保留同步协议编排，在明确的 I/O 边界让出执行权。

不做全局 monkey patch。一个线程承载协程，HTTP/退避不占工作线程；短阻塞操作
通过 blocking() 离开循环。greenlet 只用于衔接现有同步协议与 asyncio。
"""
from __future__ import annotations

import asyncio
import time
from concurrent.futures import Future
from contextvars import copy_context, ContextVar
from functools import wraps
from threading import Event, Thread

from greenlet import getcurrent, greenlet


_runtime_context = ContextVar('image_async_runtime', default=None)

def cooperative() -> bool:
    return bool(getattr(getcurrent(), '_image_cooperative', False))


def await_io(awaitable):
    current = getcurrent()
    if not getattr(current, '_image_cooperative', False):
        awaitable.close()
        raise RuntimeError('image async I/O called outside the image runtime')
    return current.parent.switch(awaitable)


async def run_cooperative(handler, *args):
    """每个 asyncio Task 有自己的 Context；阶段、预算、账号信息随切换保留。"""
    from services.image_loop_work import get_loop_work
    work = get_loop_work()
    child = greenlet(handler, parent=getcurrent())
    child._image_cooperative = True
    child.gr_context = getcurrent().gr_context
    def advance(callback, *values):
        from services.image_performance import current_stage, record, slow_slice, SLOW_SLICE_SECS
        entry_stage = child.gr_context.get(current_stage, 'other')
        started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            return callback(*values)
        finally:
            elapsed, cpu = time.perf_counter() - started, time.thread_time() - cpu_started
            work.record('business_slice', started, elapsed, cpu)
            # 让出点所属的最内层业务阶段；结束片段退回外层阶段或 other。
            stage = child.gr_context.get(current_stage, 'other')
            if stage == 'other':
                stage = entry_stage
            record('sync_slice', elapsed, stage=stage)
            record('sync_slice_cpu', cpu, stage=stage)
            if elapsed >= SLOW_SLICE_SECS:
                slow_slice(stage, started, elapsed, cpu)

    result = advance(child.switch, *args)
    while not child.dead:
        try:
            runtime = _runtime_context.get()
            if runtime is not None:
                from services.image_metrics import current_metrics
                metrics = current_metrics.get()
                if metrics is not None:
                    runtime._tasks[asyncio.current_task()] = metrics.diagnostics
            value = await result
        except BaseException as exc:
            from services.image_diagnostics import ImageRequestCancelled, remaining_image_budget
            if isinstance(exc, asyncio.CancelledError):
                try:
                    remaining_image_budget()
                except (Exception,) as budget_error:
                    exc = budget_error
                else:
                    exc = ImageRequestCancelled('image execution cancelled')
            result = advance(child.throw, exc)
        else:
            result = advance(child.switch, value)
    return result


async def run_blocking(handler, *args, **kwargs):
    from services.async_completion import await_submitted
    from services.thread_pool import image_part_executor
    from services.image_performance import current_stage, record
    context = copy_context()
    stage = current_stage.get()
    scheduled = time.perf_counter()
    timing = {}

    def execute():
        timing['started'] = time.perf_counter()
        try:
            return handler(*args, **kwargs)
        finally:
            timing['finished'] = time.perf_counter()

    # 提交与工作完成直接串接；即使取消也接回选号/写盘结果再由上层释放资源。
    waiter = asyncio.create_task(await_submitted(image_part_executor.submit, context.run, execute))
    try:
        while True:
            try:
                return await asyncio.shield(waiter)
            except asyncio.CancelledError:
                if waiter.done():
                    return waiter.result()
    finally:
        if 'started' in timing:
            record('offload_queue', timing['started'] - scheduled, stage=stage)
        if 'finished' in timing:
            record('offload_work', timing['finished'] - timing['started'], stage=stage)
            record('offload_resume', time.perf_counter() - timing['finished'], stage=stage)


def blocking(handler):
    """仅协作路径离开循环；原有同步调用与测试不变。"""
    @wraps(handler)
    def invoke(*args, **kwargs):
        if cooperative():
            return await_io(run_blocking(handler, *args, **kwargs))
        return handler(*args, **kwargs)
    return invoke


def parallel_calls(handler, arguments):
    async def child(args):
        try:
            return await run_cooperative(handler, *args)
        finally:
            runtime = _runtime_context.get()
            if runtime is not None:
                runtime._tasks.pop(asyncio.current_task(), None)
    async def gather():
        return await asyncio.gather(*(child(args) for args in arguments), return_exceptions=True)
    return await_io(gather())


class ImageAsyncRuntime:
    def __init__(self, executor):
        self.executor = executor
        self.active = 0
        self._closed = False
        self._ready = Event()
        self._thread = Thread(target=self._serve, name='image-async-io', daemon=True)
        self._thread.start()
        self._ready.wait()

    def _serve(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._condition = asyncio.Condition()
        self._tasks = {}
        self._watch = self.loop.create_task(self._watch_requests())
        self._ready.set()
        try:
            self.loop.run_forever()
        finally:
            self._watch.cancel()
            self.loop.run_until_complete(asyncio.gather(self._watch, return_exceptions=True))
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            work = getattr(self.loop, '_image_loop_work', None)
            if work is not None:
                work.flush(force=True)
            self.loop.close()

    async def _watch_requests(self):
        from services.image_metrics import runtime_phases
        from services.image_loop_work import get_loop_work
        work = get_loop_work()
        while True:
            target = self.loop.time() + 0.1
            await asyncio.sleep(0.1)
            lag = max(0.0, self.loop.time() - target)
            runtime_phases.observe('image_io_loop_lag', lag)
            work.flush(lag)
            for task, diagnostics in tuple(self._tasks.items()):
                if diagnostics is not None and diagnostics.cancelled.is_set() and not task.cancelling():
                    task.cancel()

    async def _execute(self, future, handler, args):
        from services.image_metrics import current_metrics, phase
        metrics = current_metrics.get()
        task = asyncio.current_task()
        self._tasks[task] = metrics.diagnostics if metrics else None
        runtime_token = _runtime_context.set(self)
        acquired = False
        try:
            with phase('image_admission_wait'):
                async with self._condition:
                    while True:
                        if future.cancelled():
                            return
                        remaining = metrics.diagnostics.remaining() if metrics else None
                        if self.executor.try_start():
                            break
                        try:
                            if remaining is None:
                                await self._condition.wait()
                            else:
                                async with asyncio.timeout(remaining):
                                    await self._condition.wait()
                        except TimeoutError:
                            metrics.diagnostics.remaining()
                            raise
                    if not future.set_running_or_notify_cancel():
                        self.executor.finish_running()
                        return
                    self.active += 1
                    acquired = True
            result = await run_cooperative(handler, *args)
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                from services.image_diagnostics import ImageRequestCancelled
                exc = ImageRequestCancelled('image execution cancelled')
            if not future.done():
                future.set_exception(exc)
        else:
            if not future.done():
                future.set_result(result)
        finally:
            self._tasks.pop(task, None)
            _runtime_context.reset(runtime_token)
            if acquired:
                async with self._condition:
                    self.active -= 1
                    self.executor.finish_running()
                    self._condition.notify_all()

    def submit(self, handler, *args):
        if self._closed:
            raise RuntimeError('image async runtime is closed')
        future = Future()
        # 从提交方复制请求 context，不能继承首个创建运行时的请求。
        context = copy_context()
        self.loop.call_soon_threadsafe(lambda: self.loop.create_task(self._execute(future, handler, args)), context=context)
        # 未开始的取消也唤醒准入等待，不能让已取消条目一直滞留队尾。
        future.add_done_callback(lambda _: self.loop.call_soon_threadsafe(self._wake) if not self._closed else None)
        return future

    def _wake(self):
        async def notify():
            async with self._condition:
                self._condition.notify_all()
        self.loop.create_task(notify())

    def wake(self):
        if not self.loop.is_closed():
            self.loop.call_soon_threadsafe(self._wake)

    def reconfigure(self, workers):
        self.wake()

    def shutdown(self, wait=True):
        if self._closed:
            if wait:
                self._thread.join()
            return
        self._closed = True
        async def drain():
            while self._tasks:
                await asyncio.sleep(0.01)
            self.loop.call_soon(self.loop.stop)
        drained = asyncio.run_coroutine_threadsafe(drain(), self.loop)
        if wait:
            drained.result()
            self._thread.join()


def supports_cooperative(handler, args) -> bool:
    if getattr(handler, '__module__', '') not in {
        'services.protocol.openai_v1_image_generations', 'services.protocol.openai_v1_image_edit',
    } or getattr(handler, '__name__', '') != 'handle':
        return False
    body = args[0] if args and isinstance(args[0], dict) else {}
    if body.get('stream'):
        return False
    from utils.helper import is_codex_image_model
    return not is_codex_image_model(str(body.get('model') or 'gpt-image-2'))
