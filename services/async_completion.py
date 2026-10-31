from __future__ import annotations

import asyncio
import time
from threading import Lock
from contextvars import copy_context
from concurrent.futures import Future
from collections import deque
from queue import SimpleQueue
from threading import Thread

from services.image_metrics import current_metrics, runtime_phases


class _LoopCompletions:
    """同一事件循环的线程完成通知合并唤醒，不轮询，不改变执行器容量。"""
    MAX_DELIVERIES_PER_TICK = 64

    def __init__(self, loop):
        self.loop = loop
        self.lock = Lock()
        self.ready = deque()
        self.scheduled = False

    def enqueue(self, future, waiter):
        with self.lock:
            self.ready.append((future, waiter))
            schedule = not self.scheduled
            self.scheduled = True
        if schedule:
            try:
                self.loop.call_soon_threadsafe(self.deliver)
            except RuntimeError:
                # 事件循环已经关闭；底层执行器的资源释放回调仍然执行。
                with self.lock:
                    self.ready.clear()
                    self.scheduled = False

    def deliver(self):
        with self.lock:
            ready = [self.ready.popleft() for _ in range(min(len(self.ready), self.MAX_DELIVERIES_PER_TICK))]
            has_more = bool(self.ready)
            self.scheduled = has_more
        started = time.perf_counter()
        for future, waiter in ready:
            if waiter.done():
                continue
            if future.cancelled():
                waiter.cancel()
            else:
                try:
                    result = future.result()
                except BaseException as error:
                    waiter.set_exception(error)
                else:
                    waiter.set_result(result)
        runtime_phases.observe("completion_delivery", time.perf_counter() - started)
        if has_more:
            # 让同一事件循环先处理其它就绪事件，避免大批完成通知形成长回调。
            self.loop.call_soon(self.deliver)


async def await_worker(future, name=None):
    loop = asyncio.get_running_loop()
    bridge = getattr(loop, "_cg2api_completions", None)
    if bridge is None:
        bridge = loop._cg2api_completions = _LoopCompletions(loop)
    waiter = loop.create_future()
    finished = []

    def completed(worker):
        at = time.perf_counter()
        finished.append(at)
        bridge.enqueue(worker, waiter)

    future.add_done_callback(completed)
    try:
        return await waiter
    except asyncio.CancelledError:
        future.cancel()  # 未开始的工作可取消；已开始的工作仍由执行器自然收尾。
        raise
    finally:
        if finished and name:
            elapsed = time.perf_counter() - finished[0]
            runtime_phases.observe(name + "_resume", elapsed)
            metrics = current_metrics.get()
            if metrics is not None:
                metrics.add(name + "_resume", elapsed)


class _SubmissionDispatcher:
    """固定线程只执行提交和启动，业务任务仍在各自的有界执行器中。"""
    def __init__(self):
        self.queue = SimpleQueue()
        for index in range(4):
            Thread(target=self._run, name=f"executor-submit-{index}", daemon=True).start()

    def _run(self):
        while True:
            future, context, submit, args = self.queue.get()
            if not future.set_running_or_notify_cancel():
                continue
            try:
                result = context.run(submit, *args)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

    def submit(self, submit, args):
        future = Future()
        self.queue.put((future, copy_context(), submit, args))
        return future


# 模块初始化时建立，接入请求不会触发线程创建，也不依赖通用 AnyIO 池。
_submission_dispatcher = _SubmissionDispatcher()


async def submit_off_loop(submit, *args):
    """执行器提交/线程启动在后台完成，取消时仍接管已提交任务的清理。"""
    future = _submission_dispatcher.submit(submit, args)
    submission = asyncio.create_task(await_worker(future, "executor_submit"))
    try:
        return await asyncio.shield(submission)
    except asyncio.CancelledError:
        def abandon(completed):
            if not completed.cancelled():
                try:
                    completed.result().cancel()
                except BaseException:
                    pass  # 取走异常，提交方负责释放准入容量。
        submission.add_done_callback(abandon)
        raise


async def await_submitted(submit, *args):
    """后台提交后直接连接工作结果，只让业务协程收到一次完成通知。

    取消发生在提交前、提交中或结果等待中，都能接管随后出现的工作 Future。
    调用方需要拿回资源时可 shield 本协程，沿用 run_blocking 的收尾约定。
    """
    result = Future()
    submitted = _submission_dispatcher.submit(submit, args)
    worker = []

    def finished(completed):
        # 只有提交失败/取消或工作完成中的一个路径会进入；与取消原子竞争。
        if not result.set_running_or_notify_cancel():
            return
        if completed.cancelled():
            # 工作池主动取消时向等待方保留取消语义。
            result.set_exception(asyncio.CancelledError())
            return
        try:
            value = completed.result()
        except BaseException as exc:
            result.set_exception(exc)
        else:
            result.set_result(value)

    def attached(completed):
        if completed.cancelled() or completed.exception() is not None:
            finished(completed)
            return
        work = completed.result()
        worker.append(work)
        if result.cancelled():
            work.cancel()
        work.add_done_callback(finished)

    def cancelled(completed):
        if completed.cancelled():
            submitted.cancel()
            if worker:
                worker[0].cancel()

    result.add_done_callback(cancelled)
    submitted.add_done_callback(attached)
    return await await_worker(result)
