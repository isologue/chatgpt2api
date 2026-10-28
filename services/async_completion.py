from __future__ import annotations

import asyncio
import time
from threading import Lock

from services.image_metrics import current_metrics, runtime_phases


class _LoopCompletions:
    """同一事件循环的线程完成通知合并唤醒，不轮询，不改变执行器容量。"""
    MAX_DELIVERIES_PER_TICK = 64

    def __init__(self, loop):
        self.loop = loop
        self.lock = Lock()
        self.ready = []
        self.scheduled = False

    def enqueue(self, future, waiter, finished):
        with self.lock:
            self.ready.append((future, waiter, finished))
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
            ready = self.ready[:self.MAX_DELIVERIES_PER_TICK]
            self.ready = self.ready[len(ready):]
            has_more = bool(self.ready)
            self.scheduled = has_more
        started = time.perf_counter()
        for future, waiter, _ in ready:
            if waiter.done():
                continue
            if future.cancelled():
                waiter.cancel()
            else:
                error = future.exception()
                if error is None:
                    waiter.set_result(future.result())
                else:
                    waiter.set_exception(error)
        runtime_phases.observe("completion_delivery", time.perf_counter() - started)
        if has_more:
            # 让同一事件循环先处理其它就绪事件，避免大批完成通知形成长回调。
            self.loop.call_soon(self.deliver)


async def await_worker(future, name):
    loop = asyncio.get_running_loop()
    bridge = getattr(loop, "_cg2api_completions", None)
    if bridge is None:
        bridge = loop._cg2api_completions = _LoopCompletions(loop)
    waiter = loop.create_future()
    finished = []

    def completed(worker):
        at = time.perf_counter()
        finished.append(at)
        bridge.enqueue(worker, waiter, at)

    future.add_done_callback(completed)
    try:
        return await waiter
    except asyncio.CancelledError:
        future.cancel()  # 未开始的工作可取消；已开始的工作仍由执行器自然收尾。
        raise
    finally:
        if finished:
            elapsed = time.perf_counter() - finished[0]
            runtime_phases.observe(name + "_resume", elapsed)
            metrics = current_metrics.get()
            if metrics is not None:
                metrics.add(name + "_resume", elapsed)
