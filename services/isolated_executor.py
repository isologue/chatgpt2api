from __future__ import annotations

import asyncio
from contextvars import copy_context
from threading import BoundedSemaphore, Lock

from services.image_metrics import phase
from services.async_completion import await_worker, submit_off_loop
from services.thread_pool import _ResizableThreadPool


class IsolatedExecutor:
    """协程等待有界执行器，不占用通用 AnyIO 接入线程。"""

    def __init__(self, workers, capacity, name):
        self._permits = BoundedSemaphore(capacity)
        self._lock = Lock()
        self._pending = self._active = 0
        self._capacity = capacity
        self._name = name
        self._pool = _ResizableThreadPool(workers, name)

    async def run(self, handler, *args, **kwargs):
        context = copy_context()
        queue_phase = phase(self._name + "_queue")
        queue_phase.__enter__()
        queue_lock = Lock()
        queue_ended = False

        def finish_queue():
            nonlocal queue_ended
            with queue_lock:
                if not queue_ended:
                    queue_ended = True
                    queue_phase.__exit__(None, None, None)

        try:
            while not self._permits.acquire(blocking=False):
                await asyncio.sleep(0.005)
        except BaseException:
            finish_queue()
            raise
        with self._lock:
            self._pending += 1

        def execute():
            finish_queue()
            with self._lock:
                self._active += 1
            try:
                with phase(self._name + "_work"):
                    return handler(*args, **kwargs)
            finally:
                with self._lock:
                    self._active -= 1

        def schedule():
            # 提交和回调注册整体在后台完成，取消提交协程也不会泄漏许可。
            try:
                future = self._pool.submit(context.run, execute)
            except BaseException:
                finish_queue()
                self._finish(None)
                raise
            future.add_done_callback(lambda _: finish_queue())
            future.add_done_callback(self._finish)
            return future

        future = await submit_off_loop(schedule)
        return await await_worker(future, self._name)

    def _finish(self, _):
        with self._lock:
            self._pending -= 1
        self._permits.release()

    def stats(self):
        with self._lock:
            return {"capacity": self._capacity, "pending": self._pending, "active": self._active,
                    "queued": self._pending - self._active}

    def reconfigure(self, workers):
        self._pool.reconfigure(workers)


completion_executor = IsolatedExecutor(16, 1024, "image_completion")
monitoring_executor = IsolatedExecutor(2, 8, "monitoring")

image_stats_executor = IsolatedExecutor(2, 8, "monitoring_image_stats")
