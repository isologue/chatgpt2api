from __future__ import annotations

import anyio.to_thread


async def apply_anyio_thread_limit(limit: int) -> None:
    """Set the default AnyIO worker-thread capacity for the current event loop."""
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = limit


# Image jobs have their own bounded executor, leaving AnyIO capacity for polling,
# account management and health checks even while upstream generation is slow.
import asyncio
import time
from contextvars import copy_context

from services.image_metrics import current_metrics, phase
import os
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Condition, Lock

from fastapi import HTTPException


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(os.getenv(name, str(default)))))
    except ValueError:
        return default


class BoundedImageExecutor:
    def __init__(self, workers: int = 32, queue_size: int = 128):
        self._lock = Lock()
        self._workers = max(1, workers)
        self._queue_size = max(0, queue_size)
        self._capacity = self._workers + self._queue_size
        self._inflight = 0
        self._pool = _ResizableThreadPool(self._workers, "image-worker")
        self._async_runtime = None
        self._execution_condition = Condition(self._lock)
        self._running = 0

    def reserve(self) -> None:
        with self._lock:
            if self._inflight >= self._capacity:
                raise HTTPException(
                    status_code=429,
                    detail="image queue is full; retry later",
                    headers={"Retry-After": "5"},
                )
            self._inflight += 1

    def release(self) -> None:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)

    def try_start(self):
        with self._lock:
            if self._running >= self._workers:
                return False
            self._running += 1
            return True

    def finish_running(self):
        with self._execution_condition:
            self._running -= 1
            self._execution_condition.notify_all()
        if self._async_runtime is not None:
            self._async_runtime.wake()

    def submit_reserved(self, handler, *args):
        def execute():
            with self._execution_condition:
                while self._running >= self._workers:
                    self._execution_condition.wait()
                self._running += 1
            try:
                return handler(*args)
            finally:
                self.finish_running()
        try:
            future = self._pool.submit(execute)
        except BaseException:
            self.release()
            raise
        future.add_done_callback(lambda _: self.release())
        return future

    def submit_cooperative_reserved(self, handler, *args):
        from services.image_async import ImageAsyncRuntime
        try:
            with self._lock:
                if self._async_runtime is None:
                    self._async_runtime = ImageAsyncRuntime(self)
                runtime = self._async_runtime
            future = runtime.submit(handler, *args)
        except BaseException:
            self.release()
            raise
        future.add_done_callback(lambda _: self.release())
        return future

    def submit(self, handler, *args):
        self.reserve()
        return self.submit_reserved(handler, *args)

    def reconfigure(self, workers: int, queue_size: int) -> None:
        workers = max(1, workers)
        queue_size = max(0, queue_size)
        with self._lock:
            self._workers = workers
            self._queue_size = queue_size
            self._capacity = workers + queue_size
            self._execution_condition.notify_all()
        self._pool.reconfigure(workers)
        if self._async_runtime is not None:
            self._async_runtime.reconfigure(workers)

    def stats(self) -> dict[str, int | bool]:
        with self._lock:
            workers = self._workers
            queue_size = self._queue_size
            capacity = self._capacity
            inflight = self._inflight
        queued = max(0, inflight - workers)
        return {
            "workers": workers,
            "queue_size": queue_size,
            "capacity": capacity,
            "inflight": inflight,
            "queued": min(queue_size, queued),
            "available": max(0, capacity - inflight),
            "full": inflight >= capacity,
        }

    def shutdown(self, wait=True):
        if self._async_runtime is not None:
            self._async_runtime.shutdown(wait=wait)
        self._pool.shutdown(wait=wait, cancel_futures=True)


class _ResizableThreadPool:
    """Keep new work on a resized pool while already submitted work drains."""

    def __init__(self, workers: int, thread_name_prefix: str):
        self._lock = Lock()
        self._thread_name_prefix = thread_name_prefix
        self._workers = workers
        self._closed = False
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name_prefix)
        self._pending: dict[ThreadPoolExecutor, int] = {self._pool: 0}

    def submit(self, handler, *args):
        with self._lock:
            if self._closed:
                raise RuntimeError("executor is closed")
            pool = self._pool
            self._pending[pool] = self._pending.get(pool, 0) + 1
        try:
            future = pool.submit(handler, *args)
        except BaseException:
            self._finish(pool)
            raise
        future.add_done_callback(lambda _: self._finish(pool))
        return future

    def _finish(self, pool: ThreadPoolExecutor) -> None:
        retire = False
        with self._lock:
            remaining = max(0, self._pending.get(pool, 0) - 1)
            if remaining or (pool is self._pool and not self._closed):
                self._pending[pool] = remaining
            else:
                self._pending.pop(pool, None)
                retire = pool is not self._pool
        if retire:
            pool.shutdown(wait=False, cancel_futures=False)

    def reconfigure(self, workers: int) -> None:
        workers = max(1, workers)
        with self._lock:
            if workers == self._workers and not self._closed:
                return
            self._closed = False
            self._workers = workers
            current = self._pool
            next_pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=self._thread_name_prefix)
            self._pool = next_pool
            self._pending[next_pool] = 0
            if self._pending.get(current, 0) == 0:
                self._pending.pop(current, None)
                current.shutdown(wait=False, cancel_futures=False)

    def shutdown(self, wait=True, cancel_futures=False):
        with self._lock:
            self._closed = True
            pools = list(set(self._pending) | {self._pool})
            self._pending.clear()
        for pool in pools:
            pool.shutdown(wait=wait, cancel_futures=cancel_futures)


class BoundedBackgroundExecutor:
    """Bounded best-effort executor for cleanup work that is not request-critical."""
    def __init__(self, workers: int, queue_size: int):
        self._permits = BoundedSemaphore(workers + queue_size)
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="background-worker")

    def submit(self, handler, *args):
        if not self._permits.acquire(blocking=False):
            return None
        try:
            future = self._pool.submit(handler, *args)
        except BaseException:
            self._permits.release()
            raise
        future.add_done_callback(lambda _: self._permits.release())
        return future

    def shutdown(self, wait=True):
        self._pool.shutdown(wait=wait, cancel_futures=True)


image_executor = BoundedImageExecutor(
    _env_int("CHATGPT2API_IMAGE_WORKERS", 32, 1, 10000),
    _env_int("CHATGPT2API_IMAGE_QUEUE_SIZE", 128, 0, 10000),
)


async def run_image_in_threadpool(handler, *args):
    submitted = time.perf_counter()
    metrics = current_metrics.get()
    if metrics is not None:
        from services.config import config
        metrics.diagnostics.configure_budget(config.image_total_timeout_secs)
    context = copy_context()
    if metrics is not None:
        metrics.add("request_dispatch", submitted - metrics.started)

    def execute():
        metrics = current_metrics.get()
        if metrics is not None:
            metrics.add("image_queue_wait", time.perf_counter() - submitted)
        if metrics is None:
            return handler(*args)
        metrics.diagnostics.worker_enter()
        try:
            metrics.diagnostics.remaining()
            return handler(*args)
        finally:
            metrics.diagnostics.worker_exit()

    from services.async_completion import await_worker, submit_off_loop
    # 准入只做短计数，立即返回429；真正提交和线程启动交给固定后台线程。
    image_executor.reserve()
    from services.image_async import supports_cooperative
    submit = image_executor.submit_cooperative_reserved if supports_cooperative(handler, args) else image_executor.submit_reserved
    with phase("image_dispatch_wait"):
        if supports_cooperative(handler, args):
            future = await submit_off_loop(submit, execute)
        else:
            future = await submit_off_loop(submit, context.run, execute)
    from services.image_diagnostics import ImageDeadlineExceeded, ImageRequestCancelled
    try:
        return await await_worker(future, "image_worker")
    except (ImageDeadlineExceeded, ImageRequestCancelled) as exc:
        from services.protocol.conversation import ImageGenerationError
        cancelled = isinstance(exc, ImageRequestCancelled)
        raise ImageGenerationError(
            "The image generation request was cancelled." if cancelled else
            "The image generation request exceeded its total time budget.",
            status_code=499 if cancelled else 504,
            error_type="client_error" if cancelled else "server_error",
            code="client_disconnected" if cancelled else "image_timeout",
        ) from exc


# Separate pools avoid parent jobs waiting for children in the same executor.
image_part_executor = _ResizableThreadPool(
    workers=_env_int("CHATGPT2API_IMAGE_WORKERS", 32, 1, 10000),
    thread_name_prefix="image-part",
)
image_cleanup_executor = BoundedBackgroundExecutor(
    _env_int("CHATGPT2API_IMAGE_CLEANUP_WORKERS", 4, 1, 32),
    _env_int("CHATGPT2API_IMAGE_CLEANUP_QUEUE_SIZE", 128, 0, 10000),
)


# SSE 和下载复用传输线程；父任务与多图子任务各有一套执行器，容量随设置同步。
image_stream_executor = _ResizableThreadPool(
    workers=2 * image_executor.stats()["workers"], thread_name_prefix="image-stream",
)


def configure_image_executors(workers: int, queue_size: int) -> None:
    """Apply image concurrency settings to the running process."""
    image_executor.reconfigure(workers, queue_size)
    image_part_executor.reconfigure(workers)
    image_stream_executor.reconfigure(2 * max(1, workers))


def close_image_async_runtime() -> None:
    """服务停止时先排空协作任务，释放其事件循环；普通执行器仍可重新配置。"""
    runtime = image_executor._async_runtime
    if runtime is not None:
        runtime.shutdown()
        with image_executor._lock:
            image_executor._async_runtime = None
