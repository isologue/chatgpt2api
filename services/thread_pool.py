from __future__ import annotations

import anyio.to_thread


async def apply_anyio_thread_limit(limit: int) -> None:
    """Set the default AnyIO worker-thread capacity for the current event loop."""
    limiter = anyio.to_thread.current_default_thread_limiter()
    limiter.total_tokens = limit


# Image jobs have their own bounded executor, leaving AnyIO capacity for polling,
# account management and health checks even while upstream generation is slow.
import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore, Lock

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

    def submit_reserved(self, handler, *args):
        try:
            future = self._pool.submit(handler, *args)
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
        self._pool.reconfigure(workers)

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
        self._pool.shutdown(wait=wait, cancel_futures=True)


class _ResizableThreadPool:
    """Keep new work on a resized pool while already submitted work drains."""

    def __init__(self, workers: int, thread_name_prefix: str):
        self._lock = Lock()
        self._thread_name_prefix = thread_name_prefix
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=thread_name_prefix)
        self._pending: dict[ThreadPoolExecutor, int] = {self._pool: 0}

    def submit(self, handler, *args):
        with self._lock:
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
            if remaining:
                self._pending[pool] = remaining
            else:
                self._pending.pop(pool, None)
                retire = pool is not self._pool
        if retire:
            pool.shutdown(wait=False, cancel_futures=False)

    def reconfigure(self, workers: int) -> None:
        workers = max(1, workers)
        with self._lock:
            current = self._pool
            next_pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix=self._thread_name_prefix)
            self._pool = next_pool
            self._pending[next_pool] = 0
            if self._pending.get(current, 0) == 0:
                self._pending.pop(current, None)
                current.shutdown(wait=False, cancel_futures=False)

    def shutdown(self, wait=True, cancel_futures=False):
        with self._lock:
            pools = list(self._pending)
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
    return await asyncio.wrap_future(image_executor.submit(handler, *args))


# Separate pools avoid parent jobs waiting for children in the same executor.
image_part_executor = _ResizableThreadPool(
    workers=_env_int("CHATGPT2API_IMAGE_WORKERS", 32, 1, 10000),
    thread_name_prefix="image-part",
)
account_info_executor = ThreadPoolExecutor(max_workers=32, thread_name_prefix="account-info")
image_cleanup_executor = BoundedBackgroundExecutor(
    _env_int("CHATGPT2API_IMAGE_CLEANUP_WORKERS", 4, 1, 32),
    _env_int("CHATGPT2API_IMAGE_CLEANUP_QUEUE_SIZE", 128, 0, 10000),
)


def configure_image_executors(workers: int, queue_size: int) -> None:
    """Apply image concurrency settings to the running process."""
    image_executor.reconfigure(workers, queue_size)
    image_part_executor.reconfigure(workers)
