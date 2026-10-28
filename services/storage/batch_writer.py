from __future__ import annotations

import asyncio
import atexit
import queue
import threading
import time
from concurrent.futures import Future
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class _AsyncWriteFuture(Future):
    def __init__(self, loop):
        super().__init__()
        self.loop = loop
        self.waiter = loop.create_future()
        self.queue_ms = self.write_ms = 0.0
        self.confirmed_at = None


def _deliver_async_batch(futures):
    # 同一批只唤醒事件循环一次，不为每条日志各写一次 self-pipe。
    for future in futures:
        if future.waiter.done():
            continue
        error = future.exception()
        if error is None:
            future.waiter.set_result(None)
        else:
            future.waiter.set_exception(error)


class BatchWriter(Generic[T]):
    """有界、单写线程；Future 只在整批持久化成功后确认，关闭时排空队列。"""

    def __init__(self, write: Callable[[list[T]], None], *, name: str,
                 close: Callable[[], None] | None = None, capacity: int = 1024,
                 batch_size: int = 128, batch_wait: float = 0.004):
        if capacity < 1 or batch_size < 1 or batch_wait < 0:
            raise ValueError("capacity and batch_size must be positive; batch_wait must be nonnegative")
        self._write = write
        self._close = close
        self._name = name
        self._queue: queue.Queue[tuple[T, Future, float]] = queue.Queue(maxsize=capacity)
        self._batch_size = batch_size
        self._batch_wait = batch_wait
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._accepted = self._completed = self._failed = self._batches = 0
        self._last_queue_wait_ms = self._max_queue_wait_ms = 0.0
        self._last_write_ms = self._max_write_ms = 0.0
        self._last_ack_resume_ms = self._max_ack_resume_ms = 0.0
        self._async_wakeups = 0
        self._active_items = self._last_batch_items = self._max_batch_items = 0
        self._active_since = None
        atexit.register(self.close)

    def submit(self, item: T, *, block: bool = True, _future: Future | None = None) -> Future:
        while True:
            with self._lock:
                if self._closed:
                    raise RuntimeError(self._name + " is closed")
                # 生产者都持同一把锁；消费者只会腾出空间。
                if self._queue.full():
                    if not block:
                        raise queue.Full
                else:
                    future = _future if _future is not None else Future()
                    # 已接收的写入不因客户端断开/取消而丢失。
                    future.set_running_or_notify_cancel()
                    self._queue.put_nowait((item, future, time.monotonic()))
                    self._accepted += 1
                    if self._thread is None:
                        self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
                        self._thread.start()
                    return future
            # 仅同步调用方在队列满时等待；不持状态锁。
            time.sleep(0.002)

    async def submit_async(self, item: T) -> None:
        from services.image_metrics import current_metrics, phase

        future = _AsyncWriteFuture(asyncio.get_running_loop())
        with phase("persistence_enqueue"):
            while True:
                try:
                    self.submit(item, block=False, _future=future)
                    break
                except queue.Full:
                    # 未接收之前取消不产生写入；接收后取消也不丢日志。
                    await asyncio.sleep(0.002)
        try:
            await future.waiter
        finally:
            if future.confirmed_at is not None:
                resume_ms = (time.monotonic() - future.confirmed_at) * 1000
                with self._lock:
                    self._last_ack_resume_ms = resume_ms
                    self._max_ack_resume_ms = max(self._max_ack_resume_ms, resume_ms)
                metrics = current_metrics.get()
                if metrics is not None:
                    metrics.add("persistence_queue", future.queue_ms / 1000)
                    metrics.add("persistence_write", future.write_ms / 1000)
                    metrics.add("persistence_ack_resume", resume_ms / 1000)

    def _run(self) -> None:
        try:
            while True:
                try:
                    first = self._queue.get(timeout=0.05)
                except queue.Empty:
                    with self._lock:
                        if self._closed and self._queue.empty():
                            return
                    continue
                batch = [first]
                deadline = time.monotonic() + self._batch_wait
                while len(batch) < self._batch_size:
                    try:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        batch.append(self._queue.get(timeout=remaining))
                    except queue.Empty:
                        break
                error = None
                started = time.monotonic()
                queue_wait_ms = max((started - submitted) * 1000 for _, _, submitted in batch)
                with self._lock:
                    self._active_items = len(batch)
                    self._active_since = started
                try:
                    self._write([item for item, _, _ in batch])
                except BaseException as exc:
                    error = exc
                with self._lock:
                    self._last_queue_wait_ms = queue_wait_ms
                    self._max_queue_wait_ms = max(self._max_queue_wait_ms, queue_wait_ms)
                    self._last_write_ms = (time.monotonic() - started) * 1000
                    self._max_write_ms = max(self._max_write_ms, self._last_write_ms)
                    self._last_batch_items = len(batch)
                    self._max_batch_items = max(self._max_batch_items, len(batch))
                    self._active_items = 0
                    self._active_since = None
                    self._batches += 1
                    self._completed += len(batch)
                    if error is not None:
                        self._failed += len(batch)
                confirmed_at = time.monotonic()
                async_batches = {}
                for _, future, submitted in batch:
                    if isinstance(future, _AsyncWriteFuture):
                        future.queue_ms = (started - submitted) * 1000
                        future.write_ms = (confirmed_at - started) * 1000
                        future.confirmed_at = confirmed_at
                        async_batches.setdefault(future.loop, []).append(future)
                    if error is None:
                        future.set_result(None)
                    else:
                        future.set_exception(error)
                    self._queue.task_done()
                for loop, futures in async_batches.items():
                    try:
                        loop.call_soon_threadsafe(_deliver_async_batch, futures)
                    except RuntimeError:
                        # 调用方事件循环已经退出，持久化仍已完成。
                        continue
                    with self._lock:
                        self._async_wakeups += 1
        finally:
            if self._close is not None:
                self._close()

    def stats(self) -> dict[str, int | float | bool]:
        with self._lock:
            return {"capacity": self._queue.maxsize, "queued": self._queue.qsize(), "full": self._queue.full(),
                    "pending": self._accepted - self._completed,
                    "completed": self._completed, "failed": self._failed, "batches": self._batches,
                    "last_queue_wait_ms": round(self._last_queue_wait_ms, 3),
                    "max_queue_wait_ms": round(self._max_queue_wait_ms, 3),
                    "last_write_ms": round(self._last_write_ms, 3),
                    "max_write_ms": round(self._max_write_ms, 3),
                    "last_ack_resume_ms": round(self._last_ack_resume_ms, 3),
                    "max_ack_resume_ms": round(self._max_ack_resume_ms, 3),
                    "async_wakeups": self._async_wakeups,
                    "active_items": self._active_items,
                    "active_for_ms": round((time.monotonic() - self._active_since) * 1000, 3) if self._active_since is not None else 0.0,
                    "last_batch_items": self._last_batch_items, "max_batch_items": self._max_batch_items}

    def close(self) -> None:
        with self._lock:
            self._closed = True
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
        atexit.unregister(self.close)
