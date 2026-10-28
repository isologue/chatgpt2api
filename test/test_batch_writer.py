from __future__ import annotations

import asyncio
import queue
from threading import Event

import pytest

from services.storage.batch_writer import BatchWriter


def test_batch_acknowledgement_and_close_drain():
    entered, release = Event(), Event()
    written = []

    def write(items):
        entered.set()
        assert release.wait(5)
        written.extend(items)

    writer = BatchWriter(write, name="test-batch", batch_size=64, batch_wait=0.02)
    try:
        futures = [writer.submit(n) for n in range(64)]
        assert entered.wait(5)
        assert not any(future.done() for future in futures)
        release.set()
        writer.close()
        assert written == list(range(64))
        assert all(future.result(timeout=1) is None for future in futures)
        assert writer.stats()["pending"] == 0
        assert writer.stats()["batches"] < 64
        with pytest.raises(RuntimeError, match="closed"):
            writer.submit(65)
    finally:
        release.set()
        writer.close()


def test_bounded_queue_and_failed_batch_do_not_stall_later_writes():
    entered, release = Event(), Event()
    written = []

    def write(items):
        if items == [0]:
            entered.set()
            assert release.wait(5)
            raise OSError("disk failure")
        written.extend(items)

    writer = BatchWriter(write, name="test-bounded", capacity=1, batch_size=1)
    try:
        first = writer.submit(0)
        assert entered.wait(5)
        second = writer.submit(1, block=False)
        with pytest.raises(queue.Full):
            writer.submit(2, block=False)
        assert writer.stats()["queued"] == 1
        release.set()
        with pytest.raises(OSError, match="disk failure"):
            first.result(timeout=5)
        second.result(timeout=5)
        writer.submit(2).result(timeout=5)
        assert written == [1, 2]
        assert writer.stats()["failed"] == 1
    finally:
        release.set()
        writer.close()


def test_async_backpressure_and_cancellation_keep_accepted_writes():
    entered, release = Event(), Event()
    written = []

    def write(items):
        entered.set()
        assert release.wait(5)
        written.extend(items)

    writer = BatchWriter(write, name="test-async", capacity=1, batch_size=1)
    try:
        async def scenario():
            writer.submit(0)
            assert await asyncio.to_thread(entered.wait, 5)
            pending = asyncio.create_task(writer.submit_async(1))
            await asyncio.sleep(0.01)
            assert writer.stats()["pending"] == 2
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            blocked = asyncio.create_task(writer.submit_async(2))
            await asyncio.sleep(0.01)
            assert not blocked.done()
            assert writer.stats()["queued"] == 1
            release.set()
            await asyncio.wait_for(blocked, 5)
        asyncio.run(scenario())
        assert written == [0, 1, 2]
    finally:
        release.set()
        writer.close()


def test_async_confirmation_coalesces_wakeups_without_ack_before_commit():
    entered, release = Event(), Event()
    written = []
    def write(items):
        if not written:
            entered.set()
            assert release.wait(5)
        written.extend(items)
    writer = BatchWriter(write, name="test-async-batch", capacity=1024, batch_size=128, batch_wait=0.01)
    try:
        async def scenario():
            from services.image_metrics import ImageMetrics, current_metrics
            metrics = ImageMetrics()
            token = current_metrics.set(metrics)
            try:
                tasks = [asyncio.create_task(writer.submit_async(n)) for n in range(1000)]
                while not entered.is_set():
                    await asyncio.sleep(0.001)
                assert not any(task.done() for task in tasks)
                release.set()
                await asyncio.wait_for(asyncio.gather(*tasks), 10)
                timings = metrics.snapshot()
                assert timings["persistence_queue_ms"] >= 0
                assert timings["persistence_write_ms"] >= 0
                assert timings["persistence_ack_resume_ms"] >= 0
            finally:
                current_metrics.reset(token)
        asyncio.run(scenario())
        assert written == list(range(1000))
        assert writer.stats()["async_wakeups"] < 100
        assert writer.stats()["pending"] == 0
    finally:
        release.set()
        writer.close()


def test_async_write_failure_reaches_caller_and_later_writes_recover():
    def write(items):
        if "fail" in items:
            raise OSError("commit failed")
    writer = BatchWriter(write, name="test-async-failure")
    try:
        async def scenario():
            with pytest.raises(OSError, match="commit failed"):
                await writer.submit_async("fail")
            await writer.submit_async("after")
        asyncio.run(scenario())
        assert writer.stats()["failed"] == 1
        assert writer.stats()["pending"] == 0
    finally:
        writer.close()


def test_async_confirmation_is_routed_to_each_owning_loop():
    from concurrent.futures import ThreadPoolExecutor
    written = []
    writer = BatchWriter(lambda items: written.extend(items), name="test-multi-loop", batch_wait=0.02)
    try:
        def run(start):
            async def scenario():
                await asyncio.gather(*(writer.submit_async(n) for n in range(start, start + 50)))
            asyncio.run(scenario())
        with ThreadPoolExecutor(2) as pool:
            futures = [pool.submit(run, offset) for offset in (0, 50)]
            for future in futures:
                future.result(timeout=5)
        assert sorted(written) == list(range(100))
        assert writer.stats()["failed"] == 0
    finally:
        writer.close()
