import asyncio
from threading import Event
from unittest import mock

import anyio
import pytest
from starlette.concurrency import run_in_threadpool

from services.image_metrics import ImageMetrics, current_metrics, runtime_phases
from services.isolated_executor import IsolatedExecutor
from services.log_service import LoggedCall, LogService
from services.thread_pool import _ResizableThreadPool


async def wait_event(event):
    for _ in range(2000):
        if event.is_set():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("event timed out")


def test_monitor_and_completion_do_not_wait_for_general_anyio_pool():
    async def scenario():
        entered, release = Event(), Event()
        limiter = anyio.to_thread.current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        executor = IsolatedExecutor(1, 2, "isolated_test")
        def block():
            entered.set()
            assert release.wait(5)
        blocker = asyncio.create_task(run_in_threadpool(block))
        try:
            await wait_event(entered)
            assert await asyncio.wait_for(executor.run(lambda: "available"), 1) == "available"
            assert not blocker.done()
        finally:
            release.set()
            await blocker
            executor._pool.shutdown()
            limiter.total_tokens = previous
    asyncio.run(scenario())


def test_isolated_queue_timing_includes_actual_executor_wait():
    async def scenario():
        executor = IsolatedExecutor(1, 2, "queue_test")
        entered, release = Event(), Event()
        def block():
            entered.set()
            assert release.wait(5)
        first = asyncio.create_task(executor.run(block))
        metrics = ImageMetrics()
        try:
            await wait_event(entered)
            token = current_metrics.set(metrics)
            second = asyncio.create_task(executor.run(lambda: 2))
            current_metrics.reset(token)
            await asyncio.sleep(0.06)
            assert executor.stats()["queued"] == 1
            assert runtime_phases.snapshot()["queue_test_queue"]["active"] == 1
            release.set()
            assert await asyncio.wait_for(second, 2) == 2
            assert metrics.snapshot()["queue_test_queue_ms"] >= 50
            await first
            assert runtime_phases.snapshot()["queue_test_queue"]["active"] == 0
        finally:
            release.set()
            await first
            executor._pool.shutdown()
    asyncio.run(scenario())


def test_isolated_cancelled_queued_task_releases_capacity():
    async def scenario():
        executor = IsolatedExecutor(1, 2, "cancel_test")
        entered, release = Event(), Event()
        def block():
            entered.set()
            assert release.wait(5)
        first = asyncio.create_task(executor.run(block))
        try:
            await wait_event(entered)
            second = asyncio.create_task(executor.run(lambda: 2))
            await asyncio.sleep(0.02)
            second.cancel()
            with pytest.raises(asyncio.CancelledError):
                await second
            await asyncio.sleep(0.02)
            assert executor.stats()["pending"] == 1
            release.set()
            await first
            assert await executor.run(lambda: 3) == 3
            assert executor.stats()["pending"] == 0
        finally:
            release.set()
            await first
            executor._pool.shutdown()
    asyncio.run(scenario())


def test_cancel_during_log_prepare_still_persists_real_bodies(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as logs:
        async def scenario():
            entered, release = Event(), Event()
            call = LoggedCall({"id": "test", "name": "test", "role": "admin"}, "/v1/images/generations", "model", "prompt", request_body={"prompt": "cat"})
            original = call._prepare_log_item
            def prepare(*args, **kwargs):
                entered.set()
                assert release.wait(5)
                return original(*args, **kwargs)
            with mock.patch("services.log_service.log_service", logs), mock.patch.object(call, "_prepare_log_item", side_effect=prepare):
                task = asyncio.create_task(call.log_async("done", response_body={"data": [{"url": "test"}]}))
                try:
                    await wait_event(entered)
                    task.cancel()
                    await asyncio.sleep(0.02)
                    assert not task.done()
                finally:
                    release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
        asyncio.run(scenario())
        item = logs.get(logs.list()[0]["id"])
        assert item["detail"]["request_body"] == {"prompt": "cat"}
        assert item["detail"]["response_body"] == {"data": [{"url": "test"}]}


def test_resizable_pool_keeps_idle_pool_for_close_and_reopens():
    executor = _ResizableThreadPool(1, "lifecycle_test")
    assert executor.submit(lambda: 1).result(2) == 1
    original = executor._pool
    executor.reconfigure(1)
    assert executor._pool is original
    executor.shutdown()
    assert original._shutdown
    with pytest.raises(RuntimeError, match="closed"):
        executor.submit(lambda: 2)
    executor.reconfigure(1)
    assert executor.submit(lambda: 3).result(2) == 3
    executor.shutdown()
