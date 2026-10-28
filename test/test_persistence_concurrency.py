from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest import mock

import anyio
import pytest
from starlette.concurrency import run_in_threadpool

from services.log_service import LoggedCall, LogService
from services.storage.image_index_storage import ImageIndexStorage


def test_image_index_batches_concurrent_writes_without_loss(tmp_path):
    store = ImageIndexStorage(tmp_path / "images.json")
    try:
        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(lambda n: store.put(str(n), {"n": n}), range(256)))
        assert store.all() == {str(n): {"n": n} for n in range(256)}
        assert store._writer.stats()["batches"] < 256
    finally:
        store.close()


def test_image_scan_reconcile_keeps_concurrent_new_and_updated_records(tmp_path):
    store = ImageIndexStorage(tmp_path / "images.json")
    try:
        store.replace({"old": {"version": 1}, "stale": {"version": 1}})
        before = store.all()
        store.put("new", {"version": 2})
        store.put("old", {"version": 2})
        store.reconcile(before, {"old": {"version": 3}, "legacy": {"version": 1}})
        assert store.all() == {"old": {"version": 2}, "new": {"version": 2}, "legacy": {"version": 1}}
    finally:
        store.close()


def test_log_queries_do_not_wait_for_uncommitted_batch(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        service.add("call", detail={"endpoint": "/v1/images/generations", "status": "success"})
        entered, release = Event(), Event()
        original = service._index_batch

        def pause(*args):
            original(*args)
            entered.set()
            assert release.wait(5)

        with mock.patch.object(service, "_index_batch", side_effect=pause), ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(service.add, "call", detail={"status": "success"})
            try:
                assert entered.wait(5)
                def query():
                    return service.list_page()["total"], service.image_stats("2000-01-01", "2099-12-31")
                count, stats = pool.submit(query).result(timeout=2)
                assert count == 1
                assert stats["success"] == 1
            finally:
                release.set()
            writer.result(timeout=5)
        assert service.list_page()["total"] == 2


def test_log_failed_commit_rolls_back_body_and_recovers(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        service.add("call", summary="before")
        original = service.path.read_bytes()
        # Force a transaction failure after appending the new body.
        index_line = service._index_batch
        def fail(*args):
            index_line(*args)
            raise OSError("index failure")
        with mock.patch.object(service, "_index_batch", side_effect=fail):
            with pytest.raises(OSError, match="index failure"):
                service.add("call", summary="failed")
        assert service.path.read_bytes() == original
        service.add("call", summary="after")
        assert [item["summary"] for item in service.list()] == ["after", "before"]


def test_async_call_log_confirmation_leaves_anyio_thread_available(tmp_path, monkeypatch):
    with LogService(tmp_path / "logs.jsonl") as service:
        monkeypatch.setattr("services.log_service.log_service", service)
        entered, release = Event(), Event()
        original = service._write_batch

        def write(items):
            entered.set()
            assert release.wait(5)
            original(items)

        service._writer._write = write
        async def scenario():
            limiter = anyio.to_thread.current_default_thread_limiter()
            previous = limiter.total_tokens
            limiter.total_tokens = 1
            try:
                call = LoggedCall({}, "/v1/images/generations", "gpt-image-2", "test", request_body={"prompt": "cat"})
                task = asyncio.create_task(call.log_async("done", response_body={"data": [{"url": "test"}]}))
                while not entered.is_set():
                    await asyncio.sleep(0.001)
                assert not task.done()
                assert await asyncio.wait_for(run_in_threadpool(lambda: "available"), 1) == "available"
                release.set()
                await asyncio.wait_for(task, 5)
            finally:
                release.set()
                limiter.total_tokens = previous
        asyncio.run(scenario())
        item = service.get(service.list()[0]["id"])
        assert item["detail"]["request_body"] == {"prompt": "cat"}
        assert item["detail"]["response_body"] == {"data": [{"url": "test"}]}
