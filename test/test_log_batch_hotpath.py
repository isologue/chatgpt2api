import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest import mock

import pytest

from services.log_service import LogService
from services.image_metrics import RuntimePhases
from services.image_statistics import image_result_counts
from services.openai_backend_api import OpenAIBackendAPI
from services.storage.batch_writer import BatchWriter


def records(count):
    items = []
    for n in range(count):
        status = ("success", "failed", "safety_rejected")[n % 3]
        body = {"data": [{"b64_json": "x" * 2000}, {"url": "test"}]} if status == "success" else {"error": {"code": "content_policy_violation" if status == "safety_rejected" else "upstream_error", "message": "test"}}
        items.append({"id": str(n), "time": "2026-10-28 12:00:00", "type": "call", "summary": "test",
                      "detail": {"endpoint": "/v1/images/generations", "status": status, "is_async": bool(n % 2),
                                 "key_name": "token-name", "response_format": "b64_json", "duration_ms": n % 7,
                                 "request_body": {"prompt": "unchanged", "extra": [1, None]}, "response_body": body}})
    return items


def test_large_batch_uses_bounded_multi_value_sql_preserves_bodies_and_aggregates(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        service.initialize()
        connection = service._write_connection = sqlite3.connect(service._index_path)
        statements = []
        connection.set_trace_callback(statements.append)
        items = records(256)
        try:
            service._write_batch(items[:128])
            service._write_batch(items[128:])
            assert len(statements) < 100  # 旧路径每条至少4次SQL，256条超过1000次。
            assert [json.loads(line) for line in service.path.read_text(encoding="utf-8").splitlines()] == items
            for item in items:
                assert service.get(item["id"])["detail"]["response_body"] == item["detail"]["response_body"]
            totals = [sum(image_result_counts(item)[i] for item in items) for i in range(4)]
            stats = service.image_stats("2026-10-28", "2026-10-28")
            assert [stats[k] for k in ("success", "failed", "excluded", "success_images")] == totals
            assert stats["duration_samples"] == totals[0] + totals[1]
            with service._index() as con:
                assert con.execute("SELECT count(*) FROM entry_filters WHERE is_async=1").fetchone()[0] == 128
                assert con.execute("SELECT sum(count) FROM image_latency_daily").fetchone()[0] == totals[0] + totals[1]
            sample = service.persistence_stats()["last_batch"]
            assert sample["succeeded"] and sample["items"] == 128
            assert sample["bytes"] > 0 and sample["thread_cpu_ms"] >= 0
            assert set(sample["stages"]) >= {"log_index_update_ms", "log_body_append_ms", "log_body_fsync_ms", "log_index_commit_ms"}
        finally:
            service._close_connection()


def test_batch_failure_rolls_back_every_table_and_body_then_recovers(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        service.add("account", summary="before")
        before = service.path.read_bytes()
        original = service._index_batch
        def fail(*args):
            original(*args)
            raise OSError("batch failure")
        with mock.patch.object(service, "_index_batch", side_effect=fail):
            futures = [service._writer.submit(item) for item in records(128)]
            for future in futures:
                with pytest.raises(OSError, match="batch failure"):
                    future.result(timeout=5)
        assert service.path.read_bytes() == before
        assert service.list_page()["total"] == 1
        assert service.image_stats("2026-10-28", "2026-10-28")["total"] == 0
        assert not service.persistence_stats()["last_batch"]["succeeded"]
        futures = [service._writer.submit(item) for item in records(128)]
        for future in futures:
            future.result(timeout=5)
        assert service.list_page()["total"] == 129


def test_async_500_logs_keep_exact_bodies_and_durable_ack(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        async def run():
            await asyncio.wait_for(asyncio.gather(*(service.add_async("call", detail=item["detail"]) for item in records(500))), 15)
        asyncio.run(run())
        assert service.list_page()["total"] == 500
        assert service._writer.stats()["pending"] == 0
        assert service._writer.stats()["failed"] == 0
        assert service._writer.stats()["max_batch_items"] <= 128


def test_phase_locks_are_independent_and_counts_stay_exact():
    phases = RuntimePhases()
    phases.begin("blocked")
    value = phases._value("blocked")
    with value["lock"], ThreadPoolExecutor(2) as pool:
        # 一个阶段的慢读/写不应堵住另一个阶段。
        pool.submit(phases.begin, "other").result(timeout=1)
    phases.end("blocked", 1)
    def run(_):
        phases.begin("parallel")
        phases.end("parallel", .001)
    with ThreadPoolExecutor(32) as pool:
        list(pool.map(run, range(1000)))
    phases.end("other", 0)
    stats = phases.snapshot()
    assert stats["parallel"]["completed"] == 1000
    assert stats["parallel"]["active"] == 0
    assert stats["parallel"]["sample_count"] == 256
    assert stats["blocked"]["max_ms"] == 1000


def test_backend_close_releases_lazy_stream_executor_without_waiting_or_cancelling():
    entered, release = Event(), Event()
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    from curl_cffi import requests
    session = backend.session = requests.Session()
    def active():
        entered.set()
        assert release.wait(3)
        return "finished"
    executor = session.executor
    future = executor.submit(active)
    try:
        assert entered.wait(1)
        backend.close()
        assert executor._shutdown
        assert not future.cancelled() and not future.done()
        backend.close()
        release.set()
        assert future.result(timeout=2) == "finished"
    finally:
        release.set()
        executor.shutdown()


def test_backend_close_does_not_create_stream_executor():
    from curl_cffi import requests
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.session = requests.Session()
    backend.close()
    assert backend.session._executor is None


def test_writer_reports_current_batch_without_waiting_for_io():
    entered, release = Event(), Event()
    def write(items):
        entered.set()
        assert release.wait(3)
    writer = BatchWriter(write, name="batch-observe")
    try:
        future = writer.submit(1)
        assert entered.wait(1)
        stats = writer.stats()
        assert stats["active_items"] == 1 and stats["active_for_ms"] >= 0
        release.set()
        future.result(timeout=2)
        stats = writer.stats()
        assert stats["active_items"] == 0 and stats["last_batch_items"] == 1
    finally:
        release.set()
        writer.close()


def test_log_chunks_bound_copying_and_do_not_duplicate_large_base64_lines():
    large = b"x" * (2 * 1024 * 1024)
    lines = [b"a" * 700000, b"b" * 700000, large, b"tail"]
    chunks = list(LogService._body_chunks(lines))
    assert b"".join(chunks) == b"".join(lines)
    assert chunks[2] is large
    assert all(len(chunk) <= 1024 * 1024 or chunk is large for chunk in chunks)
