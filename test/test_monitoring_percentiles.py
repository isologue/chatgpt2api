import json
import sqlite3
from unittest import mock

import pytest

from services.log_service import LogService
from test.test_monitoring_statistics import log_item


def seed(path, items):
    path.write_text("".join(json.dumps(item) + "\n" for item in items), encoding="utf-8")


def test_percentiles_use_selected_dates_completed_requests_and_nearest_rank(tmp_path):
    items = [log_item(str(n), "2026-10-28", duration_ms=n * 1000,
                      is_async=n % 2 == 0, response_body={"data": [{"url": "a"}, {"url": "b"}]})
             for n in range(1, 21)]
    items += [log_item("failure", "2026-10-27", "failed", duration_ms=80000, error="network error"),
              log_item("policy", "2026-10-28", "failed", duration_ms=999999, error="content_policy_violation"),
              log_item("pending", "2026-10-28", "processing", duration_ms=999999),
              log_item("text", "2026-10-28", endpoint="/v1/chat/completions", duration_ms=999999)]
    path = tmp_path / "logs.jsonl"
    seed(path, items)
    with LogService(path) as service:
        stats = service.image_stats("2026-10-28", "2026-10-28")
        assert (stats["duration_samples"], stats["duration_p50_ms"], stats["duration_p95_ms"]) == (20, 10000, 19000)
        assert stats["success_images"] == 40  # 每个请求计一次，不按返回图片数加权。
        previous = service.image_stats("2026-10-27", "2026-10-27")
        assert (previous["duration_samples"], previous["duration_p50_ms"], previous["duration_p95_ms"]) == (1, 80000, 80000)
        combined = service.image_stats("2026-10-27", "2026-10-28")
        assert (combined["duration_p50_ms"], combined["duration_p95_ms"]) == (11000, 20000)
        empty = service.image_stats("2026-10-29", "2026-10-29")
        assert empty["duration_samples"] == 0
        assert empty["duration_p50_ms"] is empty["duration_p95_ms"] is None


@pytest.mark.parametrize("value", [None, True, False, -1, "invalid", float("nan"), float("inf"), {}, []])
def test_invalid_or_missing_durations_not_counted(tmp_path, value):
    with LogService(tmp_path / "logs.jsonl") as service:
        service.add("call", detail={"endpoint": "/v1/images/generations", "status": "success", "duration_ms": value})
        assert service.image_stats("2000-01-01", "2100-01-01")["duration_samples"] == 0


def test_incremental_histogram_reuses_index_migrates_and_cleans_up(tmp_path):
    path = tmp_path / "logs.jsonl"
    seed(path, [log_item("a", "2026-10-28", duration_ms=0),
                log_item("b", "2026-10-28", duration_ms="1000.5"),
                log_item("c", "2026-10-28", duration_ms=1000.5)])
    with LogService(path) as original:
        original.initialize()
        with original._index(write=True) as connection:
            connection.execute("DROP TABLE image_latency_daily")
            connection.execute("DELETE FROM image_latency_meta")
    with LogService(path) as service:
        with mock.patch.object(service, "_parse_line", side_effect=AssertionError("must migrate summaries only")):
            stats = service.image_stats("2026-10-28", "2026-10-28")
        assert (stats["duration_samples"], stats["duration_p50_ms"]) == (3, 1000.5)
        with mock.patch("services.log_service.json.loads", side_effect=AssertionError("must not parse summaries on refresh")):
            assert service.image_stats("2026-10-28", "2026-10-28") == stats
        with service._index() as connection:
            assert connection.execute("SELECT count FROM image_latency_daily WHERE duration_ms=1000.5").fetchone() == (2,)
        service.delete(["b", "c"])
        assert service.image_stats("2026-10-28", "2026-10-28")["duration_p95_ms"] == 0
        service.delete(["a"])
        assert service.image_stats("2026-10-28", "2026-10-28")["duration_samples"] == 0
    with LogService(path) as restarted:
        assert restarted.image_stats("2026-10-28", "2026-10-28")["duration_p50_ms"] is None


def test_failed_index_write_rolls_back_latency_histogram(tmp_path):
    with LogService(tmp_path / "logs.jsonl") as service:
        detail = {"endpoint": "/v1/images/generations", "status": "success", "duration_ms": 10}
        service.add("call", detail=detail)
        index = service._index_batch
        def fail(*a):
            index(*a)
            raise OSError("index failed")
        with mock.patch.object(service, "_index_batch", side_effect=fail), pytest.raises(OSError):
            service.add("call", detail={**detail, "duration_ms": 999})
        stats = service.image_stats("2000-01-01", "2100-01-01")
        assert (stats["duration_samples"], stats["duration_p95_ms"]) == (1, 10)


def test_monitoring_api_returns_percentiles_for_requested_range(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import api.system as system
    path = tmp_path / "logs.jsonl"
    seed(path, [log_item("one", "2026-10-28", duration_ms=1234),
                log_item("two", "2026-10-27", duration_ms=9000)])
    with LogService(path) as service:
        monkeypatch.setattr(system, "require_admin", lambda _: None)
        monkeypatch.setattr(system, "log_service", service)
        monkeypatch.setattr(system.account_service, "get_stats", lambda: {})
        monkeypatch.setattr(system, "monitoring_snapshot", lambda accounts, images: {"images": images})
        app = FastAPI()
        app.include_router(system.create_router("test"))
        with TestClient(app) as client:
            response = client.get("/api/monitoring?start_date=2026-10-28&end_date=2026-10-28")
        assert response.status_code == 200
        images = response.json()["images"]
        assert (images["duration_samples"], images["duration_p50_ms"], images["duration_p95_ms"]) == (1, 1234, 1234)
