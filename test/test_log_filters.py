from __future__ import annotations

import json
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.log_service import LogService


def seed_logs(tmp_path, count=1005):
    path = tmp_path / "logs.jsonl"
    with path.open("w", encoding="utf-8") as target:
        for number in range(count):
            detail = {
                "key_name": "old-token" if number < 10 else "new-token",
                "is_async": number % 2 == 0,
                "response_format": "url" if number % 2 == 0 else "b64_json",
                "status": "success", "endpoint": "/v1/images/generations",
                "request_body": {"prompt": "test"}, "response_body": {"data": [{"url": "test"}]},
            }
            target.write(json.dumps({"id": str(number), "time": "2026-10-27 12:00:00", "type": "call", "detail": detail}) + "\n")
    return LogService(path)


def test_pagination_reaches_all_logs_and_limits_loaded_summaries(tmp_path):
    service = seed_logs(tmp_path)
    first = service.list_page(type="call", page=1, page_size=100)
    assert first["total"] == 1005
    assert len(first["items"]) == 100
    assert first["items"][0]["id"] == "1004"
    assert "request_body" not in first["items"][0]["detail"]
    # 索引同步后，深页只反序列化当前页，且无需重新读取 JSONL。
    with mock.patch.object(service, "_parse_line", side_effect=AssertionError("must use index")):
        with mock.patch("services.log_service.json.loads", wraps=json.loads) as loads:
            last = service.list_page(type="call", page=11, page_size=100)
            assert loads.call_count == 5
    assert [item["id"] for item in last["items"]] == ["4", "3", "2", "1", "0"]
    assert last["total"] == 1005
    assert service.list_page(page=10000, page_size=100)["page"] == 11
    assert len(service.list_page(page_size=1000)["items"]) == 100
    filtered = service.list_page(type="call", key_name="old-token", is_async=False, response_format="b64_json", status="success")
    assert filtered["total"] == 5
    assert [item["id"] for item in filtered["items"]] == ["9", "7", "5", "3", "1"]
    assert service.list_page(start_date="2026-10-28")["total"] == 0


def test_policy_status_matches_filters_detail_and_monitoring_without_changing_bodies(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    request = {"prompt": "content_policy_violation in a user's prompt"}
    response = {"error": {"message": "refused", "code": "content_policy_violation"}}
    service.add("call", detail={"endpoint": "/v1/images/generations", "status": "failed", "request_body": request, "response_body": response})
    service.add("call", detail={"endpoint": "/v1/images/generations", "status": "failed", "request_body": request, "error": "moderation service timed out"})
    service.add("call", detail={"endpoint": "/v1/images/generations", "status": "failed", "error": "安全审查未通过"})
    rejected = service.list_page(type="call", status="safety_rejected")
    assert rejected["total"] == 2
    assert all(item["detail"]["status"] == "safety_rejected" for item in rejected["items"])
    original = service.get(rejected["items"][-1]["id"])
    assert original["detail"]["status"] == "safety_rejected"
    assert original["detail"]["request_body"] == request
    assert original["detail"]["response_body"] == response
    failed = service.list_page(status="failed")
    assert failed["total"] == 1
    assert failed["items"][0]["detail"]["error"] == "moderation service timed out"
    stats = service.image_stats("2000-01-01", "2100-01-01")
    assert stats["excluded"] == 2
    assert stats["failed"] == 1


def test_filter_index_backfill_uses_summaries_and_rebuilds_after_delete(tmp_path):
    service = seed_logs(tmp_path, 20)
    service.list_page()
    with service._index(write=True) as connection:
        connection.execute("DROP TABLE entry_filters")
        connection.execute("DELETE FROM log_filters_meta")
    restarted = LogService(service.path)
    with mock.patch.object(restarted, "_parse_line", side_effect=AssertionError("backfill must use summaries")):
        assert restarted.list_page(key_name="old-token")["total"] == 10
    restarted.delete(["0", "1"])
    assert restarted.list_page(key_name="old-token")["total"] == 8
    restarted.add("call", detail={"key_name": "later", "is_async": False, "status": "success"})
    assert restarted.list_page(key_name="later", is_async=False)["total"] == 1


def test_key_names_are_bounded_searchable_and_literal(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    for name in ["token-a", "token-b", "100%_literal", ""]:
        service.add("call", detail={"key_name": name})
    assert len(service.list_key_names(limit=2)) == 2
    assert service.list_key_names(query="TOKEN-A") == ["token-a"]
    assert service.list_key_names(query="%_") == ["100%_literal"]
    assert service.list_page(key_name="")["total"] == 1
    assert service.list_key_names(start_date="2100-01-01") == []


def test_logs_api_pagination_filters_and_route_order(tmp_path, monkeypatch):
    import api.system as system
    service = seed_logs(tmp_path)
    monkeypatch.setattr(system, "require_admin", lambda _: None)
    monkeypatch.setattr(system, "log_service", service)
    app = FastAPI()
    app.include_router(system.create_router("test"))
    with TestClient(app) as client:
        page = client.get("/api/logs?type=call&page=11&page_size=100").json()
        assert page["total"] == 1005 and page["items"][-1]["id"] == "0"
        filtered = client.get("/api/logs?key_name=old-token&is_async=false&response_format=b64_json&status=success").json()
        assert filtered["total"] == 5
        assert client.get("/api/logs?key_name=").json()["total"] == 0
        options = client.get("/api/logs/filter-options?type=call&query=old")
        assert options.status_code == 200
        assert options.json() == {"key_names": ["old-token"]}
        for query in ["page=0", "page_size=101", "page_size=0", "status=invalid", "is_async=invalid", "response_format=invalid"]:
            assert client.get("/api/logs?" + query).status_code == 422


def test_page_query_does_not_hold_append_lock(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    service = seed_logs(tmp_path, 20)
    service.list_page()
    started, release = Event(), Event()
    original = service._query_scope

    def paused_scope(*args, **kwargs):
        started.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    with ThreadPoolExecutor(max_workers=2) as pool:
        with mock.patch.object(service, "_query_scope", side_effect=paused_scope):
            reader = pool.submit(service.list_page)
            try:
                assert started.wait(5)
                writer = pool.submit(service.add, "call", detail={"status": "success"})
                writer.result(timeout=5)
            finally:
                release.set()
            assert reader.result(timeout=5)["total"] == 20
    assert service.list_page()["total"] == 21
