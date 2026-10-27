from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from services.log_service import LogService


def test_empty_storage_reports_disk_and_index_without_creating_body(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    stats = service.storage_stats()
    assert stats["log_count"] == 0
    assert stats["log_file_size_bytes"] == 0
    assert stats["log_index_size_bytes"] == service._index_path.stat().st_size > 0
    assert stats["log_size_bytes"] == stats["log_index_size_bytes"]
    assert stats["disk_total_mb"] > 0
    assert 0 <= stats["disk_free_mb"] <= stats["disk_total_mb"]
    assert not service.path.exists()


def test_storage_counts_all_types_without_parsing_cached_bodies(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    service.add("call", detail={"response_body": {"data": [{"b64_json": "a" * 1000000}]}})
    service.add("account", summary="fixture")
    with mock.patch.object(service, "_parse_line", side_effect=AssertionError("must reuse index")), \
         mock.patch("services.log_service.json.loads", side_effect=AssertionError("must not parse summaries or bodies")):
        stats = service.storage_stats()
    assert stats["log_count"] == 2
    assert stats["log_file_size_bytes"] == service.path.stat().st_size > 1000000
    assert stats["log_size_bytes"] == stats["log_file_size_bytes"] + stats["log_index_size_bytes"]

    # 辅助文件可能在统计期间消失；稳定存在时必须计入占用。
    original_stat = Path.stat
    def stat(path, *args, **kwargs):
        if str(path) == str(service._index_path) + "-wal":
            return mock.Mock(st_size=123)
        if str(path) == str(service._index_path) + "-shm":
            return mock.Mock(st_size=456)
        return original_stat(path, *args, **kwargs)
    with mock.patch.object(Path, "stat", stat):
        assert service.storage_stats()["log_index_size_bytes"] == service._index_path.stat().st_size + 579


def test_storage_updates_after_delete_and_retention(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    old = {"id": "old", "time": "2000-01-01 00:00:00", "type": "call", "detail": {"response_body": "a" * 100000}}
    fresh = {"id": "fresh", "time": "2099-01-01 00:00:00", "type": "account", "detail": {}}
    service.path.write_text(json.dumps(old) + "\n" + json.dumps(fresh) + "\n", encoding="utf-8")
    before = service.storage_stats()
    assert before["log_count"] == 2
    assert service.cleanup_before(1700000000)["removed"] == 1
    after = service.storage_stats()
    assert after["log_count"] == 1
    assert after["log_file_size_bytes"] < before["log_file_size_bytes"]
    assert service.get("fresh")["detail"] == {}
    assert service.delete(["fresh"])["removed"] == 1
    assert service.storage_stats()["log_count"] == 0
    assert service.storage_stats()["log_file_size_bytes"] == 0


def test_storage_api_requires_admin_and_precedes_detail_route(tmp_path, monkeypatch):
    import api.system as system
    service = LogService(tmp_path / "logs.jsonl")
    service.add("call", summary="fixture")
    monkeypatch.setattr(system, "log_service", service)
    def admin(value):
        if value != "Bearer fixture":
            raise HTTPException(401)
    monkeypatch.setattr(system, "require_admin", admin)
    app = FastAPI()
    app.include_router(system.create_router("fixture"))
    with TestClient(app) as client:
        assert client.get("/api/logs/storage").status_code == 401
        with mock.patch.object(service, "get", side_effect=AssertionError("must use static route")):
            response = client.get("/api/logs/storage", headers={"Authorization": "Bearer fixture"})
        assert response.status_code == 200
        assert response.json()["log_count"] == 1
        assert "log_size_bytes" in response.json()
