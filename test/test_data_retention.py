from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from unittest import mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from services.config import ConfigStore
from services.log_service import LogService
from services.retention_service import CleanupBusyError, RetentionService
from services.retention_settings import normalize_retention


def store_at(tmp_path, initial=None):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"auth-key": "fixture", **(initial or {})}), encoding="utf-8")
    return ConfigStore(path)


def wait_idle(service, kind):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = service.snapshot(kind)
        if not result["running"]:
            return result
        time.sleep(0.01)
    raise AssertionError("cleanup did not finish")


def test_defaults_and_persisted_independent_policies(tmp_path):
    config = store_at(tmp_path, {"image_retention_days": 2})
    defaults = config.get()["data_retention"]
    assert defaults["images"] == {"enabled": False, "retention_hours": 48, "interval_minutes": 60}
    assert defaults["logs"]["enabled"] is False
    service = RetentionService(config)
    service.update("logs", {"enabled": True, "retention_hours": 24, "interval_minutes": 10})
    reloaded = ConfigStore(config.path).get_retention_settings()
    assert reloaded["logs"] == {"enabled": True, "retention_hours": 24, "interval_minutes": 10}
    assert reloaded["images"] == defaults["images"]


@pytest.mark.parametrize("key,value", [("retention_hours", 0), ("retention_hours", -1), ("retention_hours", 1.5),
    ("retention_hours", True), ("retention_hours", "24"), ("retention_hours", 876001),
    ("interval_minutes", 0), ("interval_minutes", 43201), ("enabled", "false")])
def test_invalid_policies_do_not_change_saved_config(tmp_path, key, value):
    config = store_at(tmp_path)
    original = config.path.read_bytes()
    with pytest.raises(ValueError):
        config.update({"data_retention": {"logs": {key: value}}})
    assert config.path.read_bytes() == original


def test_scheduler_is_opt_in_delayed_and_updates_live(tmp_path):
    config = store_at(tmp_path)
    now = [100000.0]
    called = []
    service = RetentionService(config, clock=lambda: now[0], handlers={"images": lambda cutoff: called.append(cutoff) or {"removed": 2}})
    service.tick()
    now[0] += 86400
    service.tick()
    assert called == []
    service.update("images", {"enabled": True, "retention_hours": 3, "interval_minutes": 1})
    service.tick()
    assert called == []
    now[0] += 60
    service.tick()
    result = wait_idle(service, "images")
    assert called == [now[0] - 3 * 3600]
    assert result["source"] == "automatic" and result["last_result"]["removed"] == 2
    service.update("images", {"enabled": False, "retention_hours": 3, "interval_minutes": 1})
    now[0] += 3600
    service.tick()
    assert len(called) == 1


def test_manual_is_background_and_cannot_overlap_scheduler(tmp_path):
    config = store_at(tmp_path)
    started, release = Event(), Event()
    def handler(_cutoff):
        started.set()
        assert release.wait(5)
        return {"removed": 1}
    service = RetentionService(config, handlers={"logs": handler})
    try:
        assert service.run("logs", 24)["running"] is True
        assert started.wait(5)
        with pytest.raises(CleanupBusyError):
            service.run("logs", 24)
        service.tick()
        assert service.snapshot("logs")["running"] is True
    finally:
        release.set()
    assert wait_idle(service, "logs")["last_result"] == {"removed": 1}


def test_worker_failure_is_visible_and_allows_retry(tmp_path):
    def fail(_):
        raise RuntimeError("fixture failure")
    service = RetentionService(store_at(tmp_path), handlers={"logs": fail})
    service.run("logs", 1)
    assert wait_idle(service, "logs")["last_error"] == "fixture failure"
    service.handlers["logs"] = lambda _: {"removed": 0}
    service.run("logs", 1)
    assert wait_idle(service, "logs")["last_error"] is None


def seed_logs(tmp_path):
    cutoff = datetime.now().replace(microsecond=0) - timedelta(hours=24)
    body = {"prompt": "fixture", "custom": {"keep": [1, None, True]}}
    records = []
    for log_id, dt, type_ in (("expired", cutoff - timedelta(seconds=1), "call"),
                               ("boundary", cutoff, "call"), ("recent", cutoff + timedelta(seconds=1), "account")):
        records.append({"id": log_id, "time": dt.isoformat(sep=" "), "type": type_, "detail": {
            "status": "success", "endpoint": "/v1/images/generations", "key_name": "fixture",
            "response_format": "b64_json", "is_async": False, "request_body": body,
            "response_body": {"data": [{"b64_json": "x" * 100000}]}}})
    records.append({"id": "bad-time", "time": "unknown", "type": "call", "detail": {}})
    service = LogService(tmp_path / "logs.jsonl")
    service.path.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
    return service, cutoff.timestamp(), records


def test_log_cleanup_strict_boundary_physical_reclaim_details_filters_stats(tmp_path):
    service, cutoff, records = seed_logs(tmp_path)
    previous_size = service.path.stat().st_size
    result = service.cleanup_before(cutoff)
    assert result["removed"] == 1 and result["reclaimed_bytes"] > 90000
    assert service.path.stat().st_size < previous_size
    assert service.get("expired") is None
    assert service.get("boundary") == records[1]
    assert service.get("recent") == records[2]
    assert service.get("bad-time") is not None
    assert service.list_page(response_format="b64_json")["total"] == 2
    assert service.image_stats("2000-01-01", "2100-01-01")["success"] == 1
    service.add("call", detail={"status": "success", "key_name": "later"})
    assert service.list_page(key_name="later")["total"] == 1
    assert not list(tmp_path.glob("*.cleanup-*"))


def test_no_expired_logs_does_not_read_or_rewrite_bodies(tmp_path):
    service, cutoff, _ = seed_logs(tmp_path)
    service.cleanup_before(cutoff)
    original = service.path.read_bytes()
    with mock.patch.object(service, "_parse_line", side_effect=AssertionError("must query summary only")):
        assert service.cleanup_before(cutoff)["removed"] == 0
    assert service.path.read_bytes() == original


def test_legacy_id_and_invalid_lines_survive_compaction(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    service.path.write_text('{"id":"old","time":"2000-01-01 00:00:00"}\ninvalid line\n{"time":"2099-01-01 00:00:00","detail":{"request_body":{"x":1}}}\n', encoding="utf-8")
    legacy = next(item for item in service.list() if item["id"] != "old")
    assert service.cleanup_before(time.time())["removed"] == 1
    assert service.get(legacy["id"])["detail"]["request_body"] == {"x": 1}
    assert b"invalid line" in service.path.read_bytes()


def test_log_append_and_detail_continue_during_compaction(tmp_path):
    service, cutoff, _ = seed_logs(tmp_path)
    service.list_page()
    paused, release = Event(), Event()
    original = service._parse_line
    once = [False]
    def parse(*args):
        if not once[0]:
            once[0] = True
            paused.set()
            assert release.wait(5)
        return original(*args)
    with ThreadPoolExecutor(max_workers=2) as pool:
        with mock.patch.object(service, "_parse_line", side_effect=parse):
            cleanup = pool.submit(service.cleanup_before, cutoff)
            try:
                assert paused.wait(5)
                writer = pool.submit(service.add, "call", detail={"key_name": "during-cleanup", "response_body": {"data": "y" * 1100000}})
                writer.result(timeout=5)
                assert service.get("boundary") is not None
            finally:
                release.set()
            assert cleanup.result(timeout=10)["removed"] == 1
    written = service.list_page(key_name="during-cleanup")
    assert written["total"] == 1
    assert len(service.get(written["items"][0]["id"])["detail"]["response_body"]["data"]) == 1100000
    assert service.list_page()["total"] == 4


def test_failed_file_replace_preserves_original_and_index(tmp_path):
    service, cutoff, _ = seed_logs(tmp_path)
    service.list_page()
    original = service.path.read_bytes()
    with mock.patch.object(Path, "replace", side_effect=OSError("fixture no space")):
        with pytest.raises(OSError):
            service.cleanup_before(cutoff)
    assert service.path.read_bytes() == original
    assert service.get("expired") is not None
    assert service.list_page()["total"] == 4
    assert not list(tmp_path.glob("*.cleanup-*"))


@pytest.fixture
def images(tmp_path, monkeypatch):
    import services.image_service as manager
    import services.image_storage_service as storage
    import services.image_tags_service as tags
    root = tmp_path / "images"
    thumbs = tmp_path / "thumbs"
    root.mkdir()
    thumbs.mkdir()
    config = SimpleNamespace(images_dir=root, image_thumbnails_dir=thumbs, base_url="",
                             get_image_storage_settings=lambda: {"mode": "local"})
    monkeypatch.setattr(storage, "config", config)
    monkeypatch.setattr(manager, "config", config)
    monkeypatch.setattr(tags, "TAGS_FILE", tmp_path / "tags.json")
    service = storage.ImageStorageService(tmp_path / "image_index.json")
    monkeypatch.setattr(manager, "image_storage_service", service)
    return manager, storage, service, tags, root, thumbs


def test_image_cleanup_local_remote_tags_thumbnails_without_decoding(images, monkeypatch):
    manager, storage, service, tags, root, thumbs = images
    cutoff = time.time() - 3600
    for name, created in (("old.png", cutoff - 1), ("boundary.png", cutoff), ("fresh.png", cutoff + 1)):
        (root / name).write_bytes(b"not decoded")
        os.utime(root / name, (created, created))
        (thumbs / (name + ".png")).write_bytes(b"thumbnail")
        tags.set_tags(name, ["fixture"])
    service._index_store.put("remote.png", {"created_at": datetime.fromtimestamp(cutoff - 2).isoformat(), "webdav": True})
    deleted = []
    monkeypatch.setattr(storage, "WebDAVClient", lambda _: SimpleNamespace(delete=lambda path: deleted.append(path) or True))
    with mock.patch.object(storage, "_image_dimensions", side_effect=AssertionError("cleanup must not decode image")):
        result = manager.cleanup_expired_images(cutoff)
    assert result["removed"] == 2 and result["failed"] == 0
    assert deleted == ["remote.png"]
    assert not (root / "old.png").exists()
    assert (root / "boundary.png").exists() and (root / "fresh.png").exists()
    assert not (thumbs / "old.png.png").exists()
    assert tags.get_tags("old.png") == [] and tags.get_tags("fresh.png") == ["fixture"]
    assert service._index_store.get("remote.png") == {}


def test_remote_failure_retains_local_file_and_metadata_and_does_not_hold_generation_lock(images, monkeypatch):
    manager, storage, service, tags, root, _ = images
    (root / "both.png").write_bytes(b"fixture")
    service._index_store.put("both.png", {"created_at": "2000-01-01 00:00:00", "webdav": True})
    tags.set_tags("both.png", ["keep"])
    def fail(_):
        acquired = service._index_lock.acquire(blocking=False)
        assert acquired, "remote delete holds generation lock"
        service._index_lock.release()
        raise storage.ImageStorageError("fixture remote unavailable")
    monkeypatch.setattr(storage, "WebDAVClient", lambda _: SimpleNamespace(delete=fail))
    result = manager.cleanup_expired_images(time.time())
    assert result["failed"] == 1 and result["removed"] == 0
    assert (root / "both.png").exists()
    assert service._index_store.get("both.png")["webdav"] is True
    assert tags.get_tags("both.png") == ["keep"]


def test_loading_image_list_has_no_cleanup_side_effect(images):
    manager, _, service, _, root, _ = images
    (root / "old.png").write_bytes(b"fixture")
    os.utime(root / "old.png", (1, 1))
    service._index_store.put("old.png", {"created_at": "1970-01-01 00:00:01", "date": "1970-01-01", "local": True, "storage": "local", "webdav": False})
    assert len(manager.list_images("")["items"]) == 1
    assert (root / "old.png").exists()


def test_retention_api_auth_validation_background_conflict(tmp_path, monkeypatch):
    import api.system as system
    release = Event()
    def handler(_):
        assert release.wait(5)
        return {"removed": 0}
    service = RetentionService(store_at(tmp_path), handlers={"logs": handler})
    monkeypatch.setattr(system, "retention_service", service)
    def admin(value):
        if value != "Bearer fixture":
            raise HTTPException(401)
    monkeypatch.setattr(system, "require_admin", admin)
    app = FastAPI()
    app.include_router(system.create_router("fixture"))
    with TestClient(app) as client:
        assert client.get("/api/retention/logs").status_code == 401
        headers = {"Authorization": "Bearer fixture"}
        assert client.post("/api/retention/images/run", headers=headers, json={"retention_hours": 0}).status_code == 422
        assert client.post("/api/retention/images/run", headers=headers, json={"retention_hours": True}).status_code == 422
        assert client.get("/api/retention/unknown", headers=headers).status_code == 422
        policy = {"enabled": False, "retention_hours": 12, "interval_minutes": 15}
        assert client.post("/api/retention/logs", headers=headers, json=policy).json()["policy"] == policy
        try:
            assert client.post("/api/retention/logs/run", headers=headers, json={"retention_hours": 12}).status_code == 202
            assert client.get("/api/retention/logs", headers=headers).json()["running"] is True
            assert client.post("/api/retention/logs/run", headers=headers, json={"retention_hours": 12}).status_code == 409
        finally:
            release.set()
        assert wait_idle(service, "logs")["last_result"] == {"removed": 0}
