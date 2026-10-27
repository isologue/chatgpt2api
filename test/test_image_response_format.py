from __future__ import annotations

import base64
import json
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai
import api.image_tasks as tasks
from services.config import ConfigStore, config
from services.image_response_format import normalize_image_response_format, resolve_image_response_format
from services.log_service import LogService


@pytest.mark.parametrize("alias", ["b64_json", "base64", "BASE64", "b64", "base64_json", "B64-JSON", " b64Json ", "base64-json"])
def test_base64_aliases(alias):
    assert normalize_image_response_format(alias) == "b64_json"


def test_config_defaults_persistence_and_validation(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"auth-key": "test-auth"}), encoding="utf-8")
    store = ConfigStore(path)
    assert store.get()["image_response_format"] == "url"
    assert store.get()["image_allow_response_format_override"] is True
    store.update({"image_response_format": "base64", "image_allow_response_format_override": False})
    reloaded = ConfigStore(path)
    assert reloaded.image_response_format == "b64_json"
    assert reloaded.image_allow_response_format_override is False
    with pytest.raises(ValueError):
        store.update({"image_response_format": "invalid"})
    with pytest.raises(ValueError):
        store.update({"image_allow_response_format_override": "false"})


@pytest.fixture
def image_client(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "data", {**config.data, "image_response_format": "url", "image_allow_response_format_override": True})
    captured = []
    identity = {"id": "test", "role": "admin"}
    monkeypatch.setattr(ai, "require_identity", lambda _: identity)
    monkeypatch.setattr(tasks, "require_identity", lambda _: identity)
    monkeypatch.setattr(ai, "check_request", lambda _: None)
    monkeypatch.setattr(tasks, "check_request", lambda _: None)
    logs = LogService(tmp_path / "logs.jsonl")
    monkeypatch.setattr("services.log_service.log_service", logs)

    def handle(payload):
        captured.append(dict(payload))
        return {"created": 1, "data": [{"url": "https://test/image.png"}]}

    def submit(identity, **payload):
        captured.append(dict(payload))
        return {"id": "task-test", "created_at": 1, "model": "gpt-image-2", "status": "queued"}

    monkeypatch.setattr(ai.openai_v1_image_generations, "handle", handle)
    monkeypatch.setattr(ai.openai_v1_image_edit, "handle", handle)
    monkeypatch.setattr(ai.image_task_service, "submit_generation", submit)
    monkeypatch.setattr(ai.image_task_service, "submit_edit", submit)
    app = FastAPI()
    app.include_router(ai.create_router())
    app.include_router(tasks.create_router())
    with TestClient(app) as client:
        yield client, captured, logs


@pytest.mark.parametrize("async_", [False, True])
@pytest.mark.parametrize("edit", [False, True])
@pytest.mark.parametrize("override,default,requested,expected", [
    (True, "url", "BASE64", "b64_json"),
    (True, "b64_json", "url", "url"),
    (True, "b64_json", None, "b64_json"),
    (False, "b64_json", "url", "b64_json"),
    (False, "url", "unknown-format", "url"),
    (False, "url", {"invalid": True}, "url"),
])
def test_policy_applies_to_all_v1_image_routes(image_client, async_, edit, override, default, requested, expected):
    client, captured, logs = image_client
    config.data.update(image_response_format=default, image_allow_response_format_override=override)
    body = {"prompt": "cat", "async": async_}
    if requested is not None:
        body["response_format"] = requested
    if edit:
        body["images"] = [{"image_url": "data:image/png;base64," + base64.b64encode(b"image").decode()}]
    response = client.post("/v1/images/edits" if edit else "/v1/images/generations", json=body)
    assert response.status_code == 200, response.text
    assert captured[-1]["response_format"] == expected
    if async_:
        assert captured[-1]["request_body"] == body
    else:
        detail = logs.get(logs.list()[0]["id"])["detail"]
        assert detail["request_body"] == body
        assert detail["response_body"] == response.json()
        assert detail["response_format"] == expected


def test_multipart_and_internal_tasks_follow_policy(image_client):
    client, captured, _ = image_client
    config.data.update(image_response_format="b64_json", image_allow_response_format_override=False)
    for async_ in ("false", "true"):
        response = client.post("/v1/images/edits", data={"prompt": "edit", "async": async_, "response_format": "unknown"}, files={"image": ("image.png", b"image", "image/png")})
        assert response.status_code == 200, response.text
        assert captured[-1]["response_format"] == "b64_json"
    for route in ("generations", "edits"):
        body = {"client_task_id": "ui-task", "prompt": "cat", "response_format": "unknown"}
        if route == "edits":
            body["images"] = [{"image_url": "data:image/png;base64,aW1hZ2U="}]
        response = client.post("/api/image-tasks/" + route, json=body)
        assert response.status_code == 200, response.text
        assert captured[-1]["response_format"] == "b64_json"


def test_unknown_override_is_rejected(image_client):
    client, captured, _ = image_client
    response = client.post("/v1/images/generations", json={"prompt": "cat", "response_format": "unknown"})
    assert response.status_code == 400
    assert not captured


def test_async_resolved_format_does_not_follow_later_config_changes():
    from services.image_task_service import ImageTaskService
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as directory:
        service = ImageTaskService(Path(directory) / "tasks.json")
        pending = []
        try:
            with mock.patch.object(service, "_schedule_reserved", side_effect=lambda *args: pending.append(args)):
                with mock.patch.dict(config.data, {"image_response_format": "b64_json", "image_allow_response_format_override": True}):
                    resolved = resolve_image_response_format(None)
                    service.submit_generation({"id": "owner"}, client_task_id="saved", prompt="cat", model="gpt-image-2", size=None, response_format=resolved)
                with mock.patch.dict(config.data, {"image_response_format": "url", "image_allow_response_format_override": False}):
                    # 执行参数和持久化任务均保存创建时的规范格式。
                    assert pending[0][3]["response_format"] == "b64_json"
                    assert service._store.get("owner:saved")["response_format"] == "b64_json"
        finally:
            service._executor.release()
            service.close()


@pytest.mark.parametrize("response_format", ["url", "b64_json"])
def test_resume_poll_retains_saved_format_and_logs_actual_result(tmp_path, monkeypatch, response_format):
    from services.image_task_service import ImageTaskService
    import services.protocol.conversation as conversation

    service = ImageTaskService(tmp_path / "tasks.json")
    key = "owner:resume"
    service._store.create(key, {"id": "resume", "owner_id": "owner", "mode": "generate", "status": "error", "response_format": response_format, "base_url": "https://my.example", "updated_ts": 1})
    captured = []
    logs = []

    class Backend:
        def __init__(self):
            pass
        def _poll_image_results(self, *args):
            return ["file"], []
        def resolve_conversation_image_urls(self, *args, **kwargs):
            return ["upstream"]
        def download_image_bytes(self, *args):
            return [b"image"]
        def close(self):
            pass

    def format_result(items, prompt, format, base, created):
        captured.append((format, base))
        return {"data": [{"url": "https://my.example/image.png"}] if format == "url" else [{"b64_json": "aW1hZ2U="}]}

    monkeypatch.setattr("services.openai_backend_api.OpenAIBackendAPI", Backend)
    monkeypatch.setattr(conversation, "format_image_result", format_result)
    monkeypatch.setattr(service, "_log_call", lambda *args, **kwargs: logs.append(kwargs))
    monkeypatch.setattr(config, "data", {**config.data, "image_response_format": "b64_json" if response_format == "url" else "url", "image_allow_response_format_override": False})
    try:
        service._run_resume_poll(key, "conversation", 5, {"id": "owner"}, "generate", "gpt-image-2")
        assert captured == [(response_format, "https://my.example")]
        assert service._store.get(key)["status"] == "success"
        assert logs[0]["status"] == "success"
        assert logs[0]["response_format"] == response_format
        assert logs[0]["response_body"]["status"] == "succeeded"
        assert ("url" if response_format == "url" else "b64_json") in logs[0]["response_body"]["data"]["data"][0]
    finally:
        service.close()
