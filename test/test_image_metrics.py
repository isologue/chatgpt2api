from __future__ import annotations

import asyncio
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from types import SimpleNamespace
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.image_metrics import ImageMetrics, ImageTimingMiddleware, current_metrics, measured, phase
from services.log_service import LoggedCall, LogService
from services.thread_pool import run_image_in_threadpool


def test_metrics_accumulate_parallel_work_and_keep_context():
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        @measured("generation_work")
        def work():
            with phase("image_save"):
                assert current_metrics.get() is metrics
            return 1

        async def scenario():
            assert await run_image_in_threadpool(work) == 1
        asyncio.run(scenario())
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(copy_context().run, work) for _ in range(32)]
            assert sum(future.result() for future in futures) == 32
        result = metrics.snapshot()
        assert result["generation_work_ms"] >= result["image_save_ms"]
        assert result["image_queue_wait_ms"] >= 0
        assert result["upstream_and_retry_ms"] >= 0
    finally:
        current_metrics.reset(token)
    assert current_metrics.get() is None


def test_download_metrics_separate_headers_body_and_bytes():
    from services.openai_backend_api import OpenAIBackendAPI

    from io import BytesIO
    from PIL import Image

    buffer = BytesIO()
    Image.new("RGB", (2, 2), "red").save(buffer, format="PNG")
    payload = buffer.getvalue()

    class Session:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return SimpleNamespace(
                status_code=200,
                headers={},
                elapsed=timedelta(milliseconds=25),
                content=b"",  # stream=True 的真实行为：不能直接读取此字段。
                iter_content=lambda **_kwargs: iter([payload[:10], payload[10:]]),
                close=lambda: None,
            )

    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.session = Session()
    try:
        assert backend.download_image_bytes(["https://image.example/a.png"]) == [payload]
    finally:
        current_metrics.reset(token)

    snapshot = metrics.snapshot()
    assert backend.session.calls[0][1]["stream"] is True
    assert snapshot["image_download_count"] == 1
    assert snapshot["image_download_bytes"] == len(payload)
    assert snapshot["image_download_ttfb_ms"] >= 25
    assert snapshot["image_download_body_ms"] >= 0
    assert snapshot["image_download_bytes_per_sec"] >= 0


def test_download_metrics_count_failed_download_without_logging_url():
    from services.openai_backend_api import OpenAIBackendAPI

    class Session:
        def get(self, _url, **_kwargs):
            return SimpleNamespace(status_code=503, headers={}, text="upstream unavailable", close=lambda: None)

    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.session = Session()
    try:
        with pytest.raises(Exception):
            backend.download_image_bytes(["https://secret.example/image.png"])
    finally:
        current_metrics.reset(token)

    snapshot = metrics.snapshot()
    assert snapshot["image_download_failures"] == 1
    assert "secret.example" not in str(snapshot)


def test_http_timing_includes_persistence_and_send_without_changing_payload(tmp_path, monkeypatch):
    events = []
    with LogService(tmp_path / "logs.jsonl") as service:
        monkeypatch.setattr("services.log_service.log_service", service)
        monkeypatch.setattr("services.image_metrics.logger.info", events.append)
        app = FastAPI()
        app.add_middleware(ImageTimingMiddleware)
        @app.post("/v1/images/generations")
        async def generate():
            call = LoggedCall({}, "/v1/images/generations", "gpt-image-2", "test", request_body={"prompt": "cat"})
            return await call.run(lambda: {"created": 1, "data": [{"url": "https://test/image.png"}]})
        with TestClient(app) as client:
            response = client.post("/v1/images/generations", json={"prompt": "cat"})
        assert response.json() == {"created": 1, "data": [{"url": "https://test/image.png"}]}
        event = next(item for item in events if item["event"] == "image_http_timing")
        assert event["status_code"] == 200
        timings = event["timings"]
        assert timings["http_total_ms"] >= timings["log_persistence_ms"]
        assert timings["response_serialization_ms"] >= 0
        assert timings["response_send_ms"] >= 0
        detail = service.get(service.list()[0]["id"])["detail"]
        assert detail["timing_id"] == event["timing_id"]
        assert detail["response_body"] == response.json()
        assert "phase_timings_ms" in detail


def test_retention_drain_waits_for_active_cleanup():
    from threading import Event
    from services.retention_service import RetentionService
    entered, release = Event(), Event()
    config = mock.Mock()
    config.get_retention_settings.return_value = {"logs": {"enabled": False, "interval_minutes": 60}}
    def cleanup(_):
        entered.set()
        assert release.wait(5)
        return {"removed": 0}
    service = RetentionService(config, handlers={"logs": cleanup})
    service.run("logs", 24)
    assert entered.wait(5)
    with ThreadPoolExecutor(max_workers=1) as pool:
        drained = pool.submit(service.drain)
        try:
            assert not drained.done()
        finally:
            release.set()
        drained.result(timeout=5)
    assert service.snapshot("logs")["running"] is False
