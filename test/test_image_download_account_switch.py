"""下载重试耗尽后切号：真实下载重试、共享预算、槽位和诊断回归。"""
import io
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from PIL import Image

from services.account_service import AccountService
from services.config import config, DEFAULT_IMAGE_ACCOUNT_VERIFICATION
from services.image_diagnostics import ImageDownloadError, attempt_context
from services.image_metrics import ImageMetrics, current_metrics
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol import conversation as flow
from test.test_account_refresh_token_status import MemoryStorage


@pytest.fixture
def scenario(monkeypatch):
    monkeypatch.setitem(config.data, "image_account_verification", {
        **DEFAULT_IMAGE_ACCOUNT_VERIFICATION, "workers": 2, "hot_pool_size": 2})
    monkeypatch.setitem(config.data, "image_account_concurrency", 1)
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", 3)
    monkeypatch.setitem(config.data, "image_download_retries", 1)
    monkeypatch.setitem(config.data, "auto_remove_rate_limited_accounts", False)
    service = AccountService(MemoryStorage([
        {"access_token": str(i), "email": f"account{i}@example.test", "quota": 10,
         "remote_verified_at": time.time()} for i in range(4)]))
    monkeypatch.setattr(service, "_log", lambda *a, **k: None)
    monkeypatch.setattr(service, "refresh_access_token", lambda token, **k: token)
    monkeypatch.setattr(flow, "account_service", service)
    monkeypatch.setattr(flow, "_remove_image_conversation_later", Mock())
    from services import openai_backend_api as backend_module
    monkeypatch.setattr(backend_module, "wait_with_image_budget", lambda *_: None)
    metrics = ImageMetrics()
    metrics.diagnostics.configure_budget(120)
    context = current_metrics.set(metrics)
    try:
        yield SimpleNamespace(service=service, metrics=metrics)
    finally:
        current_metrics.reset(context)
        service.verification.close()


def request():
    return flow.ConversationRequest(model="gpt-image-2", prompt="sandbox",
                                    account_switch_tracker=flow.ImageAccountSwitchTracker())


def install_download_flow(monkeypatch, scenario, *, failing_accounts=1):
    buffer = io.BytesIO()
    Image.new("RGB", (2, 2), "red").save(buffer, "PNG")
    payload = buffer.getvalue()
    backends = []
    urls = []
    responses = []

    def factory(access_token):
        # 新账号启动前，旧账号槽位及底层会话必须已释放。
        assert all(not scenario.service._image_inflight.get(b.access_token) for b in backends)
        assert all(b.session.close.call_count == 1 for b in backends)
        backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        backend.access_token = access_token
        fail = len(backends) < failing_accounts
        def get(url, **kwargs):
            urls.append((access_token, url))
            response = SimpleNamespace(status_code=503 if fail else 200, headers={},
                text="unavailable" if fail else "", elapsed=None,
                iter_content=lambda **kw: iter([payload]), close=Mock())
            responses.append(response)
            return response
        backend.session = SimpleNamespace(get=get, close=Mock())
        backend.resolve_conversation_image_urls = Mock(return_value=["https://image.example.test/result.png"])
        backends.append(backend)
        return backend

    def events(*args, **kwargs):
        yield {"type": "conversation.delta", "delta": "generating", "conversation_id": "conv-test"}
        yield {"type": "conversation.completed", "conversation_id": "conv-test", "file_ids": ["file-test"]}

    monkeypatch.setattr(flow, "OpenAIBackendAPI", factory)
    monkeypatch.setattr(flow, "conversation_events", events)
    monkeypatch.setattr(flow, "format_image_result", lambda *a: {"data": [{"url": "https://saved.example.test/a.png"}]})
    return backends, urls, responses


def test_download_retries_then_switches_after_real_sse_progress(scenario, monkeypatch):
    backends, urls, responses = install_download_flow(monkeypatch, scenario)
    req = request()
    outputs = flow._generate_single_image(req, 1, 1)
    assert [item.kind for item in outputs] == ["progress", "result"]
    assert len(backends) == 2 and backends[0].access_token != backends[1].access_token
    assert [token for token, url in urls] == [backends[0].access_token] * 2 + [backends[1].access_token]
    assert len({url for token, url in urls}) == 1
    assert all(r.close.call_count == 1 for r in responses)
    assert all(b.session.close.call_count == 1 for b in backends)
    assert not scenario.service._image_inflight
    # 下载问题不扣账号分、不将账号计成生图失败。
    assert sum(a["fail"] for a in scenario.service._accounts.values()) == 0
    trace = req.account_switch_tracker.summary()
    assert trace["switch_count"] == 1
    assert "下载重试耗尽" in trace["account_switch_trace"][0]["reason"]
    steps = scenario.metrics.diagnostics.snapshot()["steps"]
    assert any(s.get("action") == "account_switch" and s.get("reason") == "image_download_failed" for s in steps)
    assert attempt_context.get() is None


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_download_failure_exhausts_shared_account_budget(scenario, monkeypatch, limit):
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", limit)
    backends, urls, responses = install_download_flow(monkeypatch, scenario, failing_accounts=10)
    req = request()
    with pytest.raises(flow.ImageGenerationError) as error:
        flow._generate_single_image(req, 1, 1)
    assert error.value.code == "image_download_failed" and error.value.status_code == 502
    assert error.value.conversation_id == "conv-test"
    assert len(backends) == limit and len(urls) == limit * 2
    assert len({b.access_token for b in backends}) == limit
    assert error.value.switch_count == limit - 1
    assert not scenario.service._image_inflight
    assert all(b.session.close.call_count == 1 for b in backends)
    assert all(r.close.call_count == 1 for r in responses)


def test_download_switch_preserves_error_if_no_other_account(scenario, monkeypatch):
    backends, urls, responses = install_download_flow(monkeypatch, scenario, failing_accounts=10)
    select = scenario.service.get_available_access_token
    calls = 0
    def only_one(**kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("no available account")
        return select(**kwargs)
    monkeypatch.setattr(scenario.service, "get_available_access_token", only_one)
    with pytest.raises(flow.ImageGenerationError) as error:
        flow._generate_single_image(request(), 1, 1)
    assert error.value.code == "image_download_failed"
    assert error.value.switch_count == 0
    assert len(backends) == 1 and not scenario.service._image_inflight


@pytest.mark.parametrize("cancelled", [False, True])
def test_download_switch_does_not_reset_deadline_or_ignore_disconnect(scenario, monkeypatch, cancelled):
    backends = []
    def factory(access_token):
        backend = Mock(access_token=access_token)
        backends.append(backend)
        return backend
    def failed(*args):
        if cancelled:
            scenario.metrics.diagnostics.disconnect()
        else:
            scenario.metrics.diagnostics._deadline = time.perf_counter() - 1
        raise ImageDownloadError("download retry exhausted")
        yield
    monkeypatch.setattr(flow, "OpenAIBackendAPI", factory)
    monkeypatch.setattr(flow, "stream_image_outputs", failed)
    with pytest.raises(flow.ImageGenerationError) as error:
        flow._generate_single_image(request(), 1, 1)
    assert error.value.code == ("client_disconnected" if cancelled else "image_timeout")
    assert len(backends) == 1 and not scenario.service._image_inflight
    backends[0].close.assert_called_once()


def test_download_retry_zero_still_allows_account_switch(scenario, monkeypatch):
    monkeypatch.setitem(config.data, "image_download_retries", 0)
    backends, urls, responses = install_download_flow(monkeypatch, scenario)
    assert flow._generate_single_image(request(), 1, 1)[-1].kind == "result"
    assert len(backends) == 2 and len(urls) == 2
    assert all(r.close.call_count == 1 for r in responses)
