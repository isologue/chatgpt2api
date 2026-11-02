"""未出图切号：普通文字、SSE进度、共享尝试上限及取消/预算回归。"""
import time
from unittest.mock import Mock

import pytest

from services.config import config
from services.protocol import conversation as flow
from test.test_image_download_account_switch import scenario, request


TEXT = "目前无法在此对话中直接生成图片，但可以帮你优化提示词。"


def install_text_flow(monkeypatch, scenario, *, failures=1, text=TEXT,
                      code=None, before_failure=None, result_first=False):
    backends = []

    def factory(access_token):
        assert all(not scenario.service._image_inflight.get(b.access_token) for b in backends)
        assert all(b.close.call_count == 1 for b in backends)
        backend = Mock(access_token=access_token)
        backends.append(backend)
        return backend

    def stream(backend, req, index, total):
        yield flow.ImageOutput(kind="progress", model=req.model, index=index, total=total,
                               conversation_id="conv-test")
        if len(backends) <= failures:
            if result_first:
                yield flow.ImageOutput(kind="result", model=req.model, index=index, total=total,
                                       data=[{"url": "local"}])
            if before_failure:
                before_failure()
            if code:
                raise flow.ImageGenerationError(text, status_code=502, code=code,
                                                conversation_id="conv-test")
            yield flow.ImageOutput(kind="message", model=req.model, index=index, total=total,
                                   text=text, conversation_id="conv-test")
        else:
            yield flow.ImageOutput(kind="result", model=req.model, index=index, total=total,
                                   data=[{"url": "local"}], conversation_id="conv-test")

    monkeypatch.setattr(flow, "OpenAIBackendAPI", factory)
    monkeypatch.setattr(flow, "stream_image_outputs", stream)
    req = request()
    req.message_as_error = True
    return backends, req


def assert_released(scenario, backends):
    assert not scenario.service._image_inflight
    assert all(b.close.call_count == 1 for b in backends)
    assert len({b.access_token for b in backends}) == len(backends)


@pytest.mark.parametrize("text", [TEXT, "I cannot generate an image right now.",
                                 '{"referenced_image_ids": ["image"]}'])
def test_text_after_progress_switches_and_succeeds(scenario, monkeypatch, text):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 1)
    backends, req = install_text_flow(monkeypatch, scenario, text=text)
    result = flow._generate_single_image(req, 1, 1)
    assert result[-1].data == [{"url": "local"}]
    assert len(backends) == 2
    assert req.account_switch_tracker.summary()["switch_count"] == 1
    failed = scenario.service.get_account(backends[0].access_token)
    assert failed["image_score"] == 45 and failed["fail"] == 1
    assert scenario.service.get_account(backends[1].access_token)["image_score"] == 51
    assert_released(scenario, backends)


@pytest.mark.parametrize("retry_limit", [0, 1, 3])
def test_no_image_retry_limit_preserves_error(scenario, monkeypatch, retry_limit):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", retry_limit)
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", 4)
    backends, req = install_text_flow(monkeypatch, scenario, failures=10)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(req, 1, 1)
    assert caught.value.code == "no_image_generated"
    assert str(caught.value) == TEXT
    assert len(backends) == retry_limit + 1
    assert caught.value.switch_count == retry_limit
    assert sum(a["fail"] for a in scenario.service._accounts.values()) == len(backends)
    assert_released(scenario, backends)


@pytest.mark.parametrize("account_limit", [1, 2, 3])
def test_no_image_uses_shared_account_budget(scenario, monkeypatch, account_limit):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 10)
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", account_limit)
    backends, req = install_text_flow(monkeypatch, scenario, failures=10)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(req, 1, 1)
    assert caught.value.code == "no_image_generated"
    assert caught.value.conversation_id == "conv-test"
    assert len(backends) == account_limit
    assert caught.value.switch_count == account_limit - 1
    assert_released(scenario, backends)


@pytest.mark.parametrize("cancel", [False, True])
def test_no_image_cannot_bypass_deadline_or_cancel(scenario, monkeypatch, cancel):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 3)
    def stop():
        if cancel:
            scenario.metrics.diagnostics.cancelled.set()
        else:
            scenario.metrics.diagnostics._deadline = time.perf_counter() - 1
    backends, req = install_text_flow(monkeypatch, scenario, before_failure=stop)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(req, 1, 1)
    assert caught.value.code == ("client_disconnected" if cancel else "image_timeout")
    assert len(backends) == 1
    assert_released(scenario, backends)


def test_policy_does_not_switch_even_with_tool_json(scenario, monkeypatch):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 3)
    backends, req = install_text_flow(monkeypatch, scenario,
        text='content_policy_violation: blocked by safety policy {"referenced_image_ids": ["image"]}')
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(req, 1, 1)
    assert caught.value.code == "content_policy_violation"
    assert len(backends) == 1
    account = scenario.service.get_account(backends[0].access_token)
    assert account["image_score"] == 50 and account["fail"] == 0
    assert_released(scenario, backends)


def test_no_image_after_result_does_not_generate_again(scenario, monkeypatch):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 3)
    backends, req = install_text_flow(monkeypatch, scenario, code="no_image_generated", result_first=True)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(req, 1, 1)
    assert caught.value.code == "no_image_generated"
    assert len(backends) == 1
    assert_released(scenario, backends)


def test_legacy_tool_text_error_still_switches(scenario, monkeypatch):
    monkeypatch.setitem(config.data, "image_text_reply_account_switch_retries", 1)
    backends, req = install_text_flow(monkeypatch, scenario,
        text='{"referenced_image_ids": ["image"]}', code="legacy_error")
    result = flow._generate_single_image(req, 1, 1)
    assert result[-1].data == [{"url": "local"}]
    assert len(backends) == 2
    assert_released(scenario, backends)
