"""Result polling failures switch accounts within both configured retry and total budgets."""
import time
from unittest.mock import Mock

import pytest

from services.config import config
from services.image_diagnostics import ImageResultResolveError, attempt_context
from services.protocol import conversation as flow
from test.test_image_download_account_switch import scenario, request


def install_poll_flow(monkeypatch, *, failures=1, error_type=flow.ImagePollTimeoutError, before_failure=None):
    backends = []
    class Backend:
        def __init__(self, access_token):
            self.access_token = access_token
            self.close = Mock()
            backends.append(self)
    def stream(backend, req, index, total):
        yield flow.ImageOutput(kind="progress", model=req.model, index=index, total=total,
                               conversation_id="conv-test")
        if len(backends) <= failures:
            if before_failure:
                before_failure()
            raise error_type("sandbox polling failure", "conv-test")
        yield flow.ImageOutput(kind="result", model=req.model, index=index, total=total,
                               data=[{"url": "local"}], conversation_id="conv-test")
    monkeypatch.setattr(flow, "OpenAIBackendAPI", Backend)
    monkeypatch.setattr(flow, "stream_image_outputs", stream)
    return backends


@pytest.mark.parametrize("error_type", [flow.ImagePollTimeoutError, ImageResultResolveError])
def test_poll_failure_switches_and_success_returns(scenario, monkeypatch, error_type):
    backends = install_poll_flow(monkeypatch, error_type=error_type)
    req = request()
    outputs = flow._generate_single_image(req, 1, 1)
    assert outputs[-1].data == [{"url": "local"}]
    assert len(backends) == 2 and len({b.access_token for b in backends}) == 2
    assert not scenario.service._image_inflight
    assert all(b.close.call_count == 1 for b in backends)
    assert sum(a["fail"] for a in scenario.service._accounts.values()) == 0
    summary = req.account_switch_tracker.summary()
    assert summary["switch_count"] == 1
    assert "补查" in summary["account_switch_trace"][0]["reason"]
    assert any(s.get("action") == "account_switch" for s in scenario.metrics.diagnostics.snapshot()["steps"])
    assert attempt_context.get() is None


@pytest.mark.parametrize("retry_limit", [0, 1, 2])
def test_configured_poll_retry_limit_is_honored(scenario, monkeypatch, retry_limit):
    monkeypatch.setitem(config.data, "image_poll_timeout_account_switch_retries", retry_limit)
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", 4)
    backends = install_poll_flow(monkeypatch, failures=10)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.status_code == 504
    assert caught.value.code == "image_result_uncertain"
    assert caught.value.conversation_id == "conv-test"
    assert len(backends) == retry_limit + 1
    assert caught.value.switch_count == retry_limit
    assert not scenario.service._image_inflight
    assert all(b.close.call_count == 1 for b in backends)


@pytest.mark.parametrize("account_limit", [1, 2, 3])
def test_poll_switches_share_account_attempt_limit(scenario, monkeypatch, account_limit):
    monkeypatch.setitem(config.data, "image_poll_timeout_account_switch_retries", 50)
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", account_limit)
    backends = install_poll_flow(monkeypatch, failures=10)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.code == "image_result_uncertain"
    assert len(backends) == account_limit
    assert caught.value.switch_count == account_limit - 1
    assert not scenario.service._image_inflight


@pytest.mark.parametrize("cancel", [False, True])
def test_request_budget_and_cancel_preempt_poll_switch(scenario, monkeypatch, cancel):
    def stop():
        if cancel:
            scenario.metrics.diagnostics.cancelled.set()
        else:
            scenario.metrics.diagnostics._deadline = time.perf_counter() - 1
    backends = install_poll_flow(monkeypatch, failures=10, before_failure=stop)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.code == ("client_disconnected" if cancel else "image_timeout")
    assert len(backends) == 1
    assert not scenario.service._image_inflight
    assert backends[0].close.call_count == 1


def test_no_remaining_account_keeps_poll_error(scenario, monkeypatch):
    select = scenario.service.get_available_access_token
    seen = []
    def only_once(**kwargs):
        seen.append(1)
        if len(seen) > 1:
            raise RuntimeError("no available account")
        return select(**kwargs)
    monkeypatch.setattr(scenario.service, "get_available_access_token", only_once)
    backends = install_poll_flow(monkeypatch, failures=10)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.code == "image_result_uncertain"
    assert caught.value.conversation_id == "conv-test"
    assert len(backends) == 1
    assert not scenario.service._image_inflight


def test_sse_hard_timeout_uses_shared_poll_switch_budget(scenario, monkeypatch):
    backends = install_poll_flow(monkeypatch, failures=10,
        error_type=lambda *args: flow.ImageStreamHardTimeoutError("hard timeout"))
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.code == "image_result_uncertain"
    assert len(backends) == 3
    assert caught.value.switch_count == 2
    assert not scenario.service._image_inflight

@pytest.mark.parametrize('error_type', [flow.ImageStreamHardTimeoutError, flow.ImagePollTimeoutError])
def test_too_little_total_budget_does_not_start_another_generation(scenario, monkeypatch, error_type):
    def nearing_deadline():
        scenario.metrics.diagnostics._deadline = time.perf_counter() + 2
    backends = install_poll_flow(monkeypatch, failures=10,
        error_type=(lambda *args: error_type('timeout')), before_failure=nearing_deadline)
    with pytest.raises(flow.ImageGenerationError) as caught:
        flow._generate_single_image(request(), 1, 1)
    assert caught.value.code == 'image_result_uncertain'
    assert len(backends) == 1
    assert not scenario.service._image_inflight


def test_hard_timeout_recovers_original_conversation_without_regeneration(scenario, monkeypatch):
    from types import SimpleNamespace
    calls = []
    def events(*args, **kwargs):
        yield {'type': 'conversation.event', 'conversation_id': 'original-conversation',
               'text': 'generating', 'raw': {}}
        raise flow.ImageStreamHardTimeoutError('hard timeout')
    def resolve(backend, req, last):
        calls.append(last)
        return ['http://sandbox/image'], '', last['conversation_id']
    monkeypatch.setattr(flow, 'conversation_events', events)
    monkeypatch.setattr(flow, '_resolve_stream_image_result', resolve)
    monkeypatch.setattr(flow, 'format_image_result', lambda *a, **k: {'data': [{'url': 'saved'}]})
    backend = SimpleNamespace(download_image_bytes=lambda urls: [b'image'])
    outputs = list(flow.stream_image_outputs(backend, request()))
    assert outputs[-1].data == [{'url': 'saved'}]
    assert calls[0]['conversation_id'] == 'original-conversation'
    assert calls[0]['turn_use_case'] == 'image gen'
    assert any(step.get('action') == 'recover_original_result'
               for step in scenario.metrics.diagnostics.snapshot()['steps'])


def test_hard_timeout_recovery_keeps_safety_refusal(scenario, monkeypatch):
    from types import SimpleNamespace
    def events(*a, **k):
        yield {'type': 'conversation.event', 'conversation_id': 'original', 'raw': {}}
        raise flow.ImageStreamHardTimeoutError('hard timeout')
    monkeypatch.setattr(flow, 'conversation_events', events)
    monkeypatch.setattr(flow, '_resolve_stream_image_result',
                        lambda *a: ([], 'Your request violates our content policy.', 'original'))
    outputs = list(flow.stream_image_outputs(SimpleNamespace(), request()))
    assert outputs[-1].kind == 'message'
    assert 'content policy' in outputs[-1].text
