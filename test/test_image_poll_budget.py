"""Regression checks for independent SSE/result budgets and bounded fallback polling."""
from types import SimpleNamespace

import pytest
from curl_cffi import CurlOpt

from services.config import config
from services import image_diagnostics as diag
from services.image_metrics import ImageMetrics, current_metrics, phase
from services.image_transport import ImageBackendSession
from services.openai_backend_api import OpenAIBackendAPI, ImagePollTimeoutError
import services.protocol.conversation as flow


@pytest.fixture
def clock(monkeypatch):
    value = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(diag.time, "monotonic", lambda: value.now)
    monkeypatch.setattr(diag.time, "sleep", lambda seconds: setattr(value, "now", value.now + seconds))
    return value


def test_nested_poll_retries_share_deadline_and_reset_context(clock):
    with pytest.raises(ImagePollTimeoutError) as caught:
        with diag.image_poll_budget(10, "conv"):
            diag.wait_with_image_budget(6)
            with diag.image_poll_budget(300):
                assert diag.remaining_image_budget() == pytest.approx(4)
                diag.wait_with_image_budget(30)
    assert caught.value.conversation_id == "conv"
    assert clock.now == 1010
    assert diag.remaining_poll_budget() is None


@pytest.mark.parametrize("last", [
    {"text": "referenced_image_ids: [image]", "turn_use_case": "image gen", "conversation_id": "conv"},
    {"text": "", "turn_use_case": "image gen", "conversation_id": "conv"},
])
def test_ten_second_limit_covers_text_and_fallback_resolution(clock, monkeypatch, last):
    calls = []
    backend = SimpleNamespace(
        _query_backend_tasks=lambda **kwargs: [],
        resolve_conversation_image_urls=lambda *args, **kwargs: [],
    )
    def poll(*args):
        calls.append(args[1])
        # The old fallback would wait 300s and swallow the timeout.
        with diag.image_poll_budget(args[1], "conv"):
            diag.wait_with_image_budget(300)
    backend._poll_image_results = poll
    monkeypatch.setattr(flow, "conversation_events", lambda *args, **kwargs: iter([last]))
    monkeypatch.setattr(flow, "is_model_text_reply_instead_of_image", lambda text: bool(text))
    monkeypatch.setattr(config, "data", {**config.data, "image_poll_timeout_secs": 10, "image_poll_initial_wait_secs": 1})
    req = flow.ConversationRequest(prompt="test", model="gpt-image-2")
    with pytest.raises(ImagePollTimeoutError):
        list(flow.stream_image_outputs(backend, req))
    assert clock.now == pytest.approx(1010)
    assert calls == [10]
    assert diag.remaining_poll_budget() is None


def test_download_is_outside_poll_budget(clock, monkeypatch):
    def resolve(*args, **kwargs):
        diag.wait_with_image_budget(9)
        return ["https://fixture/image"]
    def download(urls):
        assert diag.remaining_poll_budget() is None
        diag.wait_with_image_budget(20)
        return [b"image"]
    backend = SimpleNamespace(resolve_conversation_image_urls=resolve, download_image_bytes=download)
    monkeypatch.setattr(flow, "conversation_events", lambda *args, **kwargs: iter([
        {"conversation_id": "conv", "file_ids": ["file"]}]))
    monkeypatch.setattr(flow, "format_image_result", lambda *args: {"data": [{"url": "local"}]})
    monkeypatch.setattr(config, "data", {**config.data, "image_poll_timeout_secs": 10})
    outputs = list(flow.stream_image_outputs(backend, flow.ConversationRequest(prompt="test", model="gpt-image-2")))
    assert outputs[-1].data == [{"url": "local"}]
    assert clock.now == 1029


def test_poll_network_request_is_capped_without_request_metrics(clock, monkeypatch):
    observed = []
    def request(session, method, url, **kwargs):
        observed.append((kwargs['timeout'], session.curl_options[CurlOpt.TIMEOUT_MS]))
        return SimpleNamespace(close=lambda: None)
    monkeypatch.setattr("curl_cffi.requests.Session.request", request)
    with ImageBackendSession(trust_env=False) as session:
        with diag.image_poll_budget(10):
            diag.wait_with_image_budget(7)
            session.get("http://fixture/conversation", timeout=60)
    assert observed == [(3, 3000)]


def test_total_budget_still_preempts_poll(clock):
    metrics = ImageMetrics()
    metrics.diagnostics.configure_budget(.001)
    token = current_metrics.set(metrics)
    try:
        metrics.diagnostics._deadline = metrics.started - 1
        with pytest.raises(diag.ImageDeadlineExceeded):
            with diag.image_poll_budget(10):
                diag.remaining_image_budget()
    finally:
        current_metrics.reset(token)


def test_sse_uses_independent_timeout(monkeypatch):
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.access_token = "fixture"
    backend.progress_callback = None
    backend._bootstrap = lambda: None
    backend._get_chat_requirements = lambda: None
    backend._prepare_image_conversation = lambda *args: None
    backend._start_image_generation = lambda *args: object()
    captured = []
    backend._iter_sse_payloads_capped = lambda response, cap: captured.append(cap) or iter([])
    monkeypatch.setattr(config, "data", {**config.data, "image_sse_timeout_secs": 90, "image_poll_timeout_secs": 10})
    list(backend._stream_picture_conversation("test", "gpt-image-2", []))
    assert captured == [90]


def test_done_closes_generator_without_recording_upstream_failure():
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    def payloads():
        with phase("sse_stream"):
            yield "[DONE]"
            yield "unread"
    stream = payloads()
    try:
        list(flow.iter_conversation_payloads(stream))
        stream.close()
        step = metrics.diagnostics.snapshot()["steps"][0]
        assert step["status"] == "stopped"
        assert step["error_type"] == "GeneratorExit"
    finally:
        current_metrics.reset(token)


def test_short_poll_budget_still_attempts_a_query(clock, monkeypatch):
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend._query_backend_tasks = lambda **kwargs: []
    calls = []
    def get_conversation(cid):
        calls.append(cid)
        return {}
    backend._get_conversation = get_conversation
    monkeypatch.setattr(config, "data", {**config.data, "image_poll_initial_wait_secs": 10, "image_poll_interval_secs": 10})
    with pytest.raises(ImagePollTimeoutError):
        backend._poll_image_results("conv", timeout_secs=10)
    assert calls == ["conv", "conv"]
    assert clock.now <= 1010
    assert clock.now >= 1009


def test_sse_and_poll_config_defaults_are_independent(monkeypatch):
    monkeypatch.setattr(config, "data", {"image_poll_timeout_secs": 10})
    assert config.image_sse_timeout_secs == 80
    assert config.image_poll_timeout_secs == 10
    monkeypatch.setattr(config, "data", {})
    assert config.image_poll_timeout_secs == 10
    assert config.get()["image_sse_timeout_secs"] == 80


def test_real_slow_poll_http_is_interrupted_by_local_budget():
    import threading
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(.8)
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{}')
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                pass
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    started = time.perf_counter()
    try:
        with ImageBackendSession(trust_env=False) as session:
            with pytest.raises(ImagePollTimeoutError):
                with diag.image_poll_budget(.15, "conv"):
                    session.get(f"http://127.0.0.1:{server.server_port}/conversation", timeout=60)
        assert time.perf_counter() - started < .7
        assert diag.remaining_poll_budget() is None
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


def test_settings_persist_separate_timeouts(monkeypatch):
    import copy
    monkeypatch.setattr(config, "data", copy.deepcopy(config.data))
    saved = []
    monkeypatch.setattr(config, "_save", lambda: saved.append(copy.deepcopy(config.data)))
    result = config.update({"image_sse_timeout_secs": 90, "image_poll_timeout_secs": 10})
    assert result["image_sse_timeout_secs"] == 90
    assert result["image_poll_timeout_secs"] == 10
    assert saved[0]["image_sse_timeout_secs"] == 90
    assert saved[0]["image_poll_timeout_secs"] == 10


@pytest.mark.parametrize("text", ["referenced_image_ids: [image]", ""])
def test_fallback_success_still_downloads_result(monkeypatch, text):
    calls = []
    def resolve(*args, **kwargs):
        calls.append(kwargs)
        return ["upstream"] if kwargs.get("poll") is False else []
    backend = SimpleNamespace(
        _query_backend_tasks=lambda **kwargs: [],
        resolve_conversation_image_urls=resolve,
        _poll_image_results=lambda *args: (["file"], []),
        download_image_bytes=lambda urls: [b"image"],
    )
    monkeypatch.setattr(flow, "conversation_events", lambda *args, **kwargs: iter([
        {"conversation_id": "conv", "turn_use_case": "image gen", "text": text}]))
    monkeypatch.setattr(flow, "is_model_text_reply_instead_of_image", lambda value: bool(value))
    monkeypatch.setattr(flow, "format_image_result", lambda *args: {"data": [{"url": "local"}]})
    monkeypatch.setattr(config, "data", {**config.data, "image_poll_initial_wait_secs": 0})
    monkeypatch.setattr(flow, "_get_detailed_error_from_tasks", lambda *args, **kwargs: "")
    result = list(flow.stream_image_outputs(backend, flow.ConversationRequest(prompt="test", model="gpt-image-2")))
    assert result[-1].data == [{"url": "local"}]
    assert len(calls) == 2
    assert diag.remaining_poll_budget() is None


def test_result_query_network_failure_is_classified_for_account_switch(monkeypatch):
    from services.image_diagnostics import ImageResultResolveError
    backend = SimpleNamespace()
    def query(*args, **kwargs):
        raise ConnectionError("sandbox network failure")
    backend.resolve_conversation_image_urls = query
    monkeypatch.setattr(flow, "conversation_events", lambda *args, **kwargs: iter([
        {"conversation_id": "conv", "file_ids": ["file"]}]))
    with pytest.raises(ImageResultResolveError) as caught:
        list(flow.stream_image_outputs(backend, flow.ConversationRequest(prompt="test", model="gpt-image-2")))
    assert caught.value.conversation_id == "conv"
    assert isinstance(caught.value.__cause__, ConnectionError)
    assert diag.remaining_poll_budget() is None


def test_missing_conversation_after_recovery_is_a_poll_failure(monkeypatch):
    from services.image_diagnostics import ImageResultResolveError
    backend = SimpleNamespace(resolve_conversation_image_urls=lambda *args, **kwargs: [],
                              find_conversation_by_prompt=lambda *args, **kwargs: "")
    monkeypatch.setattr(flow, "conversation_events", lambda *args, **kwargs: iter([
        {"turn_use_case": "image gen"}]))
    with pytest.raises(ImageResultResolveError):
        list(flow.stream_image_outputs(backend, flow.ConversationRequest(prompt="test", model="gpt-image-2")))
