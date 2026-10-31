"""Concurrency regressions using blocked submissions and local HTTP fixtures only."""
import asyncio
import json
import threading
import time
from concurrent.futures import Future
from contextvars import ContextVar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest
from curl_cffi import CurlOpt

from services import image_diagnostics as diag
from services.async_completion import submit_off_loop
from services.config import config
from services.image_metrics import RuntimePhases
from services.image_transport import ImageBackendSession
from services.isolated_executor import IsolatedExecutor
from services.openai_backend_api import OpenAIBackendAPI, ImageStreamHardTimeoutError
from test.test_account_image_hotpath import service


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(.001)
    await asyncio.wait_for(wait(), 3)


def test_blocked_submission_keeps_loop_responsive_and_context():
    context = ContextVar('submission-test', default=None)
    async def scenario():
        entered, release = threading.Event(), threading.Event()
        loop_thread = threading.get_ident()
        def submit():
            assert threading.get_ident() != loop_thread
            assert context.get() == 'request-context'
            entered.set()
            assert release.wait(3)
            future = Future()
            future.set_result('finished')
            return future
        token = context.set('request-context')
        task = asyncio.create_task(submit_off_loop(submit))
        context.reset(token)
        try:
            await until(entered.is_set)
            # 能执行到此处，说明阻塞中的submit没有占住事件循环。
            assert not task.done()
            release.set()
            assert (await task).result() == 'finished'
        finally:
            release.set()
    asyncio.run(scenario())


def test_cancel_during_submission_releases_isolated_capacity(monkeypatch):
    async def scenario():
        executor = IsolatedExecutor(1, 1, 'submission-cancel')
        entered, release = threading.Event(), threading.Event()
        queued = Future()
        def blocked_submit(*args):
            entered.set()
            assert release.wait(3)
            return queued
        monkeypatch.setattr(executor._pool, 'submit', blocked_submit)
        task = asyncio.create_task(executor.run(lambda: None))
        try:
            await until(entered.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert executor.stats()['pending'] == 1
            release.set()
            await until(queued.cancelled)
            await until(lambda: executor.stats()['pending'] == 0)
        finally:
            release.set()
            executor._pool.shutdown()
    asyncio.run(scenario())


def test_reserved_submission_still_rejects_full_queue(monkeypatch):
    import services.thread_pool as pools
    from fastapi import HTTPException
    executor = pools.BoundedImageExecutor(1, 0)
    executor.reserve()
    monkeypatch.setattr(pools, 'image_executor', executor)
    async def scenario():
        with pytest.raises(HTTPException) as exc:
            await pools.run_image_in_threadpool(lambda: None)
        assert exc.value.status_code == 429
        assert executor.stats()['inflight'] == 1
    try:
        asyncio.run(scenario())
    finally:
        executor.release()
        executor.shutdown()


def test_recent_lag_peak_expires_and_keeps_history_timestamp(monkeypatch):
    clock = SimpleNamespace(mono=1000., wall=2000.)
    monkeypatch.setattr('services.image_metrics.time.monotonic', lambda: clock.mono)
    monkeypatch.setattr('services.image_metrics.time.time', lambda: clock.wall)
    phases = RuntimePhases()
    phases.observe('event_loop_lag', 2.)
    clock.mono += 1; clock.wall += 1
    phases.observe('event_loop_lag', .001)
    snap = phases.snapshot()['event_loop_lag']
    assert snap['recent_max_ms'] == 2000
    assert snap['recent_max_at'] == 2000
    assert snap['recent_max_started_at'] == 1998
    assert snap['last_ms'] == 1
    assert snap['timeline'][0]['max_ms'] == 2000
    clock.mono += 301; clock.wall += 301
    phases.observe('event_loop_lag', .002)
    snap = phases.snapshot()['event_loop_lag']
    assert snap['recent_max_ms'] == 2
    assert snap['max_ms'] == 2000 and snap['max_at'] == 2000
    assert len(snap['timeline']) == 1
    json.dumps(snap, allow_nan=False)


def test_successful_selection_reuses_reservation_snapshot(service, monkeypatch):
    value, token = service
    monkeypatch.setattr(value, 'get_account', lambda *_: pytest.fail('redundant account read'))
    monkeypatch.setattr(value, '_token_expires_in', lambda *_: 3600)
    try:
        assert value.get_available_access_token() == token
    finally:
        value.release_image_slot(token)


def test_disable_after_reservation_still_blocks_generation(service, monkeypatch):
    value, token = service
    original = value._image_account_with_verification
    def verify(*args, **kwargs):
        snapshot = original(*args, **kwargs)
        value.update_account(token, {'status': '禁用'}, quiet=True)
        return snapshot
    monkeypatch.setattr(value, '_image_account_with_verification', verify)
    with pytest.raises(RuntimeError, match='unavailable'):
        value.get_available_access_token()
    assert value._image_inflight.get(token, 0) == 0


def test_shared_stream_pool_survives_one_session_close():
    one, two = ImageBackendSession(trust_env=False), ImageBackendSession(trust_env=False)
    try:
        assert one.executor is two.executor
        backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        backend.session = one
        backend.close()
        assert two.executor.submit(lambda: 'alive').result(2) == 'alive'
        assert one._executor is None
    finally:
        one.close(); two.close()


def test_native_stream_timeout_interrupts_silent_body_without_timer(monkeypatch):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            self.wfile.write(b'data: {"fixture": true}\n\n')
            self.wfile.flush()
            time.sleep(.8)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(threading, 'Timer', lambda *a, **k: pytest.fail('per-request Timer'))
    try:
        started = time.monotonic()
        with ImageBackendSession(trust_env=False) as session:
            response = session.get(f'http://127.0.0.1:{server.server_port}/', stream=True,
                                   timeout=60, image_stream_timeout=.2)
            backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
            values = backend._iter_sse_payloads_capped(response, .2)
            assert 'fixture' in next(values)
            with pytest.raises(ImageStreamHardTimeoutError):
                next(values)
        assert time.monotonic() - started < .7
    finally:
        server.shutdown(); server.server_close(); thread.join(2)


def test_stream_cap_applies_without_request_metrics(monkeypatch):
    def request(self, method, url, **kwargs):
        assert 'image_stream_timeout' not in kwargs
        assert self.curl_options[CurlOpt.TIMEOUT_MS] == 80000
        return SimpleNamespace(close=lambda: None)
    monkeypatch.setattr('curl_cffi.requests.Session.request', request)
    with ImageBackendSession(trust_env=False) as session:
        result = session.get('http://fixture', stream=True, timeout=300, image_stream_timeout=80)
        assert result._image_stream_deadline > time.monotonic()


@pytest.fixture
def clock(monkeypatch):
    value = SimpleNamespace(now=1000.)
    monkeypatch.setattr(diag.time, 'monotonic', lambda: value.now)
    monkeypatch.setattr(diag.time, 'sleep', lambda seconds: setattr(value, 'now', value.now + seconds))
    return value


def test_poll_queries_result_before_tasks_and_limits_auxiliary_budget(clock, monkeypatch):
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    order, timeouts = [], []
    backend._get_conversation = lambda cid: order.append('conversation') or {}
    def tasks(**kwargs):
        order.append('tasks'); timeouts.append(kwargs['timeout_secs']); return []
    backend._query_backend_tasks = tasks
    monkeypatch.setattr(config, 'data', {**config.data, 'image_poll_initial_wait_secs': 10, 'image_poll_interval_secs': 10})
    with pytest.raises(diag.ImagePollTimeoutError):
        backend._poll_image_results('fixture', timeout_secs=10)
    assert order == ['conversation', 'tasks', 'conversation', 'conversation', 'conversation']
    assert len(timeouts) == 1 and 0 < timeouts[0] <= 1
    assert 1009 <= clock.now < 1010


def test_poll_retry_after_beyond_budget_exits_without_early_retry(clock, monkeypatch):
    from utils.helper import UpstreamHTTPError
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    calls = []
    def conversation(cid):
        calls.append(cid)
        raise UpstreamHTTPError('fixture', status_code=429, body='upstream busy', retry_after=20)
    backend._get_conversation = conversation
    monkeypatch.setattr(config, 'data', {**config.data, 'image_poll_initial_wait_secs': 0})
    with pytest.raises(diag.ImageResultResolveError):
        backend._poll_image_results('fixture', timeout_secs=10)
    assert calls == ['fixture']
    assert clock.now == 1000


def test_native_stream_timeout_before_first_byte_is_hard_timeout():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            time.sleep(.8)
            self.send_response(200)
            self.end_headers()
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        started = time.monotonic()
        with ImageBackendSession(trust_env=False) as session:
            with pytest.raises(ImageStreamHardTimeoutError):
                session.get(f'http://127.0.0.1:{server.server_port}/', stream=True,
                            timeout=60, image_stream_timeout=.2)
        assert time.monotonic() - started < .7
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


def test_total_budget_preempts_first_byte_stream_timeout(monkeypatch):
    from services.image_metrics import ImageMetrics, current_metrics
    metrics = ImageMetrics()
    metrics.diagnostics.configure_budget(10)
    token = current_metrics.set(metrics)
    def request(self, method, url, **kwargs):
        metrics.diagnostics._deadline = metrics.started - 1
        raise TimeoutError('native transfer timed out')
    monkeypatch.setattr('curl_cffi.requests.Session.request', request)
    try:
        with ImageBackendSession(trust_env=False) as session:
            with pytest.raises(diag.ImageDeadlineExceeded):
                session.get('http://fixture', stream=True, image_stream_timeout=80)
    finally:
        current_metrics.reset(token)


def test_total_budget_preempts_body_stream_timeout(monkeypatch):
    from services.image_metrics import ImageMetrics, current_metrics
    metrics = ImageMetrics()
    metrics.diagnostics.configure_budget(10)
    token = current_metrics.set(metrics)
    response = SimpleNamespace(_image_stream_deadline=time.monotonic() - 1, close=lambda: None)
    def payloads(response):
        metrics.diagnostics._deadline = metrics.started - 1
        raise TimeoutError('native transfer timed out')
        yield  # generator raises on first iteration
    monkeypatch.setattr('services.openai_backend_api.iter_sse_payloads', payloads)
    try:
        backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        with pytest.raises(diag.ImageDeadlineExceeded):
            list(backend._iter_sse_payloads_capped(response, 80))
    finally:
        current_metrics.reset(token)


def test_cancel_during_image_submission_releases_reservation(monkeypatch):
    import services.thread_pool as pools
    async def scenario():
        executor = pools.BoundedImageExecutor(1, 0)
        monkeypatch.setattr(pools, 'image_executor', executor)
        entered, release = threading.Event(), threading.Event()
        queued = Future()
        def blocked_submit(*args):
            entered.set()
            assert release.wait(3)
            return queued
        monkeypatch.setattr(executor._pool, 'submit', blocked_submit)
        task = asyncio.create_task(pools.run_image_in_threadpool(lambda: None))
        try:
            await until(entered.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            await until(queued.cancelled)
            await until(lambda: executor.stats()['inflight'] == 0)
        finally:
            release.set()
            executor.shutdown()
    asyncio.run(scenario())
