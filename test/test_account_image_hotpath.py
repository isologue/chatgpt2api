import base64
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event, Lock
from unittest import mock

import pytest

from services.account_service import AccountService
from services.config import config
from test.test_account_refresh_token_status import MemoryStorage


def jwt(seconds):
    payload = base64.urlsafe_b64encode(json.dumps({"exp": int(time.time()) + seconds}).encode()).decode().rstrip("=")
    return "header." + payload + ".signature"


@pytest.fixture
def service(monkeypatch):
    token = jwt(3600)
    store = MemoryStorage([{"access_token": token, "refresh_token": "rt", "quota": 10,
                            "remote_verified_at": time.time()}])
    value = AccountService(store)
    monkeypatch.setattr(value, "_log", lambda *a, **kw: None)
    monkeypatch.setitem(config.data, "image_account_verification", {
        **config.image_account_verification, "cache_ttl_secs": 1200, "max_stale_secs": 1800})
    yield value, token
    value.verification.close()


def test_fresh_image_cache_avoids_refresh_and_repeated_account_lock(service, monkeypatch):
    value, token = service
    class CountingLock:
        def __init__(self):
            self.lock, self.count = Lock(), 0
        def acquire(self):
            result = self.lock.acquire()
            self.count += 1
            return result
        def release(self):
            self.lock.release()
        def __enter__(self):
            return self.acquire()
        def __exit__(self, *_):
            self.release()
    lock = CountingLock()
    monkeypatch.setattr(value, "_lock", lock)
    with mock.patch.object(value, "refresh_access_token", side_effect=AssertionError("early renewal")), \
         mock.patch.object(value, "get_account", side_effect=AssertionError("duplicate account read")):
        assert value._image_account_with_verification(token)["quota"] == 10
    assert lock.count == 1


def test_image_token_near_expiry_still_refreshes_and_reverifies(service):
    value, token = service
    value._IMAGE_TOKEN_REFRESH_SKEW_SECONDS = 7200
    new = jwt(86400 * 2)
    def rotate(*a, **kw):
        return value._apply_refreshed_tokens(token, {"access_token": new}, "test")
    with mock.patch.object(value, "refresh_access_token", side_effect=rotate) as refresh, \
         mock.patch.object(value, "fetch_remote_info", return_value={}) as verify:
        result = value._image_account_with_verification(token)
    assert result["access_token"] == new
    refresh.assert_called_once()
    verify.assert_called_once_with(new, "get_available_access_token")


def test_permanent_rt_failure_survives_restart_without_disabling_at(service):
    value, token = service
    value._record_token_refresh_error(token, "test", "oauth_refresh_http_400: refresh_token_reused")
    # 超过旧的五分钟退避，且重启后依旧不重复自动续期。
    old = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    value.update_account(token, {"last_token_refresh_error_at": old}, quiet=True)
    restarted = AccountService(value.storage)
    restarted._log = lambda *a, **kw: None
    try:
        with mock.patch.object(restarted, "_request_access_token_refresh", side_effect=AssertionError("invalid RT retried")):
            with ThreadPoolExecutor(16) as pool:
                assert set(pool.map(restarted.refresh_access_token, [token] * 100)) == {token}
            assert restarted.refresh_access_token(token, force=True, event="image_stream") == token
            assert restarted._image_account_with_verification(token)["access_token"] == token
            assert token not in restarted.list_expiring_access_tokens()
            assert token not in restarted.list_refresh_token_keepalive_tokens()
        assert restarted.get_account(token)["status"] == "正常"
        assert restarted.get_account(token)["quota"] == 10
        assert restarted.get_account(token)["refresh_token_status"] == "invalid"
    finally:
        restarted.verification.close()


def test_new_rt_resumes_automatic_renewal_and_force_keeps_manual_retry(service):
    value, token = service
    value._record_token_refresh_error(token, "test", "refresh_token_reused")
    with mock.patch.object(value, "_request_access_token_refresh", return_value={"access_token": token}) as remote:
        assert value.refresh_access_token(token, force=True, retry_terminal=True) == token
        assert remote.call_count == 1
    value._record_token_refresh_error(token, "test", "refresh_token_reused")
    value.add_account_items([{"access_token": token, "refresh_token": "replacement"}], include_items=False)
    with mock.patch.object(value, "_request_access_token_refresh", return_value={"access_token": token}) as remote:
        assert value.refresh_access_token(token) == token
        remote.assert_called_once()
        assert remote.call_args.args[0] == "replacement"


def test_transient_refresh_error_still_retries_after_backoff(service):
    value, token = service
    value._record_token_refresh_error(token, "test", "oauth_refresh_http_502: upstream unavailable")
    with mock.patch.object(value, "_request_access_token_refresh", return_value={"access_token": token}) as remote:
        assert value.refresh_access_token(token) == token
        remote.assert_not_called()
        old = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        value.update_account(token, {"last_token_refresh_error_at": old}, quiet=True)
        assert value.refresh_access_token(token) == token
        remote.assert_called_once()


@pytest.mark.parametrize("method,slow_method", [
    ("list_expiring_access_tokens", "_token_needs_refresh"),
    ("list_refresh_token_keepalive_tokens", "_refresh_token_keepalive_due_at"),
])
def test_maintenance_parsing_does_not_hold_scheduler_lock(service, method, slow_method):
    value, token = service
    entered, release = Event(), Event()
    original = getattr(value, slow_method)
    def slow(*a, **kw):
        entered.set()
        assert release.wait(5)
        return original(*a, **kw)
    with mock.patch.object(value, slow_method, side_effect=slow), ThreadPoolExecutor(2) as pool:
        scan = pool.submit(getattr(value, method))
        try:
            assert entered.wait(2)
            assert pool.submit(value.get_account, token).result(timeout=1)["quota"] == 10
        finally:
            release.set()
        scan.result(timeout=5)


@pytest.mark.parametrize("failure", [False, True])
def test_old_rt_result_cannot_overwrite_concurrent_replacement(service, failure):
    value, token = service
    entered, release = Event(), Event()
    def remote(*a, **kw):
        entered.set()
        assert release.wait(5)
        if failure:
            raise RuntimeError("refresh_token_reused")
        return {"access_token": jwt(172800), "refresh_token": "old-request-rotation"}
    with mock.patch.object(value, "_request_access_token_refresh", side_effect=remote), ThreadPoolExecutor(1) as pool:
        refresh = pool.submit(value.refresh_access_token, token)
        try:
            assert entered.wait(2)
            value.add_account_items([{"access_token": token, "refresh_token": "replacement"}], include_items=False)
        finally:
            release.set()
        assert refresh.result(timeout=5) == token
    account = value.get_account(token)
    assert account["refresh_token"] == "replacement"
    assert account["refresh_token_status"] == "unverified"
    assert not account.get("last_token_refresh_error")


def test_mark_result_updates_candidate_index_once_with_final_quota(service):
    value, token = service
    token = value.get_available_access_token()
    with mock.patch.object(value, "_sync_candidate_locked", wraps=value._sync_candidate_locked) as sync:
        result = value.mark_image_result(token, True)
    assert result["quota"] == 9 and result["success"] == 1
    assert sync.call_count == 1
    assert token not in value._image_inflight
    assert token in value._available_buckets[value._bucket_for_token[token]]


def test_account_cache_and_selection_have_separate_timing(service):
    from services.image_metrics import ImageMetrics, current_metrics
    value, token = service
    metrics = ImageMetrics()
    context = current_metrics.set(metrics)
    try:
        chosen = value.get_available_access_token()
        value.get_account(chosen)
    finally:
        current_metrics.reset(context)
        value.release_image_slot(token)
    timings = metrics.snapshot()
    for name in ("account_selection_lock_wait_ms", "account_cache_lock_wait_ms",
                 "account_read_lock_wait_ms", "account_token_inspection_ms"):
        assert timings[name] >= 0
