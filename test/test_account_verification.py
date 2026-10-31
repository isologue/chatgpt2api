from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
import time
from unittest import mock

import pytest

from services.account_service import AccountService
from services.account_verification import AccountVerification, VerificationBackoff
from services.config import config, DEFAULT_IMAGE_ACCOUNT_VERIFICATION
from services.keyed_locks import KeyedLocks
from test.test_account_refresh_token_status import MemoryStorage


@pytest.fixture
def settings(monkeypatch):
    value = {**DEFAULT_IMAGE_ACCOUNT_VERIFICATION, "workers": 2}
    monkeypatch.setitem(config.data, "image_account_verification", value)
    return value


@pytest.fixture
def verifier(settings):
    service = AccountVerification(lambda: settings)
    yield service
    service.close()


def wait_for(predicate):
    deadline = time.monotonic() + 3
    while not predicate():
        assert time.monotonic() < deadline, "condition timed out"
        time.sleep(0.002)


def test_same_account_verification_is_shared(verifier):
    entered, release = Event(), Event()
    def fetch():
        entered.set()
        assert release.wait(3)
        return {"quota": 5}
    with ThreadPoolExecutor(6) as pool:
        first = pool.submit(verifier.run, "account", fetch)
        assert entered.wait(2)
        followers = [pool.submit(verifier.run, "account", fetch) for _ in range(5)]
        try:
            wait_for(lambda: verifier.stats()["joined"] == 5)
        finally:
            release.set()
        assert [f.result(3) for f in [first, *followers]] == [{"quota": 5}] * 6
    assert verifier.stats()["completed"] == 1
    assert verifier.stats()["active"] == verifier.stats()["waiting"] == 0


def test_background_capacity_rejects_new_keys_but_joins_same_key(verifier, settings):
    settings.update(workers=1)
    entered, release = Event(), Event()
    def fetch():
        entered.set()
        assert release.wait(3)
    try:
        assert verifier.run("one", fetch, background=True)
        assert entered.wait(2)
        assert verifier.run("one", fetch, background=True)
        assert verifier.run("two", fetch, background=True)
        assert not verifier.run("three", fetch, background=True)
        assert verifier.stats()["background_pending"] == 2
        assert verifier.stats()["background_rejected"] == 1
    finally:
        release.set()
    wait_for(lambda: verifier.stats()["background_pending"] == 0)


def test_failure_backoff_blocks_repeated_remote_calls(verifier):
    remote = mock.Mock(side_effect=ValueError("upstream unavailable"))
    with pytest.raises(ValueError):
        verifier.run("one", remote)
    with pytest.raises(VerificationBackoff):
        verifier.run("one", remote)
    assert not verifier.run("one", remote, background=True)
    assert remote.call_count == 1
    verifier.forget("one")
    assert verifier.run("one", lambda: 7) == 7


def test_remote_concurrency_is_bounded_without_start_rate_cap(verifier, settings):
    settings.update(workers=2)
    guard = Lock()
    starts, active, peak = [], 0, 0
    def fetch():
        nonlocal active, peak
        with guard:
            starts.append(time.monotonic())
            active += 1
            peak = max(peak, active)
        time.sleep(0.06)
        with guard:
            active -= 1
    with ThreadPoolExecutor(6) as pool:
        list(pool.map(lambda n: verifier.run(str(n), fetch), range(6)))
    assert peak == 2


def test_close_drains_and_reconfigure_reopens(verifier):
    assert verifier.run("one", lambda: 1, background=True)
    verifier.close()
    assert verifier.stats()["background_pending"] == 0
    with pytest.raises(RuntimeError, match="closed"):
        verifier.run("two", lambda: 2)
    verifier.reconfigure()
    assert verifier.run("two", lambda: 2) == 2


def test_precise_locks_allow_hash_collisions_in_parallel():
    class Key:
        def __hash__(self):
            return 0
    keys = Key(), Key()
    locks, entered, release = KeyedLocks(), Event(), Event()
    def hold_first():
        with locks.hold(keys[0]):
            entered.set()
            assert release.wait(3)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(hold_first)
        assert entered.wait(2)
        try:
            with locks.hold(keys[1]):
                assert not first.done()
        finally:
            release.set()
        first.result(3)
    assert not locks._entries


@pytest.fixture
def account_service(settings, monkeypatch):
    monkeypatch.setitem(config.data, "auto_remove_rate_limited_accounts", False)
    service = AccountService(MemoryStorage([{"access_token": "at", "quota": 10, "status": "正常"}]))
    monkeypatch.setattr(service, "_log", lambda *a, **kw: None)
    monkeypatch.setattr(service, "refresh_access_token", lambda token, **kw: service.resolve_access_token(token))
    yield service
    service.verification.close()


def test_fresh_persisted_cache_survives_service_restart(settings):
    storage = MemoryStorage([{"access_token": "at", "quota": 10,
                              "remote_verified_at": time.time() - 30}])
    service = AccountService(storage)
    try:
        with mock.patch.object(service, "refresh_access_token", return_value="at"), \
             mock.patch.object(service, "fetch_remote_info", side_effect=AssertionError("unexpected preflight")):
            assert service._image_account_with_verification("at")["quota"] == 10
        assert service.verification.stats()["cache_hits"] == 1
    finally:
        service.verification.close()


def test_expired_cache_waits_for_foreground(account_service, settings, monkeypatch):
    entered, release = Event(), Event()
    account_service.update_account("at", {"remote_verified_at": time.time() - settings["cache_ttl_secs"] - 1})
    def remote(*args):
        entered.set()
        assert release.wait(3)
        return account_service.get_account("at")
    monkeypatch.setattr(account_service, "_fetch_remote_info", remote)
    with ThreadPoolExecutor(1) as pool:
        task = pool.submit(account_service._image_account_with_verification, "at")
        try:
            assert entered.wait(2)
            assert not task.done()
            assert account_service.verification.stats()["active_foreground"] == 1
            assert account_service.verification.stats()["cache_hits"] == 0
        finally:
            release.set()
        assert task.result(3)["quota"] == 10


@pytest.mark.parametrize("age", [None, 2000])
def test_missing_or_hard_expired_cache_requires_foreground(account_service, age, monkeypatch):
    if age is not None:
        account_service._verified_at["at"] = time.monotonic() - age
    remote = mock.Mock(return_value=account_service.get_account("at"))
    monkeypatch.setattr(account_service, "fetch_remote_info", remote)
    assert account_service._image_account_with_verification("at")["quota"] == 10
    remote.assert_called_once()


def test_generic_generation_failure_keeps_cache_but_auth_failure_invalidates(account_service):
    account_service.update_account("at", {"remote_verified_at": time.time()})
    account_service._verified_at["at"] = time.monotonic()
    account_service.mark_image_result("at", False)
    assert "at" in account_service._verified_at
    assert account_service.get_account("at")["remote_verified_at"]
    account_service.invalidate_verification("at")
    assert "at" not in account_service._verified_at
    assert account_service.get_account("at")["remote_verified_at"] is None


def test_background_refresh_preserves_concurrent_usage_and_disabled_status(account_service):
    entered, release = Event(), Event()
    def fetch():
        entered.set()
        assert release.wait(3)
        return {"quota": 10, "status": "正常"}
    with mock.patch("services.openai_backend_api.OpenAIBackendAPI") as backend, ThreadPoolExecutor(1) as pool:
        backend.return_value.get_user_info.side_effect = fetch
        future = pool.submit(account_service.fetch_remote_info, "at")
        assert entered.wait(2)
        try:
            account_service.mark_image_result("at", True)
            account_service.update_account("at", {"status": "禁用"})
        finally:
            release.set()
        future.result(3)
    account = account_service.get_account("at")
    assert (account["quota"], account["success"], account["status"]) == (9, 1, "禁用")
    assert account["remote_verified_at"] > 0
    assert "at" in account_service._verified_at


@pytest.mark.parametrize("mutation", ["invalidate", "rotate"])
def test_stale_inflight_verification_cannot_restore_invalidated_or_rotated_token(account_service, mutation):
    entered, release = Event(), Event()
    def fetch():
        entered.set()
        assert release.wait(3)
        return {"quota": 10, "status": "正常"}
    with mock.patch("services.openai_backend_api.OpenAIBackendAPI") as backend, ThreadPoolExecutor(1) as pool:
        backend.return_value.get_user_info.side_effect = fetch
        future = pool.submit(account_service.fetch_remote_info, "at")
        assert entered.wait(2)
        try:
            if mutation == "invalidate":
                account_service.invalidate_verification("at")
            else:
                account_service._apply_refreshed_tokens("at", {"access_token": "new"}, "test")
        finally:
            release.set()
        with pytest.raises(RuntimeError, match="invalidated|rotated"):
            future.result(3)
    active = account_service.resolve_access_token("at")
    assert active not in account_service._verified_at
    assert not account_service.get_account(active).get("remote_verified_at")


def test_monitoring_stats_do_not_rebuild_account_page_index(account_service, monkeypatch):
    monkeypatch.setattr(account_service, "_account_view_locked", mock.Mock(side_effect=AssertionError("full scan")))
    account_service.add_account_items([{"access_token": "second", "quota": 3, "type": "Plus"}])
    account_service.mark_image_result("at", True)
    account_service.mark_image_result("at", False)
    account_service.update_account("second", {"status": "禁用"})
    account_service._apply_refreshed_tokens("at", {"access_token": "new"}, "test")
    stats = account_service.get_stats()
    assert (stats["total"], stats["active"], stats["disabled"]) == (2, 1, 1)
    assert (stats["total_quota"], stats["total_success"], stats["total_fail"]) == (9, 1, 1)
    account_service.delete_accounts(["second"])
    assert account_service.get_stats()["total"] == 1


def test_verification_settings_normalize_and_persist(tmp_path):
    import json
    from services.config import ConfigStore
    filename = tmp_path / "config.json"
    filename.write_text(json.dumps({"auth-key": "test"}), encoding="utf-8")
    store = ConfigStore(filename)
    result = store.update({"image_account_verification": {
        "cache_ttl_secs": "600", "max_stale_secs": "10", "workers": "999",
        "queue_size": "-1", "starts_per_second": "0", "background_refresh": False,
        "query_workers": "invalid", "failure_backoff_secs": "45"},
        "image_response_workers": "64"})
    actual = result["image_account_verification"]
    assert actual == {**DEFAULT_IMAGE_ACCOUNT_VERIFICATION, "cache_ttl_secs": 600,
                      "workers": 999, "failure_backoff_secs": 45}
    reopened = ConfigStore(filename)
    assert reopened.image_account_verification == actual
    assert reopened.image_response_workers == 64
