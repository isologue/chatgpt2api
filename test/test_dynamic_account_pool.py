import json
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock

import pytest

from services.account_pool import DeadlineQueue, ImageAttemptBudget, availability_time
from services.account_pool_maintenance import AccountPoolMaintenance
from services.account_service import AccountService
from services.account_verification import AccountVerification, VerificationBackoff, LANES
from services.config import config, DEFAULT_IMAGE_ACCOUNT_VERIFICATION, ConfigStore
from test.test_account_refresh_token_status import MemoryStorage


def wait_until(predicate, timeout=4):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition timed out"
        time.sleep(0.002)


@pytest.fixture
def settings(monkeypatch):
    value = {**DEFAULT_IMAGE_ACCOUNT_VERIFICATION, "workers": 2, "hot_pool_size": 2}
    monkeypatch.setitem(config.data, "image_account_verification", value)
    monkeypatch.setitem(config.data, "image_account_concurrency", 1)
    monkeypatch.setitem(config.data, "auto_remove_rate_limited_accounts", False)
    monkeypatch.setitem(config.data, "auto_relogin_after_refresh", False)
    return value


@pytest.fixture
def service(settings, monkeypatch):
    svc = AccountService(MemoryStorage([
        {"access_token": str(n), "quota": 10, "remote_verified_at": time.time()}
        for n in range(8)]))
    monkeypatch.setattr(svc, "_log", lambda *a, **k: None)
    monkeypatch.setattr(svc, "refresh_access_token", lambda token, **k: svc.resolve_access_token(token))
    yield svc
    svc.verification.close()


def test_deadline_queue_random_update_delete_matches_reference():
    rng = random.Random(982)
    q, expected = DeadlineQueue(), {}
    for _ in range(10000):
        key = str(rng.randrange(200))
        if rng.randrange(4) == 0:
            q.remove(key)
            expected.pop(key, None)
        else:
            value = rng.randrange(1000)
            q.set(key, value)
            expected[key] = value
        assert len(q) == len(expected)
        assert q.peek() == (min((v, k) for k, v in expected.items()) if expected else None)
        assert all(q.heap[i][1] == key for key, i in q.positions.items())
    assert [q.pop_due(float("inf")) for _ in expected] == [k for v, k in sorted((v, k) for k, v in expected.items())]


def test_five_lanes_each_reach_full_c_and_cross_lane_singleflight(settings):
    verify = AccountVerification(lambda: settings)
    release = Event()
    try:
        futures = []
        for lane in LANES:
            for n in range(settings["workers"]):
                futures.append(verify.submit((lane, n), lambda: release.wait(4), lane=lane))
        wait_until(lambda: verify.stats()["active"] == 5 * settings["workers"])
        assert all(v["active"] == settings["workers"] for v in verify.stats()["lanes"].values())
        shared = verify.submit(("foreground", 0), Mock(side_effect=AssertionError("duplicate")), lane="manual")
        assert shared is futures[0]
        assert verify.stats()["inflight_accounts"] == 10
        release.set()
        assert all(f.result(3) for f in futures)
    finally:
        release.set()
        verify.close()
    assert verify.stats()["active"] == verify.stats()["waiting"] == 0


def test_default_pool_settings_and_saved_overrides(tmp_path):
    expected = {
        "cache_ttl_secs": 900, "workers": 50, "hot_pool_size": 500,
        "hot_idle_secs": 86400, "hot_refresh_interval_secs": 1800,
        "cold_refresh_interval_secs": 86400,
        "wait_timeout_secs": 60, "failure_backoff_secs": 30,
    }
    assert DEFAULT_IMAGE_ACCOUNT_VERIFICATION == expected
    filename = tmp_path / "config.json"
    filename.write_text(json.dumps({"auth-key": "local-test"}), encoding="utf-8")
    store = ConfigStore(filename)
    assert store.image_account_verification == expected
    overrides = {"cache_ttl_secs": 300, "workers": 100, "hot_pool_size": 2000}
    store.update({"image_account_verification": overrides})
    assert ConfigStore(filename).image_account_verification == {**expected, **overrides}


def test_c1000_not_clamped_and_obsolete_fields_removed(tmp_path):
    filename = tmp_path / "config.json"
    filename.write_text(json.dumps({"auth-key": "local-test"}), encoding="utf-8")
    store = ConfigStore(filename)
    saved = store.update({"image_account_verification": {"workers": 1000,
        "max_stale_secs": 9999, "query_workers": 1, "starts_per_second": 1}})
    actual = saved["image_account_verification"]
    assert actual["workers"] == 1000
    assert actual.keys() == DEFAULT_IMAGE_ACCOUNT_VERIFICATION.keys()
    assert ConfigStore(filename).image_account_verification == actual


def test_wait_timeout_keeps_existing_flight_no_duplicate(settings):
    settings["wait_timeout_secs"] = 0.05
    verify = AccountVerification(lambda: settings)
    entered, release = Event(), Event()
    def remote():
        entered.set()
        assert release.wait(3)
        return 42
    try:
        future = verify.submit("account", remote)
        assert entered.wait(1)
        with pytest.raises(VerificationBackoff, match="timed out"):
            verify.run("account", Mock(side_effect=AssertionError("duplicate")), lane="manual")
        assert verify.submit("account", remote, lane="cold_maintenance") is future
        release.set()
        assert future.result(3) == 42
        assert verify.stats()["completed"] == 1
    finally:
        release.set()
        verify.close()


@pytest.mark.parametrize("field", ["remote_verified_at", "last_image_success_at", "last_policy_confirmed_at"])
def test_any_recent_confirmation_skips_info_refresh(service, field, monkeypatch):
    service.update_account("0", {"remote_verified_at": time.time() - 9000, field: time.time() - 1})
    monkeypatch.setattr(service, "fetch_remote_info", Mock(side_effect=AssertionError("unnecessary refresh")))
    assert service._image_account_with_verification("0")["quota"] == 10


def test_invalidated_history_does_not_revive_account(service, monkeypatch):
    service.update_account("0", {"last_image_success_at": time.time()})
    service.invalidate_verification("0")
    assert availability_time(service.get_account("0")) == 0
    remote = Mock(return_value=service.get_account("0"))
    monkeypatch.setattr(service, "fetch_remote_info", remote)
    service._image_account_with_verification("0")
    remote.assert_called_once()
    service._record_invalid_token_seen("0", "test", "HTTP401")
    assert not service._is_image_account_available(service.get_account("0"))
    assert "0" not in service._bucket_for_token


def test_policy_confirmation_neutral_and_stale_attempt_cannot_revive(service):
    before = service.get_account("0")
    outcome = service.mark_image_result("0", False, outcome="policy")
    assert outcome["image_score"] == before["image_score"]
    assert outcome["success"] == before["success"]
    assert outcome["fail"] == before["fail"]
    assert outcome["last_policy_confirmed_at"] > 0
    started = time.time() - 1
    service.invalidate_verification("0")
    service.mark_image_result("0", False, outcome="policy", attempt_started_at=started)
    assert availability_time(service.get_account("0")) == 0


def test_dynamic_expansion_slots_shrink_floor_and_inflight_protection(service, settings):
    tokens = [service.get_available_access_token() for _ in range(5)]
    assert len(set(tokens)) == 5
    assert service.pool_snapshot()["size"] == 5
    for token in tokens:
        service.update_account(token, {"last_image_started_at": time.time() - settings["hot_idle_secs"] - 1})
    service.maintenance_candidates("hot_fill", 20)
    assert service.pool_snapshot()["size"] == 5  # 所有账号在途
    for token in tokens[:4]:
        service.release_image_slot(token)
        # 在途保护只延后检查，不更新实际使用时间。
        with service._lock:
            service._pool_index.idle.set(token, 0)
    service.maintenance_candidates("hot_fill", 20)
    assert service.pool_snapshot()["size"] == settings["hot_pool_size"]
    assert service.get_account(tokens[4])["image_pool_hot"]
    service.release_image_slot(tokens[4])


def test_hot_accounts_preferred_then_overflow_promoted(service):
    service.update_account("0", {"image_pool_hot": True, "hot_entered_at": time.time(), "image_score": 95})
    first = service.get_available_access_token()
    assert first == "0"
    second = service.get_available_access_token()
    assert second != first
    assert service.get_account(second)["image_pool_hot"]
    service.mark_image_result(first, True)
    service.mark_image_result(second, False)
    assert not service._image_inflight


def test_quota_exhaustion_evicts_and_fill_does_not_touch_use_time(service):
    service.update_account("0", {"quota": 1, "image_pool_hot": True, "hot_entered_at": time.time()})
    assert service.get_available_access_token() == "0"
    service.mark_image_result("0", True)
    assert not service.get_account("0")["image_pool_hot"]
    selected = service.maintenance_candidates("hot_fill", 1)
    assert len(selected) == 1 and selected[0] != "0"
    token = selected[0]
    service.maintain_account(token, "hot_fill")
    assert service.get_account(token)["image_pool_hot"]
    assert not service.get_account(token)["last_image_started_at"]


def test_restart_preserves_hot_identity_score_and_deadline(service):
    token = service.get_available_access_token()
    service.mark_image_result(token, True)
    before = service.get_account(token)
    deadline = service._pool_index.due["hot_maintenance"].heap[
        service._pool_index.due["hot_maintenance"].positions[token]][0]
    restarted = AccountService(service.storage)
    try:
        after = restarted.get_account(token)
        assert after["pool_account_id"] == before["pool_account_id"]
        assert after["image_score"] == before["image_score"]
        assert after["image_pool_hot"]
        q = restarted._pool_index.due["hot_maintenance"]
        assert q.heap[q.positions[token]][0] == deadline
    finally:
        restarted.verification.close()


def test_rotation_retains_identity_hot_score_and_inflight(service):
    service.mark_image_started("0")
    before = service.get_account("0")
    service._apply_refreshed_tokens("0", {"access_token": "rotated", "refresh_token": "new-rt"}, "test")
    after = service.get_account("rotated")
    assert after["pool_account_id"] == before["pool_account_id"]
    assert after["image_pool_hot"] and after["image_score"] == before["image_score"]
    assert "0" not in service._pool_index.hot
    assert "rotated" in service._pool_index.hot
    assert after["refresh_token"] == "new-rt"


def test_scoring_classifies_errors_and_cooldown_recovers(service):
    assert service.mark_image_result("0", True)["image_score"] == 51
    assert service.mark_image_result("0", False)["image_score"] == 51
    assert service.mark_image_result("0", False, outcome="policy")["image_score"] == 51
    assert service.mark_image_result("0", False, outcome="account_error")["image_score"] == 46
    for _ in range(3):
        account = service.mark_image_result("0", False, outcome="upstream_timeout")
    assert account["image_score"] == 46 and account["pool_retry_at"] > time.time()
    assert "0" not in service._bucket_for_token
    service.update_account("0", {"pool_retry_at": time.time() - 1})
    service.maintenance_candidates("hot_fill", 0)
    assert "0" in service._bucket_for_token


def test_shared_budget_does_not_reset_between_generation_retries(service):
    budget = ImageAttemptBudget(2)
    a = service.get_available_access_token(budget=budget)
    service.mark_image_result(a, False)
    b = service.get_available_access_token(budget=budget)
    service.mark_image_result(b, False)
    assert a != b
    with pytest.raises(RuntimeError, match="shared attempt budget"):
        service.get_available_access_token(budget=budget)
    assert not service._image_inflight


def test_maintenance_window_refills_and_does_not_scan_pool(service, monkeypatch):
    scheduler = AccountPoolMaintenance(service)
    monkeypatch.setattr(service, "list_accounts", Mock(side_effect=AssertionError("full scan")))
    for _ in range(15):
        scheduler.tick()
        time.sleep(0.01)
    assert service.pool_snapshot()["size"] == 2
    assert scheduler.snapshot()["hot_fill"]["succeeded"] >= 2
    assert all(state["pending"] <= 2 for state in scheduler.snapshot().values())
    assert all(not a.get("last_image_started_at") for a in service._accounts.values())


def test_cancelled_queued_work_does_not_run_or_backoff(settings):
    settings["workers"] = 1
    verify = AccountVerification(lambda: settings)
    entered, release = Event(), Event()
    try:
        first = verify.submit("first", lambda: (entered.set(), release.wait(3)))
        assert entered.wait(1)
        handler = Mock()
        queued = verify.submit("cancelled", handler)
        assert queued.cancel()
        release.set()
        first.result(3)
        wait_until(lambda: verify.stats()["waiting"] == 0)
        handler.assert_not_called()
        assert verify.run("cancelled", lambda: 7) == 7
    finally:
        release.set()
        verify.close()


@pytest.mark.parametrize("already_done", [False, True])
def test_maintenance_alias_tokens_finish_one_stable_identity_once(already_done):
    future = Future()
    if already_done:
        future.set_result({"quota": 1})
    service = Mock()
    service.get_account.side_effect = lambda token: {"pool_account_id": "stable-id", "access_token": token}
    service.verification.submit.return_value = future
    candidates = {"hot_fill": ["old-token"], "hot_maintenance": ["new-token"]}
    service.maintenance_candidates.side_effect = lambda lane, capacity: candidates.pop(lane, []) if capacity else []
    scheduler = AccountPoolMaintenance(service)
    scheduler.tick()
    assert service.verification.submit.call_count == 1
    assert sum(v["pending"] for v in scheduler.snapshot().values()) == 1
    if not already_done:
        future.set_result({"quota": 1})
    scheduler.tick()
    service.maintenance_finished.assert_called_once_with("old-token", "hot_fill", failed=False)
    assert sum(v["completed"] for v in scheduler.snapshot().values()) == 1
    assert not scheduler._pending_keys


def test_maintenance_scheduler_error_uses_redacted_project_logger(monkeypatch):
    import services.account_pool_maintenance as module
    scheduler = AccountPoolMaintenance(Mock())
    stop = Event()
    def fail_tick():
        stop.set()
        raise RuntimeError("private-token-and-upstream-detail")
    monkeypatch.setattr(scheduler, "tick", fail_tick)
    monkeypatch.setattr(scheduler.wake, "wait", lambda **kwargs: None)
    log = Mock()
    monkeypatch.setattr(module, "logger", log)
    scheduler.run(stop)
    log.warning.assert_called_once_with({"event": "account_pool_scheduler_error", "error_type": "RuntimeError"})


@pytest.mark.parametrize("lane", ["foreground", "manual"])
def test_waiting_admission_does_not_inflate_background_rejections(settings, lane):
    settings.update(workers=1, wait_timeout_secs=5)
    verify = AccountVerification(lambda: settings)
    entered, release = Event(), Event()
    def block():
        entered.set()
        assert release.wait(3)
    try:
        first = verify.submit("first", block, lane=lane)
        assert entered.wait(1)
        second = verify.submit("second", lambda: 1, lane=lane)
        assert second is not None
        settings["wait_timeout_secs"] = 0.25
        handler = Mock()
        with pytest.raises(VerificationBackoff, match="admission timed out"):
            verify.run("waiting-request", handler, lane=lane)
        assert verify.stats()["background_rejected"] == 0
        handler.assert_not_called()
        release.set()
        first.result(3)
        assert second.result(3) == 1
        assert verify.run("waiting-request", lambda: 2, lane=lane) == 2
    finally:
        release.set()
        verify.close()


def test_resized_pool_gate_times_out_without_exceeding_new_c(settings):
    settings.update(workers=3, wait_timeout_secs=5)
    verify = AccountVerification(lambda: settings)
    release = Event()
    try:
        old = [verify.submit(str(i), lambda: release.wait(3), lane="manual") for i in range(2)]
        wait_until(lambda: verify.stats()["lanes"]["manual"]["active"] == 2)
        settings.update(workers=2, wait_timeout_secs=0.15)
        verify.reconfigure()
        handler = Mock()
        queued = verify.submit("new-pool", handler, lane="manual")
        assert queued is not None
        with pytest.raises(VerificationBackoff, match="queue timed out"):
            queued.result(2)
        handler.assert_not_called()
        assert verify.stats()["lanes"]["manual"]["active"] == 2
        release.set()
        assert all(f.result(3) for f in old)
    finally:
        release.set()
        verify.close()


def test_reconfigure_down_keeps_old_and_new_pools_under_same_lane_limit(settings):
    verify = AccountVerification(lambda: settings)
    release = Event()
    started = Event()
    try:
        old = [verify.submit(str(i), lambda: release.wait(3), lane="manual") for i in range(2)]
        wait_until(lambda: verify.stats()["lanes"]["manual"]["active"] == 2)
        settings["workers"] = 1
        verify.reconfigure()
        assert verify.submit("new", lambda: started.set(), lane="manual") is None
        assert not started.is_set()
        release.set()
        for future in old:
            future.result(3)
        assert verify.run("new", lambda: 7, lane="manual") == 7
    finally:
        release.set()
        verify.close()


def test_manual_batches_share_c_and_refill_one_at_a_time(service, monkeypatch):
    from threading import Lock
    release_first, release_all, first_started, third_started = Event(), Event(), Event(), Event()
    guard = Lock()
    state = {"active": 0, "peak": 0, "calls": 0}
    def remote(token, *args):
        with guard:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            state["calls"] += 1
            index = state["calls"]
            if index == 2:
                first_started.set()
            if index == 3:
                third_started.set()
        try:
            assert (release_first if index == 1 else release_all).wait(4)
            return service.get_account(token)
        finally:
            with guard:
                state["active"] -= 1
    monkeypatch.setattr(service, "_fetch_remote_info", remote)
    try:
        with ThreadPoolExecutor(2) as batches:
            a = batches.submit(service.refresh_accounts, ["0", "1", "2", "3"], include_items=False)
            b = batches.submit(service.refresh_accounts, ["4", "5", "6", "7"], include_items=False)
            try:
                assert first_started.wait(2)
                release_first.set()
                assert third_started.wait(2), "must refill after one completion, not wait for the whole batch"
                assert state["peak"] == 2
            finally:
                release_all.set()
            assert a.result(4)["refreshed"] + b.result(4)["refreshed"] == 8
    finally:
        release_first.set()
        release_all.set()


def test_rotation_during_verification_joins_same_stable_identity(service, monkeypatch):
    entered, release = Event(), Event()
    calls = []
    def remote(token, *args):
        calls.append(token)
        entered.set()
        assert release.wait(3)
        return service.get_account(token)
    monkeypatch.setattr(service, "_fetch_remote_info", remote)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(service.fetch_remote_info, "0")
        try:
            assert entered.wait(1)
            service._apply_refreshed_tokens("0", {"access_token": "new-token"}, "test")
            second = pool.submit(service.fetch_remote_info, "new-token", lane="manual")
            wait_until(lambda: service.verification.stats()["joined"] >= 1)
            release.set()
            assert first.result(3)["access_token"] == second.result(3)["access_token"] == "new-token"
            assert calls == ["0"]
        finally:
            release.set()


def test_background_failure_protection_does_not_evict_hot_accounts(service):
    scheduler = AccountPoolMaintenance(service)
    service.mark_image_started("0")
    for _ in range(16):
        scheduler._observe_transport_result(RuntimeError("TLS connection timeout"))
    assert scheduler.snapshot()["hot_fill"]["protection_until"] > time.time()
    assert service.pool_snapshot()["size"] == 1
    scheduler.tick()
    assert scheduler.snapshot()["hot_fill"]["pending"] == 0
    # 认证失败不被当成系统级传输故障。
    other = AccountPoolMaintenance(service)
    for _ in range(32):
        other._observe_transport_result(RuntimeError("HTTP 401 token invalidated"))
    assert other.snapshot()["hot_fill"]["protection_until"] == 0


def test_upstream_policy_records_once_without_score_and_releases_slot(service, monkeypatch):
    from services.protocol import conversation as flow
    monkeypatch.setattr(flow, "account_service", service)
    monkeypatch.setattr(flow, "OpenAIBackendAPI", Mock())
    monkeypatch.setattr(flow, "_remove_image_conversation_later", lambda *a, **k: None)
    token = service._acquire_next_candidate_token()
    service.release_image_slot(token)
    def reject(*args):
        raise flow.ImageContentPolicyError("content policy violation")
        yield
    monkeypatch.setattr(flow, "stream_image_outputs", reject)
    with pytest.raises(flow.ImageGenerationError) as error:
        flow._generate_single_image_impl(flow.ConversationRequest(model="gpt-image-2", prompt="sandbox"), 1, 1)
    assert error.value.code == "content_policy_violation"
    confirmed = [a for a in service._accounts.values() if a.get("last_policy_confirmed_at")]
    assert len(confirmed) == 1 and confirmed[0]["image_score"] == 50
    assert sum(a["fail"] for a in service._accounts.values()) == 0
    assert not service._image_inflight


def test_protocol_poll_failure_switches_with_shared_account_budget(service, monkeypatch):
    from services.protocol import conversation as flow
    monkeypatch.setitem(config.data, "image_account_selection_max_attempts", 2)
    monkeypatch.setitem(config.data, "image_poll_timeout_account_switch_retries", 50)
    monkeypatch.setattr(flow, "account_service", service)
    monkeypatch.setattr(flow, "OpenAIBackendAPI", Mock())
    monkeypatch.setattr(flow, "_remove_image_conversation_later", lambda *a, **k: None)
    calls = []
    def timeout(*args):
        calls.append(1)
        raise flow.ImagePollTimeoutError("sandbox timeout")
        yield
    monkeypatch.setattr(flow, "stream_image_outputs", timeout)
    with pytest.raises(flow.ImageGenerationError, match="result is still being resolved") as error:
        flow._generate_single_image_impl(flow.ConversationRequest(model="gpt-image-2", prompt="sandbox"), 1, 1)
    assert error.value.code == "image_result_uncertain"
    assert error.value.status_code == 504
    assert len(calls) == 2
    assert not service._image_inflight


def test_pool_failures_endpoint_is_bounded_and_authenticated(service, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import api.system as routes
    scheduler = AccountPoolMaintenance(service)
    for n in range(220):
        scheduler._failures.append(dict(account=f"sandbox{n}", error="HTTP 503", time=n, lane="hot_fill"))
    scheduler._totals["hot_fill"]["failed"] = 220
    monkeypatch.setattr(routes, "pool_maintenance", scheduler)
    app = FastAPI()
    app.include_router(routes.create_router("test"))
    with TestClient(app) as client:
        url = "/api/monitoring/account-pool/failures"
        assert client.get(url).status_code == 401
        monkeypatch.setattr(routes, "require_admin", lambda _: None)
        result = client.get(url).json()
        assert result["total"] == 220 and result["retained"] == 200
        assert len(result["items"]) == 20
        assert result["items"][0]["account"] == "sandbox219"
        assert client.get(url + "?offset=190").json()["items"][-1]["account"] == "sandbox20"
        assert client.get(url + "?limit=101").status_code == 422


def test_maintenance_skip_cannot_satisfy_expired_foreground_x(service, settings, monkeypatch):
    from services.account_pool import CachedAccountInfo
    service.update_account("0", {"remote_verified_at": time.time() - settings["cache_ttl_secs"] - 10})
    entered, release = Event(), Event()
    current = service.get_account("0")
    def maintenance_skip():
        entered.set()
        assert release.wait(3)
        return CachedAccountInfo(current)
    key = current["pool_account_id"]
    skipped = service.verification.submit(key, maintenance_skip, lane="hot_maintenance")
    assert entered.wait(1)
    remote = Mock(side_effect=lambda *a: service.update_account("0", {"remote_verified_at": time.time()}, quiet=True))
    monkeypatch.setattr(service, "_fetch_remote_info", remote)
    with ThreadPoolExecutor(1) as pool:
        foreground = pool.submit(service._image_account_with_verification, "0")
        try:
            wait_until(lambda: service.verification.stats()["joined"] >= 1)
            release.set()
            skipped.result(3)
            assert foreground.result(3)["remote_verified_at"] > current["remote_verified_at"]
            remote.assert_called_once()
        finally:
            release.set()


def test_result_rates_exclude_policy_and_distinguish_first_vs_switched():
    from services.account_pool import ImageAttemptResults
    results = ImageAttemptResults()
    results.record(success=True, policy=False, attempts=1)
    results.record(success=True, policy=False, attempts=3)
    results.record(success=False, policy=False, attempts=2)
    results.record(success=False, policy=True, attempts=1)
    stats = results.snapshot()
    assert stats["total"] == 3 and stats["policy_rejected"] == 1
    assert stats["first_attempt_success_rate"] == 33.33
    assert stats["switched_success_rate"] == 50


def test_protocol_wrapper_records_one_final_result_not_every_account(service, monkeypatch):
    from services.account_pool import ImageAttemptResults
    from services.protocol import conversation as flow
    results = ImageAttemptResults()
    monkeypatch.setattr(flow, "image_attempt_results", results)
    monkeypatch.setattr(flow, "account_service", service)
    monkeypatch.setattr(flow, "OpenAIBackendAPI", Mock())
    monkeypatch.setattr(flow, "_remove_image_conversation_later", lambda *a, **k: None)
    def generated(*args):
        yield flow.ImageOutput(kind="result", model="gpt-image-2", index=1, total=1, data=[{"url": "https://sandbox/image.png"}])
    monkeypatch.setattr(flow, "stream_image_outputs", generated)
    flow._generate_single_image(flow.ConversationRequest(model="gpt-image-2", prompt="sandbox"), 1, 1)
    assert results.snapshot()["total"] == results.snapshot()["first_attempt_success"] == 1
    assert not service._image_inflight
