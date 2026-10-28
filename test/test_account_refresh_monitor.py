from concurrent.futures import ThreadPoolExecutor
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.account_refresh_monitor import AccountRefreshMonitor
from services.account_service import AccountService
from services.config import config
from test.test_account_refresh_token_status import MemoryStorage


def test_round_lifecycle_and_snapshot_isolation():
    monitor = AccountRefreshMonitor()
    assert monitor.snapshot()["status"] == "idle"
    round_id = monitor.start(300)
    monitor.plan(round_id, 3, 1)
    monitor.set_phase(round_id, "refresh")
    monitor.update(round_id, "refresh", "started")
    snapshot = monitor.snapshot()
    assert snapshot["refresh"] == dict(total=3, completed=0, succeeded=0, failed=0, active=1, pending=2)
    snapshot["refresh"]["active"] = 99
    assert monitor.snapshot()["refresh"]["active"] == 1
    monitor.update(round_id, "refresh", "finished", True)
    for ok in (True, False):
        monitor.update(round_id, "refresh", "started")
        monitor.update(round_id, "refresh", "finished", ok)
    monitor.set_phase(round_id, "keepalive")
    assert monitor.snapshot()["status"] == "running"
    monitor.update(round_id, "keepalive", "started")
    monitor.update(round_id, "keepalive", "finished", True)
    monitor.finish(round_id)
    final = monitor.snapshot()
    assert final["status"] == "completed_with_errors"
    assert final["refresh"] == dict(total=3, completed=3, succeeded=2, failed=1, active=0, pending=0)
    assert final["finished_at"] >= final["started_at"]
    assert final["next_run_at"] == final["finished_at"] + 300
    assert final["elapsed_seconds"] >= 0
    assert final["last_success_at"] is not None
    duration = final["elapsed_seconds"]
    assert monitor.snapshot()["elapsed_seconds"] == duration
    new_id = monitor.start(600)
    monitor.update(round_id, "refresh", "started")
    assert monitor.snapshot()["refresh"]["active"] == 0
    assert monitor.snapshot()["round"] == new_id
    assert monitor.snapshot()["last_success_at"] == final["last_success_at"]


def test_empty_round_and_incomplete_round():
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 0, 0)
    monitor.finish(round_id)
    assert monitor.snapshot()["status"] == "completed"
    round_id = monitor.start(60)
    monitor.plan(round_id, 2, 0)
    monitor.finish(round_id)
    assert monitor.snapshot()["status"] == "failed"
    assert monitor.snapshot()["refresh"]["pending"] == 2
    assert monitor.snapshot()["error"]


def test_full_refresh_schedule_survives_restart_and_keeps_absolute_time(tmp_path):
    now = [1000.0]
    state_path = tmp_path / "account-refresh-schedule.json"
    monitor = AccountRefreshMonitor(state_path, clock=lambda: now[0])
    round_id = monitor.start(3600)
    monitor.plan(round_id, 0, 0)
    monitor.finish(round_id, full_refresh=True)

    assert monitor.snapshot()["last_full_refresh_finished_at"] == 1000.0
    assert monitor.snapshot()["next_full_refresh_at"] == 4600.0

    now[0] = 4000.0
    restarted = AccountRefreshMonitor(state_path, clock=lambda: now[0])
    assert restarted.next_full_refresh_at(3600) == 4600.0
    assert not restarted.full_refresh_due(3600)
    round_id = restarted.start(3600)
    restarted.plan(round_id, 0, 0)
    restarted.finish(round_id, full_refresh=False)
    assert restarted.snapshot()["last_full_refresh_finished_at"] == 1000.0
    assert restarted.snapshot()["next_run_at"] == 4600.0

    now[0] = 4600.0
    assert restarted.full_refresh_due(3600)


def test_concurrent_progress_is_constant_size_and_consistent():
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 2000, 0)
    def work(number):
        monitor.update(round_id, "refresh", "started")
        monitor.update(round_id, "refresh", "finished", number % 2 == 0)
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(work, range(2000)))
    monitor.finish(round_id)
    assert monitor.snapshot()["refresh"] == dict(total=2000, completed=2000, succeeded=1000, failed=1000, active=0, pending=0)


@pytest.fixture
def service(monkeypatch):
    monkeypatch.setitem(config.data, "auto_relogin_after_refresh", False)
    instance = AccountService(MemoryStorage())
    yield instance
    instance.verification.close()


def test_refresh_callbacks_include_network_errors_missing_accounts_and_dedup(service, monkeypatch):
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 4, 0)
    def fetch(token, *args):
        if token == "network":
            raise RuntimeError("TLS connect error")
        if token == "invalid":
            raise ValueError("invalid access token")
        return None if token == "missing" else {"email": "test@example.com"}
    monkeypatch.setattr(service, "fetch_remote_info", fetch)
    monkeypatch.setattr(service, "list_accounts", Mock(side_effect=AssertionError("must not scan pool")))
    result = service.refresh_accounts(["ok", "network", "invalid", "missing", "ok"], include_items=False,
        on_progress=lambda event, success=None: monitor.update(round_id, "refresh", event, success))
    monitor.finish(round_id)
    assert result["refreshed"] == 1
    assert "items" not in result
    assert monitor.snapshot()["refresh"] == dict(total=4, completed=4, succeeded=1, failed=3, active=0, pending=0)


def test_live_active_and_pending_counts(service, monkeypatch):
    entered, release = Event(), Event()
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 15, 0)
    def fetch(*args):
        entered.set()
        assert release.wait(5)
        return {}
    monkeypatch.setattr(service, "fetch_remote_info", fetch)
    with ThreadPoolExecutor(1) as pool:
        task = pool.submit(service.refresh_accounts, list(map(str, range(15))), include_items=False,
            on_progress=lambda event, success=None: monitor.update(round_id, "refresh", event, success))
        try:
            assert entered.wait(2)
            counts = monitor.snapshot()["refresh"]
            assert 1 <= counts["active"] <= 10
            assert counts["active"] + counts["pending"] == 15
            assert counts["pending"] >= 5
            assert counts["completed"] == 0
        finally:
            release.set()
        assert task.result(5)["refreshed"] == 15
    monitor.finish(round_id)
    assert monitor.snapshot()["status"] == "completed"


def test_keepalive_callbacks_success_failure_and_missing(service, monkeypatch):
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 0, 3)
    monkeypatch.setattr(service, "resolve_access_token", lambda token: token)
    monkeypatch.setattr(service, "refresh_access_token", lambda token, **kw: token)
    monkeypatch.setattr(service, "get_account", lambda token: {"last_token_refresh_error": "failed"} if token == "bad" else {} if token == "missing" else {"email": "test@example.com"})
    monkeypatch.setattr(service, "list_accounts", Mock(side_effect=AssertionError("must not scan pool")))
    result = service.keepalive_refresh_tokens(["ok", "bad", "missing"], include_items=False,
        on_progress=lambda event, success=None: monitor.update(round_id, "keepalive", event, success))
    monitor.finish(round_id)
    assert result["refreshed"] == 1
    assert "items" not in result
    assert monitor.snapshot()["keepalive"] == dict(total=3, completed=3, succeeded=1, failed=2, active=0, pending=0)


def test_keepalive_unexpected_exception_finishes_active_item(service, monkeypatch):
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 0, 1)
    monkeypatch.setattr(service, "resolve_access_token", Mock(side_effect=RuntimeError("unavailable")))
    with pytest.raises(RuntimeError):
        service.keepalive_refresh_tokens(["one"], include_items=False,
            on_progress=lambda event, success=None: monitor.update(round_id, "keepalive", event, success))
    monitor.finish(round_id, "巡检中断")
    assert monitor.snapshot()["status"] == "failed"
    assert monitor.snapshot()["keepalive"]["active"] == 0
    assert monitor.snapshot()["keepalive"]["failed"] == 1


@pytest.mark.parametrize("broken", [False, True])
def test_watcher_round_and_next_run(monkeypatch, broken):
    import api.support as support
    monitor = AccountRefreshMonitor()
    stop = Event()
    class StopAfterRound(Event):
        def wait(self, timeout=None):
            self.set()
            return True
    stop = StopAfterRound()
    fake = SimpleNamespace(
        list_limited_tokens=lambda: ["a"], list_normal_tokens=lambda: ["a", "b"],
        list_expiring_access_tokens=lambda: ["b"],
        list_refresh_token_keepalive_tokens=lambda: ["b", "c"],
    )
    def refresh(tokens, *, include_items, on_progress, on_failure):
        assert tokens == ["a", "b"] and not include_items
        for ok in [True, False]:
            on_progress("started")
            if not ok:
                on_failure(dict(account="failed@example.com", error="HTTP 429", time=1))
            on_progress("finished", ok)
        if broken:
            raise RuntimeError("secret must never appear in snapshot")
    def keepalive(tokens, *, include_items, on_progress, on_failure):
        assert tokens == ["c"] and not include_items
        on_progress("started")
        assert monitor.snapshot()["phase"] == "keepalive"
        assert monitor.snapshot()["status"] == "running"
        on_progress("finished", True)
        return {"errors": []}
    fake.refresh_accounts = refresh
    fake.keepalive_refresh_tokens = keepalive
    monkeypatch.setattr(support, "account_service", fake)
    monkeypatch.setattr(support, "account_refresh_monitor", monitor)
    monkeypatch.setitem(config.data, "refresh_account_interval_minute", 5)
    monkeypatch.setitem(config.data, "account_refresh_startup_mode", "full")
    thread = support.start_limited_account_watcher(stop)
    thread.join(5)
    assert not thread.is_alive()
    state = monitor.snapshot()
    assert state["status"] == ("failed" if broken else "completed_with_errors")
    assert state["interval_seconds"] == 300
    assert state["refresh"]["total"] == 2
    assert state["next_run_at"] >= state["finished_at"]
    assert "secret" not in str(state)
    assert monitor.failures(state["round"], "refresh")["items"][0]["account"] == "failed@example.com"


def test_watcher_startup_deferred_refreshes_only_expiring_and_keepalive(monkeypatch):
    import api.support as support

    monitor = AccountRefreshMonitor()

    class StopAfterRound(Event):
        def wait(self, timeout=None):
            self.set()
            return True

    stop = StopAfterRound()
    calls = []
    fake = SimpleNamespace(
        list_limited_tokens=lambda: (_ for _ in ()).throw(AssertionError("limited accounts must be deferred")),
        list_normal_tokens=lambda: (_ for _ in ()).throw(AssertionError("normal accounts must be deferred")),
        list_expiring_access_tokens=lambda: ["expiring"],
        list_refresh_token_keepalive_tokens=lambda: ["expiring", "keepalive"],
    )

    def refresh(tokens, *, include_items, on_progress, on_failure):
        calls.append(("refresh", tokens))
        assert tokens == ["expiring"] and not include_items
        on_progress("started")
        on_progress("finished", True)

    def keepalive(tokens, *, include_items, on_progress, on_failure):
        calls.append(("keepalive", tokens))
        assert tokens == ["keepalive"] and not include_items
        on_progress("started")
        on_progress("finished", True)
        return {"errors": []}

    fake.refresh_accounts = refresh
    fake.keepalive_refresh_tokens = keepalive
    monkeypatch.setattr(support, "account_service", fake)
    monkeypatch.setattr(support, "account_refresh_monitor", monitor)
    monkeypatch.setitem(config.data, "refresh_account_interval_minute", 5)
    monkeypatch.setitem(config.data, "account_refresh_startup_mode", "deferred")

    thread = support.start_limited_account_watcher(stop)
    thread.join(5)

    assert not thread.is_alive()
    state = monitor.snapshot()
    assert state["status"] == "completed"
    assert state["refresh"] == dict(total=1, completed=1, succeeded=1, failed=0, active=0, pending=0)
    assert state["keepalive"] == dict(total=1, completed=1, succeeded=1, failed=0, active=0, pending=0)
    assert calls == [("refresh", ["expiring"]), ("keepalive", ["keepalive"])]


def test_monitoring_api_exposes_refresh_snapshot(monkeypatch, tmp_path):
    import api.system as system
    from services.log_service import LogService
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(300)
    monitor.plan(round_id, 3, 0)
    monitor.update(round_id, "refresh", "started")
    monkeypatch.setattr(system, "account_refresh_monitor", monitor)
    monkeypatch.setattr(system, "require_admin", lambda _: None)
    with LogService(tmp_path / "logs.jsonl") as logs:
        monkeypatch.setattr(system, "log_service", logs)
        app = FastAPI()
        app.include_router(system.create_router("test"))
        with TestClient(app) as client:
            response = client.get("/api/monitoring")
        assert response.status_code == 200
        assert response.json()["account_refresh"]["refresh"] == dict(total=3, completed=0, succeeded=0, failed=0, active=1, pending=2)


def test_failure_details_are_bounded_paginated_and_reset():
    monitor = AccountRefreshMonitor()
    monitor.MAX_FAILURES = 3
    round_id = monitor.start(60)
    monitor.plan(round_id, 4, 0)
    for number in range(4):
        monitor.update(round_id, "refresh", "started")
        monitor.add_failure(round_id, "refresh", dict(account=f"user{number}@example.com", error="timeout", time=number))
        monitor.update(round_id, "refresh", "finished", False)
    monitor.finish(round_id)
    first = monitor.failures(round_id, "refresh", 0, 2)
    assert first["total"] == 4 and first["retained"] == 3
    assert len(first["items"]) == 2
    assert monitor.failures(round_id, "refresh", 2, 2)["items"][0]["account"] == "user2@example.com"
    first["items"][0]["error"] = "changed"
    assert monitor.failures(round_id, "refresh")["items"][0]["error"] == "timeout"
    assert "items" not in monitor.snapshot()
    new_round = monitor.start(60)
    monitor.add_failure(round_id, "refresh", dict(account="stale", error="timeout", time=0))
    assert monitor.failures(new_round, "refresh")["retained"] == 0
    with pytest.raises(ValueError):
        monitor.failures(round_id, "refresh")


def test_failure_reason_redacts_credentials_and_keeps_actionable_reason():
    from services.account_refresh_monitor import refresh_failure_item
    account = dict(email="test@example.com", access_token="access-secret-abc", refresh_token="refresh-secret-def", password="password-secret-ghi")
    error = "HTTP 401 invalid_grant access-secret-abc refresh-secret-def password-secret-ghi Bearer unknown-secret authorization: other-secret https://proxyuser:proxypass@proxy.test/path?key=querysecret"
    item = refresh_failure_item("access-secret-abc", account, error)
    assert item["account"] == "test@example.com"
    assert "HTTP 401 invalid_grant" in item["error"]
    for value in ["access-secret-abc", "refresh-secret-def", "password-secret-ghi", "unknown-secret", "other-secret", "proxyuser", "proxypass", "querysecret"]:
        assert value not in str(item)
    anonymous = refresh_failure_item("access-secret-abc", None, "x" * 5000)
    assert anonymous["account"].startswith("账号 #")
    assert "access-secret-abc" not in str(anonymous)
    assert len(anonymous["error"]) == 1024
    assert "p4ss" not in refresh_failure_item("long-token", {"password": "p4ss"}, "failed with p4ss")["error"]


def test_refresh_failure_callback_retains_identity_before_removal(service, monkeypatch):
    failures = []
    account = dict(email="failed@example.com", access_token="secret-token-123")
    monkeypatch.setattr(service, "get_account", lambda token: account)
    def fetch(*args):
        monkeypatch.setattr(service, "get_account", lambda token: None)
        raise RuntimeError("HTTP 401 invalid token secret-token-123")
    monkeypatch.setattr(service, "fetch_remote_info", fetch)
    result = service.refresh_accounts(["secret-token-123"], include_items=False, on_failure=failures.append)
    assert result["refreshed"] == 0
    assert len(failures) == 1
    assert failures[0]["account"] == "failed@example.com"
    assert "HTTP 401" in failures[0]["error"]
    assert "secret-token-123" not in str(failures)


def test_keepalive_failure_callback_provides_returned_reason(service, monkeypatch):
    failures = []
    monkeypatch.setattr(service, "resolve_access_token", lambda token: token)
    monkeypatch.setattr(service, "refresh_access_token", lambda token, **kw: token)
    monkeypatch.setattr(service, "get_account", lambda token: dict(email="failed@example.com", last_token_refresh_error="invalid_grant: refresh token revoked"))
    service.keepalive_refresh_tokens(["token"], include_items=False, on_failure=failures.append)
    assert failures[0]["account"] == "failed@example.com"
    assert "invalid_grant" in failures[0]["error"]


def test_failures_api_auth_validation_pagination_and_stale_round(monkeypatch):
    import api.system as system
    monitor = AccountRefreshMonitor()
    round_id = monitor.start(60)
    monitor.plan(round_id, 25, 0)
    for n in range(25):
        monitor.update(round_id, "refresh", "started")
        monitor.add_failure(round_id, "refresh", dict(account=f"user{n}@example.com", error="HTTP 429", time=n))
        monitor.update(round_id, "refresh", "finished", False)
    monkeypatch.setattr(system, "account_refresh_monitor", monitor)
    app = FastAPI()
    app.include_router(system.create_router("test"))
    with TestClient(app) as client:
        url = f"/api/monitoring/account-refresh/failures?round={round_id}"
        assert client.get(url).status_code == 401
        monkeypatch.setattr(system, "require_admin", lambda _: None)
        response = client.get(url)
        assert response.status_code == 200
        assert response.json()["total"] == 25
        assert len(response.json()["items"]) == 20
        assert len(client.get(url + "&offset=20").json()["items"]) == 5
        assert client.get(url + "&kind=keepalive").json()["items"] == []
        for query in ["&offset=-1", "&limit=0", "&limit=101", "&kind=invalid"]:
            assert client.get(url + query).status_code == 422
        monitor.start(60)
        assert client.get(url).status_code == 409
