import asyncio
import copy
import time
from concurrent.futures import Future, ThreadPoolExecutor
from threading import Event, Lock, get_ident
from unittest import mock

import pytest

from services.auth_service import AuthService
from services.async_completion import await_worker
from services.monitoring_cache import SnapshotCache
from services.image_metrics import ImageMetrics, current_metrics, measure_event_loop_lag, runtime_phases
from services.account_persistence import AccountWriter, persisted


class KeyStorage:
    def __init__(self):
        self.items = []
        self.block = False
        self.entered, self.release = Event(), Event()
        self.writes = 0

    def load_auth_keys(self):
        return copy.deepcopy(self.items)

    def save_auth_keys(self, items):
        if self.block:
            self.entered.set()
            assert self.release.wait(5)
        self.items = copy.deepcopy(items)
        self.writes += 1


def test_auth_hot_path_does_not_wait_for_usage_storage_or_management_lock():
    storage = KeyStorage()
    service = AuthService(storage)
    item, key = service.create_key(role="user", name="load")
    storage.block = True
    try:
        assert service.authenticate(key)["id"] == item["id"]
        assert storage.entered.wait(2)
        # 后台线程正在持管理锁等待磁盘，鉴权仍然完全可用。
        with ThreadPoolExecutor(32) as pool:
            futures = [pool.submit(service.authenticate, key) for _ in range(500)]
            assert all(f.result(2)["id"] == item["id"] for f in futures)
        assert service.authenticate("invalid") is None
    finally:
        storage.release.set()
        service.close()
    assert storage.items[0]["last_used_at"] is not None
    assert storage.writes <= 3  # create + coalesced usage + shutdown tail


def test_usage_flush_never_resurrects_revoked_or_rotated_keys():
    storage = KeyStorage()
    service = AuthService(storage)
    try:
        item, key = service.create_key(role="user", name="load")
        service.authenticate(key)
        service.update_key(item["id"], {"key": "replacement"})
        assert service.authenticate(key) is None
        assert service.authenticate("replacement")
        service.update_key(item["id"], {"enabled": False})
        assert service.authenticate("replacement") is None
        assert service.delete_key(item["id"])
        service._flush_usage()
        assert storage.items == []
        assert service.authenticate(key) is None
    finally:
        service.close()


def test_usage_flush_failure_is_not_auth_failure_and_is_retried():
    storage = KeyStorage()
    service = AuthService(storage)
    item, key = service.create_key(role="user", name="load")
    try:
        with mock.patch.object(service, "_save", side_effect=OSError("unavailable")):
            assert service.authenticate(key)
            service._flush_usage()
        assert service.authenticate(key)
    finally:
        service.close()
    assert storage.items[0]["last_used_at"]


def test_worker_notifications_batch_once_and_propagate_errors_and_cancel():
    async def scenario():
        futures = [Future() for _ in range(100)]
        tasks = [asyncio.create_task(await_worker(f, "batch_test")) for f in futures]
        await asyncio.sleep(0)
        bridge = asyncio.get_running_loop()._cg2api_completions
        with mock.patch.object(bridge.loop, "call_soon_threadsafe", wraps=bridge.loop.call_soon_threadsafe) as wake:
            for i, f in enumerate(futures):
                f.set_result(i)
            assert wake.call_count == 1
        assert await asyncio.gather(*tasks) == list(range(100))
        error = Future()
        error.set_exception(ValueError("failed"))
        with pytest.raises(ValueError, match="failed"):
            await await_worker(error, "batch_test")
        queued = Future()
        task = asyncio.create_task(await_worker(queued, "batch_test"))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert queued.cancelled()
        await asyncio.sleep(0)
    asyncio.run(scenario())


def test_loop_lag_is_measured_and_stops_cleanly():
    async def scenario():
        task = asyncio.create_task(measure_event_loop_lag(0.01))
        await asyncio.sleep(0.02)
        time.sleep(0.04)
        await asyncio.sleep(0.02)
        assert runtime_phases.snapshot()["event_loop_lag"]["max_ms"] >= 20
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(scenario())


def test_snapshot_refresh_is_coalesced_and_reports_stale_without_faking_zero():
    async def scenario():
        cache = SnapshotCache(ttl=0.01, wait=0.01)
        calls = 0
        release = asyncio.Event()
        async def collect():
            nonlocal calls
            calls += 1
            if calls > 1:
                await release.wait()
            return {"count": calls}
        initial = await asyncio.gather(*(cache.get("same", collect) for _ in range(50)))
        assert calls == 1
        assert all(value == {"count": 1} for value, _ in initial)
        await asyncio.sleep(0.02)
        value, state = await cache.get("same", collect)
        assert value == {"count": 1}
        assert state["stale"] and state["refreshing"] and state["age_ms"] >= 10
        release.set()
        await asyncio.sleep(0)
        value, state = await cache.get("same", collect)
        assert value == {"count": 2}
        async def fail():
            raise OSError("stats busy")
        await asyncio.sleep(0.02)
        value, state = await cache.get("same", fail)
        assert value == {"count": 2} and state["refresh_failed"] and state["stale"]
        with pytest.raises(OSError):
            await cache.get("cold", fail)
    asyncio.run(scenario())


def test_snapshot_inflight_capacity_is_bounded():
    async def scenario():
        cache = SnapshotCache(capacity=1)
        release = asyncio.Event()
        async def collect():
            await release.wait()
            return {}
        first = asyncio.create_task(cache.get("one", collect))
        await asyncio.sleep(0)
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as exc:
            await cache.get("two", collect)
        assert exc.value.status_code == 503
        release.set()
        await first
    asyncio.run(scenario())


def test_account_writer_start_happens_after_state_lock_is_released():
    lock = Lock()
    class Storage:
        def save_accounts(self, items):
            assert lock.acquire(blocking=False)
            lock.release()
    writer = AccountWriter(Storage())
    class Service:
        _writer = writer
        from threading import local
        _write_context = local()
        @persisted
        def update(self):
            with lock:
                future = writer.submit(snapshot=[{"id": 1}], deferred=True)
                self._write_context.pending.append(future)
                assert not writer._running
    Service().update()


def test_local_filter_noop_does_not_use_general_pool_and_rules_still_run(monkeypatch):
    import api.ai as ai
    class Call:
        async def log_async(self, *args, **kwargs):
            pass
    async def scenario():
        with mock.patch.dict(ai.config.data, {"ai_review": {"enabled": False}, "sensitive_words": []}):
            with mock.patch.object(ai, "run_in_threadpool", side_effect=AssertionError("unnecessary offload")):
                await ai.filter_or_log(Call(), "cat")
        from fastapi import HTTPException
        with mock.patch.dict(ai.config.data, {"ai_review": {"enabled": False}, "sensitive_words": ["blocked"]}):
            with pytest.raises(HTTPException) as exc:
                await ai.filter_or_log(Call(), "blocked")
            assert exc.value.status_code == 400
    asyncio.run(scenario())


def test_account_slot_limit_survives_concurrent_selection_and_release(tmp_path):
    from services.account_service import AccountService
    from services.storage.json_storage import JSONStorageBackend
    from services.config import config
    storage = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "keys.json")
    storage.save_accounts([{"access_token": "one", "quota": 50, "status": "正常", "type": "Free"}])
    service = AccountService(storage)
    with mock.patch.dict(config.data, {"image_account_concurrency": 3}):
        service._scheduler_concurrency = 3
        for token in service._accounts:
            service._sync_candidate_locked(token)
        active = maximum = 0
        guard = Lock()
        def use():
            nonlocal active, maximum
            token = service._acquire_next_candidate_token()
            with guard:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.002)
            with guard:
                active -= 1
            service.release_image_slot(token)
        with ThreadPoolExecutor(30) as pool:
            futures = [pool.submit(use) for _ in range(150)]
            for future in futures:
                future.result(10)
        assert maximum <= 3
        assert not service._image_inflight
        assert "one" in service._available_buckets[service._bucket_for_token["one"]]


def test_account_metric_updates_are_after_unlock(tmp_path):
    from services.account_service import AccountService
    from services.storage.json_storage import JSONStorageBackend
    service = AccountService(JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "keys.json"))
    def observe(*args):
        assert service._lock.acquire(blocking=False)
        service._lock.release()
    with mock.patch.object(runtime_phases, "observe", side_effect=observe):
        with service._timed_account_lock("test_lock"):
            pass


def test_image_api_auth_and_noop_filter_bypass_saturated_general_pool(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from services.log_service import LogService
    from services.auth_service import AuthService
    import httpx
    import anyio
    import api.ai as ai
    import api.support as support
    service = AuthService(KeyStorage())
    _, key = service.create_key(role="user", name="api")
    monkeypatch.setattr(support, "auth_service", service)
    logs = LogService(tmp_path / "logs.jsonl")
    monkeypatch.setattr("services.log_service.log_service", logs)
    monkeypatch.setattr(ai.openai_v1_image_generations, "handle", lambda body: {"created": 1, "data": [{"url": "https://test/image.png"}]})
    app = FastAPI()
    app.include_router(ai.create_router())
    async def scenario():
        limiter = anyio.to_thread.current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        entered, release = Event(), Event()
        def block():
            entered.set()
            assert release.wait(5)
        from starlette.concurrency import run_in_threadpool
        blocker = asyncio.create_task(run_in_threadpool(block))
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                response = await asyncio.wait_for(client.post("/v1/images/generations", headers={"Authorization": "Bearer " + key}, json={"prompt": "cat"}), 2)
                assert response.status_code == 200
                assert response.json()["data"] == [{"url": "https://test/image.png"}]
        finally:
            release.set()
            await blocker
            limiter.total_tokens = previous
    try:
        with mock.patch.dict(ai.config.data, {"ai_review": {"enabled": False}, "sensitive_words": []}):
            asyncio.run(scenario())
    finally:
        service.close()
        logs.close()

def test_running_worker_cancel_does_not_leak_capacity():
    from services.isolated_executor import IsolatedExecutor
    async def scenario():
        executor = IsolatedExecutor(1, 1, "running_cancel")
        entered, release = Event(), Event()
        def work():
            entered.set()
            assert release.wait(5)
        task = asyncio.create_task(executor.run(work))
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert executor.stats()["pending"] == 1
            release.set()
            while executor.stats()["pending"]:
                await asyncio.sleep(0.001)
            assert await executor.run(lambda: "next") == "next"
            assert executor.stats()["pending"] == 0
        finally:
            release.set()
            executor._pool.shutdown()
    asyncio.run(scenario())


def test_monitoring_slow_date_stats_do_not_block_other_date(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI
    from services.log_service import LogService
    import api.system as system
    logs = LogService(tmp_path / "logs.jsonl")
    monkeypatch.setattr(system, "require_admin", lambda _: None)
    monkeypatch.setattr(system, "log_service", logs)
    original = logs.image_stats
    entered, release = Event(), Event()
    def stats(start, end):
        if start == "2026-10-01":
            entered.set()
            assert release.wait(5)
        return original(start, end)
    monkeypatch.setattr(logs, "image_stats", stats)
    app = FastAPI()
    app.include_router(system.create_router("test"))
    async def scenario():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            blocked = asyncio.create_task(client.get("/api/monitoring?start_date=2026-10-01"))
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.001)
                response = await asyncio.wait_for(client.get("/api/monitoring?start_date=2026-10-02"), 2)
                assert response.status_code == 200
                assert response.json()["images"]["start_date"] == "2026-10-02"
                assert "snapshot_state" in response.json()
            finally:
                release.set()
                assert (await blocked).status_code == 200
    try:
        asyncio.run(scenario())
    finally:
        logs.close()
