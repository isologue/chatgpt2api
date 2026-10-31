"""离线大池模拟，不访问上游、不读取生产凭据。运行：python -m scripts.benchmark_dynamic_account_pool"""
from __future__ import annotations

import json
import time
from threading import Lock

from services.account_service import AccountService, _RevisionDict
from services.config import config, DEFAULT_IMAGE_ACCOUNT_VERIFICATION


class IncrementalMemoryStorage:
    supports_incremental_account_writes = True
    def __init__(self, count):
        self.items = {str(n): {"access_token": str(n), "quota": 10,
                     "remote_verified_at": time.time(), "created_at": "2026-10-29T00:00:00+00:00"}
                     for n in range(count)}
        self.lock = Lock()
    def load_accounts(self):
        return list(self.items.values())
    def apply_account_changes(self, upserts, deletes):
        with self.lock:
            for token in deletes:
                self.items.pop(token, None)
            for account in upserts:
                self.items[account["access_token"]] = account


class NoScanAccounts(_RevisionDict):
    def __iter__(self):
        raise AssertionError("request path scanned all accounts")
    def values(self):
        raise AssertionError("request path scanned all account values")
    def items(self):
        raise AssertionError("request path scanned all account items")


def main(count=100000, requests=1000):
    saved = config.data.copy()
    service = None
    try:
        config.data.update(image_account_verification={**DEFAULT_IMAGE_ACCOUNT_VERIFICATION,
            "hot_pool_size": 2000}, image_account_concurrency=1, auto_remove_rate_limited_accounts=False)
        started = time.perf_counter()
        service = AccountService(IncrementalMemoryStorage(count))
        index_seconds = time.perf_counter() - started
        service._log = lambda *a, **k: None
        service.refresh_access_token = lambda token, **k: token
        service._accounts = NoScanAccounts(service._accounts, on_change=service._mark_account_revision)
        durations, reserved = [], []
        for _ in range(requests):
            started = time.perf_counter()
            reserved.append(service.get_available_access_token())
            durations.append((time.perf_counter() - started) * 1000)
        assert len(set(reserved)) == requests
        assert len(service._image_inflight) == requests
        for token in reserved:
            service.mark_image_result(token, True)
        assert not service._image_inflight
        # 维护领取和监控同样不能扫描全池；不执行任何远程查询。
        candidates = service.maintenance_candidates("hot_fill", 100)
        snapshot = service.pool_snapshot()
        sorted_ms = sorted(durations)
        print(json.dumps({"accounts": count, "reserved_unique_accounts": requests,
            "index_seconds": round(index_seconds, 3),
            "selection_including_persistence_p50_ms": round(sorted_ms[int(requests * .5)], 3),
            "selection_including_persistence_p95_ms": round(sorted_ms[int(requests * .95)], 3),
            "selection_including_persistence_max_ms": round(max(durations), 3),
            "maintenance_window": len(candidates), "hot_size": snapshot["size"],
            "all_slots_released": not service._image_inflight, "full_scan_guard": "passed",
            "note": "offline fake-account incremental-memory persistence; not real image-generation load"}, indent=2))
    finally:
        if service:
            service.verification.close()
        config.data = saved


if __name__ == "__main__":
    main()
