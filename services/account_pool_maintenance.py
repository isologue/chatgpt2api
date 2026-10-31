from __future__ import annotations

import time
import re
from collections import deque
from threading import Event, Lock

from services.account_service import account_service
from services.account_refresh_monitor import refresh_failure_item
from services.config import config
from utils.log import logger


class AccountPoolMaintenance:
    """单一调度线程领取到期账号；三条维护通道各用C个滑动窗口。"""
    LANES = ("hot_fill", "hot_maintenance", "cold_maintenance")

    def __init__(self, service):
        self.service = service
        self.wake = Event()
        self._lock = Lock()
        self._pending = {lane: {} for lane in self.LANES}
        self._pending_keys = {}
        self._totals = {lane: dict(completed=0, succeeded=0, failed=0, last_finished_at=None) for lane in self.LANES}
        self._failures = deque(maxlen=200)
        self._transport_results = deque(maxlen=32)
        self._protection_until = 0

    def snapshot(self):
        with self._lock:
            return {lane: {**self._totals[lane], "pending": len(self._pending[lane]),
                "protection_until": self._protection_until,
                "protection_reason": "repeated_transport_errors" if self._protection_until > time.time() else None}
                    for lane in self.LANES}

    def failures(self, offset=0, limit=20):
        with self._lock:
            rows = list(reversed(self._failures))
            return {"round": 0, "kind": "pool", "total": sum(v["failed"] for v in self._totals.values()),
                    "retained": len(rows), "max_retained": 200, "offset": offset, "limit": limit,
                    "items": [dict(row) for row in rows[offset:offset + limit]]}

    def _observe_transport_result(self, error):
        text = str(error or "").lower()
        transport = bool(error) and (isinstance(error, (TimeoutError, ConnectionError)) or
            any(word in text for word in ("tls", "ssl", "connection", "timed out", "timeout", "could not resolve")) or
            bool(re.search(r"http\s+5\d\d", text)))
        self._transport_results.append(transport)
        if len(self._transport_results) >= 16 and sum(self._transport_results) >= 0.8 * len(self._transport_results):
            # 只在明确的传输/上游5xx故障潮中暂停维护，不降低正常C、不阻塞前台。
            self._protection_until = time.time() + 10
            self._transport_results.clear()

    def tick(self):
        with self._lock:
            # 使用提交时的稳定身份，AT轮换后也不会被另一维护通道重复领取。
            # 本tick已完成的身份也保留到tick结束，避免跨通道重复结算。
            claimed = set(self._pending_keys.values())
            tracked_futures = set(self._pending_keys)
        for lane in self.LANES:
            with self._lock:
                done = [(f, token) for f, token in self._pending[lane].items() if f.done()]
            for future, token in done:
                error = None
                try:
                    account = future.result()
                    if account is None:
                        raise RuntimeError("账号已移除或未返回刷新信息")
                except Exception as exc:
                    error = exc
                try:
                    self.service.maintenance_finished(token, lane, failed=error is not None)
                except Exception as exc:
                    error = exc
                with self._lock:
                    self._pending[lane].pop(future, None)
                    self._pending_keys.pop(future, None)
                    counts = self._totals[lane]
                    counts["completed"] += 1
                    counts["failed" if error else "succeeded"] += 1
                    counts["last_finished_at"] = time.time()
                    self._observe_transport_result(error)
                    if error:
                        self._failures.append({**refresh_failure_item(token,
                            self.service.get_account(token), str(error)), "lane": lane})
            with self._lock:
                pending_count = len(self._pending[lane])
            capacity = max(0, config.image_account_verification["workers"] - pending_count)
            if self._protection_until > time.time():
                capacity = 0
            for token in self.service.maintenance_candidates(lane, capacity):
                account = self.service.get_account(token) or {}
                key = str(account.get("pool_account_id") or token)
                if key in claimed:
                    continue
                try:
                    future = self.service.verification.submit(key,
                        lambda token=token, lane=lane: self.service.maintain_account(token, lane), lane=lane)
                except Exception:
                    self.service.maintenance_finished(token, lane, failed=True)
                    continue
                if future is None:
                    self.service.maintenance_finished(token, lane, failed=True)
                    continue
                if future in tracked_futures:
                    claimed.add(key)
                    continue
                with self._lock:
                    self._pending[lane][future] = token
                    self._pending_keys[future] = key
                claimed.add(key)
                tracked_futures.add(future)
                future.add_done_callback(lambda _: self.wake.set())

    def run(self, stop_event):
        while not stop_event.is_set():
            self.wake.clear()
            try:
                self.tick()
            except Exception as exc:
                # 运行日志不输出账号或原始异常内容。
                logger.warning({"event": "account_pool_scheduler_error", "error_type": type(exc).__name__})
            self.wake.wait(timeout=0.5)
        # 已领取任务排空；应用随后关闭verification，不能留下未结算维护状态。
        while any(self._pending.values()):
            for lane in self.LANES:
                for future, token in list(self._pending[lane].items()):
                    try:
                        future.result()
                        failed = False
                    except Exception:
                        failed = True
                    self.service.maintenance_finished(token, lane, failed=failed)
                    with self._lock:
                        self._pending[lane].pop(future, None)
                        self._pending_keys.pop(future, None)


pool_maintenance = AccountPoolMaintenance(account_service)
