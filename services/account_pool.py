from __future__ import annotations

import hashlib
import math
import time
from datetime import datetime
from threading import Lock


def timestamp(value):
    if not value:
        return 0.0
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError, OverflowError):
            return 0.0
    return result if math.isfinite(result) and result > 0 else 0.0


def availability_time(account):
    confirmed = max(timestamp(account.get(key)) for key in (
        "remote_verified_at", "last_image_success_at", "last_policy_confirmed_at"))
    if confirmed <= timestamp(account.get("verification_invalidated_at")):
        return 0.0
    return confirmed


class DeadlineQueue:
    """可更新/删除的最小堆；每账号恰好一个条目，不积累成功热路径的墓碑。"""
    def __init__(self):
        self.heap = []
        self.positions = {}

    def __len__(self):
        return len(self.heap)

    def _swap(self, a, b):
        self.heap[a], self.heap[b] = self.heap[b], self.heap[a]
        self.positions[self.heap[a][1]] = a
        self.positions[self.heap[b][1]] = b

    def _repair(self, i):
        while i and self.heap[i] < self.heap[(i - 1) // 2]:
            parent = (i - 1) // 2
            self._swap(i, parent)
            i = parent
        while 2 * i + 1 < len(self.heap):
            child = 2 * i + 1
            if child + 1 < len(self.heap) and self.heap[child + 1] < self.heap[child]:
                child += 1
            if self.heap[i] <= self.heap[child]:
                break
            self._swap(i, child)
            i = child

    def set(self, key, deadline):
        if key in self.positions:
            i = self.positions[key]
            if self.heap[i][0] == deadline:
                return
            self.heap[i] = (deadline, key)
            self._repair(i)
        else:
            self.positions[key] = len(self.heap)
            self.heap.append((deadline, key))
            self._repair(len(self.heap) - 1)

    def remove(self, key):
        i = self.positions.pop(key, None)
        if i is None:
            return
        last = self.heap.pop()
        if i < len(self.heap):
            self.heap[i] = last
            self.positions[last[1]] = i
            self._repair(i)

    def peek(self):
        return self.heap[0] if self.heap else None

    def pop_due(self, now):
        if self.heap and self.heap[0][0] <= now:
            deadline, key = self.heap[0]
            self.remove(key)
            return key
        return None


class AccountPoolIndex:
    """由AccountService的账号锁保护；仅增量更新，不在请求中全池扫描。"""
    def __init__(self, settings):
        self.settings = settings
        self.started_at = time.time()
        self.hot = set()
        self.idle_hot = set()
        self.entries = {}
        self.due = {"hot_maintenance": DeadlineQueue(), "cold_maintenance": DeadlineQueue()}
        self.idle = DeadlineQueue()
        self.fill = DeadlineQueue()
        self.cooldown = DeadlineQueue()
        self.expanded = self.shrunk = self.evicted = 0
        self.last_reason = "startup"

    def remove(self, token):
        if token in self.hot:
            self.evicted += 1
        self.hot.discard(token)
        self.idle_hot.discard(token)
        self.entries.pop(token, None)
        self.idle.remove(token)
        self.fill.remove(token)
        self.cooldown.remove(token)
        for queue in self.due.values():
            queue.remove(token)

    def sync(self, token, account, *, available, inflight, concurrency):
        if not account:
            self.remove(token)
            return
        settings = self.settings()
        retry_at = timestamp(account.get("pool_retry_at"))
        if retry_at > time.time():
            self.cooldown.set(token, retry_at)
        else:
            self.cooldown.remove(token)
        hot = bool(account.get("image_pool_hot"))
        if token in self.hot and not hot:
            if not available:
                self.evicted += 1
                self.last_reason = "unavailable_exit"
        if hot:
            self.hot.add(token)
        else:
            self.hot.discard(token)
        if hot and available and retry_at <= time.time() and inflight < min(concurrency, int(account.get("quota") or 0)):
            self.idle_hot.add(token)
        else:
            self.idle_hot.discard(token)
        identity = str(account.get("pool_account_id") or token)
        spread = int(hashlib.blake2s(identity.encode(), digest_size=4).hexdigest(), 16) / 2**32
        interval = settings["hot_refresh_interval_secs" if hot else "cold_refresh_interval_secs"]
        confirmed = availability_time(account)
        # 没有历史确认的冷账号在一个冷池周期内均匀分散，避免启动刷新风暴。
        due = confirmed + interval * (1 + spread * 0.1) if confirmed else (
            (timestamp(account.get("created_at")) or self.started_at) + spread * interval
            if not hot else self.started_at)
        due = max(due, timestamp(account.get("pool_retry_at")))
        lane = "hot_maintenance" if hot else "cold_maintenance"
        previous = self.entries.get(token)
        state = (lane, due, account.get("status"), identity)
        if state != previous:
            self.due["cold_maintenance" if hot else "hot_maintenance"].remove(token)
            if account.get("status") != "禁用":
                self.due[lane].set(token, due)
            else:
                self.due[lane].remove(token)
            self.entries[token] = state
        if hot:
            last_use = max(timestamp(account.get("last_image_started_at")), timestamp(account.get("hot_entered_at")))
            self.idle.set(token, last_use + settings["hot_idle_secs"])
            self.fill.remove(token)
        else:
            self.idle.remove(token)
            if available and timestamp(account.get("pool_retry_at")) <= time.time():
                # 高分优先，稳定散列错峰；后台补位不改变实际使用时间。
                self.fill.set(token, -int(account.get("image_score", 50)) + spread)
            else:
                self.fill.remove(token)

    def reschedule(self, token, lane, deadline):
        # 使用独立队列值保留已取出的条目；不修改账号真实确认时间。
        self.due[lane].set(token, deadline)

    def snapshot(self):
        now = time.time()
        return {"base_size": self.settings()["hot_pool_size"], "size": len(self.hot),
                "idle_accounts": len(self.idle_hot),
                "deficit": max(0, self.settings()["hot_pool_size"] - len(self.hot)),
                "cooldown_accounts": len(self.cooldown), "expanded": self.expanded, "shrunk": self.shrunk, "evicted": self.evicted,
                "last_reason": self.last_reason,
                "maintenance": {lane: {"scheduled": len(q), "next_due_at": q.peek()[0] if q.peek() else None,
                    "oldest_overdue_secs": max(0, now - q.peek()[0]) if q.peek() else 0}
                    for lane, q in self.due.items()}}


class ImageAttemptBudget:
    """每张图共享预检与生成的账号预算，不让各重试层相乘。"""
    def __init__(self, limit):
        self.limit = max(1, int(limit))
        self.identities = set()
        self.tokens = set()

    def consume(self, account, token):
        identity = str(account.get("pool_account_id") or token)
        self.tokens.add(token)
        if identity in self.identities:
            return False
        if len(self.identities) >= self.limit:
            raise RuntimeError("image account attempt budget exhausted")
        self.identities.add(identity)
        return True


class CachedAccountInfo(dict):
    """维护因近期活性而跳过查询的结果标记，仅内存类型，不增加API字段。"""


class ImageAttemptResults:
    """按单张图统计当前进程的首尝试/切号结果，不扫描历史日志。"""
    def __init__(self):
        self.lock = Lock()
        self.counts = dict(succeeded=0, failed=0, policy_rejected=0,
                           first_attempt_success=0, switched_success=0, switched_requests=0)

    def record(self, *, success, policy, attempts):
        with self.lock:
            if policy:
                self.counts["policy_rejected"] += 1
                return
            self.counts["succeeded" if success else "failed"] += 1
            self.counts["switched_requests"] += int(attempts > 1)
            if success:
                self.counts["first_attempt_success" if attempts <= 1 else "switched_success"] += 1

    def snapshot(self):
        with self.lock:
            result = dict(self.counts)
        total = result["succeeded"] + result["failed"]
        result["total"] = total
        result["success_rate"] = round(result["succeeded"] * 100 / total, 2) if total else 0
        result["first_attempt_success_rate"] = round(result["first_attempt_success"] * 100 / total, 2) if total else 0
        result["switched_success_rate"] = round(result["switched_success"] * 100 / result["switched_requests"], 2) if result["switched_requests"] else 0
        return result


image_attempt_results = ImageAttemptResults()
