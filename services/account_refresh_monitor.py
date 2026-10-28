"""后台巡检轻量快照及有界的最近一轮失败明细，不保存凭证。"""
from __future__ import annotations

import time
import hashlib
import json
import math
import re
from pathlib import Path
from threading import Lock

DEFAULT_STATE_PATH = Path(__file__).resolve().parents[1] / "data" / "account_refresh_schedule.json"


def refresh_failure_item(token, account, error):
    """只保留管理员诊断所需字段；错误中的已知凭证及常见 token 脱敏。"""
    account = account or {}
    secrets = [token, *(account.get(key) for key in
               ("access_token", "refresh_token", "id_token", "password", "proxy"))]
    def redact(value):
        text = str(value or "")[:16000]
        for secret in sorted({str(s) for s in secrets if s}, key=len, reverse=True):
            if len(secret) >= 6 or secret == str(account.get("password") or "") or text == secret:
                text = text.replace(secret, "[REDACTED]")
        text = re.sub(r"(?i)Bearer\s+[^\s,;\"']+", "Bearer [REDACTED]", text)
        text = re.sub(r"(?i)((?:access_token|refresh_token|id_token|password|authorization|api[_-]?key)[\"']?\s*[:=]\s*)[\"']?[^\s,;}\"']+", r"\1[REDACTED]", text)
        text = re.sub(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?", "[REDACTED]", text)
        text = re.sub(r"\b(?:sk-|rt-)[A-Za-z0-9_-]{8,}", "[REDACTED]", text)
        text = re.sub(r"(https?://)[^/\s@]+@", r"\1[REDACTED]@", text)
        text = re.sub(r"(https?://[^\s?]+)\?[^\s]+", r"\1?[REDACTED]", text)
        return text[:1024]
    identity = account.get("email") or "账号 #" + hashlib.sha256(str(token).encode()).hexdigest()[:12]
    return dict(account=redact(identity)[:254], error=redact(error) or "刷新未返回账号信息（账号可能已被移除）", time=time.time())


class AccountRefreshMonitor:
    MAX_FAILURES = 10000

    def __init__(self, state_path: Path | str | None = None, clock=None):
        self._lock = Lock()
        self._clock = clock or time.time
        self._state_path = Path(state_path) if state_path else None
        self._last_full_refresh_finished_at = None
        self._next_full_refresh_at = None
        self._load_schedule()
        self._round = 0
        self._state = self._empty()
        self._state.update(
            last_full_refresh_finished_at=self._last_full_refresh_finished_at,
            next_full_refresh_at=self._next_full_refresh_at,
            next_run_at=self._next_full_refresh_at,
        )
        self._failures = {"refresh": [], "keepalive": []}
        self._failure_count = 0

    def _load_schedule(self):
        if self._state_path is None or not self._state_path.is_file():
            return
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        if not isinstance(data, dict):
            return
        for key, attr in (("last_full_refresh_finished_at", "_last_full_refresh_finished_at"),
                          ("next_full_refresh_at", "_next_full_refresh_at")):
            try:
                value = float(data.get(key))
            except (TypeError, ValueError):
                continue
            if math.isfinite(value) and value > 0:
                setattr(self, attr, value)

    def _persist_schedule_locked(self):
        if self._state_path is None:
            return
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_full_refresh_finished_at": self._last_full_refresh_finished_at,
            "next_full_refresh_at": self._next_full_refresh_at,
        }
        temporary = self._state_path.with_name(self._state_path.name + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temporary.replace(self._state_path)

    def _ensure_schedule_locked(self, interval_seconds, anchor=None):
        if self._next_full_refresh_at is None and self._last_full_refresh_finished_at is not None:
            self._next_full_refresh_at = self._last_full_refresh_finished_at + max(0, interval_seconds)
            self._persist_schedule_locked()
        if self._next_full_refresh_at is None and anchor is not None:
            self._next_full_refresh_at = anchor + max(0, interval_seconds)
            self._persist_schedule_locked()
        return self._next_full_refresh_at

    def next_full_refresh_at(self, interval_seconds):
        with self._lock:
            return self._ensure_schedule_locked(interval_seconds)

    def full_refresh_due(self, interval_seconds, now=None):
        scheduled_at = self.next_full_refresh_at(interval_seconds)
        return scheduled_at is not None and (self._clock() if now is None else now) >= scheduled_at

    @staticmethod
    def _empty():
        return dict(status="idle", phase="idle", started_at=None, finished_at=None,
                    last_success_at=None, last_full_refresh_finished_at=None,
                    next_full_refresh_at=None, next_run_at=None, interval_seconds=0,
                    error=None, refresh=dict(total=0, completed=0, succeeded=0, failed=0, active=0),
                    keepalive=dict(total=0, completed=0, succeeded=0, failed=0, active=0))

    def start(self, interval_seconds):
        with self._lock:
            self._round += 1
            last_success = self._state["last_success_at"]
            next_full_refresh_at = self._ensure_schedule_locked(interval_seconds)
            self._state = self._empty()
            self._failures = {"refresh": [], "keepalive": []}
            self._failure_count = 0
            now = self._clock()
            self._state.update(
                status="running", phase="preparing", started_at=now,
                interval_seconds=interval_seconds, last_success_at=last_success,
                last_full_refresh_finished_at=self._last_full_refresh_finished_at,
                next_full_refresh_at=next_full_refresh_at, next_run_at=next_full_refresh_at,
            )
            return self._round

    def plan(self, round_id, refresh, keepalive):
        with self._lock:
            if round_id == self._round:
                self._state["refresh"]["total"] = refresh
                self._state["keepalive"]["total"] = keepalive

    def set_phase(self, round_id, phase):
        with self._lock:
            if round_id == self._round:
                self._state["phase"] = phase

    def update(self, round_id, kind, event, success=None):
        with self._lock:
            if round_id != self._round or self._state["status"] != "running":
                return
            counts = self._state[kind]
            if event == "started":
                counts["active"] += 1
            elif event == "finished":
                counts["active"] -= 1
                counts["completed"] += 1
                counts["succeeded" if success else "failed"] += 1
                if success:
                    self._state["last_success_at"] = self._clock()

    def add_failure(self, round_id, kind, item):
        with self._lock:
            if round_id == self._round and self._state["status"] == "running" and self._failure_count < self.MAX_FAILURES:
                # 仅保存安全字段；普通快照和生图热路径不复制失败明细。
                self._failures[kind].append({key: item[key] for key in ("account", "error", "time")})
                self._failure_count += 1

    def failures(self, round_id, kind, offset=0, limit=20):
        with self._lock:
            if round_id != self._round:
                raise ValueError("后台刷新已进入新一轮，请关闭弹窗并重新打开")
            rows = self._failures[kind]
            return dict(round=self._round, kind=kind, total=self._state[kind]["failed"],
                        retained=len(rows), max_retained=self.MAX_FAILURES,
                        offset=offset, limit=limit, items=[dict(item) for item in rows[offset:offset + limit]])

    def finish(self, round_id, error=None, *, full_refresh=True):
        with self._lock:
            if round_id != self._round:
                return
            state = self._state
            groups = (state["refresh"], state["keepalive"])
            incomplete = any(g["completed"] != g["total"] or g["active"] for g in groups)
            state["status"] = ("failed" if error or incomplete else
                               "completed_with_errors" if any(g["failed"] for g in groups) else "completed")
            state["error"] = error or ("刷新轮次未完整执行" if incomplete else None)
            state["phase"] = "waiting"
            state["finished_at"] = self._clock()
            if full_refresh and not error and not incomplete:
                self._last_full_refresh_finished_at = state["finished_at"]
                self._next_full_refresh_at = state["finished_at"] + state["interval_seconds"]
                self._persist_schedule_locked()
            elif not full_refresh:
                self._ensure_schedule_locked(state["interval_seconds"], state["finished_at"])
            state["last_full_refresh_finished_at"] = self._last_full_refresh_finished_at
            state["next_full_refresh_at"] = self._next_full_refresh_at
            state["next_run_at"] = self._next_full_refresh_at or (
                state["finished_at"] + state["interval_seconds"]
            )

    def snapshot(self):
        with self._lock:
            state = dict(self._state)
            for kind in ("refresh", "keepalive"):
                counts = state[kind] = dict(state[kind])
                counts["pending"] = max(0, counts["total"] - counts["completed"] - counts["active"])
            state["round"] = self._round
        started = state["started_at"]
        state["elapsed_seconds"] = max(0, (state["finished_at"] or self._clock()) - started) if started else 0
        return state


account_refresh_monitor = AccountRefreshMonitor(DEFAULT_STATE_PATH)
