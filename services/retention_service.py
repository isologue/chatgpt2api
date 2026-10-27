from __future__ import annotations

from datetime import datetime
from threading import Event, RLock, Thread
import time

from services.config import config
from services.retention_settings import KINDS, MAX_HOURS
from utils.log import logger


class CleanupBusyError(RuntimeError):
    pass


class RetentionService:
    """每种数据最多一个清理任务；手动与定时任务共用执行状态。"""
    def __init__(self, config_store=config, *, handlers=None, clock=time.time):
        self.config = config_store
        self.handlers = handlers
        self.clock = clock
        self._lock = RLock()
        self._policies = {}
        self._states = {kind: {"running": False, "last_started_at": None, "last_finished_at": None,
                              "last_result": None, "last_error": None, "next_run_at": None} for kind in KINDS}

    @staticmethod
    def _date(value):
        return datetime.fromtimestamp(value).isoformat(timespec="seconds")

    def _sync_policy(self, kind):
        policy = self.config.get_retention_settings()[kind]
        state = self._states[kind]
        if self._policies.get(kind) != policy:
            self._policies[kind] = dict(policy)
            state["next_run_at"] = self.clock() + policy["interval_minutes"] * 60 if policy["enabled"] else None
        return policy

    def snapshot(self, kind):
        if kind not in KINDS:
            raise ValueError("unknown retention kind")
        with self._lock:
            policy = self._sync_policy(kind)
            state = dict(self._states[kind])
            if state["next_run_at"] is not None:
                state["next_run_at"] = self._date(state["next_run_at"])
            return {"policy": dict(policy), **state}

    def update(self, kind, policy):
        with self._lock:
            settings = self.config.get_retention_settings()
            settings[kind] = policy
            self.config.update({"data_retention": settings})
            return self.snapshot(kind)

    def run(self, kind, retention_hours, *, automatic=False):
        if kind not in KINDS or isinstance(retention_hours, bool) or not isinstance(retention_hours, int) or not 1 <= retention_hours <= MAX_HOURS:
            raise ValueError("invalid cleanup retention hours")
        with self._lock:
            policy = self._sync_policy(kind)
            state = self._states[kind]
            if state["running"]:
                raise CleanupBusyError("已有清理任务正在执行")
            now = self.clock()
            cutoff = now - retention_hours * 3600
            state.update(running=True, last_started_at=self._date(now), last_finished_at=None,
                         last_result=None, last_error=None, source="automatic" if automatic else "manual",
                         retention_hours=retention_hours, cutoff=self._date(cutoff))
            state["next_run_at"] = now + policy["interval_minutes"] * 60 if policy["enabled"] else None
            worker = Thread(target=self._execute, args=(kind, cutoff), daemon=True, name=f"{kind}-retention")
            try:
                worker.start()
            except Exception:
                state["running"] = False
                raise
            return self.snapshot(kind)

    def _execute(self, kind, cutoff):
        try:
            if self.handlers is not None:
                handler = self.handlers[kind]
            elif kind == "images":
                from services.image_service import cleanup_expired_images
                handler = cleanup_expired_images
            else:
                from services.log_service import log_service
                handler = log_service.cleanup_before
            result = handler(cutoff)
            with self._lock:
                self._states[kind]["last_result"] = result
            logger.info({"event": "data_retention_completed", "kind": kind, **result})
        except Exception as exc:
            with self._lock:
                self._states[kind]["last_error"] = str(exc)
            logger.error({"event": "data_retention_failed", "kind": kind, "error": str(exc)})
        finally:
            with self._lock:
                self._states[kind].update(running=False, last_finished_at=self._date(self.clock()))

    def tick(self):
        for kind in KINDS:
            with self._lock:
                policy = self._sync_policy(kind)
                state = self._states[kind]
                due = policy["enabled"] and not state["running"] and state["next_run_at"] <= self.clock()
                if due:
                    self.run(kind, policy["retention_hours"], automatic=True)

    def start_scheduler(self, stop_event: Event) -> Thread:
        def watch():
            while not stop_event.is_set():
                try:
                    self.tick()
                except Exception as exc:
                    logger.error({"event": "data_retention_scheduler_failed", "error": str(exc)})
                stop_event.wait(5)
        thread = Thread(target=watch, daemon=True, name="data-retention")
        thread.start()
        return thread


retention_service = RetentionService()
