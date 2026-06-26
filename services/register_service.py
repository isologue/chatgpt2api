from __future__ import annotations

import json
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.account_service import account_service
from services.config import DATA_DIR
from services.register import openai_register


REGISTER_FILE = DATA_DIR / "register.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _after_minutes(minutes: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def _parse_time(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _default_config() -> dict:
    return {
        **openai_register.config,
        "mode": "total",
        "target_quota": 100,
        "target_available": 10,
        "check_interval": 5,
        "enabled": False,
        "schedule_enabled": False,
        "schedule_interval_minutes": 60,
        "schedule_started_at": None,
        "last_scheduled_at": None,
        "next_scheduled_at": None,
        "stats": {
            "success": 0,
            "fail": 0,
            "done": 0,
            "running": 0,
            "threads": openai_register.config["threads"],
            "elapsed_seconds": 0,
            "avg_seconds": 0,
            "success_rate": 0,
            "current_quota": 0,
            "current_available": 0,
        },
    }


def _normalize(raw: dict) -> dict:
    cfg = _default_config()
    cfg.update({k: v for k, v in raw.items() if k not in {"stats", "logs"}})
    cfg["total"] = max(1, int(cfg.get("total") or 1))
    cfg["threads"] = max(1, int(cfg.get("threads") or 1))
    cfg["mode"] = str(cfg.get("mode") or "total").strip() if str(cfg.get("mode") or "total").strip() in {"total", "quota", "available"} else "total"
    cfg["target_quota"] = max(1, int(cfg.get("target_quota") or 1))
    cfg["target_available"] = max(1, int(cfg.get("target_available") or 1))
    cfg["check_interval"] = max(1, int(cfg.get("check_interval") or 5))
    cfg["proxy"] = str(cfg.get("proxy") or "").strip()
    if isinstance(cfg.get("mail"), dict):
        cfg["mail"] = {**cfg["mail"], "proxy": cfg["proxy"]}
    cfg["enabled"] = bool(cfg.get("enabled"))
    cfg["schedule_enabled"] = bool(cfg.get("schedule_enabled"))
    cfg["schedule_interval_minutes"] = max(1, int(cfg.get("schedule_interval_minutes") or 60))
    cfg["schedule_started_at"] = str(cfg.get("schedule_started_at") or "").strip() or None
    cfg["last_scheduled_at"] = str(cfg.get("last_scheduled_at") or "").strip() or None
    cfg["next_scheduled_at"] = str(cfg.get("next_scheduled_at") or "").strip() or None
    stats = {
        **_default_config()["stats"],
        **(raw.get("stats") if isinstance(raw.get("stats"), dict) else {}),
        "threads": cfg["threads"],
    }
    cfg["stats"] = stats
    return cfg


class RegisterService:
    def __init__(self, store_file: Path):
        self._store_file = store_file
        self._lock = threading.RLock()
        self._runner: threading.Thread | None = None
        self._scheduler: threading.Thread | None = None
        self._scheduler_wakeup = threading.Event()
        self._logs: list[dict] = []
        openai_register.register_log_sink = self._append_log
        self._config = self._load()
        if self._config["schedule_enabled"]:
            with self._lock:
                self._reset_schedule_anchor_locked()
                self._save()
            self._ensure_scheduler()
        if self._config["enabled"]:
            self.start()

    def _load(self) -> dict:
        try:
            return _normalize(json.loads(self._store_file.read_text(encoding="utf-8")))
        except Exception:
            return _normalize({})

    def _save(self) -> None:
        self._store_file.parent.mkdir(parents=True, exist_ok=True)
        self._store_file.write_text(json.dumps(self._config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def get(self) -> dict:
        with self._lock:
            return json.loads(json.dumps({**self._config, "logs": self._logs[-300:]}, ensure_ascii=False))

    def _inject_proxy_to_mail(self) -> None:
        proxy = str(self._config.get("proxy") or "").strip()
        if isinstance(self._config.get("mail"), dict):
            self._config["mail"]["proxy"] = proxy

    def _reset_schedule_anchor_locked(self) -> None:
        self._config["schedule_started_at"] = _now()
        self._config["next_scheduled_at"] = _after_minutes(int(self._config.get("schedule_interval_minutes") or 60))

    def _ensure_scheduler(self) -> None:
        if self._scheduler and self._scheduler.is_alive():
            return
        self._scheduler = threading.Thread(target=self._schedule_loop, daemon=True, name="openai-register-scheduler")
        self._scheduler.start()

    def update(self, updates: dict) -> dict:
        schedule_message = ""
        with self._lock:
            previous = self._config
            self._config = _normalize({**self._config, **updates})
            self._inject_proxy_to_mail()
            openai_register.config.update({k: self._config[k] for k in ("mail", "proxy", "total", "threads")})
            schedule_enabled_changed = bool(previous.get("schedule_enabled")) != bool(self._config.get("schedule_enabled"))
            schedule_interval_changed = int(previous.get("schedule_interval_minutes") or 60) != int(self._config.get("schedule_interval_minutes") or 60)
            if self._config["schedule_enabled"]:
                self._reset_schedule_anchor_locked()
                if schedule_enabled_changed:
                    schedule_message = f"Register scheduler enabled. First run in {self._config['schedule_interval_minutes']} minute(s)."
                elif schedule_interval_changed:
                    schedule_message = f"Register scheduler interval updated to {self._config['schedule_interval_minutes']} minute(s). Countdown restarted."
            else:
                self._config["next_scheduled_at"] = None
                if schedule_enabled_changed:
                    schedule_message = "Register scheduler disabled."
            self._save()
            result = self.get()
        if self._config["schedule_enabled"]:
            self._ensure_scheduler()
        self._scheduler_wakeup.set()
        if schedule_message:
            self._append_log(schedule_message, "yellow")
        return result

    def start(self, trigger: str = "manual") -> dict:
        with self._lock:
            if self._runner and self._runner.is_alive():
                self._config["enabled"] = True
                self._save()
                return self.get()
            self._config["enabled"] = True
            self._inject_proxy_to_mail()
            if trigger != "schedule":
                self._logs = []
            metrics = self._pool_metrics()
            self._config["stats"] = {
                "job_id": uuid.uuid4().hex,
                "success": 0,
                "fail": 0,
                "done": 0,
                "running": 0,
                "threads": self._config["threads"],
                **metrics,
                "started_at": _now(),
                "updated_at": _now(),
            }
            openai_register.config.update({k: self._config[k] for k in ("mail", "proxy", "total", "threads")})
            with openai_register.stats_lock:
                openai_register.stats.update({"done": 0, "success": 0, "fail": 0, "start_time": time.time()})
            self._save()
            self._runner = threading.Thread(target=self._run, daemon=True, name="openai-register")
            self._runner.start()
            self._append_log(f"Register job started ({trigger}), mode={self._config['mode']}, threads={self._config['threads']}", "yellow")
            return self.get()

    def stop(self) -> dict:
        with self._lock:
            self._config["enabled"] = False
            self._config["stats"]["updated_at"] = _now()
            self._save()
            self._append_log("Register stop requested. Waiting for running workers to finish.", "yellow")
            return self.get()

    def reset(self) -> dict:
        with self._lock:
            self._logs = []
            self._config["stats"] = {
                "success": 0,
                "fail": 0,
                "done": 0,
                "running": 0,
                "threads": self._config["threads"],
                "elapsed_seconds": 0,
                "avg_seconds": 0,
                "success_rate": 0,
                **self._pool_metrics(),
                "updated_at": _now(),
            }
            with openai_register.stats_lock:
                openai_register.stats.update({"done": 0, "success": 0, "fail": 0, "start_time": 0.0})
            self._save()
            return self.get()

    def _append_log(self, text: str, color: str = "") -> None:
        with self._lock:
            self._logs.append({"time": _now(), "text": str(text), "level": str(color or "info")})
            self._logs = self._logs[-300:]

    def _pool_metrics(self) -> dict:
        items = account_service.list_accounts()
        normal = [item for item in items if item.get("status") == "正常"]
        return {
            "current_quota": sum(int(item.get("quota") or 0) for item in normal if not item.get("image_quota_unknown")),
            "current_available": len(normal),
        }

    def _target_reached(self, cfg: dict, submitted: int) -> bool:
        mode = str(cfg.get("mode") or "total")
        metrics = self._pool_metrics()
        self._bump(**metrics)
        if mode == "quota":
            reached = metrics["current_quota"] >= int(cfg.get("target_quota") or 1)
            self._append_log(
                f"Quota check: available={metrics['current_available']}, quota={metrics['current_quota']}, target={cfg.get('target_quota')}, action={'skip' if reached else 'continue'}",
                "yellow",
            )
            return reached
        if mode == "available":
            reached = metrics["current_available"] >= int(cfg.get("target_available") or 1)
            self._append_log(
                f"Available check: available={metrics['current_available']}, target={cfg.get('target_available')}, quota={metrics['current_quota']}, action={'skip' if reached else 'continue'}",
                "yellow",
            )
            return reached
        return submitted >= int(cfg.get("total") or 1)

    def _bump(self, **updates) -> None:
        with self._lock:
            self._config["stats"].update(updates)
            stats = self._config["stats"]
            started_at = str(stats.get("started_at") or "")
            if started_at:
                try:
                    elapsed = max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(started_at)).total_seconds())
                except Exception:
                    elapsed = 0.0
                success = int(stats.get("success") or 0)
                fail = int(stats.get("fail") or 0)
                stats["elapsed_seconds"] = round(elapsed, 1)
                stats["avg_seconds"] = round(elapsed / success, 1) if success else 0
                stats["success_rate"] = round(success * 100 / max(1, success + fail), 1)
            self._config["stats"]["updated_at"] = _now()
            self._save()

    def _run(self) -> None:
        threads = int(self.get()["threads"])
        submitted, done, success, fail = 0, 0, 0, 0
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = set()
            while True:
                cfg = self.get()
                while self.get()["enabled"] and not self._target_reached(cfg, submitted) and len(futures) < threads:
                    submitted += 1
                    futures.add(executor.submit(openai_register.worker, submitted))
                self._bump(running=len(futures), done=done, success=success, fail=fail)
                if not futures and (not self.get()["enabled"] or str(cfg.get("mode") or "total") == "total"):
                    break
                if not futures:
                    time.sleep(max(1, int(cfg.get("check_interval") or 5)))
                    continue
                finished, futures = wait(futures, return_when=FIRST_COMPLETED)
                for future in finished:
                    done += 1
                    try:
                        result = future.result()
                        success += 1 if result.get("ok") else 0
                        fail += 0 if result.get("ok") else 1
                    except Exception:
                        fail += 1
        self._bump(running=0, done=done, success=success, fail=fail, finished_at=_now())
        with self._lock:
            self._config["enabled"] = False
            self._save()
        self._append_log(f"Register job finished, success={success}, fail={fail}", "yellow")

    def _schedule_loop(self) -> None:
        while True:
            timeout = 60.0
            with self._lock:
                if self._config["schedule_enabled"]:
                    next_run = _parse_time(self._config.get("next_scheduled_at"))
                    if next_run is None:
                        self._reset_schedule_anchor_locked()
                        self._save()
                    else:
                        timeout = max(0.5, min(60.0, (next_run - datetime.now(timezone.utc)).total_seconds()))
            if self._scheduler_wakeup.wait(timeout=max(0.5, timeout)):
                self._scheduler_wakeup.clear()
                continue
            self._scheduler_wakeup.clear()

            should_start = False
            log_message = ""
            with self._lock:
                if not self._config["schedule_enabled"]:
                    continue
                next_run = _parse_time(self._config.get("next_scheduled_at"))
                if next_run is None or next_run > datetime.now(timezone.utc):
                    continue
                interval = int(self._config.get("schedule_interval_minutes") or 60)
                self._config["next_scheduled_at"] = _after_minutes(interval)
                if self._runner and self._runner.is_alive():
                    self._save()
                    log_message = f"Scheduled trigger skipped because a register job is still running. Next run in {interval} minute(s)."
                else:
                    self._config["last_scheduled_at"] = _now()
                    self._save()
                    should_start = True
                    log_message = f"Scheduled trigger fired. Next run in {interval} minute(s)."
            if log_message:
                self._append_log(log_message, "yellow")
            if should_start:
                self.start(trigger="schedule")


register_service = RegisterService(REGISTER_FILE)
