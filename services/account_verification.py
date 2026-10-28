from __future__ import annotations

import time
from concurrent.futures import Future
from threading import Condition

from services.image_metrics import phase
from services.thread_pool import _ResizableThreadPool


class VerificationBackoff(RuntimeError):
    pass


class AccountVerification:
    """按账号合并校验；前后台共享并发、启动速率与失败退避。"""

    def __init__(self, settings):
        self._settings = settings
        self._condition = Condition()
        self._flights = {}
        self._retry = {}
        # 前台(生图选号校验)与后台(全量刷新)各自独立的在途计数与启动速率闸门。
        # 否则后台全量刷新会占满共享的并发/速率额度，把前台生图校验挤进长队列
        # (压测实测 account_verification_queue 尾部排队到 ~100s)。
        self._active_fg = self._active_bg = self._waiting = self._background = 0
        self._next_start_fg = self._next_start_bg = 0.0
        self._closed = False
        self._hits = self._stale_hits = self._misses = self._joined = 0
        self._completed = self._failed = self._rejected = 0
        self._pool = _ResizableThreadPool(settings()["workers"], "account-refresh")

    @staticmethod
    def _background_workers(settings) -> int:
        # 后台全量刷新只用一部分并发额度，为前台生图校验让出主路；前台独享完整 workers。
        return max(1, settings["workers"] // 2)

    def reconfigure(self):
        if self._closed:
            self._pool = _ResizableThreadPool(self._settings()["workers"], "account-refresh")
            self._closed = False
        else:
            self._pool.reconfigure(self._settings()["workers"])
        with self._condition:
            self._condition.notify_all()

    def cache_hit(self, *, stale=False):
        with self._condition:
            self._hits += 1
            self._stale_hits += int(stale)

    def run(self, key, handler, *, background=False):
        with self._condition:
            if self._closed:
                raise RuntimeError("account verification is closed")
            future = self._flights.get(key)
            if future is not None:
                self._joined += int(not background)
                if background:
                    return True
                owner = False
            else:
                retry = self._retry.get(key)
                if retry is not None and retry[0] > time.monotonic():
                    if background:
                        return False
                    raise VerificationBackoff("account verification retry deferred")
                settings = self._settings()
                if background and self._background >= settings["workers"] + settings["queue_size"]:
                    self._rejected += 1
                    return False
                future = Future()
                self._flights[key] = future
                self._misses += int(not background)
                self._waiting += 1
                owner = True
                if background:
                    self._background += 1
        if background:
            try:
                self._pool.submit(self._execute, key, future, handler, True)
            except BaseException as exc:
                self._finish(key, future, None, exc, True, started=False)
                raise
            return True
        if owner:
            self._execute(key, future, handler, False)
        with phase("account_verification_wait"):
            return future.result()

    def _execute(self, key, future, handler, background):
        result = error = None
        started = False
        try:
            with phase("account_verification_queue"):
                with self._condition:
                    while True:
                        settings = self._settings()
                        if background:
                            limit = self._background_workers(settings)
                            active = self._active_bg
                            delay = self._next_start_bg - time.monotonic()
                        else:
                            limit = settings["workers"]
                            active = self._active_fg
                            delay = self._next_start_fg - time.monotonic()
                        if active < limit and delay <= 0:
                            next_start = time.monotonic() + 1.0 / settings["starts_per_second"]
                            if background:
                                self._active_bg += 1
                                self._next_start_bg = next_start
                            else:
                                self._active_fg += 1
                                self._next_start_fg = next_start
                            self._waiting -= 1
                            started = True
                            break
                        self._condition.wait(timeout=max(0.001, delay) if delay > 0 else None)
            with phase("account_verification"):
                result = handler()
        except BaseException as exc:
            error = exc
        self._finish(key, future, result, error, background, started=started)

    def _finish(self, key, future, result, error, background, *, started):
        with self._condition:
            if started:
                if background:
                    self._active_bg -= 1
                else:
                    self._active_fg -= 1
            else:
                self._waiting -= 1
            self._background -= int(background)
            self._completed += 1
            if error is not None:
                self._failed += 1
                previous = self._retry.get(key, (0, 0))[1]
                failures = min(previous + 1, 8)
                delay = min(3600, self._settings()["failure_backoff_secs"] * 2 ** (failures - 1))
                self._retry[key] = (time.monotonic() + delay, failures)
            else:
                self._retry.pop(key, None)
            # 发布结果与移除进行中条目在同一临界区，避免新请求插入重复校验。
            if error is None:
                future.set_result(result)
            else:
                future.set_exception(error)
            self._flights.pop(key, None)
            self._condition.notify_all()

    def forget(self, key):
        with self._condition:
            self._retry.pop(key, None)

    def stats(self):
        settings = self._settings()
        with self._condition:
            requests = self._hits + self._misses + self._joined
            return {**settings, "active": self._active_fg + self._active_bg,
                    "active_foreground": self._active_fg, "active_background": self._active_bg,
                    "background_workers": self._background_workers(settings),
                    "waiting": self._waiting,
                    "background_pending": self._background,
                    "background_capacity": settings["workers"] + settings["queue_size"],
                    "cache_hits": self._hits, "stale_hits": self._stale_hits,
                    "cache_misses": self._misses, "joined": self._joined,
                    "cache_hit_rate": round(self._hits * 100 / requests, 2) if requests else 0.0,
                    "completed": self._completed, "failed": self._failed,
                    "background_rejected": self._rejected}

    def close(self):
        with self._condition:
            self._closed = True
        self._pool.shutdown(wait=True)
