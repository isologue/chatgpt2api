from __future__ import annotations

import time
from concurrent.futures import Future, TimeoutError
from contextvars import copy_context
from threading import Condition

from services.image_metrics import phase
from services.image_diagnostics import detached_budget, remaining_image_budget
from services.thread_pool import _ResizableThreadPool

LANES = ("foreground", "hot_fill", "hot_maintenance", "cold_maintenance", "manual")


class VerificationBackoff(RuntimeError):
    pass


class AccountVerification:
    """五条独立的C并发通道；同账号跨通道single-flight，等待有界。"""

    def __init__(self, settings):
        self._settings = settings
        self._condition = Condition()
        self._flights = {}
        self._retry = {}
        self._active = dict.fromkeys(LANES, 0)
        self._pending = dict.fromkeys(LANES, 0)
        self._pools = {lane: _ResizableThreadPool(settings()["workers"], "verify-" + lane) for lane in LANES}
        self._closed = False
        self._hits = self._misses = self._joined = 0
        self._completed = self._failed = self._rejected = 0

    def reconfigure(self):
        with self._condition:
            self._closed = False
            for pool in self._pools.values():
                pool.reconfigure(self._settings()["workers"])
            self._condition.notify_all()

    def cache_hit(self, **_):
        with self._condition:
            self._hits += 1

    def submit(self, key, handler, *, lane="foreground"):
        return self._submit(key, handler, lane=lane, count_rejection=lane != "foreground")

    def _submit(self, key, handler, *, lane, count_rejection):
        if lane not in LANES:
            raise ValueError("unknown verification lane: " + lane)
        with self._condition:
            if self._closed:
                raise RuntimeError("account verification is closed")
            if key in self._flights:
                self._joined += 1
                return self._flights[key]
            retry = self._retry.get(key)
            if retry and retry[0] > time.monotonic():
                raise VerificationBackoff("account verification retry deferred")
            # 队列容量由C自动推导，不暴露第二个并发旋钮；维护满时下轮补位。
            if self._pending[lane] >= 2 * self._settings()["workers"]:
                if count_rejection:
                    self._rejected += 1
                return None
            future = Future()
            self._flights[key] = future
            self._pending[lane] += 1
            self._misses += 1
            try:
                deadline = time.monotonic() + self._settings().get("wait_timeout_secs", 60)
                context = copy_context()
                context.run(detached_budget.set, True)
                self._pools[lane].submit(context.run, self._execute, key, future, handler, lane, deadline)
            except BaseException:
                self._flights.pop(key, None)
                self._pending[lane] -= 1
                self._condition.notify_all()
                raise
            return future

    def run(self, key, handler, *, lane="foreground", background=False):
        # background仅保留兼容入口；维护调度器使用submit和完成回调。
        if background and lane == "foreground":
            lane = "hot_maintenance"
        deadline = time.monotonic() + self._settings().get("wait_timeout_secs", 60)
        with phase("account_verification_wait"):
            while True:
                remaining_image_budget()
                try:
                    # 等待式调用的轮询不算后台跳过；仅立即拒绝的后台提交计数。
                    future = self._submit(key, handler, lane=lane,
                        count_rejection=background and lane != "foreground")
                except VerificationBackoff:
                    if background:
                        return False
                    raise
                if background:
                    return future is not None
                remaining = deadline - time.monotonic()
                if future is not None:
                    try:
                        while True:
                            remaining_image_budget()
                            left = deadline - time.monotonic()
                            if left <= 0:
                                raise VerificationBackoff("account verification wait timed out")
                            try:
                                return future.result(timeout=min(left, remaining_image_budget(0.1)))
                            except TimeoutError:
                                # 不取消共享future，也不误吞其业务异常。
                                if future.done():
                                    return future.result()
                    except TimeoutError as exc:
                        raise VerificationBackoff("account verification wait timed out") from exc
                if remaining <= 0:
                    raise VerificationBackoff("account verification admission timed out")
                with self._condition:
                    self._condition.wait(timeout=min(remaining, 0.1))

    def _execute(self, key, future, handler, lane, deadline):
        result = error = None
        started = False
        try:
            if not future.set_running_or_notify_cancel():
                raise VerificationBackoff("account verification was cancelled before execution")
            with phase("account_verification_queue"):
                with self._condition:
                    # 调整C后新旧线程池会共存：必须用同一闸门限制合计执行数。
                    # deadline同时覆盖线程池内排队，不能随线程池并发限制一起删除。
                    while self._active[lane] >= self._settings()["workers"]:
                        if time.monotonic() >= deadline:
                            raise VerificationBackoff("account verification queue timed out")
                        self._condition.wait(timeout=min(0.1, max(0, deadline - time.monotonic())))
                    if time.monotonic() >= deadline:
                        raise VerificationBackoff("account verification expired before execution")
                    self._active[lane] += 1
                    started = True
            with phase("account_verification"):
                result = handler()
        except BaseException as exc:
            error = exc
        with self._condition:
            if started:
                self._active[lane] -= 1
            self._pending[lane] -= 1
            self._completed += 1
            if error is not None:
                self._failed += 1
                if started and not isinstance(error, VerificationBackoff):
                    failures = min(self._retry.get(key, (0, 0))[1] + 1, 8)
                    self._retry[key] = (time.monotonic() + min(3600,
                        self._settings()["failure_backoff_secs"] * 2 ** (failures - 1)), failures)
            else:
                self._retry.pop(key, None)
            if self._flights.get(key) is future:
                self._flights.pop(key)
            self._condition.notify_all()
        # 远程handler已结束才移除flight；发布结果不持调度锁，避免Future回调造成锁顺序反转。
        if not future.cancelled():
            if error is None:
                future.set_result(result)
            else:
                future.set_exception(error)

    def forget(self, key):
        with self._condition:
            self._retry.pop(key, None)

    def stats(self):
        settings = self._settings()
        with self._condition:
            requests = self._hits + self._misses + self._joined
            lanes = {lane: {"limit": settings["workers"], "active": self._active[lane],
                            "waiting": self._pending[lane] - self._active[lane]} for lane in LANES}
            background = sum(self._pending[lane] for lane in LANES if lane != "foreground")
            return {**settings, "lanes": lanes, "active": sum(self._active.values()),
                    "active_foreground": self._active["foreground"],
                    "active_background": sum(self._active.values()) - self._active["foreground"],
                    "waiting": sum(self._pending.values()) - sum(self._active.values()),
                    "background_pending": background, "background_capacity": 8 * settings["workers"],
                    "cache_hits": self._hits, "cache_misses": self._misses, "joined": self._joined,
                    "cache_hit_rate": round(self._hits * 100 / requests, 2) if requests else 0.0,
                    "completed": self._completed, "failed": self._failed, "inflight_accounts": len(self._flights),
                    "background_rejected": self._rejected}

    def close(self):
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        for pool in self._pools.values():
            pool.shutdown(wait=True)
