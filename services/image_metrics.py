from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from collections import deque
from threading import Lock
from uuid import uuid4

from utils.log import logger


class RuntimePhases:
    """进程级阶段快照，每阶段仅保留最近 256 个耗时，不含账号或正文。"""

    def __init__(self):
        self._lock = Lock()
        self._values = {}

    def _value(self, name):
        value = self._values.get(name)
        if value is None:
            with self._lock:
                value = self._values.get(name)
                if value is None:
                    value = {"lock": Lock(), "active": 0, "completed": 0,
                             "recent": deque(maxlen=256), "max_ms": 0.0}
                    self._values[name] = value
        return value

    def begin(self, name):
        value = self._value(name)
        with value["lock"]:
            value["active"] += 1

    def end(self, name, elapsed):
        value = self._value(name)
        milliseconds = max(0.0, elapsed * 1000)
        with value["lock"]:
            value["active"] -= 1
            value["completed"] += 1
            value["recent"].append(milliseconds)
            value["max_ms"] = max(value["max_ms"], milliseconds)

    def observe(self, name, elapsed):
        """记录已经结束的工作，不在业务锁内获取统计锁。"""
        value = self._value(name)
        milliseconds = max(0.0, elapsed * 1000)
        with value["lock"]:
            value["completed"] += 1
            value["recent"].append(milliseconds)
            value["max_ms"] = max(value["max_ms"], milliseconds)

    def snapshot(self):
        # 注册锁仅保护新阶段，不让所有生图线程和监控共用一把计数锁。
        with self._lock:
            registered = tuple(self._values.items())
        result = {}
        for name, value in registered:
            with value["lock"]:
                active, completed = value["active"], value["completed"]
                samples, maximum = tuple(value["recent"]), value["max_ms"]
            ordered = sorted(samples)
            result[name] = {"active": active, "completed": completed, "sample_count": len(samples),
                            "p95_ms": round(ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)], 3) if ordered else 0.0,
                            "last_ms": round(samples[-1], 3) if samples else 0.0,
                            "max_ms": round(maximum, 3)}
        return result


runtime_phases = RuntimePhases()


class ImageMetrics:
    """保存阶段耗时和少量传输计数；不保存请求正文、账号或图片。"""

    def __init__(self):
        self.id = uuid4().hex
        self.started = time.perf_counter()
        self._lock = Lock()
        self._values = {}
        self._counters = {}

    def add(self, name, seconds):
        with self._lock:
            self._values[name] = self._values.get(name, 0.0) + max(0.0, seconds * 1000)

    def add_count(self, name, value=1):
        with self._lock:
            self._counters[name] = self._counters.get(name, 0) + max(0, int(value))

    def snapshot(self):
        with self._lock:
            values = dict(self._values)
            counters = dict(self._counters)
        if "generation_work" in values:
            # 这是上游调用、轮询、重试及其余处理的合计，不将其冒充纯网络耗时。
            values["upstream_and_retry"] = max(0.0, values["generation_work"] - sum(
                values.get(name, 0.0) for name in ("account_lookup", "image_download", "image_save")))
        result = {name + "_ms": round(value, 3) for name, value in values.items()}
        result.update(counters)
        body_ms = values.get("image_download_body", 0.0)
        bytes_read = counters.get("image_download_bytes", 0)
        if bytes_read and body_ms > 0:
            result["image_download_bytes_per_sec"] = round(bytes_read / (body_ms / 1000), 3)
        return result


current_metrics: ContextVar[ImageMetrics | None] = ContextVar("image_metrics", default=None)
queued_since: ContextVar[float | None] = ContextVar("image_queued_since", default=None)


@contextmanager
def phase(name):
    metrics = current_metrics.get()
    started = time.perf_counter()
    runtime_phases.begin(name)
    try:
        yield
    finally:
        elapsed = time.perf_counter() - started
        runtime_phases.end(name, elapsed)
        if metrics is not None:
            metrics.add(name, elapsed)


def measured(name):
    def decorate(handler):
        @wraps(handler)
        def wrapper(*args, **kwargs):
            with phase(name):
                return handler(*args, **kwargs)
        return wrapper
    return decorate


def traced_image_task(handler):
    @wraps(handler)
    def wrapper(self, key, *args, **kwargs):
        metrics = ImageMetrics()
        token = current_metrics.set(metrics)
        started = time.perf_counter()
        try:
            submitted = queued_since.get()
            if submitted is not None:
                metrics.add("image_queue_wait", time.perf_counter() - submitted)
            return handler(self, key, *args, **kwargs)
        finally:
            metrics.add("task_total", time.perf_counter() - started)
            current_metrics.reset(token)
            logger.info({"event": "image_task_timing", "task_id": key, "timing_id": metrics.id, "timings": metrics.snapshot()})
    return wrapper


class ImageTimingMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "POST" or scope.get("path") not in {
            "/v1/images/generations", "/v1/images/edits",
        }:
            return await self.app(scope, receive, send)
        metrics = ImageMetrics()
        token = current_metrics.set(metrics)
        started = time.perf_counter()
        status = None

        async def timed_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            with phase("response_send"):
                await send(message)

        try:
            await self.app(scope, receive, timed_send)
        finally:
            metrics.add("http_total", time.perf_counter() - started)
            current_metrics.reset(token)
            logger.info({"event": "image_http_timing", "endpoint": scope["path"],
                         "status_code": status, "timing_id": metrics.id, "timings": metrics.snapshot()})


async def measure_event_loop_lag(interval=0.5):
    """低频测量事件循环调度延迟，固定空间，不保留请求或账号。"""
    loop = asyncio.get_running_loop()
    while True:
        target = loop.time() + interval
        await asyncio.sleep(interval)
        runtime_phases.observe("event_loop_lag", max(0.0, loop.time() - target))
