from __future__ import annotations

import asyncio
import time
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
from collections import deque
from threading import Lock
from uuid import uuid4
from weakref import WeakSet
from services.image_diagnostics import ImageDiagnostics

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
                             "recent": deque(maxlen=256), "max_ms": 0.0, "running": {}, "sequence": 0,
                             "lag_history": deque(maxlen=3600 if name == "image_io_loop_lag" else 1200) if name in {"event_loop_lag", "image_io_loop_lag"} else None,
                             "max_at": None}
                    self._values[name] = value
        return value

    def begin(self, name):
        value = self._value(name)
        with value["lock"]:
            value["active"] += 1
            value["sequence"] += 1
            handle = value["sequence"]
            value["running"][handle] = time.perf_counter()
            return handle

    def end(self, name, elapsed, handle=None):
        value = self._value(name)
        milliseconds = max(0.0, elapsed * 1000)
        with value["lock"]:
            value["active"] = max(0, value["active"] - 1)
            if handle is None:
                handle = next(iter(value["running"]), None)
            value["running"].pop(handle, None)
            value["completed"] += 1
            value["recent"].append(milliseconds)
            value["max_ms"] = max(value["max_ms"], milliseconds)

    def observe(self, name, elapsed):
        """记录已经结束的工作，不在业务锁内获取统计锁。"""
        value = self._value(name)
        milliseconds = max(0.0, elapsed * 1000)
        observed_at = time.time() if name in {"event_loop_lag", "image_io_loop_lag"} else None
        observed_mono = time.monotonic() if observed_at is not None else None
        with value["lock"]:
            value["completed"] += 1
            value["recent"].append(milliseconds)
            if observed_at is not None:
                value["lag_history"].append((observed_mono, observed_at, milliseconds))
                if value["max_at"] is None or milliseconds > value["max_ms"]:
                    value["max_at"] = observed_at
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
                running = tuple(value["running"].values())
                lag_history = tuple(value["lag_history"] or ())
                max_at = value["max_at"]
            ages = [max(0.0, time.perf_counter() - start) * 1000 for start in running]
            ordered = sorted(samples)
            result[name] = {"active": active, "completed": completed, "sample_count": len(samples),
                            "p95_ms": round(ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)], 3) if ordered else 0.0,
                            "p50_ms": round(ordered[(len(ordered) - 1) // 2], 3) if ordered else 0.0,
                            "oldest_active_ms": round(max(ages, default=0.0), 3),
                            "over_30s": sum(age >= 30000 for age in ages),
                            "over_120s": sum(age >= 120000 for age in ages),
                            "last_ms": round(samples[-1], 3) if samples else 0.0,
                            "max_ms": round(maximum, 3)}
            if name in {"event_loop_lag", "image_io_loop_lag"}:
                # 仅延迟采样保存时间线；固定空间，监控快照在统计锁外聚合。
                recent = [entry for entry in lag_history if entry[0] >= time.monotonic() - 300]
                peak = max(recent, key=lambda entry: entry[2], default=None)
                buckets = {}
                for mono, at, ms in recent:
                    key = int(mono // 5)
                    bucket = buckets.setdefault(key, {"at": at, "max_ms": 0.0,
                        "max_at": at, "last_ms": 0.0, "samples": 0})
                    if ms >= bucket["max_ms"]:
                        bucket["max_ms"], bucket["max_at"] = round(ms, 3), at
                    bucket["last_ms"], bucket["at"] = round(ms, 3), at
                    bucket["samples"] += 1
                result[name].update({
                    "max_at": max_at,
                    "last_at": lag_history[-1][1] if lag_history else None,
                    "recent_window_secs": 300,
                    "recent_max_ms": round(peak[2], 3) if peak else 0.0,
                    "recent_max_at": peak[1] if peak else None,
                    "recent_max_started_at": peak[1] - peak[2] / 1000 if peak else None,
                    "timeline": list(buckets.values()),
                })
        return result


runtime_phases = RuntimePhases()

# 只弱引用仍有工作在执行的请求，不保存历史请求或正文。
_live_metrics = WeakSet()
_live_lock = Lock()


def image_diagnostics_snapshot():
    with _live_lock:
        metrics = tuple(_live_metrics)
    active = 0
    disconnected = 0
    oldest = 0.0
    for metric in metrics:
        # 监控不复制整条时间线，保持快照开销与在途请求数成正比。
        diagnostic = metric.diagnostics
        with diagnostic._lock:
            workers = diagnostic._workers
            detached = diagnostic._disconnected_at is not None
        if workers:
            active += 1
            disconnected += int(detached)
            oldest = max(oldest, (time.perf_counter() - diagnostic.started) * 1000)
    return {"active_requests": active, "disconnected_running": disconnected,
            "oldest_request_ms": round(oldest, 3)}



class ImageMetrics:
    """保存阶段耗时和少量传输计数；不保存请求正文、账号或图片。"""

    def __init__(self):
        self.id = uuid4().hex
        self.started = time.perf_counter()
        self.diagnostics = ImageDiagnostics(self.started)
        with _live_lock:
            _live_metrics.add(self)
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
    from services.image_performance import current_stage, STAGES
    stage_token = current_stage.set(name) if name in STAGES else None
    metrics = current_metrics.get()
    started = time.perf_counter()
    handle = runtime_phases.begin(name)
    trace = metrics.diagnostics.begin(name) if metrics is not None else None
    error = None
    try:
        yield
    except BaseException as exc:
        error = exc
        raise
    finally:
        elapsed = time.perf_counter() - started
        if stage_token is not None:
            current_stage.reset(stage_token)
        runtime_phases.end(name, elapsed, handle)
        if metrics is not None:
            metrics.diagnostics.end(trace, error)
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
                metrics.diagnostics = ImageDiagnostics(submitted)
            from services.config import config
            metrics.diagnostics.configure_budget(config.image_total_timeout_secs)
            metrics.diagnostics.worker_enter()
            # 任务服务必须先接管状态，再由图片编排报告过期；否则长队列中的
            # 已过期任务会一直留在 queued，而没有可查询的失败终态。
            return handler(self, key, *args, **kwargs)
        finally:
            metrics.diagnostics.worker_exit()
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

        # 单独读取 disconnect，业务等待工作线程时也能发现调用方已断开。
        # 有界队列不预读完整上传体；只有一个协程访问原始 ASGI receive。
        messages = asyncio.Queue(maxsize=1)

        async def read_messages():
            try:
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        metrics.diagnostics.disconnect()
                    await messages.put(message)
                    if message["type"] == "http.disconnect":
                        return
            except asyncio.CancelledError:
                raise

        reader = asyncio.create_task(read_messages())
        try:
            await self.app(scope, messages.get, timed_send)
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
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
