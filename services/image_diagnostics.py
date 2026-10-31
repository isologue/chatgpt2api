"""图片诊断的有界步骤记录。只保存阶段及执行元数据，不接收正文、URL 或令牌。"""
from __future__ import annotations

import time
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Event, Lock


# 子图并行时，各线程保持自己的尝试信息，不能共享一个可变的当前账号字段。
attempt_context: ContextVar[dict | None] = ContextVar('image_diagnostic_attempt', default=None)

# 只记录有业务意义的步骤；高频锁计时仍由原有汇总统计承担。
TRACE_PHASES = frozenset({
    'image_queue_wait', 'image_admission_wait', 'image_dispatch_wait', 'sse_transport', 'account_slot_wait', 'account_foreground_verification',
    'account_verification_wait', 'account_token_refresh',
    'request_auth', 'request_filter', 'account_lookup', 'account_verification_queue',
    'account_verification', 'access_token_refresh', 'reference_upload', 'upstream_bootstrap',
    'requirements_token', 'pow_solve', 'conversation_prepare', 'generation_submit',
    'upstream_generation', 'sse_stream', 'result_poll', 'result_query', 'result_resolve',
    'retry_wait', 'image_download', 'image_download_http', 'image_download_body',
    'image_validate', 'image_save', 'log_confirmation', 'response_serialization',
})


class ImageTaskError(RuntimeError):
    """图片任务异常，携带对话 ID。"""
    def __init__(self, message: str = "", conversation_id: str = "") -> None:
        super().__init__(message)
        self.conversation_id = conversation_id


class ImageStreamHardTimeoutError(RuntimeError):
    """上游图片生成请求的响应头和 SSE 传输超过硬上限。"""


class ImageResultResolveError(ImageTaskError):
    """上游生成流结束后，补查或结果解析失败。"""


class ImagePollTimeoutError(ImageResultResolveError):
    """结果补查预算耗尽。"""


_poll_deadline: ContextVar[tuple[float, str] | None] = ContextVar('image_poll_deadline', default=None)


def remaining_poll_budget(cap: float | None = None) -> float | None:
    state = _poll_deadline.get()
    if state is None:
        return cap
    deadline, conversation_id = state
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ImagePollTimeoutError("图片结果补查超时，未在配置的补查时间内获取结果。", conversation_id)
    return min(remaining, cap) if cap is not None else remaining


def poll_pause(seconds: float, *, cap: float | None = None) -> bool:
    """只使用剩余补查时间的一部分退避，至少为下一次查询留下 0.5 秒。"""
    remaining = remaining_image_budget(cap)
    if remaining is None:
        wait_with_image_budget(max(0.0, seconds))
        return True
    if remaining <= 0.5:
        return False
    pause = min(max(0.0, seconds), remaining / 4, remaining - 0.5)
    if pause:
        wait_with_image_budget(pause)
    return True


@contextmanager
def image_poll_budget(seconds: float, conversation_id: str = ""):
    # 兜底、网络重试和嵌套轮询共用截止时间，不能重新获得完整预算。
    outer = _poll_deadline.get()
    deadline = time.monotonic() + max(0.0, seconds)
    token = _poll_deadline.set((min(deadline, outer[0]) if outer else deadline,
                                conversation_id or (outer[1] if outer else "")))
    try:
        yield
        remaining_image_budget()
    finally:
        _poll_deadline.reset(token)


class ImageDeadlineExceeded(TimeoutError):
    """所有阶段、切号和重试共用的执行预算已耗尽。"""


class ImageRequestCancelled(RuntimeError):
    """同步调用方已断开；不再启动新的上游工作。"""


class ImageDiagnostics:
    MAX_STEPS = 160

    def __init__(self, started: float | None = None):
        self.started = time.perf_counter() if started is None else started
        self.started_at = time.time() - (time.perf_counter() - self.started)
        self._lock = Lock()
        self._steps = deque(maxlen=self.MAX_STEPS)
        self._sequence = 0
        self._dropped = 0
        self._deadline: float | None = None
        self.cancelled = Event()
        self._disconnected_at: float | None = None
        self._workers = 0

    def configure_budget(self, seconds: float) -> None:
        # 重复进入或切号只能缩短截止时间，不能重新获得完整预算。
        deadline = self.started + max(0.001, float(seconds))
        with self._lock:
            self._deadline = min(self._deadline, deadline) if self._deadline is not None else deadline

    def remaining(self, cap: float | None = None) -> float | None:
        if self.cancelled.is_set():
            raise ImageRequestCancelled('image request cancelled: caller disconnected')
        with self._lock:
            deadline = self._deadline
        remaining = deadline - time.perf_counter() if deadline is not None else None
        if remaining is not None and remaining <= 0:
            raise ImageDeadlineExceeded('image request total time budget exceeded')
        if cap is None:
            return remaining
        return min(float(cap), remaining) if remaining is not None else float(cap)

    def disconnect(self) -> None:
        with self._lock:
            if self._disconnected_at is None:
                self._disconnected_at = time.time()
        self.cancelled.set()

    def worker_enter(self) -> None:
        with self._lock:
            self._workers += 1

    def worker_exit(self) -> None:
        with self._lock:
            self._workers = max(0, self._workers - 1)

    def begin(self, name: str):
        if name not in TRACE_PHASES:
            return None
        started = time.perf_counter()
        context = attempt_context.get() or {}
        with self._lock:
            self._sequence += 1
            step = {
                'id': self._sequence, 'phase': name,
                'offset_ms': round(max(0.0, started - self.started) * 1000, 3),
                'started_at': self.started_at + started - self.started,
                'status': 'running',
            }
            # 显式白名单，绝不把 token、请求体、带签名下载地址混入诊断字段。
            for key in ('image_index', 'attempt', 'account'):
                value = context.get(key)
                if isinstance(value, (str, int)):
                    step[key] = value[:160] if isinstance(value, str) else value
            if len(self._steps) == self.MAX_STEPS:
                self._dropped += 1
            self._steps.append(step)
        return step, started

    def end(self, handle, error: BaseException | None = None) -> None:
        if handle is None:
            return
        step, started = handle
        elapsed = max(0.0, time.perf_counter() - started)
        with self._lock:
            step.update(duration_ms=round(elapsed * 1000, 3),
                        ended_at=step['started_at'] + elapsed,
                        status='stopped' if isinstance(error, GeneratorExit) and step['phase'] == 'sse_stream'
                        else 'failed' if error is not None else 'succeeded')
            if error is not None:
                # 不保存 str(error)：原始异常可能包含代理认证信息或带签名 URL。
                step['error_type'] = type(error).__name__

    def event(self, action: str, reason: str) -> None:
        """调用方仅传固定枚举，不传原始异常或用户输入。"""
        now = time.perf_counter()
        with self._lock:
            self._sequence += 1
            if len(self._steps) == self.MAX_STEPS:
                self._dropped += 1
            self._steps.append({
                'id': self._sequence, 'phase': 'decision', 'action': action[:64],
                'reason': reason[:96], 'offset_ms': round((now - self.started) * 1000, 3),
                'started_at': self.started_at + now - self.started,
                'ended_at': self.started_at + now - self.started,
                'duration_ms': 0, 'status': 'recorded',
                **{key: value for key, value in (attempt_context.get() or {}).items()
                   if key in ('image_index', 'attempt') and isinstance(value, int)},
            })

    def transport_summary(self, values: dict) -> None:
        """传输摘要只接受固定数值和结束类型，不记录地址、正文或认证信息。"""
        now = time.perf_counter()
        fields = {key: round(float(values[key]), 3) for key in (
            'headers_ms', 'first_chunk_ms', 'last_chunk_ms', 'max_gap_ms',
            'idle_after_last_ms', 'bytes', 'chunks') if key in values}
        fields['first_chunk_received'] = bool(values.get('first_chunk_received'))
        ending = values.get('termination')
        fields['termination'] = ending if ending in {'eof', 'closed', 'error', 'body_limit'} else 'error'
        with self._lock:
            self._sequence += 1
            if len(self._steps) == self.MAX_STEPS:
                self._dropped += 1
            self._steps.append({
                'id': self._sequence, 'phase': 'sse_transport', 'status': 'recorded',
                'offset_ms': round((now - self.started) * 1000, 3),
                'started_at': self.started_at + now - self.started,
                'duration_ms': 0, 'transport': fields,
                **{key: value for key, value in (attempt_context.get() or {}).items()
                   if key in ('image_index', 'attempt') and isinstance(value, int)},
            })

    def performance(self, stage: str, component: str, seconds: float) -> None:
        from services.image_performance import STAGES, COMPONENTS
        if stage not in STAGES or component not in COMPONENTS:
            return
        milliseconds = max(0.0, seconds * 1000)
        with self._lock:
            summaries = getattr(self, '_performance', None)
            if summaries is None:
                summaries = self._performance = {}
            key = (stage, component)
            entry = summaries.setdefault(key, {'stage': stage, 'component': component,
                'count': 0, 'total_ms': 0.0, 'max_ms': 0.0})
            entry['count'] += 1
            entry['total_ms'] += milliseconds
            entry['max_ms'] = max(entry['max_ms'], milliseconds)

    def slow_slice(self, stage: str, started: float, elapsed: float, cpu: float) -> None:
        from services.image_performance import STAGES
        if stage not in STAGES:
            stage = 'other'
        with self._lock:
            slices = getattr(self, '_slow_slices', None)
            if slices is None:
                slices = self._slow_slices = {}
            previous = slices.get(stage)
            count = previous['count'] + 1 if previous else 1
            if previous is None or elapsed * 1000 > previous['duration_ms']:
                slices[stage] = {'stage': stage, 'count': count,
                    'started_at': self.started_at + started - self.started,
                    'duration_ms': elapsed * 1000, 'cpu_ms': cpu * 1000}
            else:
                previous['count'] = count

    def snapshot(self) -> dict:
        now = time.perf_counter()
        with self._lock:
            steps = [dict(step) for step in self._steps]
            performance = [{**entry, 'total_ms': round(entry['total_ms'], 3),
                            'max_ms': round(entry['max_ms'], 3)}
                           for entry in getattr(self, '_performance', {}).values()]
            slow_slices = [{**entry, 'duration_ms': round(entry['duration_ms'], 3),
                            'cpu_ms': round(entry['cpu_ms'], 3)}
                           for entry in getattr(self, '_slow_slices', {}).values()]
            dropped = self._dropped
            disconnected_at = self._disconnected_at
            workers = self._workers
            deadline = self._deadline
        for step in steps:
            if step['status'] == 'running':
                step['duration_ms'] = round(max(0.0, (now - self.started) * 1000 - step['offset_ms']), 3)
        return {
            'version': 1, 'started_at': self.started_at,
            'elapsed_ms': round(max(0.0, now - self.started) * 1000, 3),
            'budget_ms': round((deadline - self.started) * 1000, 3) if deadline is not None else None,
            'steps': steps, 'dropped_steps': dropped,
            'performance': performance, 'slow_slices': slow_slices,
            'caller_disconnected': disconnected_at is not None,
            'disconnected_at': disconnected_at, 'active_workers': workers,
        }


# 共享 single-flight 的远程校验不隶属于第一个调用方的取消/总预算。
# 其前台等待仍受调用方预算约束，后台操作保留自身的网络超时。
detached_budget: ContextVar[bool] = ContextVar('image_detached_budget', default=False)


class ImageDownloadError(RuntimeError):
    """同一生成结果的下载重试已耗尽，由调用层按共享账号预算决定是否切号。"""


def remaining_image_budget(cap: float | None = None) -> float | None:
    from services.image_metrics import current_metrics
    metrics = current_metrics.get()
    remaining = cap if metrics is None or detached_budget.get() else metrics.diagnostics.remaining(cap)
    return remaining_poll_budget(remaining)


def wait_with_image_budget(seconds: float) -> None:
    from services.image_metrics import current_metrics, phase
    metrics = current_metrics.get()
    duration = max(0.0, float(seconds))
    from services.image_async import cooperative, await_io
    if cooperative():
        import asyncio
        with phase('retry_wait'):
            remaining = remaining_image_budget(duration)
            await_io(asyncio.sleep(remaining if remaining is not None else duration))
            remaining_image_budget()
        return
    with phase('retry_wait'):
        remaining = remaining_image_budget(duration)
        if metrics is None or detached_budget.get():
            time.sleep(remaining if remaining is not None else duration)
            remaining_image_budget()
            return
        metrics.diagnostics.cancelled.wait(remaining)
        remaining_image_budget()


def record_image_decision(action: str, reason: str) -> None:
    from services.image_metrics import current_metrics
    metrics = current_metrics.get()
    if metrics is not None:
        metrics.diagnostics.event(action, reason)
