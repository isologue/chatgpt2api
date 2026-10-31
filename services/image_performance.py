"""图片性能细分：固定枚举、固定空间，不记录函数参数或传输正文。"""
from contextvars import ContextVar


STAGES = frozenset({
    'account_lookup', 'reference_upload', 'upstream_bootstrap', 'requirements_token',
    'pow_solve', 'conversation_prepare', 'generation_submit', 'sse_stream',
    'result_poll', 'result_query', 'result_resolve', 'image_download',
    'image_download_http', 'image_download_body', 'image_validate', 'image_save', 'other',
})
COMPONENTS = frozenset({
    'http_headers', 'http_body', 'http_total', 'curl_dns', 'curl_connect',
    'curl_tls', 'curl_first_byte', 'offload_queue', 'offload_work', 'offload_resume',
    'cpu_submit', 'cpu_queue', 'cpu_work', 'cpu_work_cpu', 'cpu_return', 'cpu_resume', 'cpu_fallback',
    'sync_slice', 'sync_slice_cpu',
})
current_stage = ContextVar('image_performance_stage', default='other')
SLOW_SLICE_SECS = 0.05


def record(component, seconds, *, stage=None):
    from services.image_metrics import current_metrics
    metrics = current_metrics.get()
    if metrics is not None:
        metrics.diagnostics.performance(stage or current_stage.get(), component, seconds)


def slow_slice(stage, started, elapsed, cpu):
    from services.image_metrics import current_metrics, runtime_phases
    metrics = current_metrics.get()
    if metrics is not None:
        metrics.diagnostics.slow_slice(stage, started, elapsed, cpu)
    runtime_phases.observe('image_sync_slice_slow', elapsed)
