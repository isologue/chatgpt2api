"""验证计时能区分真实等待、计算和循环阻塞，不访问真实上游。"""
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import get_context
import json
import threading
import time

import pytest

from services import cpu_pool
from services.image_async import await_io, run_blocking, run_cooperative
from services.image_diagnostics import ImageDiagnostics
from services.image_metrics import ImageMetrics, current_metrics, phase
from services.image_performance import COMPONENTS, STAGES, current_stage
from services.image_transport import ImageBackendSession
from test.test_image_async_runtime import http_server


def process_sleep(seconds):
    time.sleep(seconds)
    return 'completed'


def process_fail():
    raise ValueError('expected worker failure')


def entries(metrics):
    return {(r['stage'], r['component']): r for r in metrics.diagnostics.snapshot()['performance']}


def test_diagnostic_storage_is_bounded_and_only_accepts_known_numeric_metadata():
    diagnostic = ImageDiagnostics(time.perf_counter())
    for i in range(1000):
        diagnostic.performance('https://secret/' + str(i), 'http_headers', 1)
        diagnostic.performance('upstream_bootstrap', 'Bearer secret' + str(i), 1)
    diagnostic.performance('upstream_bootstrap', 'http_headers', .02)
    diagnostic.performance('upstream_bootstrap', 'http_headers', .03)
    for stage in STAGES:
        for component in COMPONENTS:
            diagnostic.performance(stage, component, .001)
    snapshot = diagnostic.snapshot()
    assert len(snapshot['performance']) == len(STAGES) * len(COMPONENTS)
    row = next(r for r in snapshot['performance'] if r['stage'] == 'upstream_bootstrap' and r['component'] == 'http_headers')
    assert row == {'stage': 'upstream_bootstrap', 'component': 'http_headers', 'count': 3, 'total_ms': 51.0, 'max_ms': 30.0}
    assert 'secret' not in json.dumps(snapshot)
    row['total_ms'] = -1
    assert next(r for r in diagnostic.snapshot()['performance'] if r['stage'] == 'upstream_bootstrap' and r['component'] == 'http_headers')['total_ms'] == 51


def test_slow_slices_keep_only_slowest_per_stage_and_counts():
    diagnostic = ImageDiagnostics(time.perf_counter())
    for i in range(100):
        diagnostic.slow_slice('requirements_token', time.perf_counter(), .1 + i / 1000, .01)
    rows = diagnostic.snapshot()['slow_slices']
    assert len(rows) == 1 and rows[0]['count'] == 100
    assert rows[0]['duration_ms'] == 199
    assert rows[0]['cpu_ms'] == 10


def test_sync_slice_excludes_async_wait_and_identifies_wall_clock_stall():
    async def scenario():
        metrics = ImageMetrics()
        token = current_metrics.set(metrics)
        def work():
            with phase('upstream_bootstrap'):
                time.sleep(.07)
                await_io(asyncio.sleep(.2))
                return 'ok'
        try:
            assert await run_cooperative(work) == 'ok'
            slow = metrics.diagnostics.snapshot()['slow_slices']
            row = next(r for r in slow if r['stage'] == 'upstream_bootstrap')
            assert 60 <= row['duration_ms'] < 200
            assert row['cpu_ms'] < row['duration_ms']
            assert entries(metrics)[('upstream_bootstrap', 'sync_slice')]['max_ms'] < 200
            assert current_stage.get() == 'other'
        finally:
            current_metrics.reset(token)
    asyncio.run(scenario())


def test_stage_context_isolated_across_concurrent_tasks_and_worker_exceptions():
    async def scenario():
        async def task(stage):
            metrics = ImageMetrics()
            token = current_metrics.set(metrics)
            try:
                with phase(stage):
                    await asyncio.sleep(.01)
                    assert current_stage.get() == stage
                    with pytest.raises(ValueError, match='expected'):
                        await run_blocking(process_fail)
                assert current_stage.get() == 'other'
                return entries(metrics)
            finally:
                current_metrics.reset(token)
        first, second = await asyncio.gather(task('upstream_bootstrap'), task('image_save'))
        assert ('upstream_bootstrap', 'offload_work') in first
        assert ('image_save', 'offload_work') in second
        assert not any(stage == 'image_save' for stage, _ in first)
        assert not any(stage == 'upstream_bootstrap' for stage, _ in second)
    asyncio.run(scenario())


def test_worker_queue_execution_and_resume_are_measured_separately(monkeypatch):
    from services import thread_pool
    async def scenario():
        metrics = ImageMetrics()
        token = current_metrics.set(metrics)
        started, release = threading.Event(), threading.Event()
        with ThreadPoolExecutor(max_workers=1) as pool:
            monkeypatch.setattr(thread_pool, 'image_part_executor', pool)
            def occupy():
                started.set()
                release.wait(3)
            pool.submit(occupy)
            assert await asyncio.to_thread(started.wait, 2)
            loop = asyncio.get_running_loop()
            def work():
                time.sleep(.04)
                # 在工作完成前安排一个循环阻塞，证明恢复等待与工作耗时独立。
                loop.call_soon_threadsafe(time.sleep, .08)
                return 42
            try:
                with phase('image_save'):
                    waiter = asyncio.create_task(run_blocking(work))
                    await asyncio.sleep(.06)
                    release.set()
                    assert await waiter == 42
                rows = entries(metrics)
                assert rows[('image_save', 'offload_queue')]['total_ms'] >= 50
                assert rows[('image_save', 'offload_work')]['total_ms'] >= 30
                assert rows[('image_save', 'offload_resume')]['total_ms'] >= 60
            finally:
                release.set()
                current_metrics.reset(token)
    asyncio.run(scenario())


@pytest.mark.parametrize('path', ['/hold', '/sse'])
def test_real_http_metadata_has_native_and_application_timings_without_content(path):
    async def scenario():
        metrics = ImageMetrics()
        token = current_metrics.set(metrics)
        async with http_server() as (url, _, __):
            def work():
                with phase('upstream_bootstrap'):
                    session = ImageBackendSession(trust_env=False)
                    try:
                        response = session.get(url + path, headers={'X-Private-Test': 'credential'}, timeout=3)
                        assert response.content
                    finally:
                        session.close()
            try:
                await run_cooperative(work)
                rows = entries(metrics)
                if path == '/hold':
                    assert rows[('upstream_bootstrap', 'http_headers')]['total_ms'] >= 100
                else:
                    assert rows[('upstream_bootstrap', 'http_body')]['total_ms'] >= 50
                assert rows[('upstream_bootstrap', 'http_headers')]['count'] == 1
                assert rows[('upstream_bootstrap', 'curl_first_byte')]['count'] == 1
                assert rows[('upstream_bootstrap', 'http_total')]['total_ms'] >= rows[('upstream_bootstrap', 'http_body')]['total_ms']
                assert 'credential' not in json.dumps(metrics.diagnostics.snapshot())
                assert url not in json.dumps(metrics.diagnostics.snapshot())
            finally:
                current_metrics.reset(token)
    asyncio.run(scenario())


def test_real_process_pool_splits_queued_compute_and_result_return(monkeypatch):
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        with ProcessPoolExecutor(max_workers=1, mp_context=get_context('spawn')) as pool:
            monkeypatch.setattr(cpu_pool, 'get_pool', lambda: pool)
            pool.submit(process_sleep, 0).result(timeout=15)
            first = pool.submit(process_sleep, .12)
            with phase('pow_solve'):
                assert cpu_pool.run(process_sleep, .04, timeout=10) == 'completed'
            first.result()
            rows = entries(metrics)
            assert rows[('pow_solve', 'cpu_queue')]['total_ms'] >= 70
            assert rows[('pow_solve', 'cpu_work')]['total_ms'] >= 30
            assert rows[('pow_solve', 'cpu_work_cpu')]['total_ms'] < rows[('pow_solve', 'cpu_work')]['total_ms']
            assert rows[('pow_solve', 'cpu_return')]['total_ms'] >= 0
            with pytest.raises(ValueError, match='expected worker failure'):
                cpu_pool.run(process_fail, timeout=10)
            with pytest.raises(TimeoutError):
                cpu_pool.run(process_sleep, .1, timeout=.005)
    finally:
        current_metrics.reset(token)


def test_cpu_submission_failure_keeps_original_fallback_behavior(monkeypatch):
    class BrokenPool:
        def submit(self, *args):
            raise RuntimeError('cannot schedule')
    monkeypatch.setattr(cpu_pool, 'get_pool', BrokenPool)
    monkeypatch.setattr(cpu_pool, 'reset_pool', lambda: None)
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        with phase('pow_solve'):
            assert cpu_pool.run(process_sleep, .01) == 'completed'
        assert entries(metrics)[('pow_solve', 'cpu_fallback')]['count'] == 1
    finally:
        current_metrics.reset(token)


def test_broken_process_result_falls_back_once_without_swallowing_business_errors(monkeypatch):
    from concurrent.futures import Future
    from concurrent.futures.process import BrokenProcessPool
    class BrokenPool:
        def submit(self, *args):
            future = Future()
            future.set_exception(BrokenProcessPool('worker terminated'))
            return future
    calls = []
    def work():
        calls.append(1)
        raise ValueError('business failure')
    monkeypatch.setattr(cpu_pool, 'get_pool', BrokenPool)
    monkeypatch.setattr(cpu_pool, 'reset_pool', lambda: None)
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        with phase('pow_solve'):
            with pytest.raises(ValueError, match='business failure'):
                cpu_pool.run(work)
        assert calls == [1]
        assert entries(metrics)[('pow_solve', 'cpu_fallback')]['count'] == 1
    finally:
        current_metrics.reset(token)
