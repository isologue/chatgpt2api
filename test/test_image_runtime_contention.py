"""CPU/正文并发回归：只使用本机进程与内存流，不访问上游。"""
import asyncio
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import get_context
from types import SimpleNamespace

import pytest

from services import cpu_pool, thread_pool
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageResponse
from services.image_diagnostics import ImageDeadlineExceeded, ImageRequestCancelled
from services.image_metrics import ImageMetrics, current_metrics, phase
from test.test_image_performance import entries, process_sleep, process_fail
from utils.pow import build_proof_token, parse_pow_resources, parse_pow_resources_bytes


def test_cooperative_cpu_work_uses_process_future_without_waiting_thread(monkeypatch):
    async def scenario():
        metrics = ImageMetrics()
        context = current_metrics.set(metrics)
        beats = []
        async def heartbeat():
            for _ in range(4):
                await asyncio.sleep(.005)
                beats.append(1)
        def work():
            with phase('pow_solve'):
                return cpu_pool.run(process_sleep, .08, timeout=10)
        monkeypatch.setattr(thread_pool.image_part_executor, 'submit', lambda *a: pytest.fail(
            'a process result must not occupy an image-part worker'))
        try:
            result, _ = await asyncio.gather(run_cooperative(work), heartbeat())
            assert result == 'completed' and len(beats) == 4
            for name in ('cpu_queue', 'cpu_work', 'cpu_work_cpu', 'cpu_return', 'cpu_resume'):
                assert entries(metrics)[('pow_solve', name)]['total_ms'] >= 0
            assert entries(metrics)[('pow_solve', 'cpu_work')]['total_ms'] >= 60
            with pytest.raises(ValueError, match='expected worker failure'):
                await run_cooperative(lambda: cpu_pool.run(process_fail, timeout=10))
            with pytest.raises(TimeoutError):
                await run_cooperative(lambda: cpu_pool.run(process_sleep, .1, timeout=.005))
            html = '<html data-build="sample"><script src="/c/build/_sdk.js"></script><script src="a&amp;b"></script></html>'
            assert await run_cooperative(lambda: cpu_pool.run(parse_pow_resources, html)) == parse_pow_resources(html)
            assert await run_cooperative(lambda: cpu_pool.run(
                parse_pow_resources_bytes, html.encode(), "utf-8")) == parse_pow_resources(html)
            import base64
            import hashlib
            import json
            proof = await run_cooperative(lambda: cpu_pool.run(
                build_proof_token, 'sandbox-seed', '00', 'sandbox-agent', ['/sdk.js'], 'sandbox-build'))
            assert proof.startswith('gAAAAAB')
            encoded = proof[len('gAAAAAB'):]
            assert hashlib.sha3_512(b'sandbox-seed' + encoded.encode()).digest()[:1] <= b'\x00'
            assert json.loads(base64.b64decode(encoded))[4] == 'sandbox-agent'
        finally:
            current_metrics.reset(context)
    with ProcessPoolExecutor(max_workers=1, mp_context=get_context('spawn')) as pool:
        monkeypatch.setattr(cpu_pool, 'get_pool', lambda: pool)
        pool.submit(process_sleep, 0).result(timeout=15)
        asyncio.run(scenario())


@pytest.mark.parametrize('timeout', [False, True])
def test_cancel_and_timeout_stop_waiting_for_running_cpu_without_losing_result(monkeypatch, timeout):
    future, submitted = Future(), threading.Event()
    future.set_running_or_notify_cancel()
    class Pool:
        def submit(self, *args):
            submitted.set()
            return future
    monkeypatch.setattr(cpu_pool, 'get_pool', Pool)
    async def scenario():
        task = asyncio.create_task(run_cooperative(lambda: cpu_pool.run(
            process_sleep, 0, timeout=.03 if timeout else None)))
        try:
            async with asyncio.timeout(2):
                while not submitted.is_set():
                    await asyncio.sleep(.001)
            if not timeout:
                task.cancel()
            with pytest.raises(TimeoutError if timeout else ImageRequestCancelled):
                await asyncio.wait_for(task, 1)
            assert future.running()  # 已运行的进程任务自然收尾，业务协程不再阻塞。
        finally:
            if not future.done():
                future.set_result(cpu_pool._timed_call(process_sleep, (0,)))
            await asyncio.sleep(0)
    asyncio.run(scenario())


@pytest.mark.parametrize('where', ['submit', 'result'])
def test_cooperative_broken_cpu_pool_falls_back_off_loop_once(monkeypatch, where):
    reset, called = [], []
    class Pool:
        def submit(self, *args):
            if where == 'submit':
                raise RuntimeError('closed pool')
            future = Future()
            future.set_exception(BrokenProcessPool('broken pool'))
            return future
    monkeypatch.setattr(cpu_pool, 'get_pool', Pool)
    monkeypatch.setattr(cpu_pool, 'reset_pool', lambda: reset.append(1))
    async def scenario():
        loop_thread = threading.get_ident()
        def work():
            called.append(threading.get_ident())
            raise ValueError('business failure')
        with pytest.raises(ValueError, match='business failure'):
            await run_cooperative(lambda: cpu_pool.run(work))
        assert len(called) == 1 and called[0] != loop_thread
        assert reset == [1]
    asyncio.run(scenario())


_END = object()

def buffered_response(chunks, cap=1024*1024):
    queue = asyncio.Queue()
    for chunk in chunks:
        queue.put_nowait(chunk)
    async def content():
        while True:
            chunk = await queue.get()
            if chunk is _END:
                return
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk
    response = SimpleNamespace(queue=queue, aiter_content=content)
    return AsyncImageResponse(None, response, time.perf_counter(), None, cap, False)


def test_ready_body_batching_keeps_all_bytes_and_native_chunk_counts():
    async def scenario():
        chunks = [bytes([i])*8192 for i in range(17)]
        response = buffered_response([*chunks, _END])
        batched = await run_cooperative(lambda: list(response.iter_content(chunk_size=65536)))
        assert b''.join(batched) == b''.join(chunks)
        assert [len(c) for c in batched] == [65536, 65536, 8192]
        assert response.chunks == 17 and response.bytes_read == 17*8192
        assert response.ended == 'eof'
    asyncio.run(scenario())


def test_batching_does_not_wait_for_full_chunk_or_delay_first_available_bytes():
    async def scenario():
        response = buffered_response([b'first'])
        result = await asyncio.wait_for(run_cooperative(
            lambda: next(response.iter_content(chunk_size=65536))), .5)
        assert result == b'first' and response.ended != 'eof'
    asyncio.run(scenario())


@pytest.mark.parametrize('error', ['limit', 'transport'])
def test_body_batching_preserves_limits_and_transport_errors(error):
    async def scenario():
        data = [b'x'*16384]*3
        if error == 'transport':
            data.append(ConnectionError('stream interrupted'))
        else:
            data.append(_END)
        response = buffered_response(data, cap=32768 if error == 'limit' else 1024*1024)
        with pytest.raises(RuntimeError if error == 'limit' else ConnectionError,
                           match='size limit' if error == 'limit' else 'stream interrupted'):
            await run_cooperative(lambda: list(response.iter_content(chunk_size=65536)))
    asyncio.run(scenario())


def test_fully_buffered_body_yields_so_cancellation_can_interrupt_consumption():
    async def scenario():
        response = buffered_response([b'x'*16384]*64+[_END], cap=2*1024*1024)
        task = asyncio.create_task(run_cooperative(lambda: b''.join(
            response.iter_content(chunk_size=65536))))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(ImageRequestCancelled):
            await task
        assert response.bytes_read < 1024*1024 and not response.response.queue.empty()
    asyncio.run(scenario())


def test_cpu_compute_keeps_synchronous_compatibility(monkeypatch):
    monkeypatch.setattr(cpu_pool, 'get_pool', lambda: pytest.fail('sync metadata must not create a process pool'))
    thread = threading.get_ident()
    assert cpu_pool.compute(threading.get_ident) == thread


def test_total_budget_releases_image_slot_while_cpu_process_is_still_running(monkeypatch):
    from services.thread_pool import BoundedImageExecutor
    from test.test_image_async_runtime import submit, drain
    future, submitted = Future(), threading.Event()
    future.set_running_or_notify_cancel()
    class Pool:
        def submit(self, *args):
            submitted.set()
            return future
    monkeypatch.setattr(cpu_pool, 'get_pool', Pool)
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(.2)
        context = current_metrics.set(metrics)
        try:
            job = submit(executor, lambda: cpu_pool.run(process_sleep, 0))
            with pytest.raises(ImageDeadlineExceeded):
                await asyncio.wait_for(asyncio.wrap_future(job), 2)
            assert submitted.is_set() and future.running()
            assert executor.stats()['inflight'] == 0
        finally:
            current_metrics.reset(context)
            if not future.done():
                future.set_result(cpu_pool._timed_call(process_sleep, (0,)))
            await drain(executor)
    asyncio.run(scenario())


def test_cpu_business_runtime_error_does_not_restart_or_rerun(monkeypatch):
    class Pool:
        def submit(self, *args):
            future = Future()
            future.set_exception(RuntimeError('business runtime failure'))
            return future
    monkeypatch.setattr(cpu_pool, 'get_pool', Pool)
    monkeypatch.setattr(cpu_pool, 'reset_pool', lambda: pytest.fail('business failure reset the pool'))
    async def scenario():
        with pytest.raises(RuntimeError, match='business runtime failure'):
            await run_cooperative(lambda: cpu_pool.run(lambda: pytest.fail('business was rerun')))
    asyncio.run(scenario())


@pytest.mark.parametrize('ending', ['eof', 'error'])
def test_buffered_stream_cleanup_is_still_bounded_by_total_budget(ending):
    from curl_cffi.requests.models import Response, STREAM_END
    from curl_cffi.requests.exceptions import RequestException
    async def scenario():
        response = Response()
        response.queue = asyncio.Queue()
        response.curl = object()  # aiter_content 只需确认存在句柄，不访问网络。
        response.queue.put_nowait(STREAM_END if ending == 'eof' else RequestException('stream failed'))
        response.astream_task = asyncio.create_task(asyncio.sleep(60))
        wrapper = AsyncImageResponse(None, response, time.perf_counter(), None, 1024, False)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(.03)
        token = current_metrics.set(metrics)
        try:
            with pytest.raises(ImageDeadlineExceeded):
                await asyncio.wait_for(wrapper._next(), .5)
            assert response.astream_task.done()
        finally:
            current_metrics.reset(token)
            response.astream_task.cancel()
            await asyncio.gather(response.astream_task, return_exceptions=True)
    asyncio.run(scenario())
