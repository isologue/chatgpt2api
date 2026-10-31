"""真实本机 HTTP 验证协作 I/O、容量、预算、取消及账号状态隔离。"""
import asyncio
import json
import threading
import time
from contextlib import asynccontextmanager

import pytest
from fastapi import HTTPException

from services.image_async import await_io, blocking, parallel_calls
from services.image_diagnostics import ImageDeadlineExceeded, ImageRequestCancelled
from services.image_metrics import ImageMetrics, current_metrics
from services.image_transport import ImageBackendSession
from services.openai_backend_api import OpenAIBackendAPI
from services.thread_pool import BoundedImageExecutor
from test.test_image_download_account_switch import scenario


@asynccontextmanager
async def http_server():
    pending = set()
    requests = []
    active = peak = 0

    async def handle(reader, writer):
        nonlocal active, peak
        pending.add(asyncio.current_task())
        try:
            line = await reader.readline()
            path = line.decode().split()[1]
            headers = {}
            while True:
                line = await reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break
                key, value = line.decode().split(':', 1)
                headers[key.lower()] = value.strip()
            if headers.get('content-length'):
                await reader.readexactly(int(headers['content-length']))
            requests.append((path, headers))
            active += 1
            peak = max(peak, active)
            if path == '/headers':
                await asyncio.sleep(2)
            if path == '/backend-api/f/conversation':
                event = {'conversation_id': 'sandbox-conversation', 'message': {
                    'author': {'role': 'tool'}, 'metadata': {'async_task_type': 'image_gen'},
                    'content': {'content_type': 'multimodal_text', 'parts': [
                        {'content_type': 'image_asset_pointer', 'asset_pointer': 'file-service://file_sandbox'}]}}}
                body = ('data: ' + json.dumps(event) + '\n\ndata: [DONE]\n\n').encode()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: '
                             + str(len(body)).encode() + b'\r\nConnection: close\r\n\r\n' + body)
            elif path in ('/sse', '/silent'):
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n')
                writer.write(b'data: first\n\n')
                await writer.drain()
                await asyncio.sleep(2 if path == '/silent' else 0.08)
                writer.write(b'data: last\n\n')
            else:
                if path == '/hold':
                    await asyncio.sleep(0.15)
                if path in ('/large', '/unknown'):
                    body = b'x' * 2048
                elif path == '/backend-api/sentinel/chat-requirements/prepare':
                    body = json.dumps({'prepare_token': 'sandbox', 'proofofwork': {'required': False}}).encode()
                elif path == '/backend-api/sentinel/chat-requirements/finalize':
                    body = json.dumps({'token': 'sandbox'}).encode()
                elif path == '/backend-api/f/conversation/prepare':
                    body = json.dumps({'conduit_token': 'sandbox'}).encode()
                elif path.endswith('/download'):
                    body = json.dumps({'download_url': f'http://{headers["host"]}/image'}).encode()
                elif path == '/backend-api/files':
                    body = json.dumps({'file_id': 'file_input', 'upload_url': f'http://{headers["host"]}/upload'}).encode()
                elif path == '/image':
                    import io
                    from PIL import Image
                    image = io.BytesIO()
                    Image.new('RGB', (4, 4), 'red').save(image, format='PNG')
                    body = image.getvalue()
                else:
                    body = json.dumps(headers).encode()
                length = b'' if path == '/unknown' else b'Content-Length: ' + str(len(body)).encode() + b'\r\n'
                writer.write(b'HTTP/1.1 200 OK\r\n' + length
                             + b'Set-Cookie: identity=' + headers.get('authorization', 'none').encode()
                             + b'\r\nConnection: close\r\n\r\n' + body)
            await writer.drain()
        except (ConnectionError, BrokenPipeError):
            pass
        finally:
            active -= 1
            writer.close()
            await writer.wait_closed()
            pending.discard(asyncio.current_task())

    server = await asyncio.start_server(handle, '127.0.0.1', 0, backlog=1024)
    url = f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}'
    try:
        yield url, requests, lambda: peak
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(pending):
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


def submit(executor, handler):
    executor.reserve()
    return executor.submit_cooperative_reserved(handler)


async def drain(executor):
    await asyncio.to_thread(executor.shutdown)


def test_real_http_200_jobs_do_not_create_per_request_threads():
    async def scenario():
        executor = BoundedImageExecutor(200, 0)
        baseline = threading.active_count()
        async with http_server() as (url, requests, peak):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + '/hold', stream=True)
                    return b''.join(response.iter_content())
                finally:
                    session.close()
            try:
                futures = [submit(executor, work) for _ in range(200)]
                results = await asyncio.gather(*(asyncio.wrap_future(f) for f in futures))
                assert len(results) == 200
                assert peak() >= 100
                assert threading.active_count() - baseline < 12
                assert not executor.stats()['inflight']
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_sse_summary_measures_chunks_gaps_and_eof():
    async def scenario():
        executor = BoundedImageExecutor(2, 0)
        metrics = ImageMetrics()
        context = current_metrics.set(metrics)
        metrics.diagnostics.configure_budget(3)
        async with http_server() as (url, _, __):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + '/sse', stream=True, image_stream_timeout=1)
                    try:
                        return list(response.iter_lines())
                    finally:
                        response.close()
                finally:
                    session.close()
            try:
                lines = await asyncio.wrap_future(submit(executor, work))
                assert b'data: first' in lines and b'data: last' in lines
                summary = next(s['transport'] for s in metrics.diagnostics.snapshot()['steps'] if s['phase'] == 'sse_transport')
                assert summary['first_chunk_received']
                assert summary['bytes'] == len(b'data: first\n\ndata: last\n\n')
                assert summary['chunks'] >= 2
                assert summary['max_gap_ms'] >= 50
                assert summary['termination'] == 'eof'
                assert 'http' not in json.dumps(summary)
            finally:
                current_metrics.reset(context)
                await drain(executor)
    asyncio.run(scenario())


@pytest.mark.parametrize('path', ['/silent', '/headers'])
def test_total_budget_interrupts_silent_body_and_headers(path):
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(0.2)
        context = current_metrics.set(metrics)
        async with http_server() as (url, _, __):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + path, stream=True, image_stream_timeout=2)
                    try:
                        return list(response.iter_content())
                    finally:
                        response.close()
                finally:
                    session.close()
            try:
                started = time.perf_counter()
                with pytest.raises(ImageDeadlineExceeded):
                    await asyncio.wrap_future(submit(executor, work))
                assert time.perf_counter() - started < 1
                assert not executor.stats()['inflight']
            finally:
                current_metrics.reset(context)
                await drain(executor)
    asyncio.run(scenario())


def test_cancel_closes_silent_connection_and_releases_capacity():
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(10)
        context = current_metrics.set(metrics)
        async with http_server() as (url, requests, _):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + '/silent', stream=True, image_stream_timeout=8)
                    try:
                        return list(response.iter_content())
                    finally:
                        response.close()
                finally:
                    session.close()
            try:
                future = submit(executor, work)
                while not requests:
                    await asyncio.sleep(0.005)
                metrics.diagnostics.disconnect()
                with pytest.raises(ImageRequestCancelled):
                    await asyncio.wait_for(asyncio.wrap_future(future), 1)
                assert executor.stats()['available'] == 1
            finally:
                current_metrics.reset(context)
                await drain(executor)
    asyncio.run(scenario())


def test_account_auth_cookie_and_context_are_isolated():
    async def scenario():
        from services.image_diagnostics import attempt_context
        executor = BoundedImageExecutor(25, 0)
        async with http_server() as (url, _, __):
            def work(index):
                session = ImageBackendSession(trust_env=False, headers={'Authorization': str(index)})
                token = attempt_context.set({'attempt': index})
                try:
                    session.get(url + '/echo')
                    response = session.get(url + '/echo')
                    return response.json(), attempt_context.get()['attempt']
                finally:
                    attempt_context.reset(token)
                    session.close()
            try:
                results = await asyncio.gather(*(asyncio.wrap_future(submit(executor, lambda i=i: work(i))) for i in range(25)))
                for index, (headers, attempt) in enumerate(results):
                    assert headers['authorization'] == str(index)
                    assert headers['cookie'] == f'identity={index}'
                    assert attempt == index
                assert attempt_context.get() is None
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_small_metadata_json_avoids_worker_handoff(monkeypatch):
    from services import image_async_transport as transport

    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        async with http_server() as (url, _, __):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + '/echo')
                    return response.json()
                finally:
                    session.close()

            monkeypatch.setattr(transport, 'run_blocking', lambda *args, **kwargs: pytest.fail(
                'small metadata JSON should not be offloaded'))
            try:
                result = await asyncio.wrap_future(submit(executor, work))
                assert isinstance(result, dict) and result
            finally:
                await drain(executor)

    asyncio.run(scenario())


def test_capacity_is_shared_by_legacy_and_cooperative_jobs_and_queue_full_is_429():
    async def scenario():
        executor = BoundedImageExecutor(1, 1)
        release = threading.Event()
        entered = threading.Event()
        def legacy():
            entered.set()
            release.wait(2)
        first = executor.submit(legacy)
        try:
            while not entered.is_set():
                await asyncio.sleep(0.005)
            second = submit(executor, lambda: 'second')
            with pytest.raises(HTTPException) as caught:
                executor.reserve()
            assert caught.value.status_code == 429
            await asyncio.sleep(0.03)
            assert not second.running()
            executor.reconfigure(2, 0)
            assert await asyncio.wrap_future(second) == 'second'
            release.set()
            await asyncio.wrap_future(first)
        finally:
            release.set()
            await drain(executor)
    asyncio.run(scenario())


def test_real_download_keeps_image_validation_in_worker(monkeypatch):
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        async with http_server() as (url, _, __):
            def work():
                backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
                backend.session = ImageBackendSession(trust_env=False)
                try:
                    return backend.download_image_bytes([url + '/image'])
                finally:
                    backend.close()
            try:
                images = await asyncio.wrap_future(submit(executor, work))
                assert len(images) == 1 and images[0].startswith(b'\x89PNG')
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_cancel_queued_cooperative_job_releases_reservation():
    async def scenario():
        executor = BoundedImageExecutor(1, 1)
        release = threading.Event()
        entered = threading.Event()
        def work():
            entered.set()
            release.wait(2)
        first = executor.submit(work)
        try:
            while not entered.is_set():
                await asyncio.sleep(0.005)
            second = submit(executor, lambda: pytest.fail('cancelled job started'))
            assert second.cancel()
            await asyncio.sleep(0.02)
            assert executor.stats()['inflight'] == 1
            release.set()
            await asyncio.wrap_future(first)
        finally:
            release.set()
            await drain(executor)
    asyncio.run(scenario())


@pytest.mark.parametrize('path', ['/silent', '/headers'])
def test_stream_hard_timeout_and_total_budget_are_separate(path):
    from services.image_diagnostics import ImageStreamHardTimeoutError
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(3)
        context = current_metrics.set(metrics)
        async with http_server() as (url, _, __):
            def work():
                backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
                backend.session = ImageBackendSession(trust_env=False)
                try:
                    response = backend.session.get(url + path, stream=True, image_stream_timeout=0.15)
                    return list(backend._iter_sse_payloads_capped(response, 0.15))
                finally:
                    backend.close()
            try:
                with pytest.raises(ImageStreamHardTimeoutError):
                    await asyncio.wrap_future(submit(executor, work))
                assert metrics.diagnostics.remaining() > 1
                assert not executor.stats()['inflight']
            finally:
                current_metrics.reset(context)
                await drain(executor)
    asyncio.run(scenario())


def test_declared_and_chunked_body_limits(monkeypatch):
    from services import image_async_transport as transport
    monkeypatch.setattr(transport, 'MAX_IMAGE_BODY', 1024)
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        async with http_server() as (url, _, __):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url + '/large', stream=True)
                    try:
                        return b''.join(response.iter_content())
                    finally:
                        response.close()
                finally:
                    session.close()
            try:
                with pytest.raises(Exception, match='(size|Size|limit)'):
                    await asyncio.wrap_future(submit(executor, work))
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_parallel_children_use_separate_contexts_and_overlap():
    from services.image_diagnostics import attempt_context
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        def child(index):
            context = attempt_context.set({'attempt': index})
            try:
                await_io(asyncio.sleep(0.05))
                return attempt_context.get()['attempt']
            finally:
                attempt_context.reset(context)
        def work():
            started = time.perf_counter()
            result = parallel_calls(child, [(i,) for i in range(4)])
            return result, time.perf_counter() - started
        try:
            result, duration = await asyncio.wrap_future(submit(executor, work))
            assert result == list(range(4))
            assert duration < 0.18
            assert attempt_context.get() is None
        finally:
            await drain(executor)
    asyncio.run(scenario())


def local_backend(monkeypatch, url, scenario):
    from services import openai_backend_api as backend_module
    from services.protocol import conversation as flow
    from services.config import config
    class LocalBackend(OpenAIBackendAPI):
        def __init__(self, access_token):
            super().__init__(access_token)
            self.base_url = url
    monkeypatch.setattr(backend_module, 'account_service', scenario.service)
    monkeypatch.setattr(backend_module.proxy_settings, 'build_session_kwargs', lambda **k: {'trust_env': False})
    monkeypatch.setattr(flow, 'OpenAIBackendAPI', LocalBackend)
    monkeypatch.setattr(flow, 'save_image_bytes', lambda *a: url + '/saved-image')
    monkeypatch.setitem(config.data, 'image_check_before_hit_enabled', False)
    monkeypatch.setitem(config.data, 'image_settle_enabled', False)
    monkeypatch.setitem(config.data, 'image_parallel_generation', True)


@pytest.mark.parametrize('edit', [False, True])
@pytest.mark.parametrize('n', [1, 2])
@pytest.mark.parametrize('response_format', ['url', 'b64_json'])
def test_protocol_end_to_end_uses_real_async_sse_download_and_pool(scenario, monkeypatch, edit, n, response_format):
    from services import thread_pool as pools
    from services.protocol import openai_v1_image_edit, openai_v1_image_generations
    async def run():
        executor = BoundedImageExecutor(4, 1)
        monkeypatch.setattr(pools, 'image_executor', executor)
        async with http_server() as (url, requests, _):
            local_backend(monkeypatch, url, scenario)
            payload = {'model': 'gpt-image-2', 'prompt': 'sandbox', 'n': n, 'response_format': response_format}
            handler = openai_v1_image_generations.handle
            if edit:
                import io
                from PIL import Image
                image = io.BytesIO()
                Image.new('RGB', (4, 4), 'blue').save(image, format='PNG')
                payload['images'] = [(image.getvalue(), 'test.png', 'image/png')]
                handler = openai_v1_image_edit.handle
            try:
                result = await pools.run_image_in_threadpool(handler, payload)
                assert len(result['data']) == n
                assert result['usage']['total_tokens'] >= 1
                assert all('revised_prompt' not in item for item in result['data'])
                if response_format == 'b64_json':
                    import base64
                    assert all(base64.b64decode(item['b64_json']).startswith(b'\x89PNG') for item in result['data'])
                assert len([p for p, _ in requests if p == '/backend-api/f/conversation']) == n
                assert len([p for p, _ in requests if p.endswith('chat-requirements/finalize')]) == n
                if edit:
                    assert len([p for p, _ in requests if p == '/upload']) == n
                assert executor._async_runtime is not None
                assert not scenario.service._image_inflight
                assert getattr(scenario.service._write_context, 'pending', None) is None
            finally:
                await drain(executor)
    asyncio.run(run())


def test_async_true_task_uses_same_runtime_and_persists_final_result(scenario, monkeypatch, tmp_path):
    from services.image_task_service import ImageTaskService
    from services import image_task_service as task_module
    from services import thread_pool as pools
    async def run():
        executor = BoundedImageExecutor(2, 1)
        monkeypatch.setattr(task_module, 'image_executor', executor)
        async with http_server() as (url, requests, _):
            local_backend(monkeypatch, url, scenario)
            service = ImageTaskService(tmp_path / 'tasks.json', executor=executor)
            monkeypatch.setattr(service, '_log_call', lambda *a, **k: None)
            identity = {'id': 'test', 'name': 'test'}
            try:
                created = await asyncio.to_thread(service.submit_generation, identity,
                    client_task_id='sandbox-task', prompt='sandbox', model='gpt-image-2', size=None)
                assert created['status'] == 'queued'
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    items = await asyncio.to_thread(service.list_tasks, identity, ['sandbox-task'])
                    item = items['items'][0]
                    if item['status'] in ('success', 'error'):
                        break
                    await asyncio.sleep(0.01)
                assert item['status'] == 'success', item
                assert len(item['data']) == 1
                assert executor._async_runtime is not None
                assert not scenario.service._image_inflight
            finally:
                await asyncio.to_thread(service.close)
                await drain(executor)
    asyncio.run(run())


def test_unknown_length_metadata_is_bounded_while_reading(monkeypatch):
    from services import image_async_transport as transport
    monkeypatch.setattr(transport, 'MAX_METADATA_BODY', 1024)
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        async with http_server() as (url, _, __):
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    return session.get(url + '/unknown').content
                finally:
                    session.close()
            try:
                with pytest.raises(Exception, match='(body size limit|maximum allowed file size)'):
                    await asyncio.wrap_future(submit(executor, work))
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_download_retries_same_url_after_http_failure(monkeypatch):
    from services import openai_backend_api as backend_module
    from services.image_diagnostics import wait_with_image_budget
    from services.config import config
    monkeypatch.setitem(config.data, 'image_download_retries', 1)
    monkeypatch.setattr(backend_module, 'wait_with_image_budget', lambda _: wait_with_image_budget(0.01))
    async def scenario():
        executor = BoundedImageExecutor(1, 0)
        async with http_server() as (url, requests, _):
            def work():
                backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
                backend.session = ImageBackendSession(trust_env=False)
                original = backend.session.get
                failures = []
                def get(target, **kwargs):
                    failures.append(target)
                    if len(failures) == 1:
                        from types import SimpleNamespace
                        return SimpleNamespace(status_code=503, text='retry', headers={}, close=lambda: None)
                    return original(target, **kwargs)
                backend.session.get = get
                try:
                    result = backend.download_image_bytes([url + '/image'])
                    return result, failures
                finally:
                    backend.close()
            try:
                result, attempts = await asyncio.wrap_future(submit(executor, work))
                assert result[0].startswith(b'\x89PNG')
                assert attempts == [url + '/image'] * 2
            finally:
                await drain(executor)
    asyncio.run(scenario())


def test_cancel_during_offload_submission_keeps_account_result_for_release(scenario, monkeypatch):
    from services import thread_pool
    from services.image_diagnostics import remaining_image_budget
    original = thread_pool.image_part_executor.submit
    submitted = threading.Event()
    def delayed_submit(*args):
        future = original(*args)
        submitted.set()
        time.sleep(0.15)
        return future
    monkeypatch.setattr(thread_pool.image_part_executor, 'submit', delayed_submit)
    async def run():
        executor = BoundedImageExecutor(1, 0)
        def work():
            token = scenario.service.get_available_access_token()
            try:
                remaining_image_budget()
            finally:
                scenario.service.release_image_slot(token)
        try:
            future = submit(executor, work)
            while not submitted.is_set():
                await asyncio.sleep(0.005)
            scenario.metrics.diagnostics.disconnect()
            with pytest.raises(ImageRequestCancelled):
                await asyncio.wrap_future(future)
            assert not scenario.service._image_inflight
            assert not executor.stats()['inflight']
        finally:
            await drain(executor)
    asyncio.run(run())


def test_queued_request_deadline_does_not_wait_for_running_job():
    async def run():
        executor = BoundedImageExecutor(1, 1)
        release = threading.Event()
        entered = threading.Event()
        def legacy():
            entered.set()
            release.wait(2)
        first = executor.submit(legacy)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(0.1)
        context = current_metrics.set(metrics)
        try:
            while not entered.is_set():
                await asyncio.sleep(0.005)
            second = submit(executor, lambda: pytest.fail('expired queued request started'))
            with pytest.raises(ImageDeadlineExceeded):
                await asyncio.wait_for(asyncio.wrap_future(second), 0.5)
            assert executor.stats()['inflight'] == 1
            release.set()
            await asyncio.wrap_future(first)
        finally:
            current_metrics.reset(context)
            release.set()
            await drain(executor)
    asyncio.run(run())


@pytest.mark.parametrize('expire', [False, True])
def test_queued_async_task_detaches_creator_and_records_expired_terminal(scenario, monkeypatch, tmp_path, expire):
    from services.image_task_service import ImageTaskService
    from services.config import config
    if expire:
        monkeypatch.setitem(config.data, 'image_total_timeout_secs', 1)
    async def run():
        executor = BoundedImageExecutor(1, 1)
        release = threading.Event()
        entered = threading.Event()
        def legacy():
            entered.set()
            release.wait(3)
        first = executor.submit(legacy)
        async with http_server() as (url, requests, _):
            local_backend(monkeypatch, url, scenario)
            service = ImageTaskService(tmp_path / 'tasks.json', executor=executor)
            monkeypatch.setattr(service, '_log_call', lambda *a, **k: None)
            identity = {'id': 'test', 'name': 'test'}
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.005)
                await asyncio.to_thread(service.submit_generation, identity,
                    client_task_id='queued-task', prompt='sandbox', model='gpt-image-2', size=None)
                # 创建连接结束不应取消后台已接纳的任务。
                scenario.metrics.diagnostics.disconnect()
                await asyncio.sleep(1.1 if expire else 0.15)
                release.set()
                await asyncio.wrap_future(first)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    item = (await asyncio.to_thread(service.list_tasks, identity, ['queued-task']))['items'][0]
                    if item['status'] in ('success', 'error'):
                        break
                    await asyncio.sleep(0.01)
                assert item['status'] == ('error' if expire else 'success'), item
                if expire:
                    assert 'total time budget' in item['error']
                    assert not any(path == '/backend-api/f/conversation' for path, _ in requests)
                assert not scenario.service._image_inflight
            finally:
                release.set()
                await asyncio.to_thread(service.close)
                await drain(executor)
    asyncio.run(run())
