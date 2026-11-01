"""下载中断的安全数值诊断与真实本机超时，不发上游请求。"""
import asyncio
import pytest
from curl_cffi.requests.exceptions import RequestException
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession
from services.image_metrics import ImageMetrics, current_metrics, phase


def test_partial_download_timeout_keeps_bytes_and_curl_code_without_url():
    async def scenario():
        release = asyncio.Event()
        handlers = set()
        async def serve(reader, writer):
            task = asyncio.current_task(); handlers.add(task)
            try:
                await reader.readuntil(b'\r\n\r\n')
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nhello')
                await writer.drain()
                await release.wait()
            finally:
                writer.close(); await writer.wait_closed(); handlers.discard(task)
        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        url = 'http://127.0.0.1:' + str(server.sockets[0].getsockname()[1]) + '/secret?signed=token'
        metrics = ImageMetrics(); metrics.diagnostics.configure_budget(2)
        token = current_metrics.set(metrics)
        def call():
            session = AsyncImageSession(trust_env=False)
            response = None
            try:
                with phase('image_download_http'):
                    response = session.get(url, timeout=.10, stream=True)
                with phase('image_download_body'):
                    list(response.iter_content())
            finally:
                if response is not None: response.close()
                session.close()
        try:
            with pytest.raises(RequestException): await run_cooperative(call)
            diagnostic = metrics.diagnostics.snapshot()
            step = next(s for s in diagnostic['steps'] if s['phase'] == 'image_download_transport')
            transport = step['transport']
            assert transport['bytes'] == 5
            assert transport['expected_bytes'] == 100
            assert transport['curl_error_code'] == 28
            assert transport['termination'] == 'error'
            assert transport['body_ms'] > 0 and transport['bytes_per_sec'] > 0
            assert transport['dispatch_count'] > 0 and transport['dispatch_max_wait_ms'] >= 0
            assert any(x['component'] == 'curl_queue_wait' for x in diagnostic['performance'])
            assert metrics.snapshot()['image_download_wire_bytes'] == 5
            failure = next(s for s in diagnostic['steps'] if s['phase'] == 'image_download_body')
            assert failure['curl_error_code'] == 28
            assert 'signed' not in str(diagnostic) and 'secret' not in str(diagnostic)
        finally:
            current_metrics.reset(token); release.set()
            server.close(); await server.wait_closed()
            await asyncio.gather(*tuple(handlers))
    asyncio.run(scenario())


def test_download_transport_whitelist_rejects_raw_fields_and_unknown_phase():
    metrics = ImageMetrics()
    metrics.diagnostics.transport_summary({'bytes': 10, 'url': 'secret', 'curl_error_code': 28,
        'termination': 'error', 'expected_bytes': 100}, phase='image_download_transport')
    metrics.diagnostics.transport_summary({'bytes': 99}, phase='untrusted')
    steps = metrics.diagnostics.snapshot()['steps']
    assert len(steps) == 1 and steps[0]['transport']['curl_error_code'] == 28
    assert 'secret' not in str(steps)
