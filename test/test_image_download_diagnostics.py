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
            assert transport['curl_received_bytes'] == 5
            assert transport['curl_transfer_ms'] > 0
            assert transport['completion_to_read_end_ms'] >= 0
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


def test_download_completion_records_before_buffered_body_is_consumed():
    """传输完成时间与业务读完时间独立；不把缓存读取等待误判为网络传输。"""
    async def scenario():
        payload = b"x" * 8192
        handlers = set()
        async def serve(reader, writer):
            task = asyncio.current_task(); handlers.add(task)
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 8192\r\nConnection: close\r\n\r\n" + payload)
                await writer.drain()
            finally:
                writer.close(); await writer.wait_closed(); handlers.discard(task)
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        url = "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1]) + "/private?key=secret"
        metrics = ImageMetrics(); metrics.diagnostics.configure_budget(5)
        token = current_metrics.set(metrics)
        session = AsyncImageSession(trust_env=False)
        response = None
        try:
            with phase("image_download_http"):
                response = await session._request("GET", url, stream=True)
            await asyncio.wait_for(asyncio.shield(response.response.astream_task), 2)
            assert response._dispatch_wait["received_bytes"] == len(payload)
            await asyncio.sleep(.06)
            chunks = []
            while True:
                try: chunks.append(await response._next())
                except StopAsyncIteration: break
            assert b"".join(chunks) == payload
            await response._close()
            summary = next(s["transport"] for s in metrics.diagnostics.snapshot()["steps"]
                           if s["phase"] == "image_download_transport")
            assert summary["curl_received_bytes"] == summary["bytes"] == len(payload)
            assert summary["completion_to_read_end_ms"] >= 50
            assert summary["curl_completed_ms"] >= summary["curl_transfer_ms"] - 1
            assert "secret" not in str(summary) and "private" not in str(summary)
            assert asyncio.get_running_loop()._image_download_limiter.active == 0
        finally:
            if response is not None: await response._close()
            await session._close(); current_metrics.reset(token)
            server.close(); await server.wait_closed()
            await asyncio.gather(*tuple(handlers))
    asyncio.run(scenario())


@pytest.mark.parametrize("explicit", [None, 65536])
def test_receive_buffer_is_local_to_download_and_preserves_explicit_options(explicit):
    from curl_cffi import CurlOpt
    async def scenario():
        handlers = set()
        async def serve(reader, writer):
            task = asyncio.current_task(); handlers.add(task)
            try:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                await writer.drain()
            finally:
                writer.close(); await writer.wait_closed(); handlers.discard(task)
        server = await asyncio.start_server(serve, "127.0.0.1", 0)
        url = "http://127.0.0.1:" + str(server.sockets[0].getsockname()[1])
        options = {} if explicit is None else {CurlOpt.BUFFERSIZE: explicit}
        session = AsyncImageSession(trust_env=False, curl_options=options)
        original = session.session.curl_options
        captured = []
        original_request = session.session.request
        async def capture(*args, **kwargs):
            captured.append(dict(session.session.curl_options))
            return await original_request(*args, **kwargs)
        session.session.request = capture
        try:
            for stage in ("image_download_http", "sse_stream", "upstream_bootstrap"):
                with phase(stage):
                    response = await session._request("GET", url, stream=True)
                try:
                    while True:
                        try: await response._next()
                        except StopAsyncIteration: break
                finally: await response._close()
                assert session.session.curl_options is original
            assert captured[0][CurlOpt.BUFFERSIZE] == (explicit or 256 * 1024)
            for opts in captured[1:]:
                assert opts.get(CurlOpt.BUFFERSIZE) == explicit
        finally:
            await session._close()
            server.close(); await server.wait_closed()
            await asyncio.gather(*tuple(handlers))
    asyncio.run(scenario())
