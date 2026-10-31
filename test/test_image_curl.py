"""curl 补偿调度回归；仅访问本机 HTTP，覆盖多账号隔离及关闭竞态。"""
import asyncio
import time
from types import SimpleNamespace

import pytest
from curl_cffi.requests.models import Response

from services.image_curl import ImageAsyncCurl, ImageCurlSession, _Watchdog
from services.image_async import run_cooperative
from utils.pow import parse_pow_resources, parse_pow_resources_bytes


def test_two_sessions_keep_drivers_and_cookies_private_when_one_is_closed():
    async def scenario():
        received = []
        release = asyncio.Event()
        handlers = set()
        async def handle(reader, writer):
            task = asyncio.current_task(); handlers.add(task)
            try:
                raw = (await reader.readuntil(b'\r\n\r\n')).decode()
                received.append(raw)
                if 'GET /hold ' in raw:
                    await release.wait()
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nSet-Cookie: own=only; Path=/\r\nConnection: close\r\n\r\nok')
                await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()
                handlers.discard(task)
        server = await asyncio.start_server(handle, '127.0.0.1', 0)
        url = 'http://127.0.0.1:'+str(server.sockets[0].getsockname()[1])
        first = ImageCurlSession(headers={'Authorization': 'A'}, trust_env=False)
        second = ImageCurlSession(headers={'Authorization': 'B'}, trust_env=False)
        try:
            await first.get(url+'/one')
            assert first.cookies.get('own') == 'only'
            assert not second.cookies.get('own')
            pending = asyncio.create_task(second.get(url+'/hold'))
            async with asyncio.timeout(2):
                while len(received) < 2: await asyncio.sleep(.001)
            hub = asyncio.get_running_loop()._image_curl_watchdog
            assert hub.members == {first.acurl, second.acurl}
            assert first.acurl is not second.acurl
            await first.close()
            assert not pending.done() and first._acurl._curlm is None
            assert hub.members == {second.acurl}
            release.set()
            assert (await pending).content == b'ok'
            assert 'Authorization: B' in received[1] and 'Cookie:' not in received[1]
        finally:
            release.set()
            if first._acurl._curlm:
                await first.close()
            await second.close()
            server.close(); await server.wait_closed()
            if handlers: await asyncio.gather(*tuple(handlers))
        assert not getattr(asyncio.get_running_loop(), '_image_curl_watchdog', None)
    asyncio.run(scenario())


def test_watchdog_ignores_idle_and_recent_callbacks_but_drains_stalled_handle():
    async def scenario():
        loop = asyncio.get_running_loop()
        called = []
        def member(name, *, active=True, fresh=False):
            class Driver:
                _curlm = True
                _curl2future = {1: object()} if active else {}
                _last_action = float('inf') if fresh else 0.
                def process_data(self, *args):
                    called.append(name)
                    self._last_action = time.monotonic()
                    if name == 'stalled': self._curl2future.clear()
            return Driver()
        idle, recent, stalled = member('idle', active=False), member('recent', fresh=True), member('stalled')
        hub = loop._image_curl_watchdog = _Watchdog(loop)
        hub.members.update((idle, recent, stalled))
        try:
            await asyncio.sleep(.26)
            assert called == ['stalled']
        finally:
            for driver in (idle, recent, stalled): hub.remove(driver)
            await asyncio.gather(hub.task, return_exceptions=True)
    asyncio.run(scenario())


def test_many_due_drivers_yield_and_close_without_leaking_watchdog(monkeypatch):
    async def scenario():
        loop = asyncio.get_running_loop()
        drivers = [ImageAsyncCurl() for _ in range(100)]
        await asyncio.sleep(0)
        hub = loop._image_curl_watchdog
        assert len(hub.members) == 100
        calls, beats = [], []
        for driver in drivers:
            driver._last_action = 0
            driver._curl2future = {object(): object()}
            monkeypatch.setattr(driver, 'process_data', lambda *args: calls.append(1))
        async def beat():
            for _ in range(4):
                await asyncio.sleep(.03); beats.append(len(calls))
        await beat()
        assert 0 < len(calls) <= 200
        for driver in drivers: driver._curl2future.clear()
        await asyncio.gather(*(driver.close() for driver in drivers))
        assert hub.task.done() and not getattr(loop, '_image_curl_watchdog', None)
        assert all(driver._curlm is None for driver in drivers)
    asyncio.run(scenario())


@pytest.mark.parametrize('encoding,content', [
    ('utf-8', '<html data-build="中文"><script src="a&amp;b"></script></html>'.encode()),
    ('gb18030', '<html data-build="中文"></html>'.encode('gb18030')),
    ('unknown-encoding', b'<html data-build="fallback"></html>'),
    ('utf-8', b'<html data-build="bad-\xff"></html>'),
    ('utf-8', b''),
])
def test_bytes_parser_preserves_response_text_decoding(encoding, content):
    response = Response(); response.content = content; response.encoding = encoding
    assert parse_pow_resources_bytes(content, encoding) == parse_pow_resources(response.text)


def test_bootstrap_decodes_inside_worker_not_before_submission(monkeypatch):
    from services import openai_backend_api as backend_module
    # 选用真正实现 _bootstrap 的类，避免依赖账号/磁盘初始化。
    cls = next(value for value in vars(backend_module).values()
               if isinstance(value, type) and '_bootstrap' in value.__dict__)
    client = object.__new__(cls)
    client.base_url = 'http://sandbox'
    client._bootstrap_headers = lambda: {}
    class RawResponse:
        content = b'<html data-build="bytes"><script src="/sdk"></script></html>'
        encoding = 'utf-8'
        @property
        def text(self): pytest.fail('Response.text ran on the image loop')
    client.session = SimpleNamespace(get=lambda *a, **k: RawResponse())
    monkeypatch.setattr(backend_module, 'ensure_ok', lambda *a: None)
    calls = []
    def compute(handler, *args):
        calls.append((handler, args)); return handler(*args)
    monkeypatch.setattr(backend_module.cpu_pool, 'compute', compute)
    asyncio.run(run_cooperative(client._bootstrap))
    assert client.pow_script_sources == ['/sdk'] and client.pow_data_build == 'bytes'
    assert calls[0][0] is parse_pow_resources_bytes and isinstance(calls[0][1][0], bytes)

def test_watchdog_still_delivers_timeout_when_normal_notifications_are_lost(monkeypatch):
    from curl_cffi.requests.exceptions import Timeout
    async def scenario():
        pending = set()
        async def handle(reader, writer):
            pending.add(asyncio.current_task())
            try:
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()
                pending.discard(asyncio.current_task())
        server = await asyncio.start_server(handle, '127.0.0.1', 0)
        session = ImageCurlSession(trust_env=False)
        driver = session.acurl
        original = driver.process_data
        def lost_notification(*args):
            hub = getattr(asyncio.get_running_loop(), '_image_curl_watchdog', None)
            if hub and asyncio.current_task() is hub.task:
                return original(*args)
        monkeypatch.setattr(driver, 'process_data', lost_notification)
        try:
            url = 'http://127.0.0.1:'+str(server.sockets[0].getsockname()[1])
            with pytest.raises(Timeout):
                await asyncio.wait_for(session.get(url, timeout=.2), 2)
        finally:
            await session.close()
            server.close(); await server.wait_closed()
            for task in tuple(pending): task.cancel()
            await asyncio.gather(*tuple(pending), return_exceptions=True)
        assert not driver._curl2future and driver._curlm is None
    asyncio.run(scenario())


def test_close_before_registration_and_new_hub_after_last_close():
    async def scenario():
        first = ImageAsyncCurl()
        await first.close()  # _force_timeout 尚未启动，不能残留注册。
        assert not getattr(asyncio.get_running_loop(), '_image_curl_watchdog', None)
        second = ImageAsyncCurl()
        await asyncio.sleep(0)
        hub = asyncio.get_running_loop()._image_curl_watchdog
        await second.close()
        third = ImageAsyncCurl()
        await asyncio.sleep(0)
        new_hub = asyncio.get_running_loop()._image_curl_watchdog
        assert new_hub is not hub and hub.task.done()
        await third.close()
        assert new_hub.task.done()
    asyncio.run(scenario())

def test_wrapper_total_timeout_does_not_wait_for_watchdog_grace(monkeypatch):
    from services.image_async_transport import AsyncImageSession
    from services.image_metrics import ImageMetrics, current_metrics
    from services.image_diagnostics import ImageDeadlineExceeded
    async def scenario():
        # 故意丢弃所有 curl 通知，验证业务总预算仍独立于 1 秒兜底周期。
        monkeypatch.setattr(ImageAsyncCurl, 'process_data', lambda *args: None)
        metrics = ImageMetrics(); metrics.diagnostics.configure_budget(.05)
        token = current_metrics.set(metrics)
        session = AsyncImageSession(trust_env=False)
        started = time.perf_counter()
        try:
            with pytest.raises((ImageDeadlineExceeded, TimeoutError)):
                await asyncio.wait_for(session._request('GET', 'http://127.0.0.1:9/'), .5)
            assert time.perf_counter() - started < .5
            assert session.closed and session.session._acurl._curlm is None
        finally:
            await session._close()
            current_metrics.reset(token)
    asyncio.run(scenario())
