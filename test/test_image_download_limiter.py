import asyncio
import json
import time
from contextlib import contextmanager

import pytest

from services.config import ConfigStore, config
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession
from services.image_diagnostics import ImageDeadlineExceeded
from services.image_download_limiter import DownloadLimiter
from services.image_metrics import ImageMetrics, current_metrics, phase


@contextmanager
def budget(seconds):
    metrics = ImageMetrics()
    metrics.diagnostics.configure_budget(seconds)
    token = current_metrics.set(metrics)
    try:
        yield metrics
    finally:
        current_metrics.reset(token)


def test_waiting_download_uses_total_budget_and_does_not_leak_a_slot(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        lease = await limiter.acquire()
        with budget(.02) as metrics:
            with pytest.raises(ImageDeadlineExceeded):
                await limiter.acquire()
            steps = metrics.diagnostics.snapshot()['steps']
            assert any(s['phase'] == 'image_download_wait' and s['status'] == 'failed' for s in steps)
        assert limiter.active == 1
        await lease.release()
        await lease.release()
        assert limiter.active == 0
        next_lease = await asyncio.wait_for(limiter.acquire(), .2)
        await next_lease.release()
    asyncio.run(scenario())


def test_cancelled_waiter_does_not_prevent_other_downloads(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        first = await limiter.acquire()
        cancelled = asyncio.create_task(limiter.acquire())
        survivor = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        await first.release()
        lease = await asyncio.wait_for(survivor, .2)
        assert limiter.active == 1
        await lease.release()
        assert limiter.active == 0
    asyncio.run(scenario())


def test_raising_download_limit_wakes_capacity_on_next_release(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        first = await limiter.acquire()
        waiters = [asyncio.create_task(limiter.acquire()) for _ in range(4)]
        await asyncio.sleep(0)
        monkeypatch.setitem(config.data, 'image_download_concurrency', 4)
        await first.release()
        leases = await asyncio.wait_for(asyncio.gather(*waiters), .2)
        assert limiter.active == 4
        await asyncio.gather(*(lease.release() for lease in leases))
        assert limiter.active == 0
    asyncio.run(scenario())


def test_download_admission_delays_network_start_but_not_metadata(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        handlers = set()
        first_body = asyncio.Event()
        release = asyncio.Event()
        paths = []
        async def handle(reader, writer):
            handlers.add(asyncio.current_task())
            try:
                raw = await reader.readuntil(b'\r\n\r\n')
                path = raw.split(b' ')[1].decode()
                paths.append(path)
                writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\no')
                await writer.drain()
                if path == '/first':
                    first_body.set()
                    await release.wait()
                writer.write(b'k')
                await writer.drain()
            finally:
                writer.close()
                try: await writer.wait_closed()
                except ConnectionError: pass
                handlers.discard(asyncio.current_task())
        server = await asyncio.start_server(handle, '127.0.0.1', 0)
        url = 'http://127.0.0.1:' + str(server.sockets[0].getsockname()[1])
        def work(path, download):
            session = AsyncImageSession(trust_env=False)
            response = None
            try:
                with phase('image_download_http' if download else 'upstream_bootstrap'):
                    response = session.get(url + path, stream=download, timeout=1 if path == '/first' or not download else .15)
                return b''.join(response.iter_content(chunk_size=65536)) if download else response.content
            finally:
                if response: response.close()
                session.close()
        async def request(path, download=True):
            with budget(2):
                return await run_cooperative(work, path, download)
        first = asyncio.create_task(request('/first'))
        second = None
        try:
            await asyncio.wait_for(first_body.wait(), 1)
            second = asyncio.create_task(request('/second'))
            assert await asyncio.wait_for(request('/metadata', False), 1) == b'ok'
            assert '/second' not in paths
            # 超过单次下载超时的等待不应提前打开连接或触发重试。
            await asyncio.sleep(.2)
            assert '/second' not in paths
            release.set()
            assert await first == b'ok'
            assert await asyncio.wait_for(second, 1) == b'ok'
            assert paths.count('/second') == 1
            assert asyncio.get_running_loop()._image_download_limiter.active == 0
        finally:
            release.set()
            await asyncio.gather(*(t for t in (first, second) if t), return_exceptions=True)
            server.close(); await server.wait_closed()
            await asyncio.gather(*tuple(handlers), return_exceptions=True)
    asyncio.run(scenario())


def test_download_limit_persists_without_hidden_upper_bound(tmp_path):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({'auth-key': 'test'}), encoding='utf8')
    store = ConfigStore(path)
    assert store.get()['image_download_concurrency'] == 64
    store.update({'image_download_concurrency': '10000'})
    assert ConfigStore(path).image_download_concurrency == 10000


@pytest.mark.parametrize('failure', [OSError, asyncio.CancelledError])
def test_header_failure_or_cancellation_returns_download_slot(monkeypatch, failure):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def fail(*args, **kwargs):
        raise failure('fixture')
    monkeypatch.setattr(AsyncImageSession, '_request_admitted', fail)
    async def scenario():
        session = AsyncImageSession(trust_env=False)
        try:
            with budget(1), phase('image_download_http'):
                with pytest.raises(failure):
                    await session._request('GET', 'http://unused.test', stream=True)
            assert asyncio.get_running_loop()._image_download_limiter.active == 0
        finally:
            await session._close()
    asyncio.run(scenario())


def test_newcomer_cannot_steal_reserved_slot_from_existing_waiter(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        first = await limiter.acquire()
        order = []
        async def use(name):
            lease = await limiter.acquire()
            order.append(name)
            await asyncio.sleep(0)
            await lease.release()
        waiting = asyncio.create_task(use('waiting'))
        await asyncio.sleep(0)
        # 新请求先排入 loop 的 ready 队列，旧等待者还未获得运行机会。
        newcomer = asyncio.create_task(use('newcomer'))
        await first.release()
        await asyncio.wait_for(asyncio.gather(waiting, newcomer), .5)
        assert order == ['waiting', 'newcomer']
        assert limiter.active == 0 and not limiter.waiters
    asyncio.run(scenario())


def test_cancel_after_slot_handoff_returns_reserved_capacity(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        first = await limiter.acquire()
        waiting = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        await first.release()
        waiting.cancel()  # 已预留名额、尚未恢复业务协程的边界。
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert limiter.active == 0 and not limiter.waiters
        final = await asyncio.wait_for(limiter.acquire(), .2)
        await final.release()
    asyncio.run(scenario())


def test_lowering_limit_keeps_existing_downloads_and_does_not_overadmit(monkeypatch):
    monkeypatch.setitem(config.data, 'image_download_concurrency', 3)
    async def scenario():
        limiter = DownloadLimiter()
        leases = [await limiter.acquire() for _ in range(3)]
        monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
        waiting = asyncio.create_task(limiter.acquire())
        await asyncio.sleep(0)
        for lease in leases[:2]:
            await lease.release()
            assert not waiting.done()
        await leases[2].release()
        final = await asyncio.wait_for(waiting, .2)
        assert limiter.active == 1
        await final.release()
    asyncio.run(scenario())


@pytest.mark.parametrize('bulk', [False, True])
def test_completed_body_releases_download_slot_before_image_validation(monkeypatch, bulk):
    from types import SimpleNamespace
    from services.image_async_transport import AsyncImageResponse
    monkeypatch.setitem(config.data, 'image_download_concurrency', 1)
    async def scenario():
        limiter = DownloadLimiter()
        lease = await limiter.acquire()
        async def content():
            yield b'body'
        raw = SimpleNamespace(aiter_content=content, astream_task=None, headers={})
        with phase('image_download_http'):
            response = AsyncImageResponse(SimpleNamespace(responses=set()), raw,
                time.perf_counter(), None, 1024, False, download_lease=lease)
        if bulk:
            assert await response._read_image_chunks() == [b'body']
            assert response.bytes_read == 4 and response.chunks == 1
            assert response.ended == 'eof'
        else:
            assert await response._next() == b'body'
            assert limiter.active == 1
            with pytest.raises(StopAsyncIteration):
                await response._next()
        # 图片校验可能还在等待工作线程；网络名额已经可供下一张图片使用。
        next_lease = await asyncio.wait_for(limiter.acquire(), .2)
        assert not response._closed and limiter.active == 1
        await response._close()
        assert limiter.active == 1
        await next_lease.release()
        assert limiter.active == 0
    asyncio.run(scenario())
