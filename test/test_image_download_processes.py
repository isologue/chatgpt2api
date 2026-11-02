"""真实子进程和本机 HTTP 验证，不 mock IPC 或图片传输。"""
import asyncio
from contextlib import asynccontextmanager
import os

import pytest

from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession
from services.image_download_pool import DownloadProcessPool, DownloadWorkerError
from services.image_metrics import phase

BODY = bytes(range(256)) * 16384  # 4MiB，跨越多个 IPC 帧。


@asynccontextmanager
async def fixture_server():
    tasks, observed = set(), []
    hold = asyncio.Event()
    async def handle(reader, writer):
        tasks.add(asyncio.current_task())
        try:
            raw = await reader.readuntil(b'\r\n\r\n')
            path = raw.split(b' ')[1]
            observed.append(raw)
            body = BODY if path == b'/large' else b'ok'
            if path == b'/stall':
                body = b'not-complete'
            size = len(body) + (100 if path == b'/truncated' else 0)
            writer.write(b'HTTP/1.1 200 OK\r\nConnection: close\r\nSet-Cookie: result=one; Path=/\r\nContent-Length: ' + str(size).encode() + b'\r\n\r\n')
            await writer.drain()
            if path == b'/stall':
                writer.write(body[:1]); await writer.drain()
                await hold.wait()
            writer.write(body)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(asyncio.current_task())
    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    try:
        yield 'http://127.0.0.1:' + str(server.sockets[0].getsockname()[1]), observed
    finally:
        hold.set()
        server.close(); await server.wait_closed()
        await asyncio.gather(*tuple(tasks), return_exceptions=True)


def download(url, token, cookie, summaries=None, bulk=True):
    session = AsyncImageSession(trust_env=False, headers={'Authorization': 'Bearer ' + token})
    session.cookies.set('owner', cookie, domain='127.0.0.1', path='/')
    response = None
    try:
        with phase('image_download_http'):
            response = session.get(url, stream=True, timeout=3)
        with phase('image_download_body'):
            result = b''.join(response.read_image_chunks() if bulk else
                response.iter_content(chunk_size=256 * 1024))
        if summaries is not None:
            summaries.append(response.response.transport_summary)
        assert session.cookies.get('result') == 'one'
        return result
    finally:
        if response is not None:
            response.close()
        session.close()


@pytest.mark.parametrize('bulk', [False, True])
def test_workers_transfer_full_body_isolate_accounts_and_shutdown(bulk):
    async def run():
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(2)
        await pool.warm()
        processes = [w.process for w in pool.workers]
        assert all(p is not None and p.pid != os.getpid() for p in processes)
        try:
            async with fixture_server() as (url, seen):
                summaries = []
                results = await asyncio.gather(*(run_cooperative(download, url + '/large',
                    f'token-{i}', f'cookie-{i}', summaries, bulk) for i in range(8)))
                assert all(result == BODY for result in results)
                assert len(seen) == 8
                for i in range(8):
                    raw = next(raw for raw in seen if f'Bearer token-{i}\r\n'.encode() in raw)
                    assert f'owner=cookie-{i}'.encode() in raw
                    assert all(f'cookie-{j}'.encode() not in raw for j in range(8) if j != i)
                assert all(s['bytes'] == len(BODY) and s['termination'] == 'eof' for s in summaries)
                assert all(s['ipc_total_ms'] >= s['worker_total_ms'] for s in summaries)
                assert loop._image_download_limiter.active == 0
        finally:
            await pool.close()
        assert all(p.returncode is not None for p in processes)
    asyncio.run(run())


def test_truncated_response_and_dead_worker_release_slot_and_can_retry():
    async def run():
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(1)
        try:
            await pool.warm()
            async with fixture_server() as (url, seen):
                with pytest.raises(DownloadWorkerError):
                    await run_cooperative(download, url + '/truncated', 'a', 'a')
                assert loop._image_download_limiter.active == 0
                task = asyncio.create_task(run_cooperative(download, url + '/stall', 'a', 'a'))
                while len(seen) < 2:
                    await asyncio.sleep(.01)
                worker = pool.workers[0]
                old = worker.process
                old.kill()
                with pytest.raises(DownloadWorkerError):
                    await asyncio.wait_for(task, 3)
                assert loop._image_download_limiter.active == 0
                assert await run_cooperative(download, url + '/ok', 'b', 'b') == b'ok'
                assert worker.process.pid != old.pid
                assert loop._image_download_limiter.active == 0
        finally:
            await pool.close()
    asyncio.run(run())


def test_cancel_download_stops_worker_transfer_without_killing_other_requests():
    async def run():
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(1)
        try:
            await pool.warm()
            async with fixture_server() as (url, seen):
                task = asyncio.create_task(run_cooperative(download, url + '/stall', 'a', 'a'))
                while not seen:
                    await asyncio.sleep(.01)
                pid = pool.workers[0].process.pid
                task.cancel()
                from services.image_diagnostics import ImageRequestCancelled
                with pytest.raises(ImageRequestCancelled):
                    await asyncio.wait_for(task, 3)
                assert loop._image_download_limiter.active == 0
                assert not pool.workers[0].pending
                assert await run_cooperative(download, url + '/ok', 'b', 'b') == b'ok'
                assert pool.workers[0].process.pid == pid
        finally:
            await pool.close()
    asyncio.run(run())


def test_download_deadline_is_enforced_in_worker_and_releases_slot():
    async def run():
        from test.test_image_download_limiter import budget
        from services.image_diagnostics import ImageDeadlineExceeded
        from services.image_async import await_io
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(1)
        try:
            await pool.warm()
            async with fixture_server() as (url, seen):
                with budget(.08):
                    with pytest.raises(ImageDeadlineExceeded):
                        await run_cooperative(download, url + '/stall', 'a', 'a')
                assert loop._image_download_limiter.active == 0
                assert await run_cooperative(download, url + '/ok', 'b', 'b') == b'ok'
        finally:
            await pool.close()
    asyncio.run(run())


def test_download_total_limit_is_shared_by_all_worker_processes(monkeypatch):
    from services.config import config
    monkeypatch.setitem(config.data, 'image_download_concurrency', 2)
    async def run():
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(4)
        tasks = []
        try:
            await pool.warm()
            async with fixture_server() as (url, seen):
                tasks = [asyncio.create_task(run_cooperative(download, url + '/stall', str(i), str(i))) for i in range(8)]
                async with asyncio.timeout(2):
                    while len(seen) < 2:
                        await asyncio.sleep(.01)
                await asyncio.sleep(.08)
                assert len(seen) == 2
                assert loop._image_download_limiter.active == 2
                for task in tasks: task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                assert loop._image_download_limiter.active == 0
                assert not any(worker.pending for worker in pool.workers)
        finally:
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await pool.close()
    asyncio.run(run())


def test_simultaneous_worker_failure_does_not_kill_replacement_process():
    async def run():
        loop = asyncio.get_running_loop()
        pool = loop._image_download_pool = DownloadProcessPool(1)
        tasks = []
        try:
            await pool.warm()
            async with fixture_server() as (url, seen):
                tasks = [asyncio.create_task(run_cooperative(download, url + '/stall', str(i), str(i))) for i in range(8)]
                async with asyncio.timeout(2):
                    while len(seen) < 8:
                        await asyncio.sleep(.01)
                pool.workers[0].process.kill()
                results = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
                assert all(isinstance(result, DownloadWorkerError) for result in results)
                assert loop._image_download_limiter.active == 0
                assert await run_cooperative(download, url + '/large', 'new', 'new') == BODY
                assert not pool.workers[0].pending
        finally:
            for task in tasks: task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await pool.close()
    asyncio.run(run())
