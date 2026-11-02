"""主图片循环的下载进程池；全局名额仍由 DownloadLimiter 唯一控制。"""
from __future__ import annotations

import asyncio
from datetime import timedelta
import os
from pathlib import Path
import secrets
import sys
import time

from curl_cffi import CurlInfo
from curl_cffi.requests.headers import Headers
from curl_cffi.requests.models import Response

from services.image_download_ipc import (MAX_BODY, cookie_values, decode,
    receive, restore_cookies, send)


class DownloadWorkerError(RuntimeError):
    def __init__(self, kind='WorkerUnavailable', code=None):
        self.code = code
        super().__init__(f'image download worker: {kind}' + (f' (curl {code})' if code else ''))


class WorkerResponse(Response):
    def __init__(self, worker, identity, owner):
        super().__init__()
        self.worker, self.identity, self.owner = worker, identity, owner
        self.process = worker.process
        self.queue = asyncio.Queue()
        self.headers_ready = asyncio.get_running_loop().create_future()
        self.finished = asyncio.Event()
        self.astream_task = None
        self.transport_summary = {}
        self.received = 0
        self.acknowledged = False
        self.started = time.perf_counter()
        self._closed = False

    async def aiter_content(self, **kwargs):
        while True:
            item = await self.queue.get()
            if item is None:
                return
            if isinstance(item, Exception):
                raise item
            yield item

    def fail(self, error):
        if not self.headers_ready.done():
            # 用结果传递失败，避免取消与死亡竞态产生未提取的 Future 异常。
            self.headers_ready.set_result(error)
        self.queue.put_nowait(error)
        self.finished.set()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        try:
            if not self.acknowledged:
                try:
                    async with asyncio.timeout(2):
                        if not self.finished.is_set():
                            await self.worker.command(b'C', self.identity)
                            await self.finished.wait()
                        if not self.acknowledged:
                            await self.worker.abort(self)
                except (TimeoutError, ConnectionError, OSError):
                    # 无法确认撤销时终止进程，之后才交还全局下载名额。
                    await self.worker.abort(self)
        finally:
            self.worker.pending.pop(self.identity, None)
            while not self.queue.empty():
                self.queue.get_nowait()


class DownloadWorker:
    def __init__(self):
        self.pending = {}
        self.process = self.reader_task = self.writer = None
        self.lock, self.start_lock = asyncio.Lock(), asyncio.Lock()
        self.sequence = 0
        self.listener = None
        self.closed = False

    async def start(self):
        async with self.start_lock:
            if self.closed:
                raise DownloadWorkerError('PoolClosed')
            if self.process is not None and self.process.returncode is None and self.reader_task is not None and not self.reader_task.done():
                return
            await self._stop()
            token = secrets.token_hex(32)
            connected = asyncio.get_running_loop().create_future()
            candidates = set()

            async def accept(reader, writer):
                task = asyncio.current_task()
                candidates.add(task)
                handed = False
                try:
                    async with asyncio.timeout(3):
                        kind, identity, data = await receive(reader)
                    valid = kind == b'A' and identity == 0 and secrets.compare_digest(decode(data).get('token', ''), token)
                    if valid and not connected.done():
                        connected.set_result((reader, writer))
                        handed = True
                except (Exception,):
                    pass
                finally:
                    if not handed:
                        writer.close()
                        await writer.wait_closed()
                    candidates.discard(task)

            server = self.listener = await asyncio.start_server(accept, '127.0.0.1', 0)
            try:
                env = {**os.environ, 'IMAGE_DOWNLOAD_WORKER_TOKEN': token}
                self.process = await asyncio.create_subprocess_exec(sys.executable, '-m',
                    'services.image_download_worker', str(server.sockets[0].getsockname()[1]),
                    cwd=str(Path(__file__).resolve().parents[1]), env=env,
                    stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL)
                async with asyncio.timeout(15):
                    reader, self.writer = await connected
                self.reader_task = asyncio.create_task(self.read(reader))
            except BaseException:
                if connected.done() and not connected.cancelled() and self.writer is None:
                    _, writer = connected.result()
                    writer.close()
                await self._stop()
                raise
            finally:
                server.close()
                # Python 3.13 wait_closed 还等待已接受的长连接；在 stop 时等待。
                for task in tuple(candidates):
                    task.cancel()
                await asyncio.gather(*tuple(candidates), return_exceptions=True)

    async def command(self, kind, identity, value=None):
        if self.writer is None:
            raise DownloadWorkerError()
        await send(self.writer, self.lock, kind, identity, value or {})

    async def read(self, reader):
        try:
            while True:
                kind, identity, data = await receive(reader)
                response = self.pending.get(identity)
                if response is None:
                    continue  # 已撤销的传输可能还有一帧在管道内。
                if kind == b'B':
                    response.received += len(data)
                    if response.received > MAX_BODY:
                        raise ValueError('download response exceeds limit')
                    if not response._closed:
                        response.queue.put_nowait(data)
                elif kind == b'H':
                    fields = decode(data)
                    response.status_code = fields['status']
                    response.reason = fields['reason']
                    response.url, response.primary_ip = fields['url'], fields['primary_ip']
                    response.headers = Headers([tuple(pair) for pair in fields['headers']])
                    response.elapsed = timedelta(seconds=fields['elapsed'])
                    response.infos = {CurlInfo(int(k)): v for k, v in fields['infos'].items()}
                    if not response.headers_ready.done():
                        response.headers_ready.set_result(None)
                elif kind == b'T':
                    fields = decode(data)
                    response.transport_summary.update(fields['summary'])
                    response.transport_summary['ipc_total_ms'] = (time.perf_counter() - response.started) * 1000
                    restore_cookies(response.owner.session.cookies.jar, fields['cookies'])
                    response.acknowledged = True
                    error = fields['error']
                    if error:
                        response.fail(DownloadWorkerError(error['type'], error['code']))
                    else:
                        if not response.headers_ready.done():
                            raise ValueError('download ended without headers')
                        response.queue.put_nowait(None)
                        response.finished.set()
                    # 已结束的响应由调用方持有，不能让慢校验阻碍负载均衡。
                    self.pending.pop(identity, None)
                else:
                    raise ValueError('invalid download response')
        except BaseException:
            for response in tuple(self.pending.values()):
                response.fail(DownloadWorkerError())
            self.pending.clear()
            if self.process is not None and self.process.returncode is None:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass

    async def abort(self, response):
        # 多个旧响应同时处理崩溃时，只能终止它们原来的进程，不能误杀补起的新进程。
        async with self.start_lock:
            if self.process is response.process:
                await self._stop()

    async def stop(self):
        async with self.start_lock:
            await self._stop()

    async def _stop(self):
        for response in tuple(self.pending.values()):
            response.fail(DownloadWorkerError())
        self.pending.clear()
        reader_task, self.reader_task = self.reader_task, None
        if reader_task is not None and reader_task is not asyncio.current_task():
            reader_task.cancel()
            await asyncio.gather(reader_task, return_exceptions=True)
        process, self.process = self.process, None
        if process is not None:
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            try:
                async with asyncio.timeout(2):
                    await process.wait()
            except TimeoutError:
                process.kill()
                await process.wait()
        writer, self.writer = self.writer, None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

        if self.listener is not None:
            await self.listener.wait_closed()
            self.listener = None

    async def request(self, method, url, owner, timeout, kwargs):
        deadline = time.monotonic() + timeout
        response = None
        try:
            async with asyncio.timeout(timeout):
                await self.start()
                self.sequence += 1
                response = WorkerResponse(self, self.sequence, owner)
                session = owner.session
                # 当前账号 Session 的状态快照。Cookie 保留 domain/path/secure，
                # 不退化为 Cookie 头；重定向仍由 curl 的原认证边界处理。
                options = {key: getattr(session, key) for key in (
                    'verify', 'impersonate', 'default_headers', 'trust_env', 'proxies',
                    'proxy_auth', 'auth', 'allow_redirects', 'max_redirects', 'http_version',
                    'interface', 'cert', 'ja3', 'akamai', 'extra_fp')}
                options['headers'] = session.headers.multi_items()
                options['curl_options'] = {str(int(k)): v for k, v in session.curl_options.items()}
                options['curl_infos'] = [CurlInfo.NAMELOOKUP_TIME, CurlInfo.CONNECT_TIME,
                    CurlInfo.APPCONNECT_TIME, CurlInfo.STARTTRANSFER_TIME]
                self.pending[response.identity] = response
                await self.command(b'R', response.identity, {'method': method, 'url': url,
                    'deadline': deadline, 'session': options, 'cookies': cookie_values(session.cookies.jar),
                    'kwargs': kwargs})
                error = await response.headers_ready
                if error is not None:
                    raise error
                return response
        except BaseException:
            if response is not None:
                await response.aclose()
            raise


class DownloadProcessPool:
    def __init__(self, count=4):
        self.workers = [DownloadWorker() for _ in range(count)]
        self.cursor = 0
        self.closed = False

    async def warm(self):
        # 预热不携带调用方 ContextVar 或账号，启动失败由实际下载尝试报告。
        await asyncio.gather(*(w.start() for w in self.workers), return_exceptions=True)

    async def request(self, method, url, owner, timeout, kwargs):
        if self.closed:
            raise DownloadWorkerError('PoolClosed')
        ordered = self.workers[self.cursor:] + self.workers[:self.cursor]
        worker = min(ordered, key=lambda w: len(w.pending))
        self.cursor = (self.workers.index(worker) + 1) % len(self.workers)
        return await worker.request(method, url, owner, timeout, kwargs)

    async def close(self):
        self.closed = True
        for worker in self.workers:
            worker.closed = True
        # 先等待可能仍在进行的预热，再回收进程，防止关闭后启动新进程。
        async def close_worker(worker):
            async with worker.start_lock:
                await worker._stop()
        await asyncio.gather(*(close_worker(w) for w in self.workers))
