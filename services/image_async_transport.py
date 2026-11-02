"""图片链路的异步 HTTP 适配器；每个账号尝试独享 headers/cookies/指纹/代理。"""
from __future__ import annotations

import asyncio
import math
import time

from curl_cffi import CurlInfo, CurlOpt
from curl_cffi.requests.models import loads as load_json
from services.image_curl import ImageCurlSession as AsyncSession
from services.image_ca import image_ca_options

from services.image_async import await_io, cooperative, run_blocking
from services.image_diagnostics import remaining_image_budget
from services.image_metrics import current_metrics
from services.image_performance import current_stage, record

MAX_IMAGE_BODY = 64 * 1024 * 1024
MAX_METADATA_BODY = 16 * 1024 * 1024
MAX_SSE_LINE = 1024 * 1024
# 小型 JSON 通常是账号/生成元数据。它已经完整读入且有正文上限，
# 直接解析可避免一次线程提交和一次完成通知；较大的 JSON 仍离开 I/O loop。
MAX_INLINE_JSON_BODY = 256 * 1024
# 已缓冲正文也要定期让出，防止一个响应连读时饿死其他请求。
READ_YIELD_BYTES = 256 * 1024
READ_YIELD_CHUNKS = 64
READ_YIELD_SECS = 0.002
LINE_YIELD_COUNT = 64


class AsyncImageResponse:
    def __init__(self, owner, response, started, stream_deadline, cap, is_sse, dispatch_wait=None, download_lease=None):
        self.owner = owner
        self.response = response
        self.started = started
        self.headers_at = time.perf_counter()
        self.stage = current_stage.get()
        from services.image_loop_work import get_loop_work
        self._download_work = (get_loop_work()
            if self.stage in {'image_download', 'image_download_http'} else None)
        self._diagnostic_driver = getattr(getattr(owner, 'session', None), '_acurl', None)
        self._read_stage = self.stage
        self._dispatch_wait = dispatch_wait
        self._download_lease = download_lease
        self._image_stream_deadline = stream_deadline if stream_deadline is not None else float('inf')
        self.cap = cap
        self.is_sse = is_sse
        self.bytes_read = self.chunks = 0
        self._bytes_since_yield = 0
        self._chunks_since_yield = 0
        self._read_quantum = time.perf_counter()
        self.first_at = self.last_at = None
        self.read_ended_at = None
        self.max_gap = 0.0
        self.ended = 'closed'
        self._closed = False
        self._curl_error_code = None
        self._iterator = response.aiter_content().__aiter__()

    def __getattr__(self, name):
        return getattr(self.response, name)

    def json(self, **kwargs):
        content = self.response.content
        if isinstance(content, (bytes, bytearray, memoryview)) and len(content) <= MAX_INLINE_JSON_BODY:
            return load_json(content, **kwargs)
        return await_io(run_blocking(load_json, content, **kwargs))

    async def _next(self, *, budget_guarded=False):
        stage = current_stage.get()
        if self._diagnostic_driver is not None and stage != self._read_stage:
            self._read_stage = self._diagnostic_driver._transfer_stage = stage
        remaining = None if budget_guarded else remaining_image_budget()
        try:
            if (self._bytes_since_yield >= READ_YIELD_BYTES
                    or self._chunks_since_yield >= READ_YIELD_CHUNKS
                    or time.perf_counter() - self._read_quantum >= READ_YIELD_SECS):
                self._bytes_since_yield = self._chunks_since_yield = 0
                await asyncio.sleep(0)
                self._read_quantum = time.perf_counter()
                remaining = remaining_image_budget()
            queue = getattr(self.response, 'queue', None)
            stream_task = getattr(self.response, 'astream_task', None)
            # 传输已结束时，已缓冲数据可立即取出，省去逐块创建预算计时器。
            # 传输仍在进行时，EOF/错误标记还会 await aclose，必须保留超时保护。
            buffered = queue is not None and not queue.empty()
            settled = stream_task is None or stream_task.done()
            if budget_guarded or remaining is None or (buffered and settled):
                chunk = await anext(self._iterator)
            else:
                async with asyncio.timeout(remaining):
                    chunk = await anext(self._iterator)
        except StopAsyncIteration:
            self.ended = 'eof'
            self.read_ended_at = time.perf_counter()
            # EOF 已等待底层流收尾；及时交接网络名额，后续图片校验和
            # 工作线程排队不应占用下载容量。close 仍负责完整诊断与清理。
            if self._download_lease is not None:
                await self._download_lease.release()
            raise
        except BaseException as exc:
            self._curl_error_code = getattr(exc, 'code', None)
            self.ended = 'error'
            self.read_ended_at = time.perf_counter()
            # 总预算/取消优先于网络错误与阶段超时。
            remaining_image_budget()
            raise
        now = time.perf_counter()
        if chunk:
            if self._download_work is not None:
                self._download_work.download_bytes(len(chunk))
            self.bytes_read += len(chunk)
            self._bytes_since_yield += len(chunk)
            self._chunks_since_yield += 1
            self.chunks += 1
            self.max_gap = max(self.max_gap, now - (self.last_at or self.headers_at))
            self.first_at = self.first_at or now
            self.last_at = now
        if self.bytes_read > self.cap:
            self.ended = 'body_limit'
            raise RuntimeError('image upstream response exceeds the body size limit')
        if not budget_guarded:
            remaining_image_budget()
        return chunk

    def read_image_chunks(self):
        """图片正文共用一次异步等待，块列表仍交给原有工作线程合并校验。"""
        return await_io(self._read_image_chunks())

    async def _read_image_chunks(self):
        chunks = []
        try:
            # 整段正文共用总预算计时器，避免每块往返同步协议和重建计时器。
            # _next 保留周期让出、取消检查、正文上限、诊断和 EOF 名额释放。
            async with asyncio.timeout(remaining_image_budget()):
                while True:
                    try:
                        chunk = await self._next(budget_guarded=True)
                    except StopAsyncIteration:
                        break
                    if chunk:
                        chunks.append(chunk)
            remaining_image_budget()
            return chunks
        except BaseException:
            remaining_image_budget()
            raise

    async def _read_ready(self, size):
        # 同一批已到达数据共用一个预算计时器；逐个小块建/撤计时器会放大
        # HTTP/2 碎片正文的事件循环开销。EOF/错误清理仍包含在预算内。
        try:
            async with asyncio.timeout(remaining_image_budget()):
                content = await self._read_ready_batch(size)
            remaining_image_budget()
            return content
        except BaseException:
            remaining_image_budget()
            raise

    async def _read_ready_batch(self, size):
        # 只合并已经到达的块，不为凑足 chunk_size 等待新数据；SSE 仍逐块读取。
        chunks = [await self._next(budget_guarded=True)]
        length = len(chunks[0])
        queue = getattr(self.response, 'queue', None)
        while length < size and queue is not None and not queue.empty():
            try:
                chunk = await self._next(budget_guarded=True)
            except StopAsyncIteration:
                break
            chunks.append(chunk)
            length += len(chunk)
        return chunks[0] if len(chunks) == 1 else b''.join(chunks)

    def iter_content(self, chunk_size=None, decode_unicode=False):
        if decode_unicode:
            raise ValueError('image stream must be read as bytes')
        size = min(chunk_size, READ_YIELD_BYTES) if chunk_size and chunk_size > 0 else None
        while True:
            try:
                yield await_io(self._read_ready(size) if size else self._next())
            except StopAsyncIteration:
                return

    def iter_lines(self, chunk_size=None, decode_unicode=False, delimiter=None):
        pending = b''
        line_count, quantum = 0, time.perf_counter()
        for chunk in self.iter_content():
            pending += chunk
            lines = pending.split(b'\n')
            pending = lines.pop()
            for line in lines:
                line_count += 1
                # 单块可能包含大量小事件；字节数阈值无法保证解析期间的公平调度。
                if (line_count >= LINE_YIELD_COUNT
                        or (line_count % 8 == 0 and time.perf_counter() - quantum >= READ_YIELD_SECS)):
                    remaining_image_budget()
                    await_io(asyncio.sleep(0))
                    remaining_image_budget()
                    line_count, quantum = 0, time.perf_counter()
                if len(line) > MAX_SSE_LINE:
                    self.ended = 'body_limit'
                    raise RuntimeError('image SSE line exceeds the size limit')
                line = line.rstrip(b'\r')
                yield line.decode('utf-8', errors='replace') if decode_unicode else line
            if len(pending) > MAX_SSE_LINE:
                self.ended = 'body_limit'
                raise RuntimeError('image SSE line exceeds the size limit')
        if pending:
            yield pending.decode('utf-8', errors='replace') if decode_unicode else pending

    async def _close(self):
        if self._closed:
            return
        self._closed = True
        try:
            stream_task = self.response.astream_task
            if stream_task is not None and not stream_task.done():
                # curl_cffi aclose() 本身只等待结束；静默连接要先撤销 handle 才能立即释放。
                self.response.quit_now.set()
                self.owner.session.acurl.remove_handle(self.response.curl)
                await asyncio.gather(stream_task, return_exceptions=True)
            if hasattr(self.response, 'transport_summary'):
                await self.response.aclose()
            self.owner.responses.discard(self)
            ended_at = time.perf_counter()
            if self._dispatch_wait:
                record('curl_queue_wait', self._dispatch_wait['total_ms'] / 1000, stage=self.stage)
                record('curl_queue_wait_max', self._dispatch_wait['max_ms'] / 1000, stage=self.stage)
            record('http_body', ended_at - self.headers_at, stage=self.stage)
            record('http_total', ended_at - self.started, stage=self.stage)
            is_download = self.stage in {'image_download', 'image_download_http'}
            if self.is_sse or is_download:
                now = time.perf_counter()
                metrics = current_metrics.get()
                if metrics is not None:
                    headers = getattr(self.response, 'headers', {})
                    length = headers.get('content-length', '')
                    expected = int(length) if str(length).isdigit() and len(str(length)) < 16 else 0
                    body_secs = max(.000001, ended_at - self.headers_at)
                    completion = self._dispatch_wait or {}
                    wire_at = completion.get('completed_at')
                    read_end = self.read_ended_at or ended_at
                    transfer = ({
                        'curl_received_bytes': completion['received_bytes'],
                        'curl_transfer_ms': completion['transfer_ms'],
                        'curl_first_byte_ms': completion['first_byte_ms'],
                        'curl_completed_ms': (wire_at - self.started) * 1000,
                        'completion_to_read_end_ms': max(0., read_end - wire_at) * 1000,
                    } if wire_at is not None and all(key in completion for key in
                        ('received_bytes', 'transfer_ms', 'first_byte_ms')) else {})
                    if is_download:
                        metrics.add_count('image_download_wire_bytes', self.bytes_read)
                    metrics.diagnostics.transport_summary({
                        'headers_ms': (self.headers_at - self.started) * 1000,
                        'first_chunk_ms': ((self.first_at or now) - self.started) * 1000,
                        'last_chunk_ms': ((self.last_at or self.headers_at) - self.started) * 1000,
                        'max_gap_ms': self.max_gap * 1000,
                        'idle_after_last_ms': (now - (self.last_at or self.headers_at)) * 1000,
                        'bytes': self.bytes_read, 'chunks': self.chunks,
                        'first_chunk_received': self.first_at is not None,
                        'termination': self.ended,
                        **({'body_ms': body_secs * 1000, 'bytes_per_sec': self.bytes_read / body_secs,
                            'expected_bytes': expected, 'curl_error_code': self._curl_error_code,
                            **transfer,
                            'dispatch_count': (self._dispatch_wait or {}).get('count', 0),
                            'dispatch_wait_ms': (self._dispatch_wait or {}).get('total_ms', 0),
                            'dispatch_max_wait_ms': (self._dispatch_wait or {}).get('max_ms', 0)}
                           if is_download else {}),
                        **getattr(self.response, 'transport_summary', {}),
                    }, phase='image_download_transport' if is_download else 'sse_transport')

        finally:
            if self._download_lease is not None:
                await self._download_lease.release()

    def close(self):
        if self._closed:
            return
        await_io(self._close())


class AsyncImageSession:
    def __init__(self, **kwargs):
        self.loop = asyncio.get_running_loop()
        # 每次账号尝试串行复用一个 handle；整体并发由图片任务 W/Q 控制，
        # 账号间不共享可变认证状态，也不受 AsyncSession 默认 10 的全局瓶颈限制。
        # 只读取连接计时，禁止采集请求头、URL、地址等 getinfo 字段。
        kwargs['curl_infos'] = list(dict.fromkeys([*kwargs.get('curl_infos', []),
            CurlInfo.NAMELOOKUP_TIME, CurlInfo.CONNECT_TIME, CurlInfo.APPCONNECT_TIME,
            CurlInfo.STARTTRANSFER_TIME]))
        self._session_kwargs = kwargs
        self.session = AsyncSession(max_clients=1, **kwargs)
        self.responses = set()
        self.closed = False

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return getattr(self.session, name)

    def request(self, method, url, **kwargs):
        return await_io(self._request(method, url, **kwargs))

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def patch(self, url, **kwargs):
        return self.request('PATCH', url, **kwargs)

    async def _request(self, method, url, **kwargs):
        lease = None
        if kwargs.get('stream') and current_stage.get() in {'image_download', 'image_download_http'}:
            from services.image_download_limiter import acquire_download_slot
            lease = await acquire_download_slot()
        try:
            return await self._request_admitted(method, url, lease, **kwargs)
        except BaseException:
            if lease is not None:
                await lease.release()
            raise

    async def _request_admitted(self, method, url, download_lease, **kwargs):
        from services.image_diagnostics import ImageStreamHardTimeoutError
        remaining_image_budget()
        if self.closed:
            # 下载阶段的某次传输超时可关闭底层 multi handle；原地址的后续下载
            # 重试重新建立 session，并保留本次账号的认证与 Cookie。
            self.session = AsyncSession(max_clients=1, **self._session_kwargs)
            self.closed = False
        started = time.perf_counter()
        stream_cap = kwargs.pop('image_stream_timeout', None)
        deadline = time.monotonic() + float(stream_cap) if stream_cap is not None else None
        remaining = remaining_image_budget(float(stream_cap) if stream_cap is not None else None)
        requested = kwargs.get('timeout', self.session.timeout)
        if isinstance(requested, tuple):
            requested = sum(float(part) for part in requested)
        total = min(remaining, requested) if remaining is not None and requested else remaining or requested or 120
        total = max(0.001, total)
        kwargs['timeout'] = total
        pool = getattr(self.loop, '_image_download_pool', None)
        if download_lease is not None and pool is not None:
            response = await pool.request(method, url, self, total, kwargs)
            record('http_headers', time.perf_counter() - started)
            for component, info in (('curl_dns', CurlInfo.NAMELOOKUP_TIME),
                                    ('curl_connect', CurlInfo.CONNECT_TIME),
                                    ('curl_tls', CurlInfo.APPCONNECT_TIME),
                                    ('curl_first_byte', CurlInfo.STARTTRANSFER_TIME)):
                value = response.infos.get(info)
                if isinstance(value, (int, float)):
                    record(component, value)
            wrapped = AsyncImageResponse(self, response, started, None,
                MAX_IMAGE_BODY, False, download_lease=download_lease)
            self.responses.add(wrapped)
            return wrapped
        is_stream = kwargs.get('stream', False)
        # 元数据也按流读取，未知 Content-Length 的正文同样不能无限预缓冲。
        kwargs['stream'] = True
        cap = MAX_IMAGE_BODY if is_stream and stream_cap is None else MAX_METADATA_BODY
        original_options = self.session.curl_options
        request_options = dict(original_options)
        if download_lease is not None:
            # 批量读取已到达的正文，减少高并发时的小缓冲 recv/回调轮转。
            # 仅作用于本次下载；保留显式选项，不影响 SSE 和账号元数据。
            request_options.setdefault(CurlOpt.BUFFERSIZE, 256 * 1024)
        self.session.curl_options = image_ca_options({
            **request_options, CurlOpt.TIMEOUT_MS: max(1, math.ceil(total * 1000)),
            CurlOpt.MAXFILESIZE_LARGE: cap,
        }, self.session.verify if kwargs.get('verify') is None else kwargs['verify'])
        # 单账号一次只执行一个 HTTP 传输；记录数值摘要，不向回调上下文放入请求信息。
        dispatch_wait = {'count': 0, 'total_ms': 0., 'max_ms': 0.}
        if download_lease is not None:
            dispatch_wait['download_started'] = started
        self.session.acurl._transfer_wait = dispatch_wait
        # 回调上下文没有业务 ContextVar，显式传固定阶段枚举供归因。
        self.session.acurl._transfer_stage = current_stage.get()
        self.session.acurl._transfer_is_sse = stream_cap is not None
        task = asyncio.create_task(self.session.request(method, url, **kwargs))
        headers_recorded = False
        try:
            # shield 保留请求的资源句柄，取消/超时时由 close() 先中断 curl 再回收。
            async with asyncio.timeout(total):
                response = await asyncio.shield(task)
            record('http_headers', time.perf_counter() - started)
            headers_recorded = True
            for component, info in (('curl_dns', CurlInfo.NAMELOOKUP_TIME),
                                    ('curl_connect', CurlInfo.CONNECT_TIME),
                                    ('curl_tls', CurlInfo.APPCONNECT_TIME),
                                    ('curl_first_byte', CurlInfo.STARTTRANSFER_TIME)):
                value = response.infos.get(info)
                if isinstance(value, (int, float)):
                    record(component, value)
            remaining_image_budget()
            wrapped = AsyncImageResponse(self, response, started, deadline, cap, stream_cap is not None, dispatch_wait, download_lease)
            self.responses.add(wrapped)
            if not is_stream:
                try:
                    body = bytearray()
                    # 整段元数据共用请求剩余预算，避免每个碎片重复创建计时器。
                    # _next 仍按字节/块数/2ms 让出，并检查取消、预算及正文上限。
                    async with asyncio.timeout(remaining_image_budget()):
                        while True:
                            try:
                                body.extend(await wrapped._next(budget_guarded=True))
                            except StopAsyncIteration:
                                break
                    remaining_image_budget()
                    response.content = bytes(body)
                finally:
                    await wrapped._close()
            return wrapped
        except BaseException as exc:
            if not task.done():
                self._session_kwargs = {
                    **self._session_kwargs, 'headers': dict(self.session.headers),
                    'cookies': self.session.cookies,
                }
                await self._close()
            await asyncio.gather(task, return_exceptions=True)
            remaining_image_budget()
            if deadline is not None and time.monotonic() >= deadline:
                raise ImageStreamHardTimeoutError('图片生成请求等待响应头超过硬上限') from exc
            raise
        finally:
            self.session.curl_options = original_options
            if not headers_recorded:
                record('http_headers', time.perf_counter() - started)
                record('http_total', time.perf_counter() - started)

    async def _close(self):
        if self.closed:
            return
        self.closed = True
        for response in tuple(self.responses):
            await response._close()
        await self.session.close()

    def close(self):
        if self.closed:
            return
        if cooperative():
            await_io(self._close())
        elif not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._close(), self.loop)
