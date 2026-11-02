"""常驻下载进程。每次尝试独享 curl Session，不导入配置、数据库或账号池。"""
from __future__ import annotations

import asyncio
import math
import os
import sys
import time

from curl_cffi import CurlInfo, CurlOpt
from services.image_ca import image_ca_options
from services.image_curl import ImageCurlSession
from services.image_download_ipc import (CHUNK, MAX_BODY, cookie_values, decode,
    receive, restore_cookies, send)


async def serve(port):
    reader, writer = await asyncio.open_connection('127.0.0.1', port)
    lock = asyncio.Lock()
    await send(writer, lock, b'A', 0, {'token': os.environ.pop('IMAGE_DOWNLOAD_WORKER_TOKEN')})
    tasks = {}

    async def download(identity, request):
        session = response = None
        started = time.perf_counter()
        headers_at = first = last = None
        count = chunks = 0
        gap = 0.
        summary = {'termination': 'error'}
        dispatch = {'count': 0, 'total_ms': 0., 'max_ms': 0., 'download_started': started}
        error = None
        try:
            remaining = request['deadline'] - time.monotonic()
            if remaining <= 0:
                raise TimeoutError()
            options = request['session']
            options['headers'] = [tuple(pair) for pair in options['headers']]
            if isinstance(request['kwargs'].get('headers'), list):
                request['kwargs']['headers'] = [tuple(pair) for pair in request['kwargs']['headers']]
            options['curl_options'] = {CurlOpt(int(key)): value
                for key, value in options.get('curl_options', {}).items()}
            options['curl_options'].update({CurlOpt.TIMEOUT_MS: max(1, math.ceil(remaining * 1000)),
                CurlOpt.MAXFILESIZE_LARGE: MAX_BODY})
            options['curl_options'].setdefault(CurlOpt.BUFFERSIZE, CHUNK)
            options['curl_options'] = image_ca_options(options['curl_options'],
                request['kwargs'].get('verify', options.get('verify', True)))
            session = ImageCurlSession(max_clients=1, **options)
            restore_cookies(session.cookies.jar, request['cookies'])
            session.acurl._transfer_wait = dispatch
            session.acurl._transfer_stage = 'image_download_body'
            session.acurl._transfer_is_sse = False
            async with asyncio.timeout(remaining):
                response = await session.request(request['method'], request['url'],
                    **{**request['kwargs'], 'stream': True, 'timeout': remaining})
                headers_at = time.perf_counter()
                await send(writer, lock, b'H', identity, {
                    'status': response.status_code, 'reason': response.reason,
                    'headers': response.headers.multi_items(),
                    'url': response.url, 'primary_ip': response.primary_ip,
                    'elapsed': response.elapsed.total_seconds(),
                    'infos': {str(int(k)): v for k, v in response.infos.items()},
                })
                # 合并已经到达的块，每帧最多 256KiB，不为凑帧等待新数据。
                iterator = response.aiter_content().__aiter__()
                while True:
                    try:
                        part = await anext(iterator)
                    except StopAsyncIteration:
                        break
                    batch = [part]
                    size = len(part)
                    while size < CHUNK and not response.queue.empty():
                        try:
                            part = await anext(iterator)
                        except StopAsyncIteration:
                            break
                        batch.append(part)
                        size += len(part)
                    now = time.perf_counter()
                    count += size
                    chunks += len(batch)
                    gap = max(gap, now - (last or headers_at))
                    first = first or now
                    last = now
                    if count > MAX_BODY:
                        summary['termination'] = 'body_limit'
                        raise ValueError('image body limit')
                    data = batch[0] if len(batch) == 1 else b''.join(batch)
                    for offset in range(0, len(data), CHUNK):
                        await send(writer, lock, b'B', identity, data[offset:offset + CHUNK])
                summary['termination'] = 'eof'
        except BaseException as exc:
            # 不传递异常原文；curl 错误可能带签名 URL、代理或凭据。
            code = getattr(exc, 'code', None)
            error = {'type': type(exc).__name__, 'code': code if isinstance(code, int) else None}
        finally:
            if response is not None and response.astream_task is not None and not response.astream_task.done():
                response.quit_now.set()
                session.acurl.remove_handle(response.curl)
                await asyncio.gather(response.astream_task, return_exceptions=True)
            cookies = cookie_values(session.cookies.jar) if session is not None else []
            if session is not None:
                await session.close()
            ended = time.perf_counter()
            body = max(.000001, ended - (headers_at or started))
            summary.update({'headers_ms': ((headers_at or ended) - started) * 1000,
                'first_chunk_ms': ((first or ended) - started) * 1000,
                'last_chunk_ms': ((last or headers_at or ended) - started) * 1000,
                'first_chunk_received': first is not None, 'bytes': count, 'chunks': chunks,
                'max_gap_ms': gap * 1000, 'idle_after_last_ms': (ended - (last or ended)) * 1000,
                'body_ms': body * 1000, 'bytes_per_sec': count / body,
                'expected_bytes': int(response.headers.get('content-length', '0'))
                    if response is not None and response.headers.get('content-length', '').isdigit()
                        and len(response.headers.get('content-length', '')) < 16 else 0,
                'worker_total_ms': (ended - started) * 1000,
                'dispatch_count': dispatch['count'], 'dispatch_wait_ms': dispatch['total_ms'],
                'dispatch_max_wait_ms': dispatch['max_ms'],
                'curl_error_code': error['code'] if error else None})
            if 'completed_at' in dispatch:
                summary.update({'curl_received_bytes': dispatch.get('received_bytes', 0),
                    'curl_transfer_ms': dispatch.get('transfer_ms', 0),
                    'curl_first_byte_ms': dispatch.get('first_byte_ms', 0),
                    'curl_completed_ms': (dispatch['completed_at'] - started) * 1000,
                    'completion_to_read_end_ms': max(0, ended - dispatch['completed_at']) * 1000})
            await send(writer, lock, b'T', identity, {'summary': summary, 'error': error, 'cookies': cookies})
            tasks.pop(identity, None)

    try:
        while True:
            kind, identity, data = await receive(reader)
            if kind == b'R':
                if identity in tasks:
                    raise ValueError('duplicate download identity')
                tasks[identity] = asyncio.create_task(download(identity, decode(data)))
                # Enter try/finally before an immediately following cancellation.
                await asyncio.sleep(0)
            elif kind == b'C':
                if identity in tasks:
                    tasks[identity].cancel()
            else:
                raise ValueError('invalid download command')
    finally:
        for task in tuple(tasks.values()):
            task.cancel()
        await asyncio.gather(*tuple(tasks.values()), return_exceptions=True)
        writer.close()
        await writer.wait_closed()


if __name__ == '__main__':
    try:
        asyncio.run(serve(int(sys.argv[1])))
    except (ConnectionError, asyncio.IncompleteReadError):
        pass
