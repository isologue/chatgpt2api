"""Local-only mixed HTTP/HTML/PoW benchmark. No external requests or credentials."""
from __future__ import annotations
import argparse
import asyncio
import json
import math
import os
import sys
import threading
import time
import zlib
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import cpu_pool
from services.image_async import blocking
from services.image_metrics import ImageMetrics, current_metrics, phase, runtime_phases
from services.image_transport import ImageBackendSession
from services.async_completion import await_worker, submit_off_loop
from services.thread_pool import BoundedImageExecutor, configure_image_executors
from utils.pow import parse_pow_resources, parse_pow_resources_bytes, build_proof_token, build_legacy_requirements_token


def stats(values):
    v = sorted(values)
    return {k: round(v[max(0, math.ceil(len(v)*q)-1)]*1000, 3) for k,q in [('p50_ms',.5),('p95_ms',.95),('max_ms',1)]} if v else {}


async def main(options):
    if getattr(options, "curl_watchdog", "tuned") == "legacy":
        from curl_cffi.requests import AsyncSession
        import services.image_async_transport as transport
        transport.AsyncSession = AsyncSession
    if getattr(options, 'handoff', 'direct') == 'staged':
        # 对照只改变完成通知路径；不改变执行器、计算或网络正文。
        import services.async_completion as completions
        async def staged(submit, *args):
            future = await completions.submit_off_loop(submit, *args)
            return await completions.await_worker(future)
        completions.await_submitted = staged
    os.environ['CHATGPT2API_CPU_POOL_WORKERS'] = str(options.cpu_workers)
    await asyncio.to_thread(cpu_pool.warmup)
    html = b'<html data-build="sandbox"><head><script src="/c/sandbox/_sdk.js"></script></head><body>'
    html += b'<div class="sandbox" data-x="value">local benchmark</div>' * (options.html_kib*1024//56)
    html += b'</body></html>'
    image = b'x' * (options.image_kib*1024)
    checksum = zlib.crc32(image)
    cpu_call = blocking(cpu_pool.run) if options.mode == 'threaded' else cpu_pool.run
    parse = blocking(parse_pow_resources) if options.mode == 'threaded' else lambda html: cpu_pool.compute(parse_pow_resources, html)
    requirements = blocking(build_legacy_requirements_token) if options.mode == 'threaded' else build_legacy_requirements_token
    active, peak, open_connections = 0, 0, set()
    async def handle(reader, writer):
        nonlocal active, peak
        open_connections.add(asyncio.current_task())
        active += 1
        peak = max(peak, active)
        try:
            headers = await reader.readuntil(b'\r\n\r\n')
            path = headers.split(b' ')[1]
            body = html if path == b'/' else image if path == b'/image' else b'{"ok":true}'
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: '+str(len(body)).encode()+b'\r\nConnection: close\r\n\r\n')
            await writer.drain()
            await asyncio.sleep(.01)
            for offset in range(0, len(body), 64*1024):
                writer.write(body[offset:offset+64*1024])
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            active -= 1
            open_connections.discard(asyncio.current_task())
    server = await asyncio.start_server(handle, '127.0.0.1', 0, backlog=1024)
    url = 'http://127.0.0.1:'+str(server.sockets[0].getsockname()[1])
    configure_image_executors(options.requests, 0)
    executor = BoundedImageExecutor(options.requests, 0)
    latencies, diags, lag = [], [], []
    running = True
    thread_base = thread_peak = threading.active_count()
    async def monitor():
        nonlocal thread_peak
        while running:
            target=time.perf_counter()+.01
            await asyncio.sleep(.01)
            lag.append(max(0,time.perf_counter()-target))
            thread_peak=max(thread_peak,threading.active_count())
    def work():
        session=ImageBackendSession(trust_env=False)
        try:
            with phase('upstream_bootstrap'):
                r=session.get(url+'/', timeout=30)
                if getattr(options, "decode", "worker") == "worker" and options.mode == "native":
                    sources,build=cpu_pool.compute(parse_pow_resources_bytes, r.content, r.encoding)
                else:
                    sources,build=parse(r.text)
            with phase('requirements_token'):
                requirements('sandbox',sources,build)
            with phase('pow_solve'):
                cpu_call(build_proof_token,'sandbox-seed','ff','sandbox',sources,build)
            with phase('result_resolve'):
                for _ in range(2):
                    assert session.get(url+'/metadata',timeout=30).json()['ok']
            with phase('image_download'):
                r=session.get(url+'/image',stream=True,timeout=30)
                try:
                    data=b''.join(r.iter_content(chunk_size=64*1024))
                    assert blocking(zlib.crc32)(data)==checksum
                finally:r.close()
            return True
        finally:session.close()
    async def call():
        metric=ImageMetrics(); metric.diagnostics.configure_budget(90)
        context=current_metrics.set(metric);started=time.perf_counter()
        try:
            executor.reserve()
            future=await submit_off_loop(executor.submit_cooperative_reserved, work)
            assert await await_worker(future)
            latencies.append(time.perf_counter()-started)
            diags.append(metric.diagnostics.snapshot())
        finally:current_metrics.reset(context)
    sampler=asyncio.create_task(monitor());started=time.perf_counter()
    try:
        await asyncio.gather(*(call() for _ in range(options.requests)))
    finally:
        running=False;await sampler
        await asyncio.to_thread(executor.shutdown)
        server.close();await server.wait_closed()
        if open_connections:await asyncio.gather(*open_connections,return_exceptions=True)
        await asyncio.to_thread(cpu_pool.shutdown)
    components={}
    for d in diags:
        for item in d.get('performance',[]):
            components.setdefault(item['stage']+'/'+item['component'],[]).append(item['total_ms']/1000)
    result={
        'scope':'loopback synthetic HTML + process PoW + metadata + image body; excludes real upstream, TLS, proxy, image decode and persistence',
        'curl_watchdog':getattr(options, 'curl_watchdog', 'tuned'),'decode':getattr(options, 'decode', 'worker'),
        'mode':options.mode,'handoff':getattr(options, 'handoff', 'direct'),'requests':options.requests,'html_bytes':len(html),'image_bytes':len(image),'cpu_workers':options.cpu_workers,
        'wall_s':round(time.perf_counter()-started,3),'latency':stats(latencies),
        'api_loop_lag':stats(lag),'image_loop_lag':runtime_phases.snapshot().get('image_io_loop_lag'),
        'threads_baseline':thread_base,'threads_peak':thread_peak,'server_connections_peak':peak,
        'performance':{k:stats(v) for k,v in sorted(components.items())},
    }
    if options.output:Path(options.output).write_text(json.dumps(result,indent=2),encoding='utf8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('performance','image_loop_lag')},indent=2))
    print('image_loop_peak_ms',result.get('image_loop_lag',{}).get('max_ms'))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--curl-watchdog', choices=('legacy','tuned'), default='tuned')
    parser.add_argument('--decode', choices=('loop','worker'), default='worker')
    parser.add_argument('--handoff',choices=('staged','direct'),default='direct')
    parser.add_argument('--mode', choices=('threaded','native'), default='native')
    parser.add_argument('--requests',type=int,default=200)
    parser.add_argument('--html-kib',type=int,default=256)
    parser.add_argument('--image-kib',type=int,default=2048)
    parser.add_argument('--cpu-workers',type=int,default=4)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
