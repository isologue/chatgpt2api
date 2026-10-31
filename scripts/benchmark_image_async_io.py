"""Loopback HTTP benchmark for image runtime; never contacts an upstream service.

Run the two modes in separate processes for comparable thread baselines:
  python scripts/benchmark_image_async_io.py --mode legacy --output output/image_io_legacy.json
  python scripts/benchmark_image_async_io.py --mode cooperative --output output/image_io_async.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import threading
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.image_metrics import ImageMetrics, current_metrics, runtime_phases
from services.image_transport import ImageBackendSession
from services.thread_pool import BoundedImageExecutor, configure_image_executors
from services.async_completion import submit_off_loop


def percentile(values, fraction):
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)] * 1000, 3)


async def main(options):
    state = {'active': 0, 'peak': 0, 'arrivals': []}
    async def handle(reader, writer):
        try:
            await reader.readuntil(b'\r\n\r\n')
            state['active'] += 1
            state['peak'] = max(state['peak'], state['active'])
            state['arrivals'].append(time.perf_counter())
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 14\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: ')
            await writer.drain()
            await asyncio.sleep(options.hold)
            writer.write(b'[DONE]\n\n')
            await writer.drain()
        finally:
            state['active'] -= 1
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(handle, '127.0.0.1', 0, backlog=1024)
    url = f'http://127.0.0.1:{server.sockets[0].getsockname()[1]}/'
    configure_image_executors(options.requests, 0)
    executor = BoundedImageExecutor(options.requests, 0)
    rows = []
    try:
        for run in ('cold', 'warm'):
            state.update(active=0, peak=0, arrivals=[])
            running = True
            latencies, lag = [], []
            baseline = thread_peak = threading.active_count()
            async def sample():
                nonlocal thread_peak
                while running:
                    target = time.perf_counter() + 0.01
                    await asyncio.sleep(0.01)
                    lag.append(max(0, time.perf_counter() - target))
                    thread_peak = max(thread_peak, threading.active_count())
            def work():
                session = ImageBackendSession(trust_env=False)
                try:
                    response = session.get(url, stream=True, timeout=10, image_stream_timeout=10)
                    try:
                        return b''.join(response.iter_content())
                    finally:
                        response.close()
                finally:
                    session.close()
            async def call():
                started = time.perf_counter()
                metric = ImageMetrics()
                metric.diagnostics.configure_budget(10)
                token = current_metrics.set(metric)
                try:
                    if options.mode == 'legacy':
                        # 同步执行器需要显式复制上下文；协作提交自己复制。
                        from contextvars import copy_context
                        future = await submit_off_loop(executor.submit, copy_context().run, work)
                    else:
                        executor.reserve()
                        future = await submit_off_loop(executor.submit_cooperative_reserved, work)
                    result = await asyncio.wrap_future(future)
                    assert result == b'data: [DONE]\n\n'
                finally:
                    current_metrics.reset(token)
                latencies.append(time.perf_counter() - started)
            started = time.perf_counter()
            sampler = asyncio.create_task(sample())
            try:
                await asyncio.gather(*(call() for _ in range(options.requests)))
            finally:
                running = False
                await sampler
            arrivals = state['arrivals']
            row = {
                'mode': options.mode, 'run': run, 'requests': options.requests,
                'server_hold_s': options.hold, 'wall_s': round(time.perf_counter() - started, 4),
                'latency_p50_ms': percentile(latencies, 0.5), 'latency_p95_ms': percentile(latencies, 0.95),
                'api_loop_lag_max_ms': percentile(lag, 1), 'thread_baseline': baseline, 'thread_peak': thread_peak,
                'arrival_spread_ms': round((max(arrivals) - min(arrivals)) * 1000, 3),
                'peak_server_requests': state['peak'],
                'image_io_loop_lag': runtime_phases.snapshot().get('image_io_loop_lag'),
            }
            rows.append(row)
            print(json.dumps(row), flush=True)
    finally:
        await asyncio.to_thread(executor.shutdown)
        server.close()
        await server.wait_closed()
    if options.output:
        path = Path(options.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'scope': 'loopback HTTP only; no TLS, proxy, PoW, image generation, decoding or durable logging', 'results': rows}, indent=2), encoding='utf-8')


if __name__ == '__main__':
    warnings.filterwarnings('ignore', message='Proactor event loop*')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('legacy', 'cooperative'), required=True)
    parser.add_argument('--requests', type=int, default=200)
    parser.add_argument('--hold', type=float, default=0.5)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
