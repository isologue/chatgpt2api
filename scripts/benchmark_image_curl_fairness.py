"""本地合成 curl 就绪风暴对照；固定处理成本，无网络、磁盘或上游请求。"""
from __future__ import annotations
import argparse
import asyncio
import json
import math
from pathlib import Path
import statistics
import sys
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from curl_cffi import AsyncCurl
from services.image_curl import ImageAsyncCurl, _CallbackQueue
from services.image_loop_work import get_loop_work
from scripts.benchmark_image_curl import load_baseline


def percentile(values, q):
    values = sorted(values)
    return round(values[max(0, math.ceil(len(values) * q) - 1)] * 1000, 3) if values else None


async def trial(cls, options, duplicates):
    loop = asyncio.get_running_loop()
    queue_type = cls.process_data.__globals__.get('_CallbackQueue')
    queue = (queue_type or _CallbackQueue)(loop)
    work = get_loop_work()
    calls, latencies, lag = [], [], []
    done, stopped = asyncio.Event(), False
    expected = options.requests * (1 if queue_type is not None else duplicates)
    def socket(driver, fd, mask):
        until = time.perf_counter() + options.callback_us / 1e6
        while time.perf_counter() < until: pass
        driver._received |= mask
        calls.append(1)
        latencies.append(time.perf_counter() - started)
        if len(calls) == expected: done.set()
        return 0
    def process(driver, fd, mask): driver.socket_action(fd, mask)
    drivers = []
    for _ in range(options.requests):
        driver = object.__new__(cls)
        driver.loop = loop; driver._curlm = True; driver._closing = False
        driver._curl2future = {1: None}; driver._sockfds = {10}
        driver._pending_events = set(); driver._dispatcher = queue
        driver._socket_interests = {}; driver._paused_sockets = set()
        driver._socket_wall = driver._socket_cpu = 0.; driver._last_action = 0.
        driver._loop_work = work; driver._received = 0
        drivers.append(driver)
    async def monitor():
        while not stopped:
            target = time.perf_counter() + .001
            await asyncio.sleep(.001)
            lag.append(max(0., time.perf_counter() - target))
    observer = asyncio.create_task(monitor()); await asyncio.sleep(0)
    started, cpu_started = time.perf_counter(), time.process_time()
    try:
        with patch.object(AsyncCurl, 'process_data', process), patch.object(AsyncCurl, 'socket_action', socket):
            for driver in drivers:
                for i in range(duplicates): loop.call_soon(driver.process_data, 10, 1 if i % 2 == 0 else 2)
            async with asyncio.timeout(30): await done.wait()
            while queue.handle is not None: await asyncio.sleep(0)
    finally:
        wall, cpu = time.perf_counter() - started, time.process_time() - cpu_started
        stopped = True; await observer
    assert all(d._received == (1 if duplicates == 1 else 3) for d in drivers)
    assert not queue.pending and all(not d._pending_events for d in drivers)
    work.flush(force=True)
    return {'callbacks': len(calls), 'notifications': options.requests * duplicates,
        'wall_ms': round(wall * 1000, 3), 'cpu_ms': round(cpu * 1000, 3),
        'callback_finish_p95_ms': percentile(latencies, .95), 'loop_peak_ms': percentile(lag, 1)}


async def main(options):
    baseline, _ = load_baseline(options.baseline_ref, options.baseline_file)
    results = []
    for duplicates in (1, 3):
        for repeat in range(options.repeats):
            modes = [('baseline', baseline), ('current', ImageAsyncCurl)]
            for mode, cls in modes if repeat % 2 == 0 else reversed(modes):
                result = await trial(cls, options, duplicates)
                results.append(dict(result, mode=mode, repeat=repeat+1, duplicates=duplicates))
    summary = {}
    for duplicates in (1, 3):
        summary[str(duplicates)] = {}
        for mode in ('baseline', 'current'):
            items = [r for r in results if r['mode'] == mode and r['duplicates'] == duplicates]
            summary[str(duplicates)][mode] = {k: round(statistics.median(r[k] for r in items), 3)
                for k in ('callbacks', 'wall_ms', 'cpu_ms', 'callback_finish_p95_ms', 'loop_peak_ms')}
    data = {'scope': 'synthetic fixed-cost callbacks; no network or upstream',
        'baseline_ref': options.baseline_ref, 'requests': options.requests,
        'callback_us': options.callback_us, 'results': results, 'median': summary}
    if options.output: Path(options.output).write_text(json.dumps(data, indent=2), encoding='utf8')
    print(json.dumps(data['median'], indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', default='4c0ebd7')
    parser.add_argument('--baseline-file')
    parser.add_argument('--requests', type=int, default=200)
    parser.add_argument('--callback-us', type=float, default=200)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
