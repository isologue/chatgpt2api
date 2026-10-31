"""本地缓冲 SSE 公平调度对照：相同正文与解析工作，不访问网络。"""
import argparse
import ast
import asyncio
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import time
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services.image_async import run_cooperative
from services import image_async_transport as module
from services.image_loop_work import get_loop_work


def baseline_response(ref):
    source = subprocess.check_output(['git', 'show', ref + ':services/image_async_transport.py']).decode('utf8')
    cls = next(node for node in ast.parse(source).body if isinstance(node, ast.ClassDef) and node.name == 'AsyncImageResponse')
    scope = vars(module).copy()
    exec(compile(ast.Module(body=[cls], type_ignores=[]), '<git-stream-baseline>', 'exec'), scope)
    return scope['AsyncImageResponse']


async def trial(cls, options):
    lines = [f'data: fixture-{index}'.encode() for index in range(options.lines)]
    body = b'\n'.join(lines) + b'\n'
    lag, latency = [], []
    stopped = False
    async def monitor():
        while not stopped:
            target = time.perf_counter() + .005
            await asyncio.sleep(.005)
            lag.append(max(0, time.perf_counter() - target))
    def parse():
        async def content():
            yield body
        wrapper = cls(None, SimpleNamespace(aiter_content=content), time.perf_counter(), None, len(body) + 1, True)
        actual = []
        for line in wrapper.iter_lines():
            until = time.perf_counter() + options.parse_us / 1e6
            while time.perf_counter() < until:
                pass
            actual.append(line)
        assert actual == lines
    async def call():
        # 所有请求从同一提交时刻计时，包含获得首次执行机会之前的排队。
        await run_cooperative(parse)
        latency.append(time.perf_counter() - started)
    sampler = asyncio.create_task(monitor())
    await asyncio.sleep(0)
    started, cpu = time.perf_counter(), time.process_time()
    await asyncio.gather(*(call() for _ in range(options.requests)))
    wall_s, cpu_s = time.perf_counter() - started, time.process_time() - cpu
    stopped = True
    await sampler
    get_loop_work().flush(force=True)
    ordered = sorted(latency)
    return {'wall_s': round(wall_s, 4), 'cpu_s': round(cpu_s, 4),
        'request_p95_ms': round(ordered[math.ceil(len(ordered) * .95) - 1] * 1000, 3),
        'loop_peak_ms': round(max(lag, default=0) * 1000, 3), 'monitor_samples': len(lag),
        'validated_streams': options.requests}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-ref', default='main')
    parser.add_argument('--requests', type=int, default=200)
    parser.add_argument('--lines', type=int, default=512)
    parser.add_argument('--parse-us', type=float, default=5)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output')
    options = parser.parse_args()
    cls = baseline_response(options.baseline_ref)
    rows = []
    for repeat in range(options.repeats):
        for name, response in ([('baseline', cls), ('current', module.AsyncImageResponse)] if repeat % 2 == 0 else [('current', module.AsyncImageResponse), ('baseline', cls)]):
            row = {'mode': name, 'repeat': repeat + 1, **asyncio.run(trial(response, options))}
            rows.append(row)
            print(json.dumps(row), flush=True)
    result = {'scope': 'Synthetic already-buffered SSE plus fixed per-event CPU work; no upstream, curl callbacks or disk. Demonstrates fairness, not production speedup.',
        'requests': options.requests, 'lines_per_request': options.lines, 'parse_us': options.parse_us,
        'baseline_ref': options.baseline_ref, 'runs': rows,
        'median': {name: {key: round(statistics.median(row[key] for row in rows if row['mode'] == name), 4)
            for key in ('wall_s', 'cpu_s', 'loop_peak_ms', 'request_p95_ms')} for name in ('baseline', 'current')}}
    if options.output:
        Path(options.output).write_text(json.dumps(result, indent=2), encoding='utf8')
    print(json.dumps(result['median'], indent=2))
