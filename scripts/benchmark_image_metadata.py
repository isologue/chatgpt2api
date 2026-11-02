"""独立进程 TLS 元数据对照；仅本机合成正文，不请求上游或读取账号。"""
from __future__ import annotations

import argparse
import asyncio
import gc
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.benchmark_image_ca import tls_fixture
from scripts.benchmark_image_download import stats
from services import image_ca
from services.image_async import run_cooperative
from services.image_async_transport import AsyncImageSession
from services.image_loop_work import get_loop_work
from services.image_metrics import ImageMetrics, current_metrics, phase


async def trial(session_class, count, url, expected, bundle, directory):
    gc.collect()
    work = get_loop_work()
    work.flush(force=True)
    latencies, lags, errors = [], [], {}
    stop = asyncio.Event()

    async def sample_lag():
        while not stop.is_set():
            started = time.perf_counter()
            await asyncio.sleep(.02)
            lags.append(max(0, time.perf_counter() - started - .02) * 1000)

    def consume():
        session = session_class(verify=True, trust_env=False, impersonate='chrome110')
        try:
            with phase('upstream_bootstrap'):
                response = session.get(url, timeout=30)
            assert response.content == expected
        finally:
            session.close()

    async def one():
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(60)
        token = current_metrics.set(metrics)
        started = time.perf_counter()
        try:
            await run_cooperative(consume)
        except Exception as exc:
            name = type(exc).__name__
            errors[name] = errors.get(name, 0) + 1
        finally:
            latencies.append((time.perf_counter() - started) * 1000)
            current_metrics.reset(token)

    sampler = asyncio.create_task(sample_lag())
    started, cpu_started = time.perf_counter(), time.process_time()
    try:
        with patch.object(image_ca, 'DEFAULT_CACERT', str(bundle)), patch.object(image_ca, 'CA_DIRECTORY', directory):
            image_ca._default_ca_directory.cache_clear()
            try:
                await asyncio.gather(*(one() for _ in range(count)))
            finally:
                image_ca._default_ca_directory.cache_clear()
    finally:
        stop.set()
        await sampler
    work.flush(force=True)
    sample = work.samples(time.perf_counter())[-1]
    return dict(requests=count, errors=errors, latency_ms=stats(latencies), lag_ms=stats(lags),
        wall_ms=(time.perf_counter() - started) * 1000,
        cpu_ms=(time.process_time() - cpu_started) * 1000,
        dispatch=sample['curl_dispatch'], work=sample['work'])


async def main(args):
    classes = {'current': AsyncImageSession}
    if args.baseline_transport:
        spec = importlib.util.spec_from_file_location('metadata_transport_baseline', args.baseline_transport)
        baseline = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baseline)
        classes['baseline'] = baseline.AsyncImageSession
    rows = []
    with tempfile.TemporaryDirectory(prefix='metadata-local-') as folder:
        folder = Path(folder)
        _, bundle, directory, _ = tls_fixture(folder)
        for size in args.sizes:
            command = [sys.executable, 'scripts/benchmark_image_download.py', '--serve', '--h2',
                '--cert', str(folder / 'cert.pem'), '--key', str(folder / 'key.pem'),
                '--size', str(size), '--chunk', str(args.chunk), '--delay', '0']
            server = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
            try:
                url = 'https://127.0.0.1:' + server.stdout.readline().strip() + '/metadata'
                expected = b'x' * size
                await trial(AsyncImageSession, 20, url, expected, bundle, directory)
                for repeat in range(args.repeats):
                    modes = list(classes) if repeat % 2 == 0 else list(reversed(classes))
                    for mode in modes:
                        row = await trial(classes[mode], args.requests, url, expected, bundle, directory)
                        row.update(mode=mode, size=size, repeat=repeat + 1)
                        rows.append(row)
                        print(json.dumps({k: v for k, v in row.items() if k not in {'work', 'dispatch'}}), flush=True)
            finally:
                server.terminate()
                server.wait(timeout=10)
    if args.output:
        Path(args.output).write_text(json.dumps(rows, indent=2), encoding='utf8')
    if any(row['errors'] for row in rows):
        raise RuntimeError('Metadata comparison contains failed requests')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=500)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--sizes', nargs='+', type=int, default=[4096, 524288, 3145728])
    parser.add_argument('--chunk', type=int, default=4096)
    parser.add_argument('--baseline-transport', type=Path)
    parser.add_argument('--output', type=Path)
    asyncio.run(main(parser.parse_args()))
