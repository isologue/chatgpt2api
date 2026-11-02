"""本机 TLS 合成正文对照：隔离 curl 调用边界与后台 Python 线程竞争。"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys
import threading
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import benchmark_image_download as benchmark
from services import image_curl


async def main(args):
    binding = image_curl._native_socket_action
    if binding is None:
        raise RuntimeError('This comparison requires Linux CPython with the supported curl ABI')
    original = benchmark.trial

    async def trial(settings, mode, *values):
        stop = threading.Event()
        def compete():
            while not stop.is_set():
                total = 0
                for index in range(100000):
                    total += index % 7
                stop.wait(.001)
        threads = [threading.Thread(target=compete, daemon=True)
            for _ in range(settings.gil_workers)]
        for thread in threads:
            thread.start()
        try:
            with patch.object(image_curl, '_native_socket_action',
                    binding if mode == 'retained' else None):
                result = await original(settings, 'queued', *values)
            result.update(mode=mode, gil_workers=settings.gil_workers)
            return result
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=5)

    with patch.object(benchmark, 'trial', trial):
        await benchmark.main(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requests', type=int, default=100)
    parser.add_argument('--gil-workers', type=int, default=2)
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--size', type=int, default=512 * 1024)
    parser.add_argument('--output')
    args = parser.parse_args()
    for key, value in dict(chunk=16384, delay=.001, timeout=30, total_timeout=120,
            modes=['released', 'retained'], rate=0, download_concurrency=64,
            baseline_transport=None, h2=True, buffer=0, impersonate='chrome').items():
        setattr(args, key, value)
    asyncio.run(main(args))
