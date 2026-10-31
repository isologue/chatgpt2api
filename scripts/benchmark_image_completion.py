"""Local completion/fully buffered body benchmark; no upstream, keys or image files."""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import Future
import json
from pathlib import Path
import statistics
import sys
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from curl_cffi.requests.models import Response, STREAM_END
from services import async_completion as completions
from services.image_async_transport import AsyncImageResponse
from services.image_metrics import ImageMetrics, current_metrics



async def handoff(mode, requests, rounds):
    notifications = 0
    original = completions._LoopCompletions.enqueue
    def enqueue(self, *args):
        nonlocal notifications
        notifications += 1
        return original(self, *args)
    def submit(value):
        # 短任务在提交返回前已经完成，复现多余的中间协程恢复。
        future = Future()
        future.set_result(value)
        return future
    async def call(index):
        for number in range(rounds):
            expected = index * rounds + number
            if mode == "staged":
                future = await completions.submit_off_loop(submit, expected)
                result = await completions.await_worker(future)
            else:
                result = await completions.await_submitted(submit, expected)
            assert result == expected
    started, cpu = time.perf_counter(), time.process_time()
    with patch.object(completions._LoopCompletions, "enqueue", enqueue):
        await asyncio.gather(*(call(index) for index in range(requests)))
    return {"wall_ms": round((time.perf_counter() - started) * 1000, 3),
            "cpu_ms": round((time.process_time() - cpu) * 1000, 3),
            "completed": requests * rounds, "notifications": notifications}


async def buffered(mode, requests):
    payload, chunk_count = b"x" * 4096, 128
    timers = 0
    loop = asyncio.get_running_loop()
    original = loop.call_at
    def call_at(*args, **kwargs):
        nonlocal timers
        timers += 1
        return original(*args, **kwargs)
    class QueueView:
        # timed 强制走原先逐块 timeout；get 仍读取同一个真实缓冲队列。
        def __init__(self, queue):
            self.queue = queue
        def empty(self):
            return True if mode == "timed" else self.queue.empty()
        async def get(self):
            return await self.queue.get()
    async def call():
        raw = Response()
        raw.curl = object()
        raw.astream_task = loop.create_future()
        raw.astream_task.set_result(None)
        queue = asyncio.Queue()
        for _ in range(chunk_count):
            queue.put_nowait(payload)
        queue.put_nowait(STREAM_END)
        raw.queue = QueueView(queue)
        response = AsyncImageResponse(None, raw, time.perf_counter(), None, 1024*1024, False)
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(60)
        token = current_metrics.set(metrics)
        try:
            for _ in range(chunk_count):
                assert await response._next() == payload
            try:
                await response._next()
            except StopAsyncIteration:
                pass
            else:
                raise AssertionError("missing EOF")
            assert response.bytes_read == len(payload) * chunk_count
        finally:
            current_metrics.reset(token)
    started, cpu = time.perf_counter(), time.process_time()
    with patch.object(loop, "call_at", call_at):
        await asyncio.gather(*(call() for _ in range(requests)))
    return {"wall_ms": round((time.perf_counter() - started) * 1000, 3),
            "cpu_ms": round((time.process_time() - cpu) * 1000, 3),
            "chunks": requests * chunk_count, "budget_timers": timers}


async def main(options):
    samples = {name: [] for name in ("staged", "direct", "timed", "buffered")}
    # 预热固定提交线程，正式对照交换顺序以降低先后顺序影响。
    await handoff("direct", 10, 1)
    for number in range(options.repeat):
        for name in (("staged", "direct") if number % 2 == 0 else ("direct", "staged")):
            samples[name].append(await handoff(name, options.requests, options.rounds))
        for name in (("timed", "buffered") if number % 2 == 0 else ("buffered", "timed")):
            samples[name].append(await buffered(name, options.requests))
    result = {"scope": "local short completed futures and fully buffered 512KiB bodies only; excludes real generation, network and persistence",
              "requests": options.requests, "rounds": options.rounds, "repeat": options.repeat,
              "samples": samples,
              "median": {name: {key: round(statistics.median(sample[key] for sample in values), 3)
                                for key in values[0]} for name, values in samples.items()}}
    if options.output:
        Path(options.output).write_text(json.dumps(result, indent=2), encoding="utf8")
    print(json.dumps(result["median"], indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--rounds", type=int, default=10)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output")
    asyncio.run(main(parser.parse_args()))
