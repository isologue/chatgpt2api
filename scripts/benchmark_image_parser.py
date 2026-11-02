"""本机首页解析/PoW 混合负载对照；不联网、不读取账号，不输出正文。"""
from __future__ import annotations
import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor
from html.parser import HTMLParser
import json
import math
from multiprocessing import get_context
import os
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.pow import build_proof_token, parse_pow_resources_bytes


class ReferenceParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.script_sources = []
        self.data_build = ''

    def handle_starttag(self, tag, attrs):
        if tag == 'script':
            src = dict(attrs).get('src')
            if src:
                self.script_sources.append(src)
                match = re.search(r'c/[^/]*/_', src)
                if match:
                    self.data_build = match.group(0)


def job(mode, body, submitted):
    started, cpu = time.perf_counter(), time.process_time()
    if mode == 'proof':
        result = build_proof_token('local-proof-fixture', 'ff', 'local-fixture')
    elif mode == 'baseline':
        parser = ReferenceParser()
        parser.feed(body.decode())
        result = (parser.script_sources, parser.data_build)
    else:
        result = parse_pow_resources_bytes(body, 'utf-8')
    return result, started - submitted, time.perf_counter() - started, time.process_time() - cpu


def stats(values):
    a = sorted(values)
    return {k: round(a[math.ceil(len(a)*q)-1]*1000, 3)
            for k,q in [('p50_ms', .5), ('p95_ms', .95), ('max_ms', 1)]}


async def trial(pool, mode, html, count):
    loop = asyncio.get_running_loop()
    parses, proofs, elapsed, lags = [], [], [], []
    stop = asyncio.Event()
    async def sample():
        while not stop.is_set():
            started = time.perf_counter()
            await asyncio.sleep(.02)
            lags.append(max(0, time.perf_counter()-started-.02))
    async def one(index):
        await asyncio.sleep(index/count)
        began = time.perf_counter()
        result = await loop.run_in_executor(pool, job, mode, html, time.perf_counter())
        assert result[0] == (['/c/test/_sdk.js'], 'c/test/_')
        parses.append(result[1:])
        result = await loop.run_in_executor(pool, job, 'proof', b'', time.perf_counter())
        assert result[0].startswith('gAAAAAB')
        proofs.append(result[1:])
        elapsed.append(time.perf_counter()-began)
    sampler = asyncio.create_task(sample())
    started = time.perf_counter()
    try:
        await asyncio.gather(*(one(i) for i in range(count)))
    finally:
        stop.set()
        await sampler
    return {'mode': mode, 'requests': count, 'wall_s': round(time.perf_counter()-started, 3),
            'request': stats(elapsed), 'parse_queue': stats([x[0] for x in parses]),
            'parse_work': stats([x[1] for x in parses]), 'parse_cpu_total_ms': round(sum(x[2] for x in parses)*1000, 3),
            'proof_queue': stats([x[0] for x in proofs]), 'proof_work': stats([x[1] for x in proofs]),
            'event_loop_lag': stats(lags)}


async def main(args):
    # 普通标签和内联脚本混合，输出只有一条外部 script URL；不使用上游正文。
    html = (b'<html><script src="/c/test/_sdk.js"></script><body>' +
            b'<div class="flex" data-id="a&amp;b"><span>sample</span></div>' * 5000 +
            b'<script>' + b'window.testData={"a":"fixture","b":[1,2,3]};'*60000 +
            b'</script></body></html>')
    rows = []
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=get_context('spawn')) as pool:
        # 首轮不计入结果；实际解析而非立即返回的探针，避免冷启动偏向第二个版本。
        await trial(pool, 'baseline', html, args.requests)
        for repeat in range(args.repeats):
            for mode in (('baseline','candidate') if repeat % 2 == 0 else ('candidate','baseline')):
                row = await trial(pool, mode, html, args.requests)
                row.update(repeat=repeat+1, html_bytes=len(html), workers=args.workers)
                rows.append(row)
                print(json.dumps(row), flush=True)
    if args.output:
        Path(args.output).write_text(json.dumps(rows, indent=2), encoding='utf8')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workers', type=int, default=max(1, os.cpu_count() or 1))
    parser.add_argument('--requests', type=int, default=500)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output')
    asyncio.run(main(parser.parse_args()))
