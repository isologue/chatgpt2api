"""Compare HTTP-only load clients against an isolated loopback service; no image requests."""
from __future__ import annotations
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
import http.client
import inspect
import json
import math
from pathlib import Path
import subprocess
import sys
import threading
import time
import urllib.request


def stats(values):
    values = sorted(values)
    return {name: round(values[max(0, math.ceil(len(values) * q) - 1)], 3)
            for name, q in (("p50", .5), ("p95", .95), ("max", 1))}


async def serve():
    async def handle(reader, writer):
        try:
            headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 60)
            length = next((int(line.split(b":", 1)[1]) for line in headers.split(b"\r\n")
                           if line.lower().startswith(b"content-length:")), 0)
            if length > 32768:
                raise ValueError("synthetic request too large")
            await reader.readexactly(length)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
    server = await asyncio.start_server(handle, "127.0.0.1", 0, backlog=2048)
    print(server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


def trial(mode, count, port):
    ready = threading.Barrier(count + 1)
    body = b'{"synthetic":true}'
    def one(_):
        ready.wait(timeout=60)
        start = time.perf_counter()
        marks = {"started": start}
        if mode == "urllib_new_opener":
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            marks["prepared"] = time.perf_counter()
            req = urllib.request.Request("http://127.0.0.1:" + str(port) + "/probe", data=body)
            with opener.open(req, timeout=90) as response:
                marks["headers"] = time.perf_counter()
                payload = response.read()
        else:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=90)
            marks["prepared"] = time.perf_counter()
            try:
                conn.connect()
                marks["connected"] = time.perf_counter()
                conn.request("POST", "/probe", body, {"Connection": "close"})
                marks["sent"] = time.perf_counter()
                with conn.getresponse() as response:
                    marks["headers"] = time.perf_counter()
                    payload = response.read()
            finally:
                conn.close()
        end = time.perf_counter()
        assert payload == b"{}", "invalid synthetic response"
        return {"started": start, "prepare_ms": (marks["prepared"]-start)*1000,
                "total_ms": (end-start)*1000,
                "after_prepare_ms": (end-marks["prepared"])*1000,
                **({"connect_ms": (marks["connected"]-marks["prepared"])*1000,
                    "send_ms": (marks["sent"]-marks["connected"])*1000,
                    "headers_ms": (marks["headers"]-marks["sent"])*1000}
                   if "connected" in marks else {})}
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(one, i) for i in range(count)]
        start = time.perf_counter()
        ready.wait(timeout=60)
        rows = [f.result() for f in futures]
        wall_ms = (time.perf_counter()-start)*1000
    return {"mode": mode, "requests": count, "wall_ms": round(wall_ms, 3),
            "start_spread_ms": round((max(r["started"] for r in rows)-min(r["started"] for r in rows))*1000, 3),
            "stats_ms": {key: stats([r[key] for r in rows]) for key in rows[0] if key != "started"}}


def main(args):
    source = "import asyncio\n" + inspect.getsource(serve) + "\nasyncio.run(serve())"
    server = subprocess.Popen([sys.executable, "-u", "-c", source],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        raw = server.stdout.readline()
        if not raw.strip():
            raise RuntimeError(server.stderr.read()[-1000:])
        port = int(raw)
        rows = []
        for count in args.requests:
            for repeat in range(args.repeats):
                modes = ["urllib_new_opener", "http_connection"]
                if repeat % 2:
                    modes.reverse()
                for mode in modes:
                    row = trial(mode, count, port)
                    row["repeat"] = repeat + 1
                    rows.append(row)
                    print(json.dumps(row), flush=True)
        if args.output:
            Path(args.output).write_text(json.dumps(rows, indent=2), encoding="utf8")
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, nargs="+", default=[50, 500])
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if any(n < 1 or n > 1000 for n in args.requests) or args.repeats < 1:
        parser.error("requests must be 1..1000; repeats must be positive")
    main(args)
