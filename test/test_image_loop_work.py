"""循环工作归因和大量已缓冲 SSE 的调度公平性，不访问上游。"""
import asyncio
import time
from types import SimpleNamespace

import pytest
from curl_cffi import AsyncCurl

from services.image_async import await_io, run_cooperative
from services.image_async_transport import AsyncImageResponse
from services.image_curl import ImageAsyncCurl
from services.image_diagnostics import ImageRequestCancelled
from services.image_loop_work import LoopWorkRecorder, get_loop_work, snapshot
from services.image_metrics import ImageMetrics, current_metrics


def test_many_short_callbacks_are_visible_and_history_is_bounded(monkeypatch):
    import services.image_loop_work as module
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(module.time, "perf_counter", lambda: clock.now)
    monkeypatch.setattr(module.time, "time", lambda: 10000 + clock.now)
    recorder = LoopWorkRecorder()
    for _ in range(80):
        recorder.record("curl_socket", clock.now, .001, .0005)
        clock.now += .001
    recorder.record("not-a-category", clock.now, 10, 10)
    recorder.flush(force=True)
    item = snapshot()["timeline"][0]["work"]["curl_socket"]
    assert item["count"] == 80
    assert item["total_ms"] == 80
    assert item["max_ms"] == 1
    assert item["cpu_ms"] == 40
    assert "not-a-category" not in str(snapshot())
    for _ in range(900):
        clock.now += .5
        recorder.flush(force=True)
    assert len(recorder.samples(clock.now)) <= 600
    clock.now += 301
    assert recorder.samples(clock.now) == ()


def test_loop_work_flush_is_immutable_and_does_not_capture_request_values():
    recorder = LoopWorkRecorder()
    now = time.perf_counter()
    recorder.record("business_slice", now, .01, .002)
    recorder.flush(.03, force=True)
    previous = recorder.samples(time.perf_counter())[0]
    recorder.record("business_slice", now, .5, .4)
    assert previous["work"]["business_slice"]["count"] == 1
    assert previous["lag_ms"] == 30
    assert set(previous["work"]["business_slice"]) == {"count", "total_ms", "cpu_ms", "max_ms", "max_at"}


def test_curl_completion_excludes_socket_work(monkeypatch):
    async def scenario():
        def socket(self, *args):
            time.sleep(.015)
            return 0
        def completion(self, *args):
            self.socket_action(*args)
            time.sleep(.01)
        monkeypatch.setattr(AsyncCurl, "socket_action", socket)
        monkeypatch.setattr(AsyncCurl, "process_data", completion)
        driver = ImageAsyncCurl()
        work = get_loop_work()
        try:
            driver.process_data(-1, 0)
            work.flush(force=True)
            row = work.samples(time.perf_counter())[-1]["work"]
            assert row["curl_socket"]["count"] == 1
            assert row["curl_socket"]["total_ms"] >= 14
            assert row["curl_completion"]["total_ms"] >= 9
            assert row["curl_completion"]["total_ms"] < row["curl_socket"]["total_ms"] + 10
            assert row["curl_socket"]["cpu_ms"] < row["curl_socket"]["total_ms"]
        finally:
            await driver.close()
    asyncio.run(scenario())


def test_cooperative_slices_are_counted_without_counting_async_wait():
    async def scenario():
        def handler():
            time.sleep(.005)
            await_io(asyncio.sleep(.05))
            return "done"
        assert await run_cooperative(handler) == "done"
        work = get_loop_work()
        work.flush(force=True)
        item = work.samples(time.perf_counter())[-1]["work"]["business_slice"]
        assert item["count"] == 2
        assert 4 <= item["total_ms"] < 40
    asyncio.run(scenario())


def buffered_response(chunks):
    async def content():
        for chunk in chunks:
            yield chunk
    return AsyncImageResponse(None, SimpleNamespace(aiter_content=content),
        time.perf_counter(), None, 1024 * 1024, True)


@pytest.mark.parametrize("tiny_chunks", [False, True])
def test_buffered_sse_yields_without_losing_lines(tiny_chunks):
    async def scenario():
        expected = [f"data: 事件{i}" for i in range(512)]
        body = ("\r\n".join(expected) + "\r\n").encode()
        chunks = [bytes([value]) for value in body] if tiny_chunks else [body]
        response = buffered_response(chunks)
        observed, beats = [], []
        done = False
        async def heartbeat():
            while not done:
                beats.append(len(observed))
                await asyncio.sleep(0)
        sampler = asyncio.create_task(heartbeat())
        def parse():
            observed.extend(response.iter_lines(decode_unicode=True))
        try:
            await run_cooperative(parse)
        finally:
            done = True
            await sampler
        assert observed == expected
        assert any(0 < point < len(expected) for point in beats)
    asyncio.run(scenario())


def test_time_quantum_yields_while_parsing_fewer_than_64_events():
    async def scenario():
        response = buffered_response([b"data: event\n" * 24])
        observed, beats = [], []
        def parse():
            for line in response.iter_lines():
                observed.append(line)
                time.sleep(.0005)
        async def heartbeat():
            await asyncio.sleep(0)
            beats.append(len(observed))
        sampler = asyncio.create_task(heartbeat())
        await run_cooperative(parse)
        await sampler
        assert 0 < beats[0] < 24
        assert observed == [b"data: event"] * 24
    asyncio.run(scenario())


def test_cancellation_stops_parsing_a_buffered_sse_chunk():
    async def scenario():
        metrics = ImageMetrics()
        metrics.diagnostics.configure_budget(10)
        context = current_metrics.set(metrics)
        response = buffered_response([b"data: event\n" * 1024])
        observed = []
        def parse():
            observed.extend(response.iter_lines())
        async def cancel():
            metrics.diagnostics.cancelled.set()
        cancellation = asyncio.create_task(cancel())
        try:
            with pytest.raises(ImageRequestCancelled):
                await run_cooperative(parse)
            assert len(observed) < 1024
        finally:
            await cancellation
            current_metrics.reset(context)
    asyncio.run(scenario())
