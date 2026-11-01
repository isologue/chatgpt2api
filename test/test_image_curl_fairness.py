"""curl 回调队列的事件完整性、公平性、关闭及原生定时器契约。"""
import asyncio
from contextvars import ContextVar
import time

import pytest
from curl_cffi.aio import CURL_CSELECT_IN, CURL_CSELECT_OUT, CURL_SOCKET_TIMEOUT, ffi

from services.image_curl import ImageAsyncCurl, _CallbackQueue, _timer_callback
from services.image_loop_work import get_loop_work


class Driver:
    def __init__(self, callback):
        self._curlm = True
        self._closing = False
        self._curl2future = {1: object()}
        self._sockfds = {10}
        self._pending_events = set()
        self._process_data = callback

    def _resume_socket(self, sockfd):
        pass


async def drained(queue):
    async with asyncio.timeout(3):
        while queue.pending or queue.handle is not None:
            await asyncio.sleep(0)


def test_ready_masks_merge_and_timer_does_not_merge_with_socket():
    async def scenario():
        calls = []
        queue = _CallbackQueue(asyncio.get_running_loop())
        driver = Driver(lambda *args: calls.append(args))
        queue.submit(driver, 10, CURL_CSELECT_IN, 'socket')
        queue.submit(driver, 10, CURL_CSELECT_OUT, 'socket')
        queue.submit(driver, CURL_SOCKET_TIMEOUT, 0, 'timer')
        await drained(queue)
        assert calls == [(10, CURL_CSELECT_IN | CURL_CSELECT_OUT, 'socket'), (-1, 0, 'timer')]
        assert not driver._pending_events
        work = get_loop_work(); work.flush(force=True)
        counts = work.samples(time.perf_counter())[-1]['curl_dispatch']
        assert counts['socket_events'] == 2 and counts['coalesced_events'] == 1
        assert counts['max_pending'] == 2
    asyncio.run(scenario())


def test_timer_cancel_discards_pending_timer_but_preserves_watchdog():
    async def scenario():
        calls = []
        queue = _CallbackQueue(asyncio.get_running_loop())
        driver = Driver(lambda *args: calls.append(args))
        queue.submit(driver, -1, 0, 'timer'); queue.cancel_timer(driver)
        assert not queue.pending and queue.handle is None
        queue.submit(driver, -1, 0, 'timer')
        queue.submit(driver, -1, 0, 'watchdog')
        queue.cancel_timer(driver)
        await drained(queue)
        assert calls == [(-1, 0, 'watchdog')]
    asyncio.run(scenario())


def test_callbacks_created_during_a_batch_are_not_lost_or_double_scheduled():
    async def scenario():
        calls = []
        queue = _CallbackQueue(asyncio.get_running_loop())
        def execute(*args):
            calls.append(args)
            queue.cancel_timer(driver)  # 当前项已取出，队列为空。
            if len(calls) == 1:
                queue.submit(driver, 10, CURL_CSELECT_OUT, 'socket')
        driver = Driver(execute)
        queue.submit(driver, 10, CURL_CSELECT_IN, 'socket')
        await drained(queue)
        assert calls == [(10, CURL_CSELECT_IN, 'socket'), (10, CURL_CSELECT_OUT, 'socket')]
        assert not driver._pending_events
    asyncio.run(scenario())


def test_many_short_callbacks_allow_other_tasks_to_run_before_the_queue_finishes():
    async def scenario():
        queue = _CallbackQueue(asyncio.get_running_loop())
        calls, checkpoints = [], []
        def execute(*args):
            until = time.perf_counter() + .0003
            while time.perf_counter() < until: pass
            calls.append(args)
        drivers = [Driver(execute) for _ in range(200)]
        for driver in drivers: queue.submit(driver, 10, 1, 'socket')
        async def observer():
            await asyncio.sleep(0)
            checkpoints.append(len(calls))
        observer_task = asyncio.create_task(observer())
        await drained(queue); await observer_task
        assert len(calls) == 200 and 0 < checkpoints[0] < 200
        assert all(not d._pending_events for d in drivers)
    asyncio.run(scenario())


@pytest.mark.parametrize('state', ['closed', 'cancelled', 'removed_socket'])
def test_stale_pending_notifications_do_not_drive_curl(state):
    async def scenario():
        calls = []
        queue = _CallbackQueue(asyncio.get_running_loop())
        driver = Driver(lambda *args: calls.append(args))
        queue.submit(driver, 10, 1, 'socket')
        if state == 'closed':
            driver._closing = True; queue.remove(driver)
        elif state == 'cancelled': driver._curl2future.clear()
        else: driver._sockfds.clear()
        await drained(queue)
        assert not calls and not driver._pending_events
    asyncio.run(scenario())


def test_callback_exception_does_not_stop_other_accounts():
    async def scenario():
        loop = asyncio.get_running_loop(); queue = _CallbackQueue(loop)
        calls, errors = [], []
        def fail(*args): raise ValueError('fixture')
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        try:
            queue.submit(Driver(fail), 10, 1, 'socket')
            queue.submit(Driver(lambda *args: calls.append(args)), 10, 1, 'socket')
            await drained(queue)
            assert len(errors) == 1 and len(calls) == 1
        finally: loop.set_exception_handler(previous)
    asyncio.run(scenario())


def test_dispatch_does_not_inherit_a_request_context():
    async def scenario():
        request = ContextVar('callback-test', default='none')
        calls = []; queue = _CallbackQueue(asyncio.get_running_loop())
        token = request.set('fixture-account')
        try: queue.submit(Driver(lambda *args: calls.append(request.get())), 10, 1, 'socket')
        finally: request.reset(token)
        await drained(queue)
        assert calls == ['none']
    asyncio.run(scenario())


@pytest.mark.parametrize('milliseconds', [-1, 0, 10])
def test_native_timer_callback_honors_cancel_and_zero_semantics(monkeypatch, milliseconds):
    async def scenario():
        driver = ImageAsyncCurl(); calls = []
        monkeypatch.setattr(driver, 'process_data', lambda *args, **kwargs: calls.append((args, kwargs)))
        try:
            driver._set_timer(100)
            assert _timer_callback(ffi.NULL, milliseconds, driver._self_handle) == 0
            await asyncio.sleep(.03)
            assert len(calls) == (0 if milliseconds < 0 else 1)
            assert driver._timer is None
        finally: await driver.close()
    asyncio.run(scenario())


def test_close_removes_queued_native_notifications_before_cleanup():
    async def scenario():
        driver = ImageAsyncCurl()
        driver.process_data(-1, 0)
        assert driver._pending_events == {-1}
        await driver.close()
        assert not driver._pending_events and not driver._dispatcher.pending
        await asyncio.sleep(0)
        assert driver._curlm is None
    asyncio.run(scenario())

def test_dispatch_statistics_aggregate_counts_and_maxima_without_mutating_published_samples(monkeypatch):
    from weakref import WeakSet
    import services.image_loop_work as module
    monkeypatch.setattr(module, '_recorders', WeakSet())
    async def scenario():
        from services.image_loop_work import snapshot
        work = get_loop_work()
        work.event('socket_events', 2); work.event('max_pending', 5)
        work.event('max_wait_ms', 3.5); work.flush(force=True)
        previous = work.samples(time.perf_counter())[-1]
        work.event('socket_events', 3); work.event('max_pending', 2)
        work.event('max_wait_ms', 7.5); work.flush(force=True)
        assert previous['curl_dispatch']['socket_events'] == 2
        totals = snapshot()['timeline'][0]['curl_dispatch']
        assert totals['socket_events'] == 5
        assert totals['max_pending'] == 5 and totals['max_wait_ms'] == 7.5
    asyncio.run(scenario())

@pytest.mark.parametrize('cancel_mode', ['cancel', 'timeout'])
def test_cancelling_one_transfer_does_not_discard_other_transfers_on_same_driver(cancel_mode):
    from curl_cffi.requests.exceptions import Timeout
    from services.image_curl import ImageCurlSession
    async def scenario():
        holding = asyncio.Event(); release = asyncio.Event(); handlers = set()
        async def serve(reader, writer):
            handlers.add(asyncio.current_task())
            try:
                raw = await reader.readuntil(b'\r\n\r\n')
                if b'GET /hold ' in raw:
                    holding.set(); await release.wait()
                else:
                    await asyncio.sleep(.04)
                    writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok')
                    await writer.drain()
            finally:
                writer.close()
                try: await writer.wait_closed()
                except ConnectionError: pass
                handlers.discard(asyncio.current_task())
        server = await asyncio.start_server(serve, '127.0.0.1', 0)
        session = ImageCurlSession(trust_env=False)
        url = 'http://127.0.0.1:' + str(server.sockets[0].getsockname()[1])
        held = asyncio.create_task(session.get(url+'/hold', timeout=.1 if cancel_mode == 'timeout' else 2))
        healthy = asyncio.create_task(session.get(url+'/ok', timeout=2))
        try:
            await asyncio.wait_for(holding.wait(), 2)
            if cancel_mode == 'cancel':
                held.cancel()
                with pytest.raises(asyncio.CancelledError): await held
            else:
                with pytest.raises(Timeout): await held
            assert (await healthy).content == b'ok'
            assert not session.acurl._curl2future
        finally:
            release.set()
            await session.close()
            server.close(); await server.wait_closed()
            await asyncio.gather(*tuple(handlers), return_exceptions=True)
        assert not session.acurl._pending_events and not session.acurl._dispatcher.pending
    asyncio.run(scenario())

def test_queued_socket_pauses_readiness_and_restores_latest_interest(monkeypatch):
    from curl_cffi.aio import CURL_POLL_IN, CURL_POLL_OUT, CURL_POLL_REMOVE
    from services.image_curl import _socket_callback
    class Selector:
        def __init__(self): self.readers = {}; self.writers = {}
        def add_reader(self, fd, *args): self.readers[fd] = args
        def add_writer(self, fd, *args): self.writers[fd] = args
        def remove_reader(self, fd): self.readers.pop(fd, None)
        def remove_writer(self, fd): self.writers.pop(fd, None)
    async def scenario():
        driver = ImageAsyncCurl(); native_loop = driver.loop; selector = Selector()
        try:
            driver.loop = selector
            assert _socket_callback(ffi.NULL, 10, CURL_POLL_IN, driver._self_handle, ffi.NULL) == 0
            assert 10 in selector.readers
            driver.process_data(10, 1)
            assert not selector.readers and not selector.writers
            driver._set_socket(10, CURL_POLL_OUT)
            assert not selector.writers  # 排队时新要求不能提前恢复。
            driver._resume_socket(10)
            assert 10 not in selector.readers and 10 in selector.writers
            driver._set_socket(10, CURL_POLL_REMOVE)
            driver._resume_socket(10)
            assert not selector.readers and not selector.writers
            assert not driver._pending_events and not driver._dispatcher.pending
            driver._set_socket(10, CURL_POLL_REMOVE)  # 重复移除也安全。
        finally:
            driver.loop = native_loop
            await driver.close()
    asyncio.run(scenario())


def test_pressure_increases_cheap_callback_capacity_with_same_time_budget(monkeypatch):
    async def scenario():
        from types import SimpleNamespace
        import services.image_curl as module
        queue = _CallbackQueue(asyncio.get_running_loop())
        clock = SimpleNamespace(now=100.)
        calls = []
        def execute(*args):
            calls.append(args)
            clock.now += .00003
        for _ in range(200):
            queue.submit(Driver(execute), 10, 1, 'socket')
        queue.handle.cancel()
        for entry in queue.pending.values(): entry[2] = clock.now - 1
        # 固定工作成本验证批次预算，避免依赖宿主机计时精度。
        with monkeypatch.context() as patcher:
            patcher.setattr(module.time, 'perf_counter', lambda: clock.now)
            queue._drain()
        assert len(calls) == 64
        assert queue.pending and queue.handle is not None
        await drained(queue)
        assert len(calls) == 200
    asyncio.run(scenario())


def test_one_long_native_call_does_not_start_a_second_call_in_same_batch(monkeypatch):
    async def scenario():
        from types import SimpleNamespace
        import services.image_curl as module
        queue = _CallbackQueue(asyncio.get_running_loop())
        clock = SimpleNamespace(now=100.)
        calls = []
        def execute(*args):
            calls.append(args)
            clock.now += .03
        for _ in range(100): queue.submit(Driver(execute), 10, 1, 'socket')
        queue.handle.cancel()
        for entry in queue.pending.values(): entry[2] = 99.
        with monkeypatch.context() as patcher:
            patcher.setattr(module.time, 'perf_counter', lambda: clock.now)
            queue._drain()
        assert len(calls) == 1
        await drained(queue)
        assert len(calls) == 100
    asyncio.run(scenario())
