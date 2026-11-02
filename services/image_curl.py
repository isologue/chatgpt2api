"""账号独享 curl multi；按循环合并回调并协调补偿轮询，不共享连接、Cookie 或认证。"""
from __future__ import annotations

import asyncio
import ctypes
import sys
import time
from collections import OrderedDict
from contextvars import Context

from curl_cffi import AsyncCurl, CurlInfo, CurlMOpt
from curl_cffi.aio import (CURL_CSELECT_IN, CURL_CSELECT_OUT, CURL_POLL_IN,
    CURL_POLL_NONE, CURL_POLL_OUT, CURL_POLL_REMOVE, CURL_SOCKET_TIMEOUT, ffi, lib)
from curl_cffi.requests import AsyncSession

from services.image_loop_work import get_loop_work


def _bind_socket_action():
    """仅包装 Linux CPython 下已经加载的非阻塞 multi 驱动，不另载 curl。"""
    if (sys.platform != 'linux' or sys.implementation.name != 'cpython'
            or not getattr(sys, '_is_gil_enabled', lambda: True)()):
        return None
    try:
        address = ffi.addressof(lib, 'curl_multi_socket_action')
        signature = ffi.typeof(address)
        # 依赖升级改变 ABI 时沿用原库，不能猜测 socket 或指针类型。
        if (signature.result.cname != 'int' or
                tuple(arg.cname for arg in signature.args) != ('void *', 'int', 'int', 'int *')):
            return None
        prototype = ctypes.PYFUNCTYPE(ctypes.c_int, ctypes.c_void_p,
            ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int))
        return prototype(int(ffi.cast('uintptr_t', address)))
    except (AttributeError, TypeError, ValueError, RuntimeError):
        return None


_native_socket_action = _bind_socket_action()


@ffi.callback("int(void *, long, void *)", error=-1)
def _timer_callback(curlm, timeout_ms, clientp):
    # libcurl 的 -1 表示删除定时器，0 才表示下一轮立即驱动。
    ffi.from_handle(clientp)._set_timer(timeout_ms)
    return 0


@ffi.callback(ffi.typeof(lib.socket_function), error=-1)
def _socket_callback(curl, sockfd, what, clientp, data):
    ffi.from_handle(clientp)._set_socket(sockfd, what)
    return 0


class _CallbackQueue:
    """按循环合并就绪事件；仅限制一次回调的工作量，不限制请求并发。"""
    BATCH = 16
    QUANTUM = .002
    PRESSURE_AGE = .050

    def __init__(self, loop):
        self.loop = loop
        self.pending = OrderedDict()
        self.handle = None
        self.draining = False
        self.work = get_loop_work()

    def submit(self, driver, sockfd, bitmask, source):
        self.work.event(source + '_events')
        key = (driver, sockfd)
        item = self.pending.get(key)
        if item is not None:
            item[0] |= bitmask
            item[1].add(source)
            self.work.event('coalesced_events')
        else:
            self.pending[key] = [bitmask, {source}, time.perf_counter()]
            driver._pending_events.add(sockfd)
        self.work.event('max_pending', len(self.pending))
        if self.handle is None:
            self.handle = self.loop.call_soon(self._drain, context=Context())

    def cancel_timer(self, driver):
        key = (driver, CURL_SOCKET_TIMEOUT)
        item = self.pending.get(key)
        if item is not None:
            item[1].discard('timer')
            if not item[1]:
                self.pending.pop(key)
                driver._pending_events.discard(CURL_SOCKET_TIMEOUT)
        self._cancel_empty()

    def remove_socket(self, driver, sockfd):
        self.pending.pop((driver, sockfd), None)
        driver._pending_events.discard(sockfd)
        self._cancel_empty()

    def remove(self, driver):
        for sockfd in tuple(driver._pending_events):
            self.pending.pop((driver, sockfd), None)
        driver._pending_events.clear()
        self._cancel_empty()

    def _cancel_empty(self):
        if not self.pending and not self.draining and self.handle is not None:
            self.handle.cancel()
            self.handle = None

    def _drain(self):
        # 执行期间保留 handle 标记，避免 curl 回调重入入队时重复调度。
        self.draining = True
        started = time.perf_counter()
        self.work.event('batches')
        # 积压且最老事件已经等待时，适度扩大时间片；上限仍为 2ms。
        # 不改变 FIFO，不限制生图并发，也不允许某个连接连续插队。
        oldest = next(iter(self.pending.values()), None)
        waited = started - oldest[2] if oldest else 0.0
        pressure = min(4, 1 + int(waited / self.PRESSURE_AGE)) if len(self.pending) >= self.BATCH * 2 else 1
        quantum = self.QUANTUM
        batch = self.BATCH * pressure
        if pressure > 1:
            self.work.event('pressure_batches')
        processed = 0
        try:
            while self.pending:
                (driver, sockfd), (bitmask, sources, queued_at) = self.pending.popitem(last=False)
                driver._pending_events.discard(sockfd)
                try:
                    if (driver._curlm and not driver._closing and driver._curl2future
                            and (sockfd == CURL_SOCKET_TIMEOUT or sockfd in driver._sockfds)):
                        source = 'socket' if 'socket' in sources else 'timer' if 'timer' in sources else 'watchdog'
                        wait_ms = (time.perf_counter() - queued_at) * 1000
                        self.work.event('max_wait_ms', wait_ms)
                        self.work.event('max_' + source + '_wait_ms', wait_ms)
                        self.work.event('wait_count')
                        self.work.event('wait_total_ms', wait_ms)
                        transfer = getattr(driver, '_transfer_wait', None)
                        if transfer is not None:
                            transfer['count'] += 1
                            transfer['total_ms'] += wait_ms
                            transfer['max_ms'] = max(transfer['max_ms'], wait_ms)
                        driver._process_data(sockfd, bitmask, source)
                except Exception as exc:
                    self.loop.call_exception_handler({
                        'message': 'image curl callback failed', 'exception': exc,
                    })
                finally:
                    if sockfd != CURL_SOCKET_TIMEOUT:
                        driver._resume_socket(sockfd)
                processed += 1
                if processed >= batch or time.perf_counter() - started >= quantum:
                    break
        finally:
            self.draining = False
            self.handle = None
            if self.pending:
                self.work.event('yields')
                # 下一轮再处理；让业务任务、超时和取消有机会执行。
                self.handle = self.loop.call_later(0, self._drain, context=Context())


class _Watchdog:
    INTERVAL = 0.1
    IDLE_GRACE = 1.0
    BATCH = 16

    def __init__(self, loop):
        self.loop = loop
        self.members = set()
        # 不继承某个请求的账号/诊断上下文，也不持有历史请求。
        self.task = loop.create_task(self.run(), context=Context())

    async def run(self):
        while True:
            await asyncio.sleep(self.INTERVAL)
            quantum = time.perf_counter()
            for index, driver in enumerate(tuple(self.members), 1):
                # 正常 socket/timer 回调仍由原库管理，业务预算也独立生效。
                # 只有连续 1 秒无任何回调才补偿；不对正常传输重复驱动。
                if (driver._curlm and driver._curl2future
                        and not driver._pending_events
                        and time.monotonic() - driver._last_action >= self.IDLE_GRACE):
                    try:
                        driver.process_data(CURL_SOCKET_TIMEOUT, CURL_POLL_NONE, source="watchdog")
                    except Exception as exc:
                        self.loop.call_exception_handler({
                            'message': 'image curl watchdog callback failed', 'exception': exc,
                        })
                # 多账号同时到期时分批让出；不限制业务并发或创建额外工作线程。
                if index % self.BATCH == 0 or time.perf_counter() - quantum >= .002:
                    await asyncio.sleep(0)
                    quantum = time.perf_counter()

    def remove(self, driver):
        self.members.discard(driver)
        if not self.members:
            self.task.cancel()
            if getattr(self.loop, '_image_curl_watchdog', None) is self:
                del self.loop._image_curl_watchdog


class ImageAsyncCurl(AsyncCurl):
    def __init__(self, *args, **kwargs):
        # 首次回调前允许下一次扫描补偿，避免丢失启动通知后等满宽限期。
        self._last_action = 0.0
        self._closing = False
        self._pending_events = set()
        self._socket_interests = {}
        self._paused_sockets = set()
        loop = asyncio.get_running_loop()
        dispatcher = getattr(loop, "_image_curl_callbacks", None)
        if dispatcher is None:
            dispatcher = loop._image_curl_callbacks = _CallbackQueue(loop)
        self._dispatcher = dispatcher
        self._socket_wall = self._socket_cpu = 0.0
        self._loop_work = get_loop_work()
        super().__init__(*args, **kwargs)

    async def _force_timeout(self):
        loop = asyncio.get_running_loop()
        hub = getattr(loop, '_image_curl_watchdog', None)
        if hub is None:
            hub = loop._image_curl_watchdog = _Watchdog(loop)
        hub.members.add(self)
        try:
            await loop.create_future()
        finally:
            hub.remove(self)
            if not hub.members:
                # 等最后一个公共任务退出，避免 loop 关闭时残留定时器。
                await asyncio.gather(hub.task, return_exceptions=True)

    def _setup(self):
        super()._setup()
        self._check_error(self.setopt(CurlMOpt.TIMERFUNCTION, _timer_callback))
        self._check_error(self.setopt(CurlMOpt.SOCKETFUNCTION, _socket_callback))

    def _register_socket(self, sockfd, what):
        if what & CURL_POLL_IN:
            self.loop.add_reader(sockfd, self.process_data, sockfd, CURL_CSELECT_IN)
        if what & CURL_POLL_OUT:
            self.loop.add_writer(sockfd, self.process_data, sockfd, CURL_CSELECT_OUT)

    def _set_socket(self, sockfd, what):
        if sockfd in self._sockfds:
            self.loop.remove_reader(sockfd)
            self.loop.remove_writer(sockfd)
        if what == CURL_POLL_REMOVE:
            self._sockfds.discard(sockfd)
            self._socket_interests.pop(sockfd, None)
            self._paused_sockets.discard(sockfd)
            self._dispatcher.remove_socket(self, sockfd)
        else:
            self._sockfds.add(sockfd)
            self._socket_interests[sockfd] = what
            if sockfd not in self._paused_sockets and not self._closing:
                self._register_socket(sockfd, what)

    def _pause_socket(self, sockfd):
        if sockfd in self._socket_interests and sockfd not in self._paused_sockets:
            self._paused_sockets.add(sockfd)
            self.loop.remove_reader(sockfd)
            self.loop.remove_writer(sockfd)

    def _resume_socket(self, sockfd):
        if sockfd not in self._paused_sockets:
            return
        self._paused_sockets.discard(sockfd)
        what = self._socket_interests.get(sockfd)
        if what is not None and self._curlm and not self._closing:
            # 排队期间 libcurl 可能更新过读写要求，只恢复最新要求。
            self._register_socket(sockfd, what)

    def _set_timer(self, timeout_ms):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._dispatcher.cancel_timer(self)
        if timeout_ms < 0:
            self._loop_work.event('timer_cancels')
        elif not self._closing:
            self._timer = self.loop.call_later(timeout_ms / 1000, self._timer_fired)

    def _timer_fired(self):
        self._timer = None
        self.process_data(CURL_SOCKET_TIMEOUT, CURL_POLL_NONE, source='timer')

    def process_data(self, sockfd, ev_bitmask, *, source=None):
        if self._curlm and not self._closing:
            self._pause_socket(sockfd)
            self._dispatcher.submit(self, sockfd, ev_bitmask,
                source or ('timer' if sockfd == CURL_SOCKET_TIMEOUT else 'socket'))

    def _record_download_completion(self, curl):
        # 句柄重置前保留固定数值；不读取 URL、请求头或认证信息。
        timing = getattr(self, '_transfer_wait', None)
        if timing is None or 'download_started' not in timing:
            return
        timing['completed_at'] = time.perf_counter()
        try:
            timing['received_bytes'] = curl.getinfo(CurlInfo.SIZE_DOWNLOAD_T)
            timing['transfer_ms'] = curl.getinfo(CurlInfo.TOTAL_TIME) * 1000
            timing['first_byte_ms'] = curl.getinfo(CurlInfo.STARTTRANSFER_TIME) * 1000
        except Exception:
            # 可选诊断失败不能改变传输结果或阻止 future 收尾。
            pass

    def set_result(self, curl):
        self._record_download_completion(curl)
        return super().set_result(curl)

    def set_exception(self, curl, exception):
        self._record_download_completion(curl)
        return super().set_exception(curl, exception)

    async def close(self):
        if self._closing:
            return
        self._closing = True
        self._dispatcher.remove(self)
        try:
            await super().close()
        finally:
            self._socket_interests.clear()
            self._paused_sockets.clear()

    def _process_data(self, sockfd, ev_bitmask, source):
        self._action_source = source
        started, cpu_started = time.perf_counter(), time.thread_time()
        wall_before, cpu_before = self._socket_wall, self._socket_cpu
        try:
            return super().process_data(sockfd, ev_bitmask)
        finally:
            # 排除嵌套 socket_action，只统计完成结果分发及移除 handle 的开销。
            self._loop_work.record('curl_' + source + '_event', started,
                time.perf_counter() - started, time.thread_time() - cpu_started)
            self._loop_work.record('curl_completion', started + self._socket_wall - wall_before,
                time.perf_counter() - started - (self._socket_wall - wall_before),
                time.thread_time() - cpu_started - (self._socket_cpu - cpu_before))

    def socket_action(self, sockfd, ev_bitmask):
        started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            if _native_socket_action is None:
                return super().socket_action(sockfd, ev_bitmask)
            # libcurl multi 处理非阻塞 socket，正文/socket/timer 回调仍在本线程。
            # CFFI 在原生调用及每次 Python 回调间反复交接 GIL；竞争时微小工作
            # 也会等待多个线程时间片。仅此调用保留 GIL，外层 2ms 批次照常让出。
            # 不用于 DNS 查询、文件读写、同步 curl.perform 或任何阻塞操作。
            running = ctypes.c_int()
            code = _native_socket_action(int(ffi.cast('uintptr_t', self._curlm)),
                sockfd, ev_bitmask, ctypes.byref(running))
            self._check_error(code)
            return running.value
        finally:
            self._last_action = time.monotonic()
            elapsed, cpu = time.perf_counter() - started, time.thread_time() - cpu_started
            self._socket_wall += elapsed
            self._socket_cpu += cpu
            self._loop_work.record('curl_socket', started, elapsed, cpu)
            stage = getattr(self, '_transfer_stage', 'other')
            category = ('curl_bootstrap' if stage == 'upstream_bootstrap' else
                'curl_download' if stage in {'image_download', 'image_download_http', 'image_download_body'} else
                'curl_stream' if getattr(self, '_transfer_is_sse', False) else 'curl_metadata')
            self._loop_work.record(category, started, elapsed, cpu)
            if elapsed >= .05:
                self._loop_work.slow_callback(category, getattr(self, '_action_source', 'timer'),
                    started, elapsed, cpu, phase=stage)
                from services.image_metrics import runtime_phases
                # 固定数值快照；不采集 URL、认证、正文或调用参数。
                runtime_phases.observe('image_curl_callback_slow', elapsed)


class ImageCurlSession(AsyncSession):
    @property
    def acurl(self):
        if self._acurl is None:
            self._acurl = ImageAsyncCurl(loop=self.loop)
        return self._acurl
