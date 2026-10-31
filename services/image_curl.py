"""账号独享 curl multi；仅合并补偿轮询，不共享连接、Cookie 或认证。"""
from __future__ import annotations

import asyncio
import time
from contextvars import Context

from curl_cffi import AsyncCurl
from curl_cffi.aio import CURL_POLL_NONE, CURL_SOCKET_TIMEOUT
from curl_cffi.requests import AsyncSession

from services.image_loop_work import get_loop_work


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
                        and time.monotonic() - driver._last_action >= self.IDLE_GRACE):
                    try:
                        driver.process_data(CURL_SOCKET_TIMEOUT, CURL_POLL_NONE)
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

    def process_data(self, sockfd, ev_bitmask):
        started, cpu_started = time.perf_counter(), time.thread_time()
        wall_before, cpu_before = self._socket_wall, self._socket_cpu
        try:
            return super().process_data(sockfd, ev_bitmask)
        finally:
            # 排除嵌套 socket_action，只统计完成结果分发及移除 handle 的开销。
            self._loop_work.record('curl_completion', started + self._socket_wall - wall_before,
                time.perf_counter() - started - (self._socket_wall - wall_before),
                time.thread_time() - cpu_started - (self._socket_cpu - cpu_before))

    def socket_action(self, sockfd, ev_bitmask):
        started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            return super().socket_action(sockfd, ev_bitmask)
        finally:
            self._last_action = time.monotonic()
            elapsed, cpu = time.perf_counter() - started, time.thread_time() - cpu_started
            self._socket_wall += elapsed
            self._socket_cpu += cpu
            self._loop_work.record('curl_socket', started, elapsed, cpu)
            if elapsed >= .05:
                from services.image_metrics import runtime_phases
                # 固定数值快照；不采集 URL、认证、正文或调用参数。
                runtime_phases.observe('image_curl_callback_slow', elapsed)


class ImageCurlSession(AsyncSession):
    @property
    def acurl(self):
        if self._acurl is None:
            self._acurl = ImageAsyncCurl(loop=self.loop)
        return self._acurl
