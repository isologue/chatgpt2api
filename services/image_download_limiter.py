"""图片正文下载准入；FIFO 交接名额，排队共用原请求总预算。"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from services.config import config
from services.image_diagnostics import remaining_image_budget
from services.image_loop_work import get_loop_work
from services.image_metrics import phase


class DownloadLease:
    def __init__(self, limiter):
        self.limiter = limiter
        self.released = False

    async def release(self):
        if self.released:
            return
        self.released = True
        self.limiter.active -= 1
        self.limiter._grant()


class DownloadLimiter:
    def __init__(self):
        self.active = 0
        self.waiters = OrderedDict()
        self.work = get_loop_work()

    def _grant(self):
        # 本循环内同步预留名额，避免 notify 后新请求插队或反复唤醒争抢。
        limit = config.image_download_concurrency
        while self.waiters and self.active < limit:
            future, _ = self.waiters.popitem(last=False)
            if future.done():
                continue
            self.active += 1
            future.set_result(DownloadLease(self))
        self.work.download_state(self.active, len(self.waiters), limit)

    async def acquire(self):
        with phase('image_download_wait'):
            remaining_image_budget()
            future = asyncio.get_running_loop().create_future()
            self.waiters[future] = None
            self._grant()
            try:
                if future.done():
                    lease = future.result()
                else:
                    async with asyncio.timeout(remaining_image_budget()):
                        lease = await future
                remaining_image_budget()
                return lease
            except BaseException:
                self.waiters.pop(future, None)
                # 名额可能已经交接，但等待方尚未恢复便被取消或总预算到期。
                if future.done() and not future.cancelled():
                    await future.result().release()
                else:
                    self._grant()
                remaining_image_budget()
                raise


async def acquire_download_slot():
    loop = asyncio.get_running_loop()
    limiter = getattr(loop, '_image_download_limiter', None)
    if limiter is None:
        limiter = loop._image_download_limiter = DownloadLimiter()
    return await limiter.acquire()
