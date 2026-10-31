"""账号信息三路并行I/O使用独立事件循环，不再受小线程池或3C线程数限制。"""
from __future__ import annotations

import asyncio
from concurrent.futures import TimeoutError
from threading import Lock, Thread

from curl_cffi import requests


class AccountInfoQueries:
    def __init__(self):
        self._lock = Lock()
        self._loop = None
        self._thread = None

    def _ensure_loop(self):
        with self._lock:
            if self._loop is None:
                self._loop = asyncio.SelectorEventLoop()
                self._thread = Thread(target=self._loop.run_forever, name="account-info-io", daemon=True)
                self._thread.start()
            return self._loop

    async def _fetch(self, session_kwargs, headers, specs):
        async def one(spec):
            # 隔离cookie/连接状态；Session容量仅为本子查询，不是全局并发上限。
            async with requests.AsyncSession(max_clients=1, headers=headers, **session_kwargs) as session:
                response = await session.request(**spec, timeout=20)
                return response
        # return_exceptions确保兄弟查询完成并关闭Session后再退出。
        results = await asyncio.gather(*(one(spec) for spec in specs), return_exceptions=True)
        for value in results:
            if isinstance(value, BaseException):
                raise value
        return results

    def run(self, session_kwargs, headers, specs):
        future = asyncio.run_coroutine_threadsafe(self._fetch(session_kwargs, headers, specs), self._ensure_loop())
        try:
            return future.result(timeout=25)
        except TimeoutError:
            future.cancel()
            raise RuntimeError("account info queries timed out") from None

    def close(self):
        with self._lock:
            loop, thread = self._loop, self._thread
            if loop is None:
                return
        async def drain():
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        asyncio.run_coroutine_threadsafe(drain(), loop).result(timeout=30)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=30)
        with self._lock:
            loop.close()
            self._loop = self._thread = None


account_info_queries = AccountInfoQueries()
