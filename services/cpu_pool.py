"""CPU 密集任务进程池。

生图热路径里的 Proof-of-Work（utils.pow._pow_generate，sha3_512 暴力求解）是纯 CPU、
且输入很小 → 不释放 GIL。过去它直接在 image_executor 的工作线程里跑，高并发时上百个线程
争抢同一个 GIL，把 asyncio 事件循环饿死（压测观测到 event_loop_lag 峰值 63s，/monitor 超时）。

把这类纯函数放进独立子进程执行：各子进程有自己的 GIL，真正并行；调用线程阻塞等待结果时会
释放 GIL，主进程事件循环得以继续调度。进程数默认取 CPU 核数（可用环境变量覆盖）。图片协作路径的首页 HTML 解析与验证
脚本计算也在这里执行，协程直接等待进程结果，不占等待线程；同步兼容路径不变。
"""
from __future__ import annotations

from services.image_async import await_io, cooperative, run_blocking

import asyncio
import os
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import get_context
from threading import Lock

from utils.log import logger

_pool: ProcessPoolExecutor | None = None
_lock = Lock()


def _max_workers() -> int:
    raw = os.getenv("CHATGPT2API_CPU_POOL_WORKERS", "")
    try:
        configured = int(raw)
    except (TypeError, ValueError):
        configured = 0
    if configured > 0:
        return configured
    return max(1, os.cpu_count() or 1)


def _create_pool() -> ProcessPoolExecutor:
    # 使用 spawn：避免从多线程 + 已持有锁的主进程 fork 子进程导致死锁。
    # 目标函数所在模块（utils.pow）只依赖标准库 + pybase64，spawn 导入成本很低。
    workers = _max_workers()
    pool = ProcessPoolExecutor(max_workers=workers, mp_context=get_context("spawn"))
    logger.info({"event": "cpu_pool_start", "workers": workers})
    return pool


def get_pool() -> ProcessPoolExecutor:
    global _pool
    pool = _pool
    if pool is None:
        with _lock:
            if _pool is None:
                _pool = _create_pool()
            pool = _pool
    return pool


def reset_pool() -> None:
    global _pool
    with _lock:
        pool, _pool = _pool, None
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)


def _timed_call(func, args):
    """子进程只返回结果和时间数值；不记录或回传函数参数。"""
    started, cpu_started = time.perf_counter(), time.process_time()
    result = func(*args)
    return result, started, time.perf_counter(), time.process_time() - cpu_started


def _run_sync(func, *args, timeout: float | None = None):
    """保留原异常和回退语义，拆分进程池排队、计算与返回等待。"""
    from services.image_performance import record
    submitted = time.perf_counter()
    try:
        future = get_pool().submit(_timed_call, func, args)
    except (BrokenProcessPool, RuntimeError) as exc:
        record('cpu_submit', time.perf_counter() - submitted)
        logger.warning({"event": "cpu_pool_submit_fallback", "error": str(exc)})
        reset_pool()
        started = time.perf_counter()
        try:
            return func(*args)
        finally:
            record('cpu_fallback', time.perf_counter() - started)
    record('cpu_submit', time.perf_counter() - submitted)
    try:
        result, started, finished, cpu = future.result(timeout=timeout)
        received = time.perf_counter()
        record('cpu_queue', started - submitted)
        record('cpu_work', finished - started)
        record('cpu_work_cpu', cpu)
        record('cpu_return', received - finished)
        return result
    except BrokenProcessPool as exc:
        logger.warning({"event": "cpu_pool_broken_fallback", "error": str(exc)})
        reset_pool()
        started = time.perf_counter()
        try:
            return func(*args)
        finally:
            record('cpu_fallback', time.perf_counter() - started)


def compute(func, *args):
    """图片协作路径隔离纯计算；已有同步调用仍在原线程执行。"""
    return run(func, *args) if cooperative() else func(*args)


def run(func, *args, timeout: float | None = None):
    """同步兼容路径等待进程；图片协程直接等待进程结果，不占工作线程。"""
    if cooperative():
        return await_io(_run_async(func, args, timeout))
    return _run_sync(func, *args, timeout=timeout)


async def _run_async(func, args, timeout):
    from services.image_diagnostics import remaining_image_budget
    try:
        # 提交、排队和等待结果都使用原请求的剩余预算，不重新计时。
        async with asyncio.timeout(remaining_image_budget()):
            result = await _run_async_work(func, args, timeout)
    except TimeoutError:
        # 保留业务总预算/补查超时的具体异常；显式 CPU 超时仍原样传播。
        remaining_image_budget()
        raise
    remaining_image_budget()
    return result


async def _run_async_work(func, args, timeout):
    from services.async_completion import await_submitted
    from services.image_performance import record
    timing = {}

    def submit():
        timing['submitted'] = time.perf_counter()
        try:
            future = get_pool().submit(_timed_call, func, args)
            # 在返回提交结果前登记，已完成任务也不会把协程排队误算成 IPC。
            future.add_done_callback(lambda _: timing.__setitem__('received', time.perf_counter()))
            return future
        except (BrokenProcessPool, RuntimeError) as exc:
            timing['submit_error'] = exc
            raise
        finally:
            record('cpu_submit', time.perf_counter() - timing['submitted'])

    async def fallback(event, exc):
        logger.warning({'event': event, 'error': str(exc)})
        await run_blocking(reset_pool)
        started = time.perf_counter()
        try:
            return await run_blocking(func, *args)
        finally:
            record('cpu_fallback', time.perf_counter() - started)

    try:
        async with asyncio.timeout(timeout):
            result, started, finished, cpu = await await_submitted(submit)
    except (BrokenProcessPool, RuntimeError) as exc:
        if timing.get('submit_error') is exc:
            return await fallback('cpu_pool_submit_fallback', exc)
        if isinstance(exc, BrokenProcessPool):
            return await fallback('cpu_pool_broken_fallback', exc)
        raise  # 业务 RuntimeError 不能当作进程池损坏重跑。
    resumed = time.perf_counter()
    record('cpu_queue', started - timing['submitted'])
    record('cpu_work', finished - started)
    record('cpu_work_cpu', cpu)
    received = timing.get('received', resumed)
    record('cpu_return', received - finished)
    record('cpu_resume', resumed - received)
    return result


def _warmup_probe() -> bool:
    return True


def warmup() -> None:
    """启动阶段预热：在系统空闲时先把所有 worker 进程 spawn 起来，避免首批请求承担启动开销。"""
    try:
        pool = get_pool()
        futures = [pool.submit(_warmup_probe) for _ in range(_max_workers())]
        for fut in futures:
            fut.result(timeout=30)
        logger.info({"event": "cpu_pool_warmup_done", "workers": _max_workers()})
    except Exception as exc:  # 预热失败不致命，后续 run() 仍可按需创建或回退本进程
        logger.warning({"event": "cpu_pool_warmup_failed", "error": str(exc)})


def shutdown() -> None:
    reset_pool()
