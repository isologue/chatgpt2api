"""CPU 密集任务进程池。

生图热路径里的 Proof-of-Work（utils.pow._pow_generate，sha3_512 暴力求解）是纯 CPU、
且输入很小 → 不释放 GIL。过去它直接在 image_executor 的工作线程里跑，高并发时上百个线程
争抢同一个 GIL，把 asyncio 事件循环饿死（压测观测到 event_loop_lag 峰值 63s，/monitor 超时）。

把这类纯函数放进独立子进程执行：各子进程有自己的 GIL，真正并行；调用线程阻塞等待结果时会
释放 GIL，主进程事件循环得以继续调度。进程数默认取 CPU 核数（可用环境变量覆盖）。
"""
from __future__ import annotations

import os
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


def run(func, *args, timeout: float | None = None):
    """在 CPU 进程池里执行纯函数并返回结果。

    - 提交阶段失败（池正在关闭的 RuntimeError / 池已损坏）→ 回退本进程直接计算；
    - 执行阶段 worker 崩溃（BrokenProcessPool）→ 回退本进程直接计算；
    - func 自身抛出的异常（如 PoW 无解的 RuntimeError）正常向上传递，不被吞掉。

    func 及其参数必须可 pickle，且 func 所在模块需可被 spawn 子进程轻量导入。
    """
    try:
        future = get_pool().submit(func, *args)
    except (BrokenProcessPool, RuntimeError) as exc:
        logger.warning({"event": "cpu_pool_submit_fallback", "error": str(exc)})
        reset_pool()
        return func(*args)
    try:
        return future.result(timeout=timeout)
    except BrokenProcessPool as exc:
        logger.warning({"event": "cpu_pool_broken_fallback", "error": str(exc)})
        reset_pool()
        return func(*args)


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
