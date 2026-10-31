from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from threading import Event

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

from utils.log import logger
from services.image_metrics import ImageTimingMiddleware, measure_event_loop_lag
from services.auth_service import auth_service
from services.log_service import log_service
from services.image_storage_service import image_storage_service
from services.image_task_service import image_task_service

from api import accounts, ai, image_tasks, system
from api.errors import install_exception_handlers
from api.support import resolve_web_asset, start_limited_account_watcher
from services.backup_service import backup_service
from services.config import config
from services.retention_service import retention_service
from services.thread_pool import apply_anyio_thread_limit, configure_image_executors, close_image_async_runtime
from services.account_service import account_service
from services.isolated_executor import completion_executor
from services import cpu_pool
from services.account_info_queries import account_info_queries


def create_app() -> FastAPI:
    app_version = config.app_version

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        auth_service.start()
        await apply_anyio_thread_limit(config.anyio_thread_limit)
        configure_image_executors(config.image_workers, config.image_queue_size)
        account_service.verification.reconfigure()
        completion_executor.reconfigure(config.image_response_workers)
        # 预热 CPU 进程池：PoW 等纯 CPU 任务改到子进程并行执行，避免占满主进程 GIL 饿死事件循环。
        await run_in_threadpool(cpu_pool.warmup)
        await run_in_threadpool(log_service.initialize)
        stop_event = Event()
        thread = start_limited_account_watcher(stop_event)
        cleanup_thread = retention_service.start_scheduler(stop_event)
        backup_service.start()
        lag_task = asyncio.create_task(measure_event_loop_lag(), name="event-loop-lag")
        try:
            yield
        finally:
            lag_task.cancel()
            try:
                await lag_task
            except asyncio.CancelledError:
                pass
            stop_event.set()
            thread.join(timeout=1)
            cleanup_thread.join(timeout=1)
            backup_service.stop()
            await run_in_threadpool(thread.join)
            await run_in_threadpool(cleanup_thread.join)
            await run_in_threadpool(retention_service.drain)
            await run_in_threadpool(image_task_service.close)
            await run_in_threadpool(close_image_async_runtime)
            await run_in_threadpool(account_service.verification.close)
            await run_in_threadpool(account_info_queries.close)
            await run_in_threadpool(log_service.close)
            await run_in_threadpool(image_storage_service._index_store.close)
            await run_in_threadpool(auth_service.close)
            await run_in_threadpool(cpu_pool.shutdown)
            await run_in_threadpool(logger.flush)

    app = FastAPI(title="chatgpt2api", version=app_version, lifespan=lifespan)
    install_exception_handlers(app)
    app.add_middleware(ImageTimingMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(ai.create_router())
    app.include_router(accounts.create_router())
    app.include_router(image_tasks.create_router())
    app.include_router(system.create_router(app_version))

    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    async def serve_web(full_path: str):
        asset = resolve_web_asset(full_path)
        if asset is not None:
            return FileResponse(asset)
        if full_path.strip("/").startswith("_next/"):
            raise HTTPException(status_code=404, detail="Not Found")
        fallback = resolve_web_asset("")
        if fallback is None:
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(fallback)

    return app
