from __future__ import annotations

from services.image_async import blocking

import json
import threading
import time
from collections.abc import Callable
from concurrent.futures import wait
from datetime import datetime
from pathlib import Path
from typing import Any

from services.image_metrics import current_metrics, phase, queued_since, traced_image_task
from services.storage.image_task_storage import ImageTaskStorage
from services.thread_pool import image_executor
from services.config import DATA_DIR, config
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, json_safe, log_service
from services.protocol import openai_v1_image_edit, openai_v1_image_generations

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
TERMINAL_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _timestamp(value: object) -> float:
    if not isinstance(value, str) or not value.strip():
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _collect_image_urls(data: list[Any]) -> list[str]:
    urls: list[str] = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str) and url:
                urls.append(url)
    return urls


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "status": task.get("status"),
        "mode": task.get("mode"),
        "model": task.get("model"),
        "size": task.get("size"),
        "quality": task.get("quality"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
    }
    if task.get("conversation_id"):
        item["conversation_id"] = task.get("conversation_id")
    if task.get("data") is not None:
        item["data"] = task.get("data")
    if task.get("result_created") is not None:
        item["result_created"] = task.get("result_created")
    if task.get("usage") is not None:
        item["usage"] = task.get("usage")
    if task.get("error"):
        item["error"] = task.get("error")
    if task.get("progress"):
        item["progress"] = task.get("progress")
    if task.get("duration_ms") is not None:
        item["duration_ms"] = task.get("duration_ms")
    if task.get("status") in (TASK_STATUS_RUNNING, TASK_STATUS_QUEUED):
        if task.get("status") == TASK_STATUS_RUNNING:
            # RUNNING 状态仅在 started_ts 被设置后（image_stream_resolve_start）才计时
            base_ts = task.get("started_ts")
        else:
            # QUEUED 状态从 created_ts 开始计时（排队等待中）
            base_ts = task.get("created_ts") or task.get("updated_ts")
        if base_ts:
            item["elapsed_secs"] = round(time.time() - base_ts, 1)
    return item


class ImageTaskService:
    def __init__(
        self,
        path: Path,
        *,
        generation_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_generations.handle,
        edit_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_edit.handle,
        retention_days_getter: Callable[[], int] | None = None,
        task_storage: ImageTaskStorage | None = None,
        executor=None,
    ):
        self.path = path
        self.generation_handler = generation_handler
        self.edit_handler = edit_handler
        self.retention_days_getter = retention_days_getter or (lambda: config.image_retention_days)
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._store = task_storage or ImageTaskStorage(path)
        self._executor = executor or image_executor
        self._last_cleanup = 0.0
        self._futures = set()
        self._closed = False
        self._store.import_legacy(path, self._load_locked)
        self._store.recover(_now_iso(), time.time())
        self._cleanup_locked()

    def close(self, timeout: float | None = None) -> None:
        """Stop accepting tasks and drain this service's jobs before closing storage."""
        with self._lock:
            self._closed = True
            futures = tuple(self._futures)
        _, pending = wait(futures, timeout=timeout)
        if pending:
            # Do not dispose a locally-owned SQLite engine while a worker can
            # still be using it. The service remains closed and can be drained
            # by a later shutdown call.
            return
        close_storage = getattr(self._store, "close", None)
        if callable(close_storage):
            close_storage()

    def _schedule_reserved(self, handler, *args):
        submitted = time.perf_counter()

        def execute():
            token = queued_since.set(submitted)
            try:
                return handler(*args)
            finally:
                queued_since.reset(token)

        from services.image_async import supports_cooperative
        mode = args[1] if len(args) > 1 else ''
        image_handler = self.edit_handler if mode == 'edit' else self.generation_handler
        payload = args[2] if len(args) > 2 else {}
        cooperative_submit = getattr(self._executor, 'submit_cooperative_reserved', None)
        submit = cooperative_submit if cooperative_submit and supports_cooperative(image_handler, (payload,)) else self._executor.submit_reserved
        # 后台任务有独立预算与取消生命周期，不能继承创建请求的 disconnect。
        metrics_token = current_metrics.set(None)
        try:
            future = submit(execute)
        finally:
            current_metrics.reset(metrics_token)
        # Register before attaching the callback. A very short task can finish
        # between submit() and add_done_callback(), otherwise close() may wait
        # on a stale future forever while the completed future remains tracked.
        with self._lock:
            self._futures.add(future)

        def done(completed):
            with self._lock:
                self._futures.discard(completed)

        future.add_done_callback(done)
        return future

    def submit_generation(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        n: int = 1,
        size: str | None,
        quality: str = "auto",
        response_format: str = "url",
        base_url: str = "",
        request_body: object = None,
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "model": model,
            "n": n,
            "size": size,
            "quality": quality,
            "response_format": response_format,
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="generate", payload=payload, request_body=request_body)

    def submit_edit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        n: int = 1,
        size: str | None,
        quality: str = "auto",
        response_format: str = "url",
        base_url: str = "",
        images: list[tuple[bytes, str, str]] | None = None,
        masks: list[tuple[bytes, str, str]] | None = None,
        request_body: object = None,
    ) -> dict[str, Any]:
        payload = {
            "prompt": prompt,
            "images": images or [],
            "mask": masks or [],
            "model": model,
            "n": n,
            "size": size,
            "quality": quality,
            "response_format": response_format,
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="edit", payload=payload, request_body=request_body)

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested_ids = [_clean(task_id) for task_id in task_ids if _clean(task_id)]
        self._cleanup_locked()
        items = []
        missing_ids = []
        for task_id in requested_ids:
            task = self._store.get(_task_key(owner, task_id))
            if task is None:
                missing_ids.append(task_id)
            else:
                items.append(_public_task(task))
        if not requested_ids:
            items = [_public_task(task) for task in self._store.list_owner(owner)]
        return {"items": items, "missing_ids": missing_ids}

    def _submit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        mode: str,
        payload: dict[str, Any],
        request_body: object = None,
    ) -> dict[str, Any]:
        task_id = _clean(client_task_id)
        if not task_id:
            raise ValueError("client_task_id is required")
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        self._cleanup_locked()
        reserved = False
        scheduling_started = False
        with self._lock:
            if self._closed:
                raise RuntimeError("image task service is closed")
            self._executor.reserve()
            reserved = True
        try:
            # The database has its own idempotency constraint. Do not hold the
            # service lock while waiting on a database transaction, so polling
            # and unrelated task submissions remain responsive.
            existing = self._store.get(key)
            if existing is not None:
                self._executor.release()
                return _public_task(existing)
            now = _now_iso()
            task = {
                "id": task_id, "owner_id": owner, "status": TASK_STATUS_QUEUED,
                "mode": mode, "model": _clean(payload.get("model"), "gpt-image-2"),
                "size": _clean(payload.get("size")), "quality": _clean(payload.get("quality"), "auto"),
                "created_at": now, "updated_at": now, "created_ts": time.time(),
                "updated_ts": time.time(), "base_url": payload.get("base_url", ""), "response_format": payload.get("response_format", "url"),
                "request_body": json_safe(request_body) if request_body is not None else None,
            }
            created = self._store.create(key, task)
            if not created:
                self._executor.release()
                return _public_task(self._store.get(key))
            response = _public_task(task)
            try:
                # submit_reserved() releases the slot itself if executor.submit()
                # fails; after it succeeds, the future callback owns the slot.
                scheduling_started = True
                self._schedule_reserved(
                    self._run_task,
                    key,
                    mode,
                    payload,
                    dict(identity),
                    task["model"],
                    request_body,
                )
            except BaseException as exc:
                self._update_task(key, status=TASK_STATUS_ERROR, error=str(exc))
                raise
        except BaseException:
            # A failed create or scheduling attempt must not leak the queue slot.
            # A full queue never reserved a slot, and submit_reserved() handles
            # failures after scheduling has started.
            if reserved and not scheduling_started:
                self._executor.release()
            raise
        return response

    @traced_image_task
    def _run_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
        request_body: object = None,
    ) -> None:
        from api.image_task_response import serialize_task_status

        started = time.time()
        self._update_task(key, status=TASK_STATUS_RUNNING, error="")
        # 创建进度回调，每个步骤完成后更新任务状态
        def progress_callback(step: str) -> None:
            updates = {"progress": step}
            if step == "image_stream_resolve_start":
                updates["started_ts"] = time.time()
            self._update_task(key, **updates)
        # 将进度回调添加到 payload 中（handler 会提取并传递给 ConversationRequest）
        payload_with_progress = {**payload, "progress_callback": progress_callback}
        try:
            handler = self.edit_handler if mode == "edit" else self.generation_handler
            result = handler(payload_with_progress)
            if not isinstance(result, dict):
                raise RuntimeError("image task returned streaming result unexpectedly")
            data = result.get("data")
            account_email = _clean(result.get("_account_email") or result.get("account_email"))
            if not isinstance(data, list) or not data:
                upstream = _clean(result.get("message"))
                if upstream:
                    message = upstream
                else:
                    message = "号池中没有可用账号或所有账号均被限流，请检查号池状态（账号额度、是否被封禁、是否到达生图上限）"
                error = RuntimeError(message)
                if account_email:
                    setattr(error, "account_email", account_email)
                raise error
            usage = result.get("usage")
            duration_ms = int((time.time() - started) * 1000)
            result_created = result.get("created")
            if not isinstance(result_created, (int, float)):
                result_created = int(time.time())
            self._update_task(key, status=TASK_STATUS_SUCCESS, data=data, result_created=int(result_created), usage=usage, error="", duration_ms=duration_ms)
            public_task = self._task_for_log(key)
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成",
                request_preview=request_text(payload.get("prompt")),
                urls=_collect_image_urls(data),
                account_email=account_email,
                response_format=str(payload.get("response_format") or "url"),
                request_body=request_body if request_body is not None else {key: value for key, value in payload.items() if key != "progress_callback"},
                response_body=serialize_task_status(public_task) if public_task is not None else None,
                switch_count=int(result.get("_switch_count") or 0),
                account_time_ms=int(result.get("_account_time_ms") or 0),
                account_switch_trace=result.get("_account_switch_trace") or [],
            )
        except Exception as exc:
            error_message = str(exc) or "image task failed"
            account_email = _clean(getattr(exc, "account_email", ""))
            conversation_id = _clean(getattr(exc, "conversation_id", ""))
            duration_ms = int((time.time() - started) * 1000)
            self._update_task(key, status=TASK_STATUS_ERROR, error=error_message, data=[],
                              duration_ms=duration_ms,
                              **({"conversation_id": conversation_id} if conversation_id else {}))
            public_task = self._task_for_log(key)
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败",
                request_preview=request_text(payload.get("prompt")),
                status="failed",
                error=error_message,
                error_code=str(getattr(exc, "code", "")),
                account_email=account_email,
                response_format=str(payload.get("response_format") or "url"),
                request_body=request_body if request_body is not None else {key: value for key, value in payload.items() if key != "progress_callback"},
                response_body=serialize_task_status(public_task) if public_task is not None else None,
                switch_count=int(getattr(exc, "switch_count", 0)),
                account_time_ms=int(getattr(exc, "account_time_ms", 0)),
                account_switch_trace=getattr(exc, "account_switch_trace", []),
            )

    @blocking
    def _task_for_log(self, key: str) -> dict[str, Any] | None:
        task = self._store.get(key)
        return _public_task(task) if task else None

    @blocking
    def _log_call(
        self,
        identity: dict[str, object],
        mode: str,
        model: str,
        started: float,
        suffix: str,
        *,
        request_preview: str = "",
        status: str = "success",
        error: str = "",
        error_code: str = "",
        urls: list[str] | None = None,
        account_email: str = "",
        response_format: str = "url",
        request_body: object = None,
        response_body: object = None,
        switch_count: int = 0,
        account_time_ms: int = 0,
        account_switch_trace: object = None,
    ) -> None:
        endpoint = "/v1/images/edits" if mode == "edit" else "/v1/images/generations"
        summary_prefix = "图生图" if mode == "edit" else "文生图"
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": endpoint,
            "model": model,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
            "is_async": True,
            "error_code": error_code,
            "response_format": response_format,
        }
        if request_preview:
            detail["request_text"] = request_preview
        if request_body is not None:
            detail["request_body"] = json_safe(request_body)
        if response_body is not None:
            detail["response_body"] = json_safe(response_body)
        if error:
            detail["error"] = error
        if account_email:
            detail["account_email"] = account_email
        if urls:
            detail["urls"] = list(dict.fromkeys(urls))
        detail["switch_count"] = max(0, int(switch_count))
        detail["account_time_ms"] = max(0, int(account_time_ms))
        detail["account_switch_trace"] = account_switch_trace if isinstance(account_switch_trace, list) else []
        try:
            metrics = current_metrics.get()
            if metrics is not None:
                detail["timing_id"] = metrics.id
                detail["phase_timings_ms"] = metrics.snapshot()
                detail["diagnostics"] = metrics.diagnostics.snapshot()
            with phase("log_persistence"):
                log_service.add(LOG_TYPE_CALL, f"{summary_prefix}{suffix}", detail)
        except Exception:
            pass

    @blocking
    def _update_task(self, key: str, **updates: Any) -> None:
        self._store.update(key, {**updates, "updated_at": _now_iso(), "updated_ts": time.time()})

    def export_tasks(self) -> dict[str, Any]:
        return self._store.export()

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise RuntimeError(f"cannot migrate legacy image tasks: {self.path.name}") from exc
        raw_items = raw.get("tasks") if isinstance(raw, dict) else raw
        if not isinstance(raw_items, list):
            return {}
        tasks: dict[str, dict[str, Any]] = {}
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            task_id = _clean(item.get("id"))
            owner = _clean(item.get("owner_id"))
            if not task_id or not owner:
                continue
            status = _clean(item.get("status"))
            if status not in {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING, TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}:
                status = TASK_STATUS_ERROR
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": status,
                "mode": "edit" if item.get("mode") == "edit" else "generate",
                "model": _clean(item.get("model"), "gpt-image-2"),
                "size": _clean(item.get("size")),
                "quality": _clean(item.get("quality"), "auto"),
                "created_at": _clean(item.get("created_at"), _now_iso()),
                "updated_at": _clean(item.get("updated_at"), _clean(item.get("created_at"), _now_iso())),
                "created_ts": item.get("created_ts"),
                "updated_ts": item.get("updated_ts"),
                "started_ts": item.get("started_ts"),
                "duration_ms": item.get("duration_ms"),
            }
            data = item.get("data")
            if isinstance(data, list):
                task["data"] = data
            result_created = item.get("result_created")
            if isinstance(result_created, (int, float)):
                task["result_created"] = int(result_created)
            usage = item.get("usage")
            if isinstance(usage, dict):
                task["usage"] = usage
            error = _clean(item.get("error"))
            if error:
                task["error"] = error
            for field in ("progress", "conversation_id", "request_body", "response_format", "base_url"):
                if field in item:
                    task[field] = item[field]
            tasks[_task_key(owner, task_id)] = task
        return tasks

    def _cleanup_locked(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now - self._last_cleanup < 60.0:
                return
            try:
                retention_days = max(1, int(self.retention_days_getter()))
            except Exception:
                retention_days = 30
            self._store.cleanup(time.time() - retention_days * 86400)
            self._last_cleanup = now

    def resume_poll(
        self,
        identity: dict[str, object],
        task_id: str,
        extra_timeout_secs: float = 30.0,
    ) -> dict[str, Any]:
        """恢复对已超时任务的轮询，额外等待 extra_timeout_secs 秒。"""
        owner = _owner_id(identity)
        key = _task_key(owner, _clean(task_id))
        with self._lock:
            task = self._store.get(key)
            if task is None:
                raise ValueError("task not found")
            if task.get("status") != TASK_STATUS_ERROR:
                raise ValueError("task is not in error state")
            error_msg = _clean(task.get("error"))
            if "超时" not in error_msg:
                raise ValueError("task error is not a timeout error")
            conversation_id = _clean(task.get("conversation_id"))
            if not conversation_id:
                raise ValueError("task has no conversation_id")
            mode = task.get("mode", "generate")
            model = task.get("model", "gpt-image-2")
            if self._closed:
                raise RuntimeError("image task service is closed")
            self._executor.reserve()
            try:
                self._update_task(key, status=TASK_STATUS_RUNNING, error="")
            except BaseException:
                self._executor.release()
                raise
            response = _public_task({**task, "status": TASK_STATUS_RUNNING, "error": ""})
            try:
                self._schedule_reserved(self._run_resume_poll, key, conversation_id,
                    extra_timeout_secs, dict(identity), mode, model)
            except BaseException as exc:
                self._update_task(key, status=TASK_STATUS_ERROR, error=str(exc))
                raise
        return response

    def _run_resume_poll(
        self,
        key: str,
        conversation_id: str,
        extra_timeout_secs: float,
        identity: dict[str, object],
        mode: str,
        model: str,
    ) -> None:
        """后台线程：继续轮询已有 conversation_id 的图片结果。"""
        from api.image_task_response import serialize_task_status

        started = time.time()
        backend = None
        task = self._store.get(key)
        response_format = str(task.get("response_format") or "url") if task else "url"
        base_url = str(task.get("base_url") or "") if task else ""
        try:
            from services.openai_backend_api import OpenAIBackendAPI
            from services.protocol.conversation import format_image_result

            backend = OpenAIBackendAPI()
            file_ids, sediment_ids = backend._poll_image_results(
                conversation_id,
                extra_timeout_secs,
            )
            if not file_ids and not sediment_ids:
                raise RuntimeError(
                    f"继续等待 {extra_timeout_secs} 秒后仍未找到图片结果。"
                )

            image_urls = backend.resolve_conversation_image_urls(
                conversation_id, file_ids, sediment_ids, poll=False,
            )
            if not image_urls:
                raise RuntimeError("图片 URL 解析失败")

            image_items = [
                {"b64_json": __import__("base64").b64encode(image_data).decode("ascii")}
                for image_data in backend.download_image_bytes(image_urls)
            ]
            data = format_image_result(
                image_items,
                "",  # prompt 已不重要，结果已经拿到了
                response_format,
                base_url,
                int(time.time()),
            )["data"]
            self._update_task(key, status=TASK_STATUS_SUCCESS, data=data, error="", duration_ms=int((time.time() - started) * 1000))
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成（续轮询）",
                response_format=response_format,
                response_body=serialize_task_status(self._task_for_log(key) or {}),
                status="success",
                urls=_collect_image_urls(data),
            )
        except Exception as exc:
            error_message = str(exc) or "resume poll failed"
            duration_ms = int((time.time() - started) * 1000)
            self._update_task(key, status=TASK_STATUS_ERROR, error=error_message, data=[], duration_ms=duration_ms)
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败（续轮询）",
                response_format=response_format,
                response_body=serialize_task_status(self._task_for_log(key) or {}),
                error_code=str(getattr(exc, "code", "")),
                status="failed",
                error=error_message,
            )
        finally:
            if backend is not None:
                backend.close()


_account_backend = config.get_storage_backend()
image_task_service = ImageTaskService(
    DATA_DIR / "image_tasks.json",
    task_storage=ImageTaskStorage(DATA_DIR / "image_tasks.json", engine=getattr(_account_backend, "engine", None)),
)
