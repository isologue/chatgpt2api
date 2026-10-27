from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api.image_inputs import _parse_response_format, parse_image_edit_request, read_image_sources
from api.image_task_response import serialize_task_status
from api.support import require_identity, resolve_image_base_url
from services.content_filter import check_request
from services.image_task_service import image_task_service
from services.log_service import LoggedCall, json_safe


class ImageGenerationTaskRequest(BaseModel):
    client_task_id: str = Field(..., min_length=1)
    prompt: str = Field(..., min_length=1)
    model: str = "gpt-image-2"
    n: int = Field(default=1, ge=1, le=4)
    size: str | None = None
    quality: str = "auto"
    response_format: object = None


class ResumePollRequest(BaseModel):
    extra_timeout_secs: float = Field(default=30.0, ge=5.0, le=120.0)


def _parse_task_ids(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


async def filter_or_log(call: LoggedCall, text: str) -> None:
    try:
        await run_in_threadpool(check_request, text)
    except HTTPException as exc:
        await run_in_threadpool(call.log, "调用失败", status="failed", error=str(exc.detail))
        raise


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/v1/images/tasks/{task_id}")
    async def get_image_task(
        task_id: str,
        authorization: str | None = Header(default=None),
    ):
        identity = await run_in_threadpool(require_identity, authorization)
        result = await run_in_threadpool(image_task_service.list_tasks, identity, [task_id])
        if not result["items"]:
            raise HTTPException(status_code=404, detail={"error": "task not found", "task_id": task_id})
        return await run_in_threadpool(JSONResponse, serialize_task_status(result["items"][0]))

    @router.post("/api/image-tasks/generations")
    async def create_generation_task(
        body: ImageGenerationTaskRequest,
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = await run_in_threadpool(require_identity, authorization)
        request_body = await request.json()
        call = LoggedCall(identity, "/api/image-tasks/generations", body.model, "文生图任务", request_text=body.prompt)
        call.request_body = request_body
        await filter_or_log(call, body.prompt)
        try:
            return await run_in_threadpool(
                image_task_service.submit_generation,
                identity,
                client_task_id=body.client_task_id,
                prompt=body.prompt,
                model=body.model,
                n=body.n,
                size=body.size,
                quality=body.quality,
                response_format=_parse_response_format(body.response_format),
                base_url=resolve_image_base_url(request),
                request_body=request_body,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/image-tasks/edits")
    async def create_edit_task(
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = await run_in_threadpool(require_identity, authorization)
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        request_body = await request.json() if content_type == "application/json" else json_safe(await request.body())
        payload, image_sources, mask_sources = await parse_image_edit_request(request)
        client_task_id = str(payload.get("client_task_id") or "").strip()
        if not client_task_id:
            raise HTTPException(status_code=400, detail={"error": "client_task_id is required"})
        prompt = str(payload["prompt"])
        model = str(payload["model"])
        call = LoggedCall(identity, "/api/image-tasks/edits", model, "图生图任务", request_text=prompt)
        call.request_body = request_body
        await filter_or_log(call, prompt)
        images = await read_image_sources(image_sources)
        masks = await read_image_sources(mask_sources) if mask_sources else None
        try:
            return await run_in_threadpool(
                image_task_service.submit_edit,
                identity,
                client_task_id=client_task_id,
                prompt=prompt,
                model=model,
                n=payload["n"],
                size=payload["size"],
                quality=payload["quality"],
                response_format=payload["response_format"],
                base_url=resolve_image_base_url(request),
                images=images,
                masks=masks,
                request_body=request_body,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    @router.post("/api/image-tasks/{task_id}/resume-poll")
    async def resume_image_poll(
        task_id: str,
        body: ResumePollRequest,
        request: Request,
        authorization: str | None = Header(default=None),
    ):
        identity = await run_in_threadpool(require_identity, authorization)
        try:
            return await run_in_threadpool(
                image_task_service.resume_poll,
                identity,
                task_id,
                body.extra_timeout_secs,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    return router
