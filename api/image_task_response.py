from __future__ import annotations

from datetime import datetime
from typing import Any


def _unix_seconds(value: object) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            pass
    return 0


def serialize_task_creation(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at": _unix_seconds(task.get("created_at")),
        "model": task.get("model") or "gpt-image-2",
        "status": "queued",
        "task_id": task.get("id"),
    }


def strip_revised_prompt(data: object) -> object:
    if not isinstance(data, list):
        return data
    return [
        {key: value for key, value in item.items() if key != "revised_prompt"}
        if isinstance(item, dict)
        else item
        for item in data
    ]


def serialize_task_status(task: dict[str, Any]) -> dict[str, Any]:
    task_id = task.get("id")
    status = task.get("status")
    if status in ("queued", "running"):
        progress = task.get("progress")
        if status == "queued":
            percentage = 0
        else:
            percentage = {
                "getting_account": 8,
                "uploading": 18,
                "bootstrapping": 22,
                "getting_token": 28,
                "preparing_conversation": 35,
                "starting_generation": 40,
                "generating": 42,
                "image_stream_resolve_start": 60,
                "receiving_image": 85,
            }.get(progress, 5)
        return {"task_id": task_id, "status": "queued" if status == "queued" else "processing", "progress": f"{percentage}%"}
    if status == "success":
        return {
            "created_at": _unix_seconds(task.get("created_at")),
            "data": {
                "created": int(task.get("result_created") or _unix_seconds(task.get("updated_at"))),
                "data": strip_revised_prompt(task.get("data") or []),
            },
            "model": task.get("model") or "gpt-image-2",
            "progress": "100%",
            "status": "succeeded",
            "task_id": task_id,
        }
    return {"task_id": task_id, "status": "failed", "error": {"message": task.get("error") or "image task failed"}}
