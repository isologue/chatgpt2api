from __future__ import annotations

from typing import Any

from services.account_service import account_service
from services.openai_backend_api import OpenAIBackendAPI
from utils.helper import CODEX_IMAGE_MODEL, standardize_model_name


def list_models() -> dict[str, Any]:
    result = OpenAIBackendAPI().list_models()
    data = result.get("data")
    if not isinstance(data, list):
        return result
    normalized_data: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in data:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        model_id = standardize_model_name(normalized.get("id"))
        if not model_id or model_id in seen:
            continue
        seen.add(model_id)
        normalized["id"] = model_id
        normalized["root"] = standardize_model_name(normalized.get("root")) or model_id
        parent = standardize_model_name(normalized.get("parent"))
        normalized["parent"] = parent or None
        normalized_data.append(normalized)
    data[:] = normalized_data
    dynamic_models: set[str] = set()
    accounts = account_service.list_accounts()
    web_image_accounts = [
        account
        for account in accounts
        if isinstance(account, dict)
    ]
    codex_types = {
        normalized
        for account in accounts
        if isinstance(account, dict)
           and account_service._normalize_source_type(account.get("source_type")) == "codex"
           and (normalized := account_service._normalize_account_type(account.get("type")))
    }

    if web_image_accounts:
        dynamic_models.add("gpt-image-2")
    if codex_types & {"Plus", "Team", "Pro"}:
        dynamic_models.add(CODEX_IMAGE_MODEL)
    if "Plus" in codex_types:
        dynamic_models.add(f"plus-{CODEX_IMAGE_MODEL}")
    if "Team" in codex_types:
        dynamic_models.add(f"team-{CODEX_IMAGE_MODEL}")
    if "Pro" in codex_types:
        dynamic_models.add(f"pro-{CODEX_IMAGE_MODEL}")

    for model in sorted(dynamic_models):
        normalized_model = standardize_model_name(model)
        if normalized_model in seen:
            continue
        seen.add(normalized_model)
        data.append({
            "id": normalized_model,
            "object": "model",
            "created": 0,
            "owned_by": "chatgpt2api",
            "permission": [],
            "root": normalized_model,
            "parent": None,
        })
    return result
