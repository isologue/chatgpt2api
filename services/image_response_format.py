"""图片响应格式规范化；策略只在请求接入时应用，任务保存生效值。"""
from __future__ import annotations


def normalize_image_response_format(value: object) -> str:
    text = str(value or "").strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if text in {"url", "imageurl", "link"}:
        return "url"
    if text in {"base64", "b64", "b64json", "base64json"}:
        return "b64_json"
    raise ValueError("response_format must be 'url' or 'b64_json' (aliases: base64, b64, base64_json)")


def resolve_image_response_format(value: object = None) -> str:
    from services.config import config

    default = config.image_response_format
    if not config.image_allow_response_format_override or value is None or value == "":
        return default
    return normalize_image_response_format(value)
