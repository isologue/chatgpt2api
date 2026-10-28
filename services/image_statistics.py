"""日志图片统计口径：只排除明确的内容审查拒绝，不读取用户提示词。"""
from __future__ import annotations

import re
import math

# 使用明确的拒绝语句，审核服务超时/不可用及一般的无图回复仍属于技术失败。
_POLICY_PATTERNS = (
    r"content[_ -]policy[_ -]violation", r"moderation[_ -]blocked",
    r"safety[_ -]violation", r"unsafe[_ -]prompt",
    r"(?:violat\w*|against|rejected by|blocked by).{0,50}(?:content|safety|usage) (?:policy|policies|guidelines)",
    r"(?:content|safety|usage) (?:policy|policies|guidelines).{0,50}(?:violat|reject|block)",
    r"(?:reject\w*|block\w*).{0,40}(?:safety system|content filters?)",
    r"检测到敏感词，拒绝本次任务", r"AI\s*审核未通过", r"(?:内容|安全)审(?:查|核)(?:未通过|不通过|拒绝)",
    r"(?:违反|触发|不符合|违背).{0,20}(?:内容政策|安全政策|使用政策|内容策略|安全策略|安全准则|内容准则)",
    r"(?:违规|违禁|不当|不安全)(?:的)?(?:内容|提示词|请求)",
)
_POLICY_RE = re.compile("|".join(_POLICY_PATTERNS), re.IGNORECASE | re.DOTALL)


def is_content_policy_rejection(message: object) -> bool:
    return bool(_POLICY_RE.search(str(message or "")))


def image_result_counts(item: dict) -> tuple[int, int, int, int]:
    """返回成功请求、技术失败、审查排除、成功图片数。"""
    detail = item.get("detail") or {}
    if item.get("type") != "call" or not isinstance(detail, dict):
        return 0, 0, 0, 0
    endpoint = str(detail.get("endpoint") or "")
    if endpoint not in {"/v1/images/generations", "/v1/images/edits", "/api/image-tasks/generations", "/api/image-tasks/edits"}:
        return 0, 0, 0, 0
    status = detail.get("status")
    if status == "success":
        body = detail.get("response_body")
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, dict):
            data = data.get("data")
        count = len(data) if isinstance(data, list) else len(detail.get("urls") or [])
        return 1, 0, 0, max(1, count)
    if status not in {"failed", "safety_rejected"}:
        return 0, 0, 0, 0
    if call_log_status(detail) == "safety_rejected":
        return 0, 0, 1, 0
    return 0, 1, 0, 0


def call_log_status(detail: dict) -> str:
    """供日志列表和图片统计共用，保留真实请求体、响应体。"""
    status = str(detail.get("status") or "")
    if status != "failed":
        return status
    codes = {str(detail.get("error_code") or "").lower()}
    error = detail.get("error") or ""
    body = detail.get("response_body")
    response_error = body.get("error") if isinstance(body, dict) else None
    if isinstance(response_error, dict):
        codes.add(str(response_error.get("code") or "").lower())
        error = str(error) + " " + str(response_error.get("message") or "") + " " + str(response_error.get("code") or "")
    # error_code 仅由内部异常写入，不读取请求体，避免提示词污染统计。
    error = str(error) + " " + str(detail.get("error_code") or "")
    if codes.intersection({"content_policy_violation", "content_filter", "content_blocked", "moderation_blocked", "safety_violation"}) or is_content_policy_rejection(error):
        return "safety_rejected"
    return status


def image_result_duration_ms(item: dict) -> float | None:
    """与结果总请求口径一致；旧记录缺失/非法耗时不当作 0。"""
    success, failed, _, _ = image_result_counts(item)
    if not (success or failed):
        return None
    value = item["detail"].get("duration_ms")
    if isinstance(value, bool) or value is None:
        return None
    try:
        duration = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return duration if math.isfinite(duration) and 0 <= duration <= 9e15 else None
