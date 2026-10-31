from __future__ import annotations

from services.image_async import blocking, cooperative, parallel_calls

import pybase64 as base64
import json
import re
import threading
import time
from concurrent.futures import as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Iterator

import tiktoken

from services.account_service import account_service
from services.account_pool import ImageAttemptBudget, image_attempt_results
from services.config import config
from services.thread_pool import image_cleanup_executor, image_part_executor
from contextvars import copy_context
from services.image_diagnostics import (
    ImageDeadlineExceeded,
    image_poll_budget,
    poll_pause,
    ImageDownloadError,
    ImageResultResolveError,
    ImageRequestCancelled,
    attempt_context,
    record_image_decision,
    remaining_image_budget,
    remaining_poll_budget,
    wait_with_image_budget,
)
from services.image_metrics import measured, phase
from services.image_storage_service import image_storage_service
from services.openai_backend_api import (
    ImageContentPolicyError,
    ImagePollTimeoutError,
    ImageStreamHardTimeoutError,
    OpenAIBackendAPI,
)
from utils.helper import (
    IMAGE_MODELS,
    extract_image_from_message_content,
    is_codex_image_model,
    is_supported_image_model,
    split_image_model,
)
from utils.image_tokens import count_image_content_tokens
from utils.log import logger


class ImageGenerationError(Exception):
    def __init__(
        self,
        message: str,
        status_code: int = 502,
        error_type: str = "server_error",
        code: str | None = "upstream_error",
        param: str | None = None,
        account_email: str = "",
        conversation_id: str = "",
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.code = code
        self.param = param
        self.account_email = account_email
        self.conversation_id = conversation_id

    def to_openai_error(self) -> dict[str, Any]:
        error_dict = {
            "error": {
                "message": public_image_error_message(str(self)),
                "type": self.error_type,
                "param": self.param,
                "code": self.code,
            }
        }
        if self.account_email:
            error_dict["error"]["account_email"] = self.account_email
        return error_dict


def public_image_error_message(message: str) -> str:
    text = str(message or "").strip()
    lower = text.lower()
    if any(item in lower for item in ("backend-api/", "status=", "body=", "chatgpt.com", "upstreamhttperror")):
        return "The image generation request failed. Please try again later."
    return text or "The image generation request failed. Please try again later."


class ImageAccountSwitchTracker:
    """Track account attempts for one image request, including parallel n images."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._attempts: dict[int, list[dict[str, Any]]] = {}
        self._active: dict[int, dict[str, Any]] = {}
        self._pending_reasons: dict[int, str] = {}

    @staticmethod
    def _now() -> tuple[float, str]:
        timestamp = time.time()
        return timestamp, datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def mark_switch(self, index: int, reason: str) -> None:
        with self._lock:
            self._pending_reasons[index] = str(reason or "账号不可用，切换账号")

    def begin(self, index: int, account_name: str) -> None:
        name = str(account_name or "未知账号").strip() or "未知账号"
        with self._lock:
            current = self._active.get(index)
            if current and current["account_name"] == name:
                return
            if current:
                self._finish_locked(index, self._pending_reasons.pop(index, "账号不可用，切换账号"))
            started_ts, started_at = self._now()
            attempt = {
                "image_index": index,
                "account_name": name,
                "started_at": started_at,
                "started_ts": started_ts,
            }
            self._attempts.setdefault(index, []).append(attempt)
            self._active[index] = attempt

    def finish(self, index: int, reason: str = "生成完成") -> None:
        with self._lock:
            if index in self._active:
                self._finish_locked(index, reason)
            self._pending_reasons.pop(index, None)

    def _finish_locked(self, index: int, reason: str) -> None:
        attempt = self._active.pop(index, None)
        if not attempt:
            return
        ended_ts, ended_at = self._now()
        attempt["ended_at"] = ended_at
        attempt["duration_ms"] = max(0, int((ended_ts - float(attempt.pop("started_ts", ended_ts))) * 1000))
        attempt["reason"] = str(reason or "生成完成")

    def summary(self) -> dict[str, Any]:
        with self._lock:
            for index in tuple(self._active):
                self._finish_locked(index, "请求结束")
            attempts = [attempt for index in sorted(self._attempts) for attempt in self._attempts[index]]
            for attempt in attempts:
                attempt.pop("started_ts", None)
            switch_count = sum(max(0, len(items) - 1) for items in self._attempts.values())
            account_time_ms = sum(int(item.get("duration_ms") or 0) for item in attempts)
            return {
                "switch_count": switch_count,
                "account_time_ms": account_time_ms,
                "account_switch_trace": [dict(item) for item in attempts],
            }


def _attach_account_switch_summary(
        error: ImageGenerationError,
        tracker: ImageAccountSwitchTracker | None,
) -> ImageGenerationError:
    """将失败请求的账号尝试轨迹挂到异常，确保失败日志也能分析切号耗时。"""
    if tracker is not None:
        for key, value in tracker.summary().items():
            setattr(error, key, value)
    return error


def is_token_invalid_error(message: str) -> bool:
    text = str(message or "").lower()
    return (
        "token_invalidated" in text
        or "token_revoked" in text
        or "authentication token has been invalidated" in text
        or "invalidated oauth token" in text
    )


def is_tls_connection_error(message: str) -> bool:
    """检测 TLS/SSL 连接错误，这类错误通常可以通过重试解决。"""
    text = str(message or "").lower()
    return (
        "curl: (35)" in text
        or "tls connect error" in text
        or "openssl_internal" in text
        or "ssl: wrong_version_number" in text
        or "ssl: certificate_verify_failed" in text
        or "connection aborted" in text
        or "remote disconnected" in text
        or "connection reset by peer" in text
    )


def is_connection_timeout_error(message: str) -> bool:
    """检测连接超时错误（如 curl 28），这类错误可通过同账号短等待重试解决。"""
    text = str(message or "").lower()
    return (
        "curl: (28)" in text
        or "operation timed out" in text
        or "connection timed out" in text
        or "read timed out" in text
        or "connect timeout" in text
    )


def image_stream_error_message(message: str) -> str:
    text = str(message or "")
    if is_token_invalid_error(text):
        return "image generation failed"
    if is_tls_connection_error(text):
        return "upstream image connection failed, please retry later"
    if is_connection_timeout_error(text):
        return "upstream connection timed out, please retry later"
    return text or "image generation failed"


REFERENCED_IMAGE_IDS_RE = re.compile(r'"referenced_image_ids"\s*:\s*\[([^\]]+)\]')
# 检测模型返回的部分工具调用 JSON（如 {"size":"1920x1088","n":1}）
# 这些 JSON 包含图片生成工具的参数，但没有实际生成图片
TOOL_PARAMS_JSON_RE = re.compile(
    r'\{\s*"size"\s*:\s*"\d+x\d+"\s*,\s*"n"\s*:\s*\d+\s*\}'
)


def is_model_text_reply_instead_of_image(message: str) -> bool:
    """检测模型是否返回了文本回复（包含工具调用 JSON）而非实际生成图片。

    当上游 ChatGPT 未能触发图片生成工具时，会返回一段描述性文本，
    其中可能包含 JSON 参数（如 prompt、referenced_image_ids、size/n 等）。
    这种情况应被视为「上游未生成图片」而非「内容策略违规」。

    检测两种模式：
    1. 完整的工具调用 JSON（含 referenced_image_ids）
    2. 部分的工具参数 JSON（如 {"size":"1920x1088","n":1}）
    """
    if not message:
        return False
    if REFERENCED_IMAGE_IDS_RE.search(message):
        return True
    # 检测部分工具参数 JSON（模型返回了工具参数但未触发工具）
    if TOOL_PARAMS_JSON_RE.search(message):
        return True
    return False


@blocking
def encode_images(images: Iterable[tuple[bytes, str, str]]) -> list[str]:
    return [base64.b64encode(data).decode("ascii") for data, _, _ in images if data]


def save_image_bytes(image_data: bytes, base_url: str | None = None) -> str:
    return image_storage_service.save(image_data, base_url).url


def message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and str(item.get("type") or "") in {"text", "input_text", "output_text"}:
                parts.append(str(item.get("text") or ""))
        return "".join(parts)
    return ""


def normalize_messages(messages: object, system: Any = None) -> list[dict[str, Any]]:
    normalized = []
    if config.global_system_prompt:
        normalized.append({"role": "system", "content": config.global_system_prompt})
    system_text = message_text(system)
    if system_text:
        normalized.append({"role": "system", "content": system_text})
    if isinstance(messages, list):
        for message in messages:
            if not isinstance(message, dict):
                continue
            role = message.get("role", "user")
            content = message.get("content", "")
            text = message_text(content)
            images: list[tuple[bytes, str]] = []
            if role == "user":
                images.extend(extract_image_from_message_content(content))
                if isinstance(content, list):
                    for part in content:
                        if not isinstance(part, dict) or part.get("type") != "image":
                            continue
                        data = part.get("data")
                        if isinstance(data, (bytes, bytearray)) and all(existing[0] != bytes(data) for existing in images):
                            images.append((bytes(data), str(part.get("mime") or "image/png")))
            if images:
                parts: list[Any] = []
                if text:
                    parts.append({"type": "text", "text": text})
                for data, mime in images:
                    parts.append({"type": "image", "data": data, "mime": mime})
                normalized.append({"role": role, "content": parts})
            else:
                normalized.append({"role": role, "content": text})
    return normalized


def prompt_with_global_system(prompt: str) -> str:
    return f"{config.global_system_prompt}\n\n{prompt}" if config.global_system_prompt else prompt


def assistant_history_text(messages: list[dict[str, Any]]) -> str:
    return "".join(str(item.get("content") or "") for item in messages if item.get("role") == "assistant")


def assistant_history_messages(messages: list[dict[str, Any]]) -> list[str]:
    return [str(item.get("content") or "") for item in messages if item.get("role") == "assistant" and item.get("content")]


def build_image_prompt(prompt: str, size: str | None, quality: str = "auto") -> str:
    hints = []
    if size:
        hints.append(f"输出图片尺寸为 {size}。")
    if quality:
        hints.append(f"输出图片质量为 {quality}。")
    return f"{prompt.strip()}\n\n{''.join(hints)}" if hints else prompt


def encoding_for_model(model: str):
    try:
        return tiktoken.encoding_for_model(model)
    except KeyError:
        try:
            return tiktoken.get_encoding("o200k_base")
        except KeyError:
            return tiktoken.get_encoding("cl100k_base")


def count_message_image_tokens(messages: list[dict[str, Any]], model: str) -> int:
    return sum(count_image_content_tokens(message.get("content"), model) for message in messages)


def count_message_text_tokens(messages: list[dict[str, Any]], model: str) -> int:
    encoding = encoding_for_model(model)
    total = 0
    for message in messages:
        total += 3
        for key, value in message.items():
            if key == "content" and isinstance(value, list):
                total += len(encoding.encode(message_text(value)))
            elif isinstance(value, str):
                total += len(encoding.encode(value))
            else:
                continue
            if key == "name":
                total += 1
    return total + 3


def count_message_tokens(messages: list[dict[str, Any]], model: str) -> int:
    return count_message_text_tokens(messages, model) + count_message_image_tokens(messages, model)


@blocking
def count_text_tokens(text: str, model: str) -> int:
    return len(encoding_for_model(model).encode(text))


@blocking
def format_image_result(
    items: list[dict[str, Any]],
    prompt: str,
    response_format: str,
    base_url: str | None = None,
    created: int | None = None,
    message: str = "",
) -> dict[str, Any]:
    data: list[dict[str, Any]] = []
    for item in items:
        b64_json = str(item.get("b64_json") or "").strip()
        image_data = item.get("_image_bytes")
        if not isinstance(image_data, bytes):
            if not b64_json:
                continue
            image_data = base64.b64decode(b64_json)
        if response_format == "b64_json":
            data.append({
                "b64_json": b64_json or base64.b64encode(image_data).decode("ascii"),
                "url": save_image_bytes(image_data, base_url),
            })
        else:
            data.append({
                "url": save_image_bytes(image_data, base_url),
            })
    result: dict[str, Any] = {"created": created or int(time.time()), "data": data}
    if message and not data:
        result["message"] = message
    return result


@dataclass
class ConversationRequest:
    model: str = "auto"
    prompt: str = ""
    messages: list[dict[str, Any]] | None = None
    thinking_effort: str = ""
    images: list[str] | None = None
    n: int = 1
    size: str | None = None
    quality: str = "auto"
    response_format: str = "b64_json"
    base_url: str | None = None
    message_as_error: bool = False
    progress_callback: Any = None  # Callable[[str], None] | None
    account_switch_tracker: ImageAccountSwitchTracker | None = None


@dataclass
class ConversationState:
    text: str = ""
    raw_text: str = ""
    conversation_id: str = ""
    file_ids: list[str] = field(default_factory=list)
    sediment_ids: list[str] = field(default_factory=list)
    blocked: bool = False
    tool_invoked: bool | None = None
    turn_use_case: str = ""


@dataclass
class ImageOutput:
    kind: str
    model: str
    index: int
    total: int
    created: int = field(default_factory=lambda: int(time.time()))
    text: str = ""
    upstream_event_type: str = ""
    data: list[dict[str, Any]] = field(default_factory=list)
    account_email: str = ""
    conversation_id: str = ""

    def to_chunk(self) -> dict[str, Any]:
        chunk: dict[str, Any] = {
            "object": "image.generation.chunk",
            "created": self.created,
            "model": self.model,
            "index": self.index,
            "total": self.total,
            "progress_text": self.text,
            "upstream_event_type": self.upstream_event_type,
            "data": [],
        }
        if self.account_email:
            chunk["_account_email"] = self.account_email
        if self.conversation_id:
            chunk["_conversation_id"] = self.conversation_id
        if self.kind == "message":
            chunk.update({
                "object": "image.generation.message",
                "message": self.text,
            })
            chunk.pop("progress_text", None)
            chunk.pop("upstream_event_type", None)
        elif self.kind == "result":
            chunk.update({
                "object": "image.generation.result",
                "data": self.data,
            })
            chunk.pop("progress_text", None)
            chunk.pop("upstream_event_type", None)
        return chunk


def assistant_message_text(message: dict[str, Any]) -> str:
    content = message.get("content") or {}
    parts = content.get("parts") or []
    if isinstance(parts, list) and parts:
        text = "".join(part for part in parts if isinstance(part, str))
        if text:
            return text
    # Fallback: content_type "code" stores text in the "text" field instead of "parts"
    text_field = str(content.get("text") or "")
    if text_field:
        return text_field
    return ""


def is_visible_assistant_message(message: dict[str, Any]) -> bool:
    """Return whether an upstream assistant message is intended for the user."""
    author = message.get("author")
    if not isinstance(author, dict):
        return False
    role = str(author.get("role") or "").strip().lower()
    if role != "assistant":
        return False

    metadata = message.get("metadata") or {}
    if isinstance(metadata, dict) and metadata.get("is_visually_hidden_from_conversation") is True:
        return False

    # Tool commands are emitted as assistant messages addressed to recipients
    # such as "web". Only messages addressed to everyone are user-visible.
    recipient = str(message.get("recipient") or "").strip().lower()
    if recipient and recipient != "all":
        return False

    # Reasoning and other internal channels must not leak into API text output.
    channel = str(message.get("channel") or "").strip().lower()
    if channel and channel != "final":
        return False

    return True


def strip_history(text: str, history_text: str = "") -> str:
    text = str(text or "")
    history_text = str(history_text or "")
    while history_text and text.startswith(history_text):
        text = text[len(history_text):]
    return text


def sanitize_output_text(text: str) -> str:
    text = str(text or "")

    def is_internal_annotation_part(part: str) -> bool:
        value = part.strip()
        if not value:
            return True
        lower = value.lower()
        return bool(
            re.fullmatch(r"turn\d+[a-z]*\d*", lower)
            or re.fullmatch(r"turn\d+\w*", lower)
            or lower.startswith(("turn", "source", "sources"))
        )

    def readable_annotation_part(parts: list[str]) -> str:
        for part in parts:
            value = part.strip()
            if value and not is_internal_annotation_part(value):
                return value
        return ""

    def annotation_text(payload: str) -> str:
        parts = [part.strip() for part in payload.split("\ue202")]
        kind = (parts[0] if parts else "").lower()
        data = parts[1:]
        if kind == "url":
            label = data[0] if data else ""
            url = data[1] if len(data) > 1 else ""
            if label and url.startswith(("http://", "https://")):
                return f"{label} ({url})"
            return label or url
        if kind == "cite":
            return readable_annotation_part(data)
        return readable_annotation_part(data)

    def replace_annotation(match: re.Match[str]) -> str:
        return annotation_text(match.group(1))

    def replace_annotation_before_punctuation(match: re.Match[str]) -> str:
        leading_space = match.group(1)
        replacement = annotation_text(match.group(2))
        return f"{leading_space}{replacement}" if replacement else ""

    # ChatGPT web sometimes returns rich annotation markers using private-use
    # characters. API clients cannot render those. Preserve readable labels
    # from entity/link annotations, while removing internal citation pointers.
    text = re.sub(r"(\s*)\ue200([^\ue201]*)\ue201(?=[.,;:!?])", replace_annotation_before_punctuation, text)
    text = re.sub(r"\ue200([^\ue201]*)\ue201", replace_annotation, text)
    text = re.sub(r"\ue200[^\ue201]*$", "", text)
    return text


def assistant_raw_text(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    for candidate in (event, event.get("v")):
        if not isinstance(candidate, dict):
            continue
        message = candidate.get("message")
        if not isinstance(message, dict) or not is_visible_assistant_message(message):
            continue
        text = assistant_message_text(message)
        if text:
            return strip_history(text, history_text)
    return apply_text_patch(event, current_text, history_text)


def assistant_text(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    return sanitize_output_text(assistant_raw_text(event, current_text, history_text))


def event_assistant_text(event: dict[str, Any], history_text: str = "") -> str:
    for candidate in (event, event.get("v")):
        if not isinstance(candidate, dict):
            continue
        message = candidate.get("message")
        if isinstance(message, dict) and is_visible_assistant_message(message):
            return strip_history(assistant_message_text(message), history_text)
    return ""


def apply_text_patch(event: dict[str, Any], current_text: str = "", history_text: str = "") -> str:
    if event.get("p") == "/message/content/parts/0":
        return apply_patch_op(event, current_text, history_text)

    operations = event.get("v")
    if isinstance(operations, str) and current_text and not event.get("p") and not event.get("o"):
        return current_text + operations

    if event.get("o") == "patch" and isinstance(operations, list):
        text = current_text
        for item in operations:
            if isinstance(item, dict):
                text = apply_text_patch(item, text, history_text)
        return text

    if not isinstance(operations, list):
        return current_text

    text = current_text
    for item in operations:
        if isinstance(item, dict):
            text = apply_text_patch(item, text, history_text)
    return text


def apply_patch_op(operation: dict[str, Any], current_text: str, history_text: str = "") -> str:
    op = operation.get("o")
    value = str(operation.get("v") or "")
    if op == "append":
        return current_text + value
    if op == "replace":
        return strip_history(value, history_text)
    return current_text


def add_unique(values: list[str], candidates: list[str]) -> None:
    for candidate in candidates:
        if candidate and candidate not in values:
            values.append(candidate)


FILE_SERVICE_ID_RE = re.compile(r"file-service://([A-Za-z0-9_-]+)")
FILE_ID_RE = re.compile(r"\b(file[-_](?!service\b)[A-Za-z0-9_-]+)\b")
# 真正的图片文件 ID 格式：file_00000000 + 24位十六进制字符（共32字符）
# 用于过滤非图片文件 ID（如 file_upload_business_upsell）
REAL_IMAGE_FILE_ID_RE = re.compile(r"\bfile_00000000[a-f0-9]{24}\b")
SEDIMENT_ID_RE = re.compile(r"sediment://([A-Za-z0-9_-]+)")


def extract_conversation_ids(payload: str) -> tuple[str, list[str], list[str]]:
    conversation_match = re.search(r'"conversation_id"\s*:\s*"([^"]+)"', payload)
    conversation_id = conversation_match.group(1) if conversation_match else ""
    file_ids: list[str] = []
    # Negative lookahead excludes "file-service" (URI prefix, not a real id).
    add_unique(file_ids, FILE_SERVICE_ID_RE.findall(payload))
    # 只提取真正的图片文件 ID（file_00000000... 格式），过滤非图片文件 ID（如 file_upload_business_upsell）
    add_unique(file_ids, REAL_IMAGE_FILE_ID_RE.findall(payload))
    sediment_ids = SEDIMENT_ID_RE.findall(payload)
    return conversation_id, file_ids, sediment_ids


def is_image_tool_event(event: dict[str, Any]) -> bool:
    value = event.get("v")
    message = event.get("message") or (value.get("message") if isinstance(value, dict) else None)
    if not isinstance(message, dict):
        return False
    metadata = message.get("metadata") or {}
    author = message.get("author") or {}
    content = message.get("content") or {}
    if author.get("role") != "tool":
        return False
    if metadata.get("async_task_type") == "image_gen":
        return True
    if content.get("content_type") != "multimodal_text":
        return False
    return any(
        isinstance(part, dict) and (
                part.get("content_type") == "image_asset_pointer"
                or str(part.get("asset_pointer") or "").startswith(("file-service://", "sediment://"))
        )
        for part in content.get("parts") or []
    )


def _is_user_message_event(event: dict[str, Any]) -> bool:
    """检查事件是否来自 user 角色消息。"""
    value = event.get("v")
    message = event.get("message") or (value.get("message") if isinstance(value, dict) else None)
    if isinstance(message, dict):
        author = message.get("author") or {}
        if str(author.get("role") or "").strip().lower() == "user":
            return True
    return False


def update_conversation_state(state: ConversationState, payload: str, event: dict[str, Any] | None = None) -> None:
    conversation_id, file_ids, sediment_ids = extract_conversation_ids(payload)
    if conversation_id and not state.conversation_id:
        state.conversation_id = conversation_id
    # Accept file_id / sediment_id when any of:
    #   1) event is a complete image_gen tool message
    #   2) prior server_ste_metadata already flipped tool_invoked True (in an image_gen turn),
    #      BUT only for non-user messages — user messages contain the uploaded input image
    #      which must NOT be treated as a generated output.
    #   3) patch event whose payload references asset_pointer / file-service://,
    #      BUT only when the event is not a user message.
    is_patch_event = isinstance(event, dict) and event.get("o") == "patch"
    is_user_msg = isinstance(event, dict) and _is_user_message_event(event)
    image_context = (
        (isinstance(event, dict) and is_image_tool_event(event))
        or (state.tool_invoked is True and not is_user_msg)
        or (is_patch_event and not is_user_msg and ("asset_pointer" in payload or "file-service://" in payload))
    )
    if image_context:
        add_unique(state.file_ids, file_ids)
        add_unique(state.sediment_ids, sediment_ids)
    if not isinstance(event, dict):
        return
    state.conversation_id = str(event.get("conversation_id") or state.conversation_id)
    value = event.get("v")
    if isinstance(value, dict):
        state.conversation_id = str(value.get("conversation_id") or state.conversation_id)
    if event.get("type") == "moderation":
        moderation = event.get("moderation_response")
        if isinstance(moderation, dict) and moderation.get("blocked") is True:
            state.blocked = True
    if event.get("type") == "server_ste_metadata":
        metadata = event.get("metadata")
        if isinstance(metadata, dict):
            if isinstance(metadata.get("tool_invoked"), bool):
                state.tool_invoked = metadata["tool_invoked"]
            state.turn_use_case = str(metadata.get("turn_use_case") or state.turn_use_case)


def conversation_base_event(event_type: str, state: ConversationState, **extra: Any) -> dict[str, Any]:
    return {
        "type": event_type,
        "text": state.text,
        "conversation_id": state.conversation_id,
        "file_ids": list(state.file_ids),
        "sediment_ids": list(state.sediment_ids),
        "blocked": state.blocked,
        "tool_invoked": state.tool_invoked,
        "turn_use_case": state.turn_use_case,
        **extra,
    }


def iter_conversation_payloads(payloads: Iterator[str], history_text: str = "",
                               history_messages: list[str] | None = None) -> Iterator[dict[str, Any]]:
    state = ConversationState()
    history_messages = history_messages or []
    history_index = 0
    for payload in payloads:
        # print(f"[upstream_sse] {payload}", flush=True)
        if not payload:
            continue
        if payload == "[DONE]":
            yield conversation_base_event("conversation.done", state, done=True)
            break
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            update_conversation_state(state, payload)
            yield conversation_base_event("conversation.raw", state, payload=payload)
            continue
        if not isinstance(event, dict):
            yield conversation_base_event("conversation.event", state, raw=event)
            continue
        update_conversation_state(state, payload, event)
        if history_index < len(history_messages) and event_assistant_text(event, history_text) == history_messages[history_index]:
            history_index += 1
            state.raw_text = ""
            state.text = ""
            continue
        next_raw_text = assistant_raw_text(event, state.raw_text, history_text)
        next_text = sanitize_output_text(next_raw_text)
        state.raw_text = next_raw_text
        if next_text != state.text:
            delta = next_text[len(state.text):] if next_text.startswith(state.text) else next_text
            state.text = next_text
            yield conversation_base_event("conversation.delta", state, raw=event, delta=delta)
            continue
        yield conversation_base_event("conversation.event", state, raw=event)


def conversation_events(
    backend: OpenAIBackendAPI,
    messages: list[dict[str, Any]] | None = None,
    model: str = "auto",
    prompt: str = "",
    images: list[str] | None = None,
    size: str | None = None,
    quality: str = "auto",
    thinking_effort: str = "",
) -> Iterator[dict[str, Any]]:
    normalized = normalize_messages(messages or ([{"role": "user", "content": prompt}] if prompt else []))
    image_model = is_supported_image_model(model)
    history_text = "" if image_model else assistant_history_text(normalized)
    history_messages = [] if image_model else assistant_history_messages(normalized)
    final_prompt = prompt_with_global_system(build_image_prompt(prompt, size, quality)) if image_model else prompt
    payloads = backend.stream_conversation(
        messages=normalized,
        model=model,
        prompt=final_prompt,
        images=images if image_model else None,
        system_hints=["picture_v2"] if image_model else None,
        thinking_effort=thinking_effort if not image_model else "",
    )
    yield from iter_conversation_payloads(payloads, history_text, history_messages)


def text_backend(model: str = "auto") -> OpenAIBackendAPI:
    return OpenAIBackendAPI(access_token=account_service.get_text_access_token(model=model))


def stream_text_deltas(backend: OpenAIBackendAPI, request: ConversationRequest) -> Iterator[str]:
    attempted_tokens: set[str] = set()
    token = getattr(backend, "access_token", "")
    emitted = False
    while True:
        if token and token in attempted_tokens:
            raise RuntimeError("no available text account")
        if token:
            attempted_tokens.add(token)
        active_backend = None
        try:
            active_backend = OpenAIBackendAPI(access_token=token)
            for event in conversation_events(
                active_backend,
                messages=request.messages,
                model=request.model,
                prompt=request.prompt,
                thinking_effort=request.thinking_effort,
            ):
                if event.get("type") != "conversation.delta":
                    continue
                delta = str(event.get("delta") or "")
                if delta:
                    emitted = True
                    yield delta
            account_service.mark_text_used(token)
            return
        except Exception as exc:
            error_message = str(exc)
            if token and not emitted and is_token_invalid_error(error_message):
                refreshed_token = account_service.refresh_access_token(token, force=True, event="text_stream")
                if refreshed_token and refreshed_token != token and refreshed_token not in attempted_tokens:
                    token = refreshed_token
                else:
                    account_service.remove_invalid_token(token, "text_stream")
                    token = account_service.get_text_access_token(
                        excluded_tokens=set(attempted_tokens),
                        model=request.model,
                    )
                if token:
                    continue
            raise
        finally:
            if active_backend is not None:
                active_backend.close()


def collect_text(backend: OpenAIBackendAPI, request: ConversationRequest) -> str:
    return "".join(stream_text_deltas(backend, request))


def _get_detailed_error_from_tasks(
    backend: OpenAIBackendAPI,
    conversation_id: str,
    timeout_secs: float = 10.0,
    wait_secs: float = 2.0,
) -> str:
    """从 /backend-api/tasks/ 接口获取结构化错误信息。

    当 SSE 流检测到 moderation 拦截时，轮询 tasks 接口获取详细错误文本。
    使用结构化字段（metadata.is_error, author.role, content.content_type）判断，
    而非依赖易变的文本匹配。

    参数：
    - `backend`：OpenAIBackendAPI 实例。
    - `conversation_id`：会话 ID。
    - `timeout_secs`：请求超时秒数。
    - `wait_secs`：等待任务创建的秒数。设为 0 可跳过等待。

    返回：
    - 详细错误信息文本，如果未找到则返回空字符串。
    """
    import time as _time
    try:
        remaining = remaining_poll_budget()
        if remaining is not None:
            timeout_secs = min(timeout_secs, 1.0, remaining / 10)
            wait_secs = 0.0
        if wait_secs > 0:
            wait_with_image_budget(wait_secs)
        tasks = backend._query_backend_tasks(conversation_id=conversation_id, timeout_secs=timeout_secs)
        if not tasks:
            return ""

        for task in tasks:
            is_error, error_msg, metadata = backend.check_task_error(task)
            if is_error and error_msg:
                logger.info({
                    "event": "image_task_structured_error",
                    "conversation_id": conversation_id,
                    "error_msg": error_msg,
                    "metadata": metadata,
                })
                return error_msg
        return ""
    except (ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
        raise
    except Exception as exc:
        logger.warning({
            "event": "image_task_error_query_failed",
            "conversation_id": conversation_id,
            "error": str(exc),
        })
        return ""


def _remove_image_conversation_later(
        backend: OpenAIBackendAPI,
        conversation_id: str,
        *,
        success: bool,
) -> None:
    if not conversation_id:
        return
    if not (config.image_remove_conversation_always or (success and config.image_remove_conversation_after_result)):
        return

    def _run() -> None:
        try:
            access_token = getattr(backend, 'access_token', None)
            if access_token is None:
                backend.delete_conversation(conversation_id)
            else:
                with OpenAIBackendAPI(access_token=access_token) as cleanup_backend:
                    cleanup_backend.delete_conversation(conversation_id)
            logger.info({"event": "image_conversation_removed", "conversation_id": conversation_id})
        except Exception as exc:
            logger.warning({
                "event": "image_conversation_remove_failed",
                "conversation_id": conversation_id,
                "error": str(exc),
            })

    if image_cleanup_executor.submit(_run) is None:
        logger.warning({
            "event": "image_conversation_remove_queue_full",
            "conversation_id": conversation_id,
        })


def stream_image_outputs(
        backend: OpenAIBackendAPI,
        request: ConversationRequest,
        index: int = 1,
        total: int = 1,
) -> Iterator[ImageOutput]:
    last: dict[str, Any] = {}
    stream_interrupted = False
    try:
        with phase("upstream_generation"):
            for event in conversation_events(
                    backend,
                    prompt=request.prompt,
                    model=request.model,
                    images=request.images or [],
                    size=request.size,
                    quality=request.quality,
            ):
                last = event
                if event.get("type") == "conversation.delta":
                    yield ImageOutput(
                        kind="progress",
                        model=request.model,
                        index=index,
                        total=total,
                        text=str(event.get("delta") or ""),
                        upstream_event_type="conversation.delta",
                        conversation_id=str(event.get("conversation_id") or ""),
                    )
                    continue
                if event.get("type") == "conversation.event":
                    raw = event.get("raw")
                    raw_type = str(raw.get("type") or "") if isinstance(raw, dict) else ""
                    yield ImageOutput(
                        kind="progress",
                        model=request.model,
                        index=index,
                        total=total,
                        upstream_event_type=raw_type,
                        conversation_id=str(event.get("conversation_id") or ""),
                    )
    except ImageStreamHardTimeoutError as exc:
        conversation_id = str(last.get("conversation_id") or "")
        if not conversation_id:
            raise
        stream_interrupted = True
        if not last.get('blocked'):
            last = {**last, 'turn_use_case': 'image gen'}
        record_image_decision("recover_original_result", "sse_hard_timeout")

    # SSE 与补查独立计时；补查覆盖任务检查、对话恢复、退避和全部兜底分支。
    try:
        with image_poll_budget(config.image_poll_timeout_secs, str(last.get("conversation_id") or "")), phase("result_resolve"):
            image_urls, message, conversation_id = _resolve_stream_image_result(backend, request, last)
    except (ImageResultResolveError, ImageContentPolicyError, ImageDeadlineExceeded, ImageRequestCancelled):
        raise
    except Exception as exc:
        raise ImageResultResolveError("图片结果补查或地址解析失败。",
                                      str(last.get("conversation_id") or "")) from exc
    if image_urls:
        if request.progress_callback:
            request.progress_callback("receiving_image")
        image_items = [{"_image_bytes": data} for data in backend.download_image_bytes(image_urls[:1])]
        data = format_image_result(image_items, request.prompt, request.response_format,
                                   request.base_url, int(time.time()))["data"]
        if data:
            yield ImageOutput(kind="result", model=request.model, index=index, total=total,
                              data=data, conversation_id=conversation_id)
        return
    if stream_interrupted:
        from services.image_statistics import is_content_policy_rejection
        if not is_content_policy_rejection(message):
            raise ImagePollTimeoutError("生成流超时，补查原对话后仍未取得图片结果。", conversation_id)
    yield ImageOutput(kind="message", model=request.model, index=index, total=total,
                      text=message, conversation_id=conversation_id)


def _resolve_stream_image_result(backend: OpenAIBackendAPI, request: ConversationRequest,
                                 last: dict[str, Any]) -> tuple[list[str], str, str]:
    """查询和解析结果；不在补查预算内下载或保存图片。"""
    conversation_id = str(last.get("conversation_id") or "")
    file_ids = [str(item) for item in last.get("file_ids") or []]
    sediment_ids = [str(item) for item in last.get("sediment_ids") or []]
    message = str(last.get("text") or "").strip()
    logger.info({
        "event": "image_stream_resolve_start",
        "conversation_id": conversation_id,
        "file_ids": file_ids,
        "sediment_ids": sediment_ids,
        "tool_invoked": last.get("tool_invoked"),
        "turn_use_case": last.get("turn_use_case"),
    })
    if request.progress_callback:
        request.progress_callback("image_stream_resolve_start")
    if message and not file_ids and not sediment_ids and last.get("blocked"):
        # 尝试从 /backend-api/tasks/ 获取详细错误信息
        detailed_error = _get_detailed_error_from_tasks(backend, conversation_id)
        error_text = detailed_error or message or "Image generation was rejected by upstream policy."
        return [], error_text, conversation_id
    should_poll_for_image = bool(request.images) or last.get("turn_use_case") == "image gen"
    if message and not file_ids and not sediment_ids and not should_poll_for_image:
        return [], message, conversation_id

    # 检测模型是否返回了文本描述（含 referenced_image_ids）而非实际生成图片
    # 这说明模型已发起图片生成工具调用，但 SSE 在工具完成前断开，
    # 图片可能正在异步生成中。需要使用更积极的轮询策略来获取结果。
    is_text_reply = bool(message and is_model_text_reply_instead_of_image(message))
    if is_text_reply:
        logger.info({
            "event": "image_detected_text_reply_with_ids",
            "conversation_id": conversation_id,
            "message_preview": message[:200],
        })

    # 当检测到文本回复但 conversation_id 丢失时，尝试从最近对话列表中恢复
    # SSE 流太短时（模型返回文本而非触发图片工具），conversation_id 可能未被捕获，
    # 但图片已在上游异步生成。通过列出最近对话来恢复 conversation_id。
    if is_text_reply and not conversation_id:
        try:
            import time as _time
            recovered_id = backend.find_conversation_by_prompt(
                request.prompt, _time.time(), timeout_secs=min(2.0, (remaining_image_budget() or 8.0) / 4),
            )
            if recovered_id:
                conversation_id = recovered_id
                logger.info({
                    "event": "image_conversation_id_recovered",
                    "conversation_id": conversation_id,
                    "message_preview": message[:200],
                })
        except (ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
            raise
        except Exception as exc:
            logger.warning({
                "event": "image_conversation_id_recovery_failed",
                "error": repr(exc)[:300],
            })

    # 在轮询图片之前，先检查 /backend-api/tasks/ 是否有 moderation 拦截
    # 这样可以避免不必要的长时间轮询超时
    # 注意：当 should_poll_for_image 为 True 或检测到文本回复时，
    # 即使 tasks 报告了"错误"，也不能直接返回——因为上游可能将工具调用的 JSON 参数
    # （如 {"size":"1792x1024","n":1}）标记为 is_error，而实际上图片正在异步生成中。
    # 此时应继续轮询图片。
    detailed_error = ""
    if not file_ids and not sediment_ids and conversation_id and not should_poll_for_image and not is_text_reply:
        detailed_error = _get_detailed_error_from_tasks(backend, conversation_id, timeout_secs=5.0, wait_secs=1.0)
        if detailed_error and not should_poll_for_image and not is_text_reply:
            logger.info({
                "event": "image_task_error_before_poll",
                "conversation_id": conversation_id,
                "error": detailed_error,
            })
            return [], detailed_error, conversation_id
        if detailed_error and (should_poll_for_image or is_text_reply):
            logger.info({
                "event": "image_task_error_skipped_for_poll",
                "conversation_id": conversation_id,
                "error": detailed_error,
            })

    poll_timeout = config.image_poll_timeout_secs

    try:
        image_urls = backend.resolve_conversation_image_urls(
            conversation_id, file_ids, sediment_ids, poll_timeout_secs=poll_timeout,
        )
    except (ImageContentPolicyError, ImagePollTimeoutError) as exc:
        # 当检测到文本回复时，task error 不应直接判定为内容策略违规，
        # 因为图片可能仍在后台异步生成中
        if is_text_reply and isinstance(exc, ImageContentPolicyError):
            logger.warning({
                "event": "image_text_reply_task_error_ignored",
                "conversation_id": conversation_id,
                "error": str(exc),
            })
            image_urls = []
        else:
            raise
    except (ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
        raise
    except Exception as exc:
        # 当检测到文本回复时，首次轮询的临时网络错误不应直接中断，
        # 因为图片可能仍在后台异步生成中，后续 retry poll 会继续尝试。
        if is_text_reply and conversation_id:
            logger.warning({
                "event": "image_text_reply_first_poll_error_ignored",
                "conversation_id": conversation_id,
                "error": repr(exc)[:300],
            })
            image_urls = []
        else:
            raise

    if image_urls:
        return image_urls, "", conversation_id

    if message:
        # 检测模型是否返回了文本描述（含 referenced_image_ids）而非实际生成图片
        # 这说明模型已发起图片生成工具调用，但 SSE 在工具完成前断开。
        # 此时应再尝试轮询图片结果，而不是直接把文本当作最终输出。
        # 当 is_text_reply 但 conversation_id 丢失时，尝试从最近对话列表恢复
        if is_text_reply and conversation_id:
            logger.info({
                "event": "image_model_text_reply_retry_poll",
                "conversation_id": conversation_id,
                "message_preview": message[:200],
            })
            # 临时错误可重试，但所有尝试共用同一个结果补查截止时间。
            retry_poll_timeout = config.image_poll_timeout_secs
            MAX_POLL_RETRIES = 3
            for poll_attempt in range(1, MAX_POLL_RETRIES + 1):
                try:
                    polled_file_ids, polled_sediment_ids = backend._poll_image_results(
                        conversation_id,
                        retry_poll_timeout,
                        file_ids,
                        sediment_ids,
                    )
                    file_ids.extend(item for item in polled_file_ids if item and item not in file_ids)
                    sediment_ids.extend(item for item in polled_sediment_ids if item and item not in sediment_ids)
                    break  # 轮询成功，退出重试循环
                except (ImageContentPolicyError, ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
                    raise
                except Exception as exc:
                    error_str = str(exc)
                    is_transient = (
                        is_tls_connection_error(error_str)
                        or "upstream" in error_str.lower()
                        or "connection" in error_str.lower()
                        or "timeout" in error_str.lower()
                    )
                    logger.warning({
                        "event": "image_model_text_reply_poll_failed",
                        "conversation_id": conversation_id,
                        "poll_attempt": poll_attempt,
                        "error": repr(exc)[:300],
                        "is_transient": is_transient,
                    })
                    # 如果还有重试次数且不是超时/内容违规错误，继续重试
                    if poll_attempt < MAX_POLL_RETRIES and not isinstance(exc, (ImagePollTimeoutError, ImageContentPolicyError)):
                        # 递增退避：30s, 60s, 90s
                        backoff = 30.0 * poll_attempt
                        logger.info({
                            "event": "image_model_text_reply_poll_retry",
                            "conversation_id": conversation_id,
                            "poll_attempt": poll_attempt,
                            "backoff_secs": backoff,
                        })
                        if not is_transient or not poll_pause(backoff):
                            break
                        continue
                    # 超时错误或重试次数用尽，停止重试
                    break

            if file_ids or sediment_ids:
                image_urls = backend.resolve_conversation_image_urls(
                    conversation_id, file_ids, sediment_ids, poll=False,
                )
                if image_urls:
                    return image_urls, "", conversation_id
        elif is_text_reply:
            logger.warning({
                "event": "image_model_text_reply_no_image",
                "conversation_id": conversation_id,
                "message_preview": message[:200],
            })
        if is_text_reply:
            raise ImageResultResolveError("图片结果补查结束后仍未获取到结果。", conversation_id)
        return [], message, conversation_id

    # 兜底：当 message 为空且图片 URL 解析失败时，先尝试一次短延迟重试轮询
    # 然后抛出明确错误而非让调用方得到 "upstream completed without generating images" 这种模糊报错
    logger.warning({
        "event": "image_stream_no_result_fallback",
        "conversation_id": conversation_id,
        "file_ids": file_ids,
        "sediment_ids": sediment_ids,
        "should_poll_for_image": should_poll_for_image,
    })
    # 当 should_poll_for_image 为 True 但 conversation_id 丢失时，尝试恢复
    if should_poll_for_image and not conversation_id:
        try:
            import time as _time
            recovered_id = backend.find_conversation_by_prompt(
                request.prompt, _time.time(), timeout_secs=min(2.0, (remaining_image_budget() or 8.0) / 4),
            )
            if recovered_id:
                conversation_id = recovered_id
                logger.info({
                    "event": "image_fallback_conversation_id_recovered",
                    "conversation_id": conversation_id,
                })
        except (ImageContentPolicyError, ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
            raise
        except Exception as exc:
            logger.warning({
                "event": "image_fallback_conversation_id_recovery_failed",
                "error": repr(exc)[:300],
            })
    if should_poll_for_image and conversation_id:
        # 兜底补查不会延长结果补查截止时间。
        retry_poll_timeout = config.image_poll_timeout_secs
        MAX_FALLBACK_POLL_RETRIES = 3
        for poll_attempt in range(1, MAX_FALLBACK_POLL_RETRIES + 1):
            # 初始等待统一交给轮询器，避免外层等待和内部等待重复消耗短预算。
            logger.info({
                "event": "image_stream_retry_poll",
                "conversation_id": conversation_id,
                "poll_attempt": poll_attempt,
            })
            try:
                polled_file_ids, polled_sediment_ids = backend._poll_image_results(
                    conversation_id,
                    retry_poll_timeout,
                    file_ids,
                    sediment_ids,
                )
                file_ids.extend(item for item in polled_file_ids if item and item not in file_ids)
                sediment_ids.extend(item for item in polled_sediment_ids if item and item not in sediment_ids)
                break  # 轮询成功，退出重试循环
            except (ImageContentPolicyError, ImagePollTimeoutError, ImageDeadlineExceeded, ImageRequestCancelled):
                raise
            except Exception as exc:
                error_str = str(exc)
                is_transient = (
                    is_tls_connection_error(error_str)
                    or "upstream" in error_str.lower()
                    or "connection" in error_str.lower()
                    or "timeout" in error_str.lower()
                )
                logger.warning({
                    "event": "image_stream_retry_poll_failed",
                    "conversation_id": conversation_id,
                    "poll_attempt": poll_attempt,
                    "error": repr(exc)[:300],
                    "is_transient": is_transient,
                })
                # 如果还有重试次数且不是超时/内容违规错误，继续重试
                if poll_attempt < MAX_FALLBACK_POLL_RETRIES and not isinstance(exc, (ImagePollTimeoutError, ImageContentPolicyError)):
                    # 递增退避：30s, 60s
                    backoff = 30.0 * poll_attempt
                    logger.info({
                        "event": "image_stream_retry_poll_retry",
                        "conversation_id": conversation_id,
                        "poll_attempt": poll_attempt,
                        "backoff_secs": backoff,
                    })
                    if not is_transient or not poll_pause(backoff):
                        break
                    continue
                # 超时错误或重试次数用尽，停止重试
                break
        
        if file_ids or sediment_ids:
            image_urls = backend.resolve_conversation_image_urls(
                conversation_id, file_ids, sediment_ids, poll=False,
            )
            if image_urls:
                return image_urls, "", conversation_id
        
        # 重试后仍未获取结果，返回明确提示
        raise ImageResultResolveError("图片结果补查结束后仍未获取到结果。", conversation_id)
    elif message:
        return [], message, conversation_id
    else:
        if should_poll_for_image:
            raise ImageResultResolveError("图片结果补查无法恢复上游对话。", conversation_id)
        # conversation_id 也为空时（SSE 流极短、未捕获到会话 ID），
        # 仍然返回明确消息，避免 stream_image_outputs_with_pool 产生
        # "upstream completed without generating images" 模糊报错
        return [], 'Image generation started upstream but the response was incomplete. Please try again.', conversation_id


def _codex_response_images(value: Any) -> list[str]:
    if isinstance(value, dict):
        if value.get("type") == "image_generation_call" and isinstance(value.get("result"), str):
            result = value["result"].strip()
            if result:
                return [result.split(",", 1)[1] if result.startswith("data:image/") else result]
        images: list[str] = []
        for item in value.values():
            images.extend(_codex_response_images(item))
        return images
    if isinstance(value, list):
        images: list[str] = []
        for item in value:
            images.extend(_codex_response_images(item))
        return images
    return []


def stream_codex_image_outputs(
        backend: OpenAIBackendAPI,
        request: ConversationRequest,
        index: int = 1,
        total: int = 1,
) -> Iterator[ImageOutput]:
    with phase("upstream_generation"):
        images = _codex_response_images(list(backend.iter_codex_image_response_events(
            prompt=request.prompt,
            images=request.images or [],
            size=request.size,
            quality=request.quality,
        )))
    if not images:
        raise ImageGenerationError("No image result found in response")
    data = format_image_result(
        [{"b64_json": item, "revised_prompt": request.prompt} for item in images],
        request.prompt,
        request.response_format,
        request.base_url,
        int(time.time()),
    )["data"]
    if data:
        yield ImageOutput(kind="result", model=request.model, index=index, total=total, data=data)
        return
    raise ImageGenerationError("No image result found in response")


@measured("generation_work")
def _generate_single_image(
        request: ConversationRequest,
        index: int,
        total: int,
) -> list[ImageOutput]:
    tracker = request.account_switch_tracker
    budget = ImageAttemptBudget(config.image_account_selection_max_attempts)
    try:
        outputs = _generate_single_image_impl(request, index, total, budget=budget)
        from services.image_statistics import is_content_policy_rejection
        image_attempt_results.record(success=any(item.kind == "result" for item in outputs),
            policy=any(item.kind == "message" and is_content_policy_rejection(item.text) for item in outputs),
            attempts=len(budget.identities))
        if tracker:
            tracker.finish(index, "生成成功" if any(item.kind == "result" for item in outputs) else "未生成图片")
        return outputs
    except (ImageDeadlineExceeded, ImageRequestCancelled) as exc:
        if tracker:
            tracker.finish(index, "请求已取消" if isinstance(exc, ImageRequestCancelled) else "总预算耗尽")
        error = ImageGenerationError(
            "The image generation request was cancelled." if isinstance(exc, ImageRequestCancelled)
            else "The image generation request exceeded its total time budget.",
            status_code=499 if isinstance(exc, ImageRequestCancelled) else 504,
            error_type="client_error" if isinstance(exc, ImageRequestCancelled) else "server_error",
            code="client_disconnected" if isinstance(exc, ImageRequestCancelled) else "image_timeout",
        )
        if tracker:
            for key, value in tracker.summary().items():
                setattr(error, key, value)
        raise error from exc
    except Exception as exc:
        image_attempt_results.record(success=False,
            policy=getattr(exc, "code", "") == "content_policy_violation", attempts=len(budget.identities))
        if tracker:
            tracker.finish(index, "生成失败")
            for key, value in tracker.summary().items():
                setattr(exc, key, value)
        raise


def _generate_single_image_impl(
        request: ConversationRequest,
        index: int,
        total: int,
        *, budget: ImageAttemptBudget | None = None,
) -> list[ImageOutput]:
    """为单张图片执行生成逻辑（含重试），返回结果列表。

    该函数在独立线程中运行，每个线程使用不同的账号，
    实现并行生图，避免串行超时阻塞。
    """
    # 模型返回文本而非图片的最大重试次数
    max_text_reply_account_switch_retries = config.image_text_reply_account_switch_retries
    # TLS 连接错误最大重试次数
    MAX_TLS_RETRIES = 3
    # 连接超时错误最大重试次数（重试仍受账号总预算限制）
    MAX_CONN_TIMEOUT_RETRIES = 3
    poll_failure_retry_count = 0
    text_reply_retry_count = 0
    tls_retry_count = 0
    conn_timeout_retry_count = 0
    account_email = ""
    tracker = request.account_switch_tracker
    budget = budget or ImageAttemptBudget(config.image_account_selection_max_attempts)
    pending_download_error: ImageGenerationError | None = None
    pending_poll_error: ImageGenerationError | None = None
    download_account_switch_count = 0

    while True:
        try:
            remaining_image_budget()
            if request.progress_callback:
                request.progress_callback("getting_account")
            plan_type, _ = split_image_model(request.model)
            codex_model = is_codex_image_model(request.model)
            token = account_service.get_available_access_token(
                plan_type=plan_type,
                source_type="codex" if codex_model else None,
                plan_types=("plus", "team", "pro") if codex_model and not plan_type else None,
                budget=budget,
            )
        except (ImageDeadlineExceeded, ImageRequestCancelled):
            raise
        except RuntimeError as exc:
            # 重试时无其他可用账号，保留上一次下载/补查失败的真实类型。
            pending_error = pending_download_error or pending_poll_error
            if pending_error is not None:
                raise pending_error from exc
            raise ImageGenerationError(str(exc) or "image generation failed", account_email=account_email) from exc

        pending_download_error = None
        pending_poll_error = None
        result_recorded = False
        attempt_started_at = time.time()
        def record_result(success, outcome="neutral"):
            nonlocal result_recorded
            if not result_recorded:
                result_recorded = True
                account_service.mark_image_result(token, success, outcome=outcome,
                                                  attempt_started_at=attempt_started_at)

        emitted_for_token = False
        returned_message = False
        returned_result = False
        account = account_service.get_account(token) or {}
        account_email = str(account.get("email") or "").strip()
        attempt_context_token = attempt_context.set({
            "image_index": index,
            "attempt": len(budget.identities),
            "account": account_email or "unknown",
        })
        if tracker:
            tracker.begin(index, account_email or (token[:12] + "..."))
        logger.debug({
            "event": "image_account_lookup",
            "token_prefix": token[:12] + "..." if len(token) > 12 else token,
            "account_email": account_email,
            "account_found": bool(account),
            "index": index,
        })
        backend = None
        release_without_result = False
        try:
            backend = OpenAIBackendAPI(access_token=token)
            if request.progress_callback:
                backend.progress_callback = request.progress_callback
            stream_fn = stream_codex_image_outputs if is_codex_image_model(request.model) else stream_image_outputs
            outputs: list[ImageOutput] = []
            last_conversation_id = ""
            try:
                for output in stream_fn(backend, request, index, total):
                    last_conversation_id = output.conversation_id or last_conversation_id
                    if account_email and not output.account_email:
                        output.account_email = account_email
                    if output.kind == "message" and request.message_as_error:
                        from services.image_statistics import is_content_policy_rejection

                        policy_rejection = is_content_policy_rejection(output.text)
                        raise ImageGenerationError(
                            output.text or "Upstream returned no image.",
                            status_code=400 if policy_rejection else 502,
                            error_type="invalid_request_error" if policy_rejection else "server_error",
                            code="content_policy_violation" if policy_rejection else "no_image_generated",
                            account_email=account_email,
                            conversation_id=output.conversation_id,
                        )
                    emitted_for_token = True
                    returned_message = output.kind == "message"
                    returned_result = returned_result or output.kind == "result"
                    outputs.append(output)
            except Exception as exc:
                # 异常路径（内容政策拒绝、轮询超时等）会话 ID 只挂在异常上
                last_conversation_id = last_conversation_id or str(getattr(exc, "conversation_id", "") or "")
                raise
            finally:
                _remove_image_conversation_later(backend, last_conversation_id, success=returned_result)
            if returned_message:
                from services.image_statistics import is_content_policy_rejection
                rejected = any(item.kind == "message" and is_content_policy_rejection(item.text) for item in outputs)
                record_result(False, "policy" if rejected else "no_image_generated")
                return outputs
            if not returned_result:
                record_result(False, "no_image_generated")
                if emitted_for_token:
                    conv_id = outputs[-1].conversation_id if outputs else ""
                    raise ImageGenerationError(
                        "upstream completed without generating images",
                        status_code=400,
                        error_type="invalid_request_error",
                        code="no_image_generated",
                        account_email=account_email,
                        conversation_id=conv_id,
                    )
                return outputs
            record_result(True)
            return outputs
        except (ImageDeadlineExceeded, ImageRequestCancelled):
            # 总预算和客户端断开只释放槽位，不把账号记成失败，也不切换账号。
            release_without_result = True
            raise
        except ImageDownloadError as exc:
            # 同一结果地址的下载重试已耗尽；按共享账号预算切号重新生成。
            # 传输失败不证明账号失效，不扣账号分；finally 只释放槽位并关闭连接。
            release_without_result = True
            record_image_decision("result_download_failed", "retry_same_result_exhausted")
            pending_download_error = ImageGenerationError(
                "The generated image result could not be downloaded.",
                status_code=502,
                error_type="server_error",
                code="image_download_failed",
                account_email=account_email,
                conversation_id=getattr(exc, "conversation_id", "") or last_conversation_id,
            )
            if not returned_result and len(budget.identities) < budget.limit:
                # 取消和总预算优先，不能因下载失败重新获得一份生成预算。
                remaining_image_budget()
                download_account_switch_count += 1
                if tracker:
                    tracker.mark_switch(index, "图片下载重试耗尽后切换账号重新生成")
                record_image_decision("account_switch", "image_download_failed")
                logger.warning({
                    "event": "image_download_account_switch",
                    "account_email": account_email,
                    "retry_count": download_account_switch_count,
                    "account_attempts": len(budget.identities),
                    "max_attempts": budget.limit,
                    "index": index,
                })
                continue
            raise pending_download_error from exc
        except ImageStreamHardTimeoutError as exc:
            release_without_result = True
            record_image_decision("generation_result_uncertain", "sse_hard_timeout")
            pending_poll_error = ImageGenerationError(
                "The upstream image result is still being resolved.",
                status_code=504, error_type="server_error", code="image_result_uncertain",
                account_email=account_email,
                conversation_id=getattr(exc, "conversation_id", "") or last_conversation_id,
            )
            # 没有对话ID可补查时，也只能在共享次数与总预算内恢复；
            # 尾部不足一次最短生成窗口时不再提交新生成请求。
            remaining = remaining_image_budget()
            enough_time = remaining is None or remaining >= min(20.0, float(config.image_sse_timeout_secs))
            if (not returned_result and enough_time
                    and poll_failure_retry_count < config.image_poll_timeout_account_switch_retries
                    and len(budget.identities) < budget.limit):
                poll_failure_retry_count += 1
                if tracker:
                    tracker.mark_switch(index, "上游生成流超时后切换账号重新生成")
                record_image_decision("account_switch", "sse_hard_timeout")
                continue
            raise pending_poll_error from exc
        except ImageResultResolveError as exc:
            # 按用户配置切号重新生成；补查失败不直接证明账号失效，保持中性结算。
            release_without_result = True
            timed_out = isinstance(exc, ImagePollTimeoutError)
            reason = "result_poll_timeout" if timed_out else "result_resolve_failed"
            record_image_decision("result_poll_failed", reason)
            pending_poll_error = ImageGenerationError(
                "The upstream image result is still being resolved." if timed_out
                else "The upstream image result could not be retrieved.",
                status_code=504 if timed_out else 502,
                error_type="server_error",
                code="image_result_uncertain" if timed_out else "image_result_unavailable",
                account_email=account_email,
                conversation_id=getattr(exc, "conversation_id", "") or last_conversation_id,
            )
            max_switches = config.image_poll_timeout_account_switch_retries
            remaining = remaining_image_budget()
            enough_time = remaining is None or remaining >= min(20.0, float(config.image_sse_timeout_secs))
            if (not returned_result and enough_time and poll_failure_retry_count < max_switches
                    and len(budget.identities) < budget.limit):
                # 整个请求预算不会重置；本次 finally 会关闭连接并释放账号槽位。
                remaining_image_budget()
                poll_failure_retry_count += 1
                if tracker:
                    tracker.mark_switch(index, "图片结果补查超时后切换账号重新生成" if timed_out
                                        else "图片结果补查失败后切换账号重新生成")
                record_image_decision("account_switch", reason)
                logger.warning({
                    "event": "image_poll_failure_account_switch",
                    "reason": reason,
                    "retry_count": poll_failure_retry_count,
                    "max_retries": max_switches,
                    "account_attempts": len(budget.identities),
                    "max_attempts": budget.limit,
                    "index": index,
                })
                continue
            raise pending_poll_error from exc
        except ImageContentPolicyError as exc:
            record_result(False, "policy")
            logger.warning({
                "event": "image_stream_content_policy_error",
                "request_token": token,
                "account_email": account_email,
                "error": str(exc),
                "index": index,
            })
            raise ImageGenerationError(
                str(exc) or "Image generation was rejected by upstream policy.",
                status_code=400,
                error_type="invalid_request_error",
                code="content_policy_violation",
                account_email=account_email,
                conversation_id=getattr(exc, "conversation_id", ""),
            ) from exc
        except ImageGenerationError as exc:
            record_result(False, {"content_policy_violation": "policy", "no_image_generated": "no_image_generated"}.get(getattr(exc, "code", ""), "neutral"))
            if account_email and not getattr(exc, "account_email", ""):
                exc.account_email = account_email
            error_text = str(exc)
            # 如果是模型返回文本而非图片，尝试换账号重试
            if is_model_text_reply_instead_of_image(error_text) and not emitted_for_token:
                text_reply_retry_count += 1
                if text_reply_retry_count <= max_text_reply_account_switch_retries:
                    if tracker:
                        tracker.mark_switch(index, "上游未生成图片后切换账号")
                    logger.warning({
                        "event": "image_model_text_reply_retry",
                        "request_token": token,
                        "account_email": account_email,
                        "retry_count": text_reply_retry_count,
                        "index": index,
                        "error": error_text[:200],
                    })
                    continue
                logger.warning({
                    "event": "image_model_text_reply_exhausted_retries",
                    "request_token": token,
                    "account_email": account_email,
                    "retry_count": text_reply_retry_count,
                    "index": index,
                })
                raise ImageGenerationError(
                    "Image generation failed: the upstream model returned a text description "
                    "instead of generating an image. Please try again later.",
                    status_code=502,
                    error_type="server_error",
                    code="upstream_text_reply",
                    account_email=account_email,
                    conversation_id=getattr(exc, "conversation_id", ""),
                ) from exc
            logger.warning({
                "event": "image_stream_generation_error",
                "request_token": token,
                "account_email": account_email,
                "error": error_text,
                "index": index,
            })
            raise
        except Exception as exc:
            last_error = str(exc)
            record_result(False, "account_error" if is_token_invalid_error(last_error) else "neutral")
            logger.warning({
                "event": "image_stream_fail",
                "request_token": token,
                "account_email": account_email,
                "error": last_error,
                "index": index,
            })
            if not emitted_for_token and is_token_invalid_error(last_error):
                account_service.invalidate_verification(token)
                refreshed_token = account_service.refresh_access_token(token, force=True, event="image_stream")
                if refreshed_token and refreshed_token != token:
                    token = refreshed_token
                    continue
                account_service.remove_invalid_token(token, "image_stream")
                if tracker:
                    tracker.mark_switch(index, "账号令牌失效后切换账号")
                continue
            # TLS/SSL 连接错误：自动重试
            if not emitted_for_token and is_tls_connection_error(last_error):
                tls_retry_count += 1
                if tls_retry_count <= MAX_TLS_RETRIES:
                    logger.warning({
                        "event": "image_stream_tls_retry",
                        "request_token": token,
                        "account_email": account_email,
                        "retry_count": tls_retry_count,
                        "index": index,
                        "error": last_error[:200],
                    })
                    wait_with_image_budget(min(2.0 * tls_retry_count, 10.0))
                    continue
            # 连接超时错误（curl 28）：短等待后重试，仍使用同一总账号预算
            if not emitted_for_token and is_connection_timeout_error(last_error):
                conn_timeout_retry_count += 1
                if conn_timeout_retry_count <= MAX_CONN_TIMEOUT_RETRIES:
                    wait_secs = min(3.0 * conn_timeout_retry_count, 9.0)
                    logger.warning({
                        "event": "image_stream_conn_timeout_retry",
                        "request_token": token,
                        "account_email": account_email,
                        "retry_count": conn_timeout_retry_count,
                        "index": index,
                        "wait_secs": wait_secs,
                        "error": last_error[:200],
                    })
                    wait_with_image_budget(wait_secs)
                    continue
            raise ImageGenerationError(image_stream_error_message(last_error), account_email=account_email, conversation_id="") from exc
        finally:
            # 包括取消/SystemExit在内的异常也释放预留槽位，且单次尝试只结算一次。
            if not result_recorded:
                if release_without_result:
                    account_service.release_image_slot(token)
                else:
                    record_result(False)
            if backend is not None:
                backend.close()
            attempt_context.reset(attempt_context_token)


def stream_image_outputs_with_pool(request: ConversationRequest) -> Iterator[ImageOutput]:
    """并行生成多张图片，每张图片使用独立线程和账号，互不阻塞。"""
    if not is_supported_image_model(request.model):
        raise ImageGenerationError("unsupported image model,supported models: " + ", ".join(sorted(IMAGE_MODELS)))

    if request.account_switch_tracker is None:
        request.account_switch_tracker = ImageAccountSwitchTracker()

    if not 1 <= request.n <= 4:
        raise ImageGenerationError("n must be between 1 and 4", status_code=400, error_type="invalid_request_error")
    if request.n <= 1:
        # 单张图片，直接执行（无需线程池开销）
        try:
            outputs = _generate_single_image(request, 1, 1)
            for output in outputs:
                yield output
        except ImageGenerationError as exc:
            raise _attach_account_switch_summary(exc, request.account_switch_tracker)
        return

    # 多张图片：根据配置选择并行或串行执行
    if not config.image_parallel_generation:
        logger.info({
            "event": "image_serial_generation_start",
            "n": request.n,
            "model": request.model,
        })
        try:
            for index in range(1, request.n + 1):
                outputs = _generate_single_image(request, index, request.n)
                for output in outputs:
                    yield output
        except ImageGenerationError as exc:
            raise _attach_account_switch_summary(exc, request.account_switch_tracker)
        return

    logger.info({
        "event": "image_parallel_generation_start",
        "n": request.n,
        "model": request.model,
    })
    # 每张图片一个线程，同时启动
    futures = {}
    results: dict[int, list[ImageOutput]] = {}
    errors: dict[int, Exception] = {}
    if cooperative():
        completed = parallel_calls(_generate_single_image,
                                   [(request, index, request.n) for index in range(1, request.n + 1)])
        for index, result in enumerate(completed, 1):
            if isinstance(result, BaseException):
                errors[index] = result
            else:
                results[index] = result
    for index in (() if cooperative() else range(1, request.n + 1)):
        future = image_part_executor.submit(copy_context().run, _generate_single_image, request, index, request.n)
        futures[future] = index

    # 按完成顺序收集结果
    for future in as_completed(futures):
        index = futures[future]
        try:
            results[index] = future.result()
        except Exception as exc:
            errors[index] = exc
            logger.warning({
                "event": "image_parallel_generation_error",
                "index": index,
                "error": str(exc)[:300],
            })

    # yield 结果：跳过索引顺序限制，不再让低索引失败阻塞高索引成功结果
    emitted = False
    last_error = ""
    # 先 yield 所有成功的结果
    for index in range(1, request.n + 1):
        if index in results:
            for output in results[index]:
                emitted = True
                yield output
        elif index in errors:
            last_error = str(errors[index])
            if not emitted:
                logger.warning({
                    "event": "image_parallel_failure_before_success",
                    "failed_index": index,
                    "error": last_error[:200],
                })

    # 如果有失败但也有成功，记录警告
    if emitted:
        for index in range(1, request.n + 1):
            if index in errors:
                logger.warning({
                    "event": "image_parallel_partial_failure",
                    "failed_index": index,
                    "error": str(errors[index])[:200],
                })

    if not emitted:
        if not last_error:
            last_error = "no account in the pool could generate images — check account quota and rate-limit status"
        error = ImageGenerationError(image_stream_error_message(last_error), conversation_id="")
        raise _attach_account_switch_summary(error, request.account_switch_tracker)


def stream_image_chunks(outputs: Iterable[ImageOutput], max_images: int | None = None) -> Iterator[dict[str, Any]]:
    emitted = 0
    for output in outputs:
        chunk = output.to_chunk()
        if output.kind == "result" and max_images is not None:
            chunk["data"] = output.data[:max(0, max_images - emitted)]
            emitted += len(chunk["data"])
            if not chunk["data"]:
                continue
        yield chunk


def collect_image_outputs(
        outputs: Iterable[ImageOutput],
        tracker: ImageAccountSwitchTracker | None = None,
        max_images: int | None = None,
) -> dict[str, Any]:
    created = None
    data: list[dict[str, Any]] = []
    message = ""
    progress_parts: list[str] = []
    account_email = ""
    for output in outputs:
        created = created or output.created
        if output.account_email and not account_email:
            account_email = output.account_email
        if output.kind == "progress" and output.text:
            progress_parts.append(output.text)
        elif output.kind == "message":
            message = output.text
        elif output.kind == "result":
            data.extend(output.data if max_images is None else output.data[:max(0, max_images - len(data))])

    result: dict[str, Any] = {"created": created or int(time.time()), "data": data}
    if not data:
        text = message or "".join(progress_parts).strip()
        if text:
            result["message"] = text
    if account_email:
        result["_account_email"] = account_email
    if tracker:
        result.update({f"_{key}": value for key, value in tracker.summary().items()})
    return result
