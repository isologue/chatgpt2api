from __future__ import annotations

import hashlib
import json
import itertools
import base64
import time
import sqlite3
from contextlib import contextmanager
from threading import RLock
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from fastapi import HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, StreamingResponse

from services.config import DATA_DIR
from services.thread_pool import run_image_in_threadpool
from services.protocol.error_response import anthropic_error_response, openai_error_response
from utils.helper import anthropic_sse_stream, sse_json_stream

LOG_TYPE_CALL = "call"
LOG_TYPE_ACCOUNT = "account"
INTERNAL_RESPONSE_KEYS = {"_account_email", "_conversation_id"}


class LogService:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._index_path = path.with_suffix(".index.sqlite3")

    @staticmethod
    def _legacy_id(raw_line: str, line_number: int) -> str:
        payload = f"{line_number}:{raw_line}".encode("utf-8", errors="ignore")
        return hashlib.sha1(payload).hexdigest()[:24]

    def _parse_line(self, raw_line: str, line_number: int) -> dict[str, Any] | None:
        try:
            item = json.loads(raw_line)
        except Exception:
            return None
        if not isinstance(item, dict):
            return None
        parsed = dict(item)
        parsed["id"] = str(parsed.get("id") or self._legacy_id(raw_line, line_number))
        return parsed

    @staticmethod
    def _serialize_item(item: dict[str, Any]) -> str:
        return json.dumps(item, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _matches_filters(item: dict[str, Any], *, type: str = "", start_date: str = "", end_date: str = "") -> bool:
        t = str(item.get("time") or "")
        day = t[:10]
        if type and item.get("type") != type:
            return False
        if start_date and day < start_date:
            return False
        if end_date and day > end_date:
            return False
        return True

    @contextmanager
    def _index(self, *, write: bool = False):
        connection = sqlite3.connect(self._index_path, timeout=30)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("CREATE TABLE IF NOT EXISTS entries (seq INTEGER PRIMARY KEY, id TEXT UNIQUE, type TEXT, day TEXT, offset INTEGER, length INTEGER, summary TEXT)")
            connection.execute("CREATE INDEX IF NOT EXISTS ix_entries_type_seq ON entries(type, seq)")
            connection.execute("CREATE TABLE IF NOT EXISTS source (id INTEGER PRIMARY KEY, size INTEGER, mtime TEXT, lines INTEGER)")
            with connection:
                # Reads only need a deferred transaction. BEGIN IMMEDIATE here
                # made log listing/detail compete with every image completion.
                connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
                yield connection
        finally:
            connection.close()

    def _fingerprint(self):
        if not self.path.exists():
            return 0, "0"
        stat = self.path.stat()
        return stat.st_size, str(stat.st_mtime_ns)

    @staticmethod
    def _summary(item):
        summary = dict(item)
        if isinstance(detail := item.get("detail"), dict):
            summary["detail"] = {key: value for key, value in detail.items()
                                 if key not in {"request_body", "response_body"}}
        return summary

    def _index_line(self, connection, item, number, offset, length):
        connection.execute("INSERT OR REPLACE INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
            (number, item["id"], str(item.get("type") or ""), str(item.get("time") or "")[:10],
             offset, length, self._serialize_item(self._summary(item))))

    def _sync_index(self, connection):
        size, mtime = self._fingerprint()
        saved = connection.execute("SELECT size, mtime, lines FROM source WHERE id=1").fetchone()
        if saved and saved[:2] == (size, mtime):
            return saved[2]
        # Rebuild once for legacy logs or externally restored/edited files.
        # Stream the source: historical Base64 bodies never accumulate in RAM.
        connection.execute("DELETE FROM entries")
        number = 0
        if self.path.exists():
            with self.path.open("rb") as source:
                while True:
                    offset = source.tell()
                    line = source.readline()
                    if not line:
                        break
                    item = self._parse_line(line.decode("utf-8").rstrip("\r\n"), number)
                    if item is not None:
                        self._index_line(connection, item, number, offset, len(line))
                    number += 1
        connection.execute("INSERT OR REPLACE INTO source VALUES (1, ?, ?, ?)", (size, mtime, number))
        return number

    def add(self, type: str, summary: str = "", detail: dict[str, Any] | None = None, **data: Any) -> None:
        item = {
            "id": uuid4().hex,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": type, "summary": summary, "detail": detail or data,
        }
        # Serialization of potentially large bodies occurs outside the append lock.
        line = (self._serialize_item(item) + "\n").encode("utf-8")
        with self._lock, self._index(write=True) as connection:
            number = self._sync_index(connection)
            with self.path.open("ab") as target:
                offset = target.tell()
                target.write(line)
            self._index_line(connection, item, number, offset, len(line))
            size, mtime = self._fingerprint()
            connection.execute("INSERT OR REPLACE INTO source VALUES (1, ?, ?, ?)", (size, mtime, number + 1))

    def list(self, type: str = "", start_date: str = "", end_date: str = "", limit: int = 200) -> list[dict[str, Any]]:
        with self._lock, self._index() as connection:
            self._sync_index(connection)
            clauses, params = [], []
            for clause, value in (("type = ?", type), ("day >= ?", start_date), ("day <= ?", end_date)):
                if value:
                    clauses.append(clause)
                    params.append(value)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            rows = connection.execute("SELECT summary FROM entries" + where + " ORDER BY seq DESC LIMIT ?",
                                      [*params, max(0, limit)]).fetchall()
            return [json.loads(row[0]) for row in rows]

    def get(self, log_id: str) -> dict[str, Any] | None:
        with self._lock, self._index(write=True) as connection:
            self._sync_index(connection)
            row = connection.execute("SELECT seq, offset, length FROM entries WHERE id=?", (log_id,)).fetchone()
            if row is None:
                return None
            with self.path.open("rb") as source:
                source.seek(row[1])
                return self._parse_line(source.read(row[2]).decode("utf-8").rstrip("\r\n"), row[0])

    def delete(self, ids: list[str]) -> dict[str, int]:
        target_ids = {str(item or "").strip() for item in ids if str(item or "").strip()}
        if not self.path.exists() or not target_ids:
            return {"removed": 0}
        with self._lock, self._index() as connection:
            self._sync_index(connection)
            removed = 0
            temporary = self.path.with_suffix(self.path.suffix + ".tmp")
            try:
                with self.path.open("rb") as source, temporary.open("wb") as target:
                    for number, line in enumerate(source):
                        item = self._parse_line(line.decode("utf-8").rstrip("\r\n"), number)
                        if item is not None and item["id"] in target_ids:
                            removed += 1
                            continue
                        # Persist legacy IDs before line numbers change.
                        target.write((self._serialize_item(item) + "\n").encode("utf-8") if item else line)
                temporary.replace(self.path)
            finally:
                temporary.unlink(missing_ok=True)
            self._sync_index(connection)
            return {"removed": removed}


log_service = LogService(DATA_DIR / "logs.jsonl")


def _collect_urls(value: object) -> list[str]:
    urls: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "url" and isinstance(item, str):
                urls.append(item)
            elif key == "urls" and isinstance(item, list):
                urls.extend(str(url) for url in item if isinstance(url, str))
            else:
                urls.extend(_collect_urls(item))
    elif isinstance(value, list):
        for item in value:
            urls.extend(_collect_urls(item))
    return urls


def _collect_account_emails(value: object) -> list[str]:
    emails: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"_account_email", "account_email"} and isinstance(item, str) and item.strip():
                emails.append(item.strip())
            else:
                emails.extend(_collect_account_emails(item))
    elif isinstance(value, list):
        for item in value:
            emails.extend(_collect_account_emails(item))
    return emails


def _collect_conversation_ids(value: object) -> list[str]:
    ids: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "_conversation_id" and isinstance(item, str) and item.strip():
                ids.append(item.strip())
            else:
                ids.extend(_collect_conversation_ids(item))
    elif isinstance(value, list):
        for item in value:
            ids.extend(_collect_conversation_ids(item))
    return ids


def _strip_internal_response_fields(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_internal_response_fields(item)
            for key, item in value.items()
            if key not in INTERNAL_RESPONSE_KEYS
        }
    if isinstance(value, list):
        return [_strip_internal_response_fields(item) for item in value]
    return value


def json_safe(value: object) -> object:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _request_excerpt(text: object, limit: int = 1000) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _image_error_response(exc: Exception) -> JSONResponse:
    from services.protocol.conversation import public_image_error_message

    message = public_image_error_message(str(exc))
    if "no available image quota" in message.lower():
        return openai_error_response(
            {
                "error": {
                    "message": "no available image quota",
                    "type": "insufficient_quota",
                    "param": None,
                    "code": "insufficient_quota",
                }
            },
            429,
        )
    if hasattr(exc, "to_openai_error") and hasattr(exc, "status_code"):
        return JSONResponse(status_code=int(exc.status_code), content=exc.to_openai_error())
    return openai_error_response(message, 502)


def _protocol_error_response(exc: Exception, status_code: int, sse: str) -> JSONResponse:
    message = str(exc)
    if sse == "anthropic":
        return anthropic_error_response(message, status_code)
    return openai_error_response(message, status_code)


def _response_body(response: JSONResponse) -> object:
    try:
        return json.loads(response.body.decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _next_item(items):
    try:
        return True, next(items)
    except StopIteration:
        return False, None


@dataclass
class LoggedCall:
    identity: dict[str, object]
    endpoint: str
    model: str
    summary: str
    started: float = field(default_factory=time.time)
    request_text: str = ""
    request_shape: dict[str, int] | None = None
    log_context: dict[str, Any] = field(default_factory=dict)
    request_body: object = None

    async def run(self, handler, *args, sse: str = "openai", response_transform: Callable[[dict[str, Any]], dict[str, Any]] | None = None):
        from services.protocol.conversation import ImageGenerationError

        if args and self.request_body is None:
            self.request_body = json_safe(args[0])
        try:
            runner = run_image_in_threadpool if self.endpoint.startswith("/v1/images") else run_in_threadpool
            result = await runner(handler, *args)
        except ImageGenerationError as exc:
            response = _image_error_response(exc)
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""),
                     conversation_id=getattr(exc, "conversation_id", ""), response_body=_response_body(response))
            return response
        except HTTPException as exc:
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc.detail), response_body={"detail": exc.detail})
            raise
        except Exception as exc:
            if self.endpoint.startswith("/v1/images"):
                response = _image_error_response(exc)
            else:
                response = _protocol_error_response(exc, 502, sse)
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc),
                     account_email=getattr(exc, "account_email", ""), response_body=_response_body(response))
            return response

        if isinstance(result, dict):
            response = dict(result)
            response.pop("_account_email", None)
            if response_transform is not None:
                response = response_transform(response)
            await run_in_threadpool(self.log, "调用完成", result, response_body=response)
            if self.endpoint.startswith("/v1/images"):
                return await run_in_threadpool(JSONResponse, response)
            return response

        sender = anthropic_sse_stream if sse == "anthropic" else sse_json_stream
        try:
            has_first, first = await run_in_threadpool(_next_item, result)
        except ImageGenerationError as exc:
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""),
                     conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except HTTPException as exc:
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc.detail))
            raise
        except Exception as exc:
            await run_in_threadpool(self.log, "调用失败", status="failed", error=str(exc), account_email=getattr(exc, "account_email", ""))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)
        if not has_first:
            await run_in_threadpool(self.log, "流式调用结束")
            return StreamingResponse(sender(()), media_type="text/event-stream")
        return StreamingResponse(sender(self.stream(itertools.chain([first], result))), media_type="text/event-stream")

    def stream(self, items):
        urls: list[str] = []
        account_emails: list[str] = []
        conversation_ids: list[str] = []
        failed = False
        try:
            for item in items:
                urls.extend(_collect_urls(item))
                account_emails.extend(_collect_account_emails(item))
                conversation_ids.extend(_collect_conversation_ids(item))
                yield _strip_internal_response_fields(item)
        except Exception as exc:
            failed = True
            self.log(
                "流式调用失败",
                status="failed",
                error=str(exc),
                urls=urls,
                account_email=(account_emails[0] if account_emails else getattr(exc, "account_email", "")),
                conversation_id=(conversation_ids[0] if conversation_ids else getattr(exc, "conversation_id", "")),
            )
            if self.endpoint.startswith("/v1/images") and not hasattr(exc, "to_openai_error"):
                from services.protocol.conversation import ImageGenerationError, public_image_error_message

                raise ImageGenerationError(public_image_error_message(str(exc))) from exc
            raise
        finally:
            if not failed:
                self.log("流式调用结束", urls=urls, account_email=account_emails[0] if account_emails else "",
                         conversation_id=conversation_ids[0] if conversation_ids else "")

    def log(self, suffix: str, result: object = None, status: str = "success", error: str = "",
            urls: list[str] | None = None, account_email: str = "", conversation_id: str = "",
            response_body: object = None) -> None:
        detail = {
            "key_id": self.identity.get("id"),
            "key_name": self.identity.get("name"),
            "role": self.identity.get("role"),
            "endpoint": self.endpoint,
            "model": self.model,
            "started_at": datetime.fromtimestamp(self.started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_ms": int((time.time() - self.started) * 1000),
            "status": status,
        }
        detail.update(self.log_context)
        if self.request_body is not None:
            detail["request_body"] = self.request_body
        if response_body is not None:
            detail["response_body"] = json_safe(response_body)
        request_excerpt = _request_excerpt(self.request_text)
        if request_excerpt:
            detail["request_text"] = request_excerpt
        if self.request_shape:
            detail["request_shape"] = self.request_shape
        if error:
            detail["error"] = error
        email = str(account_email or "").strip()
        if not email:
            emails = _collect_account_emails(result)
            email = emails[0] if emails else ""
        if email:
            detail["account_email"] = email
        conv_id = str(conversation_id or "").strip()
        if not conv_id:
            conv_ids = _collect_conversation_ids(result)
            conv_id = conv_ids[0] if conv_ids else ""
        if conv_id:
            detail["conversation_id"] = conv_id
        collected_urls = [*(urls or []), *_collect_urls(result)]
        if collected_urls and not self.endpoint.startswith("/v1/search"):
            detail["urls"] = list(dict.fromkeys(collected_urls))
        log_service.add(LOG_TYPE_CALL, f"{self.summary}{suffix}", detail)
