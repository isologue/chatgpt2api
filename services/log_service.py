from __future__ import annotations

import hashlib
import json
import itertools
import base64
import time
import sqlite3
import shutil
import os
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
from services.image_statistics import call_log_status, image_result_counts
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
        self._maintenance_lock = RLock()
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
            connection.execute("CREATE INDEX IF NOT EXISTS ix_entries_type_day_seq ON entries(type, day, seq)")
            connection.execute("CREATE INDEX IF NOT EXISTS ix_entries_day_seq ON entries(day, seq)")
            connection.execute("CREATE TABLE IF NOT EXISTS entry_filters (seq INTEGER PRIMARY KEY, key_name TEXT, is_async INTEGER, response_format TEXT, status TEXT)")
            for column in ("key_name", "is_async", "response_format", "status"):
                connection.execute(f"CREATE INDEX IF NOT EXISTS ix_entry_filters_{column}_seq ON entry_filters({column}, seq)")
            connection.execute("CREATE TABLE IF NOT EXISTS log_filters_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS source (id INTEGER PRIMARY KEY, size INTEGER, mtime TEXT, lines INTEGER)")
            connection.execute("CREATE TABLE IF NOT EXISTS image_stats_daily (day TEXT PRIMARY KEY, success INTEGER NOT NULL, failed INTEGER NOT NULL, excluded INTEGER NOT NULL, success_images INTEGER NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS image_stats_meta (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)")
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
    def _with_call_status(item):
        result = dict(item)
        if item.get("type") == "call" and isinstance(detail := item.get("detail"), dict):
            result["detail"] = dict(detail)
            if "status" in detail:
                result["detail"]["status"] = call_log_status(detail)
        return result

    @classmethod
    def _summary(cls, item):
        item = cls._with_call_status(item)
        summary = dict(item)
        if isinstance(detail := item.get("detail"), dict):
            summary["detail"] = {key: value for key, value in detail.items()
                                 if key not in {"request_body", "response_body"}}
        return summary

    def _index_line(self, connection, item, number, offset, length):
        connection.execute("INSERT OR REPLACE INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
            (number, item["id"], str(item.get("type") or ""), str(item.get("time") or "")[:10],
             offset, length, self._serialize_item(self._summary(item))))
        self._index_filters(connection, number, self._summary(item))
        self._index_image_stats(connection, item)

    @staticmethod
    def _index_image_stats(connection, item):
        counts = image_result_counts(item)
        if any(counts):
            connection.execute(
                "INSERT INTO image_stats_daily VALUES (?, ?, ?, ?, ?) ON CONFLICT(day) DO UPDATE SET "
                "success=success+excluded.success, failed=failed+excluded.failed, "
                "excluded=excluded+excluded.excluded, success_images=success_images+excluded.success_images",
                (str(item.get("time") or "")[:10], *counts),
            )

    @staticmethod
    def _index_filters(connection, number, item):
        detail = item.get("detail") or {}
        if not isinstance(detail, dict):
            detail = {}
        is_async = detail.get("is_async")
        connection.execute("INSERT OR REPLACE INTO entry_filters VALUES (?, ?, ?, ?, ?)", (
            number, str(detail.get("key_name") or ""),
            int(is_async) if isinstance(is_async, bool) else None,
            str(detail.get("response_format") or ""), call_log_status(detail),
        ))

    def _sync_filter_index(self, connection):
        if connection.execute("SELECT version FROM log_filters_meta WHERE id=1").fetchone() == (1,):
            return
        # 只回填已有摘要，不加载历史请求体或 Base64 响应。
        connection.execute("DELETE FROM entry_filters")
        for number, summary in connection.execute("SELECT seq, summary FROM entries"):
            self._index_filters(connection, number, json.loads(summary))
        connection.execute("INSERT OR REPLACE INTO log_filters_meta VALUES (1, 1)")

    def _sync_index(self, connection):
        size, mtime = self._fingerprint()
        saved = connection.execute("SELECT size, mtime, lines FROM source WHERE id=1").fetchone()
        stats_version = connection.execute("SELECT version FROM image_stats_meta WHERE id=1").fetchone()
        if saved and saved[:2] == (size, mtime):
            if stats_version != (1,):
                # 升级时复用已有摘要，避免再次读取历史请求体和大 Base64。
                connection.execute("DELETE FROM image_stats_daily")
                daily = {}
                for row in connection.execute("SELECT summary FROM entries"):
                    item = json.loads(row[0])
                    counts = image_result_counts(item)
                    if any(counts):
                        day = str(item.get("time") or "")[:10]
                        previous = daily.setdefault(day, [0, 0, 0, 0])
                        for index, count in enumerate(counts):
                            previous[index] += count
                connection.executemany("INSERT INTO image_stats_daily VALUES (?, ?, ?, ?, ?)",
                                       [(day, *counts) for day, counts in daily.items()])
                connection.execute("INSERT OR REPLACE INTO image_stats_meta VALUES (1, 1)")
            self._sync_filter_index(connection)
            return saved[2]
        # Rebuild once for legacy logs or externally restored/edited files.
        # Stream the source: historical Base64 bodies never accumulate in RAM.
        connection.execute("DELETE FROM entries")
        connection.execute("DELETE FROM entry_filters")
        connection.execute("DELETE FROM image_stats_daily")
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
        connection.execute("INSERT OR REPLACE INTO image_stats_meta VALUES (1, 1)")
        connection.execute("INSERT OR REPLACE INTO log_filters_meta VALUES (1, 1)")
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

    @staticmethod
    def _list_conditions(type="", start_date="", end_date=""):
        clauses, params = [], []
        for clause, value in (("entries.type = ?", type), ("day >= ?", start_date), ("day <= ?", end_date)):
            if value:
                clauses.append(clause)
                params.append(value)
        return clauses, params

    def _query_scope(self, type="", start_date="", end_date="", key_name=None,
                     is_async=None, response_format="", status=""):
        clauses, params = self._list_conditions(type, start_date, end_date)
        filtered = False
        for column, value in (("key_name", key_name), ("is_async", is_async),
                              ("response_format", response_format or None), ("status", status or None)):
            if value is not None:
                filtered = True
                clauses.append(f"entry_filters.{column} = ?")
                params.append(value)
        source = "entries JOIN entry_filters USING(seq)" if filtered else "entries"
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        return source, where, params

    def list(self, type: str = "", start_date: str = "", end_date: str = "", limit: int = 200,
             *, key_name: str | None = None, is_async: bool | None = None,
             response_format: str = "", status: str = "") -> list[dict[str, Any]]:
        with self._lock, self._index() as connection:
            self._sync_index(connection)
            source, where, params = self._query_scope(type, start_date, end_date, key_name, is_async, response_format, status)
            rows = connection.execute(
                "SELECT summary FROM " + source + where + " ORDER BY seq DESC LIMIT ?",
                [*params, max(0, limit)],
            ).fetchall()
            return [self._with_call_status(json.loads(row[0])) for row in rows]

    def list_page(self, *, page: int = 1, page_size: int = 20, **filters) -> dict[str, Any]:
        page_size = max(1, min(100, page_size))
        with self._index() as connection:
            with self._lock:
                self._sync_index(connection)
            # WAL 快照查询不持有追加锁，避免翻页阻塞图片完成日志写入。
            source, where, params = self._query_scope(**filters)
            total = connection.execute("SELECT COUNT(*) FROM " + source + where, params).fetchone()[0]
            page = max(1, min(page, max(1, (total + page_size - 1) // page_size)))
            # 深页仅跳过索引中的序号，确定当前页后才加载少量日志摘要。
            rows = connection.execute(
                "SELECT summary FROM entries WHERE seq IN (SELECT seq FROM " + source + where +
                " ORDER BY seq DESC LIMIT ? OFFSET ?) ORDER BY seq DESC",
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            return {"items": [self._with_call_status(json.loads(row[0])) for row in rows],
                    "total": total, "page": page, "page_size": page_size}

    def storage_stats(self) -> dict[str, int]:
        # 仅复用索引计数、读取文件元数据，不扫描历史请求体或响应体。
        with self._index() as connection:
            with self._lock:
                self._sync_index(connection)
            count = connection.execute("SELECT COUNT(*) FROM entries").fetchone()[0]

        def size(path: Path) -> int:
            try:
                return path.stat().st_size
            except FileNotFoundError:
                # WAL/SHM 文件可能在读取期间被 SQLite 自动移除。
                return 0

        body_size = size(self.path)
        index_size = sum(size(path) for path in (
            self._index_path, Path(str(self._index_path) + "-wal"), Path(str(self._index_path) + "-shm"),
        ))
        usage = shutil.disk_usage(self.path.parent)
        return {
            "disk_total_mb": usage.total // (1024 * 1024),
            "disk_used_mb": usage.used // (1024 * 1024),
            "disk_free_mb": usage.free // (1024 * 1024),
            "log_count": count,
            "log_size_bytes": body_size + index_size,
            "log_file_size_bytes": body_size,
            "log_index_size_bytes": index_size,
        }

    def list_key_names(self, type: str = "", start_date: str = "", end_date: str = "",
                       query: str = "", limit: int = 50) -> list[str]:
        with self._index() as connection:
            with self._lock:
                self._sync_index(connection)
            # WAL 快照查询不持有追加锁，避免翻页阻塞图片完成日志写入。
            clauses, params = self._list_conditions(type, start_date, end_date)
            if query:
                # instr 使用字面子串搜索，令牌名称里的 % 和 _ 不是通配符。
                clauses.append("instr(lower(key_name), lower(?)) > 0")
                params.append(query)
            where = " WHERE " + " AND ".join(clauses) if clauses else ""
            return [row[0] for row in connection.execute(
                "SELECT DISTINCT key_name FROM entries JOIN entry_filters USING(seq)" + where +
                " ORDER BY key_name LIMIT ?", [*params, max(1, min(100, limit))],
            )]

    def image_stats(self, start_date: str, end_date: str) -> dict[str, Any]:
        """增量维护日汇总；正常刷新只读日期范围内的少量汇总行。"""
        with self._lock, self._index() as connection:
            self._sync_index(connection)
            row = connection.execute(
                "SELECT SUM(success), SUM(failed), SUM(excluded), SUM(success_images) "
                "FROM image_stats_daily WHERE day >= ? AND day <= ?", (start_date, end_date),
            ).fetchone()
        success, failed, excluded, success_images = (int(value or 0) for value in row)
        total = success + failed
        return {
            "success": success, "failed": failed, "excluded": excluded,
            "success_images": success_images, "total": total,
            "success_rate": round(success * 100 / total, 2) if total else 0.0,
            "start_date": start_date, "end_date": end_date,
        }

    def get(self, log_id: str) -> dict[str, Any] | None:
        with self._lock, self._index(write=True) as connection:
            self._sync_index(connection)
            row = connection.execute("SELECT seq, offset, length FROM entries WHERE id=?", (log_id,)).fetchone()
            if row is None:
                return None
            with self.path.open("rb") as source:
                source.seek(row[1])
                item = self._parse_line(source.read(row[2]).decode("utf-8").rstrip("\r\n"), row[0])
                return self._with_call_status(item) if item is not None else None

    def cleanup_before(self, cutoff: float) -> dict[str, int]:
        from services.retention_settings import timestamp

        # 先查轻量摘要；没有过期日志时不读取/重写请求体和响应体。
        with self._maintenance_lock:
            with self._index() as connection:
                with self._lock:
                    self._sync_index(connection)
                cutoff_day = datetime.fromtimestamp(cutoff).strftime("%Y-%m-%d")
                ids = set()
                for log_id, summary in connection.execute("SELECT id, summary FROM entries WHERE day <= ?", (cutoff_day,)):
                    created = timestamp(json.loads(summary).get("time"))
                    if created is not None and created < cutoff:
                        ids.add(log_id)
            return self._compact_ids(ids)

    def delete(self, ids: list[str]) -> dict[str, int]:
        target_ids = {str(item or "").strip() for item in ids if str(item or "").strip()}
        with self._maintenance_lock:
            return self._compact_ids(target_ids)

    def _compact_ids(self, target_ids: set[str]) -> dict[str, int]:
        if not target_ids or not self.path.exists():
            return {"removed": 0, "reclaimed_bytes": 0}
        temporary = self.path.with_name(self.path.stem + ".cleanup-" + uuid4().hex + ".jsonl")
        staged = LogService(temporary)
        removed = old_number = new_number = 0
        source = target = None
        try:
            # 只在取得快照边界和最终替换时持追加锁，主体压缩在锁外流式完成。
            with self._lock, self._index() as connection:
                self._sync_index(connection)
                boundary = self.path.stat().st_size
                source = self.path.open("rb")
            target = temporary.open("wb")

            def copy_until(limit, connection):
                nonlocal removed, old_number, new_number
                while source.tell() < limit:
                    line = source.readline()
                    if not line:
                        raise RuntimeError("log source changed during cleanup")
                    item = self._parse_line(line.decode("utf-8").rstrip("\r\n"), old_number)
                    old_number += 1
                    if item is not None and item["id"] in target_ids:
                        removed += 1
                        continue
                    # 重排前持久化 legacy id；正文原字段及值完整保留。
                    payload = (self._serialize_item(item) + "\n").encode("utf-8") if item is not None else line
                    offset = target.tell()
                    target.write(payload)
                    if item is not None:
                        staged._index_line(connection, item, new_number, offset, len(payload))
                    new_number += 1
                target.flush()

            with staged._index(write=True) as connection:
                copy_until(boundary, connection)
            if not removed:
                return {"removed": 0, "reclaimed_bytes": 0}
            while True:
                with self._lock:
                    current = self.path.stat()
                    if not os.path.samestat(os.fstat(source.fileno()), current):
                        raise RuntimeError("log source was replaced during cleanup")
                    end = current.st_size
                    if end < source.tell():
                        raise RuntimeError("log source was truncated during cleanup")
                    if end - source.tell() <= 1024 * 1024:
                        # 新日志通常已在前面的追赶阶段复制，此处只处理小尾部。
                        with staged._index(write=True) as connection:
                            copy_until(end, connection)
                        target.flush()
                        os.fsync(target.fileno())
                        target.close()
                        source.close()
                        # 元数据整体交换无需再扫描大正文。WAL 读者继续使用旧快照。
                        with self._index(write=True) as connection:
                            connection.execute("ATTACH DATABASE ? AS compacted", (str(staged._index_path),))
                            for table in ("entries", "entry_filters", "image_stats_daily"):
                                connection.execute(f"DELETE FROM {table}")
                                connection.execute(f"INSERT INTO {table} SELECT * FROM compacted.{table}")
                            connection.execute("INSERT OR REPLACE INTO image_stats_meta VALUES (1, 1)")
                            connection.execute("INSERT OR REPLACE INTO log_filters_meta VALUES (1, 1)")
                            temporary.replace(self.path)
                            size, mtime = self._fingerprint()
                            connection.execute("INSERT OR REPLACE INTO source VALUES (1, ?, ?, ?)", (size, mtime, new_number))
                        return {"removed": removed, "reclaimed_bytes": max(0, end - size)}
                # 锁外追赶清理期间追加的日志，不阻塞生图完成写日志。
                with staged._index(write=True) as connection:
                    copy_until(end, connection)
        finally:
            if source is not None:
                source.close()
            if target is not None:
                target.close()
            for path in (temporary, staged._index_path, Path(str(staged._index_path) + "-wal"), Path(str(staged._index_path) + "-shm")):
                path.unlink(missing_ok=True)


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
            self.log_context["error_code"] = exc.code
            response = _image_error_response(exc)
            for key in ("switch_count", "account_time_ms", "account_switch_trace"):
                if hasattr(exc, key):
                    self.log_context[key] = getattr(exc, key)
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
            self.log_context["error_code"] = exc.code
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
            self.log_context["error_code"] = getattr(exc, "code", "")
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
        if isinstance(result, dict):
            for key in ("_switch_count", "_account_time_ms", "_account_switch_trace"):
                if key in result:
                    detail[key[1:]] = result[key]
        log_service.add(LOG_TYPE_CALL, f"{self.summary}{suffix}", detail)
