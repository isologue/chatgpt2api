from __future__ import annotations

from services.image_async import blocking

import base64
import json
import re
import secrets
import time
import uuid
from collections import OrderedDict
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Condition, Lock, Thread, local
from typing import Any, Callable
from urllib.parse import urlencode

from services.image_metrics import measured, phase, current_metrics, runtime_phases
from services.account_verification import AccountVerification, VerificationBackoff
from services.account_pool import AccountPoolIndex, ImageAttemptBudget, CachedAccountInfo, availability_time, timestamp
from services.account_refresh_monitor import refresh_failure_item
from services.keyed_locks import KeyedLocks
from services.account_persistence import AccountWriter, persisted
from services.config import config
from services.log_service import (
    LOG_TYPE_ACCOUNT,
    log_service,
)
from services.storage.base import StorageBackend
from services.account_list_fields import parse_account_time, refresh_error_is_terminal, refresh_token_status, matches_account_filters
from utils.helper import anonymize_token


class _RevisionDict(dict):
    """账号修改同时维护轻量统计，分页读索引仍按版本失效。"""

    def __init__(self, *args, on_change=None, **kwargs):
        self._on_change = on_change
        super().__init__(*args, **kwargs)

    def _changed(self, previous, current):
        if self._on_change is not None:
            self._on_change(previous, current)

    def __setitem__(self, key, value):
        previous = self.get(key)
        super().__setitem__(key, value)
        self._changed(previous, value)

    def __delitem__(self, key):
        previous = self[key]
        super().__delitem__(key)
        self._changed(previous, None)

    def pop(self, key, *args):
        existed = key in self
        previous = super().pop(key, *args)
        if existed:
            self._changed(previous, None)
        return previous

    def popitem(self):
        key, value = super().popitem()
        self._changed(value, None)
        return key, value

    def clear(self):
        previous = tuple(self.values())
        super().clear()
        for value in previous:
            self._changed(value, None)

    def update(self, *args, **kwargs):
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def setdefault(self, key, default=None):
        if key not in self:
            self[key] = default
        return self[key]


class AccountService:
    """账号池服务，使用 token -> account 的 dict 保存账号。"""

    _NEW_ACCOUNT_INVALID_GRACE_SECONDS = 10 * 60
    _INVALID_CONFIRM_SECONDS = 30
    _ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 24 * 60 * 60
    _IMAGE_TOKEN_REFRESH_SKEW_SECONDS = 5 * 60
    _REFRESH_TOKEN_KEEPALIVE_SECONDS = 3 * 24 * 60 * 60
    _REFRESH_TOKEN_KEEPALIVE_ERROR_BACKOFF_SECONDS = 6 * 60 * 60
    _REFRESH_TOKEN_KEEPALIVE_BATCH_SIZE = 3
    _TOKEN_REFRESH_ERROR_BACKOFF_SECONDS = 5 * 60
    _OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
    _OAUTH_CLIENT_ID = "app_2SKx67EdpoN0G6j64rFvigXD"
    _OAUTH_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )

    # 刷新进度追踪
    _refresh_progress: dict[str, dict] = {}
    _refresh_progress_lock = Lock()
    # 重新登录进度追踪
    _relogin_progress: dict[str, dict] = {}
    _relogin_progress_lock = Lock()

    def __init__(self, storage_backend: StorageBackend):
        self.storage = storage_backend
        self._lock = Lock()
        self._token_refresh_locks = KeyedLocks()
        self.verification = AccountVerification(lambda: config.image_account_verification)
        self._write_context = local()
        self._writer = AccountWriter(storage_backend)
        self._image_slot_condition = Condition(self._lock)
        self._index = 0
        self._account_revision = 0
        self._account_view_cache = None
        self._accounts = _RevisionDict(
            self._load_accounts(), on_change=self._mark_account_revision
        )
        self._stats_summary = dict(total=0, active=0, limited=0, abnormal=0, disabled=0,
                                   quota=0, total_success=0, total_fail=0)
        self._stats_by_type = {}
        for account in self._accounts.values():
            self._apply_stats_delta(None, account)
        self._image_inflight: dict[str, int] = {}
        self._token_aliases: dict[str, str] = {}
        self._cumulative_total = self._load_cumulative_total()
        self._ready_buckets: dict[tuple, OrderedDict] = {}
        self._available_buckets: dict[tuple, OrderedDict] = {}
        self._bucket_for_token: dict[str, tuple] = {}
        self._verified_at: dict[str, float] = {}
        self._verification_versions: dict[str, int] = {}
        self._pool_index = AccountPoolIndex(lambda: config.image_account_verification)
        self._pool_reconfigure_lock = Lock()
        self._pool_schedule_settings = self._pool_schedule_signature()
        self._scheduler_concurrency = max(1, int(config.image_account_concurrency or 1))
        for token in self._accounts:
            self._sync_candidate_locked(token)

    def _mark_account_revision(self, previous=None, current=None) -> None:
        self._account_revision += 1
        self._account_view_cache = None
        if hasattr(self, "_stats_summary"):
            self._apply_stats_delta(previous, current)

    def _apply_stats_delta(self, previous, current):
        for account, sign in ((previous, -1), (current, 1)):
            if account is None:
                continue
            summary = self._stats_summary
            summary["total"] += sign
            status = account.get("status")
            for name, value in (("active", "正常"), ("limited", "限流"), ("abnormal", "异常"), ("disabled", "禁用")):
                summary[name] += sign * int(status == value)
            summary["quota"] += sign * (max(0, int(account.get("quota") or 0)) if status == "正常" else 0)
            summary["total_success"] += sign * int(account.get("success") or 0)
            summary["total_fail"] += sign * int(account.get("fail") or 0)
            kind = account.get("type", "unknown")
            count = self._stats_by_type.get(kind, 0) + sign
            if count:
                self._stats_by_type[kind] = count
            else:
                self._stats_by_type.pop(kind, None)

    def _account_view_locked(self) -> dict[str, Any]:
        """Build the immutable read index used by account pages and stats."""
        cache = getattr(self, "_account_view_cache", None)
        revision = getattr(self, "_account_revision", None)
        if cache is not None and cache.get("revision") == revision:
            return cache

        snapshot = tuple(self._accounts.values())
        buckets: dict[tuple[str, str], list[dict]] = {}
        summary = dict(total=len(snapshot), active=0, limited=0, abnormal=0, disabled=0, quota=0,
                       total_success=0, total_fail=0)
        types = set()
        sources = set()
        status_keys = {"正常": "active", "限流": "limited", "异常": "abnormal", "禁用": "disabled"}
        for item in snapshot:
            item_type = str(item.get("type") or "Free")
            item_status = str(item.get("status") or "")
            types.add(item_type)
            sources.add(self._normalize_source_type(item.get("source_type")))
            buckets.setdefault((item_type, item_status), []).append(item)
            if key := status_keys.get(item_status):
                summary[key] += 1
            if item_status == "正常":
                summary["quota"] += max(0, int(item.get("quota") or 0))
            summary["total_success"] += int(item.get("success") or 0)
            summary["total_fail"] += int(item.get("fail") or 0)

        cache = {
            "revision": revision,
            "snapshot": snapshot,
            "buckets": {key: tuple(items) for key, items in buckets.items()},
            "summary": summary,
            "types": tuple(sorted(types)),
            "sources": tuple(sorted(sources)),
        }
        self._account_view_cache = cache
        return cache

    def _get_cumulative_file(self) -> Path:
        from services.config import DATA_DIR
        return DATA_DIR / ".cumulative_total"

    def _load_cumulative_total(self) -> int:
        try:
            f = self._get_cumulative_file()
            if f.exists():
                return int(f.read_text().strip())
        except Exception:
            pass
        return len(self._accounts)

    def _save_cumulative_total(self) -> None:
        try:
            self._get_cumulative_file().write_text(str(self._cumulative_total))
        except Exception:
            pass

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _decode_jwt_payload(token: str) -> dict:
        try:
            payload = str(token or "").split(".")[1]
            payload += "=" * ((4 - len(payload) % 4) % 4)
            import base64
            import json
            data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _parse_time(value: object) -> datetime | None:
        return parse_account_time(value)

    @staticmethod
    def _refresh_token_error_is_terminal(error: object) -> bool:
        return refresh_error_is_terminal(error)

    @classmethod
    def _refresh_token_status(cls, account: dict) -> str:
        return refresh_token_status(account)

    @staticmethod
    def _timestamp_to_iso(value: object) -> str:
        try:
            ts = int(value)
        except (TypeError, ValueError):
            return ""
        tz = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz).isoformat()

    def _load_accounts(self) -> dict[str, dict]:
        accounts = self.storage.load_accounts()
        return {
            normalized["access_token"]: normalized
            for item in accounts
            if (normalized := self._normalize_account(item)) is not None
        }

    def _log(self, *args, **kwargs):
        after = getattr(self._write_context, "after", None)
        if after is None:
            log_service.add(*args, **kwargs)
        else:
            after.append(lambda: log_service.add(*args, **kwargs))

    def _queue_write(self, changes=None, *, snapshot=None) -> None:
        future = self._writer.submit(changes, snapshot=snapshot, deferred=True)
        pending = getattr(self._write_context, "pending", None)
        if pending is None:
            raise RuntimeError("account writes must run in a persisted mutation")
        pending.append(future)

    def _save_accounts(self) -> None:
        self._queue_write(snapshot=list(self._accounts.values()))

    def _save_account(self, account: dict) -> None:
        self._sync_candidate_locked(account["access_token"])
        self._image_slot_condition.notify(1)
        if getattr(self.storage, "supports_incremental_account_writes", False):
            self._queue_write({account["access_token"]: dict(account)})
        else:
            self._save_accounts()

    def _delete_account(self, access_token: str) -> None:
        previous = self._pool_index.entries.get(access_token)
        key = previous[3] if previous else access_token
        self._sync_candidate_locked(access_token)
        self._verified_at.pop(access_token, None)
        self.verification.forget(key)
        if getattr(self.storage, "supports_incremental_account_writes", False):
            self._queue_write({access_token: None})
        else:
            self._save_accounts()

    def _sync_candidate_locked(self, token: str) -> None:
        account = self._accounts.get(token)
        eligible = self._is_image_account_available(account)
        self._pool_index.sync(token, account, available=eligible,
                              inflight=self._image_inflight.get(token, 0),
                              concurrency=self._scheduler_concurrency)
        bucket = None
        if eligible and timestamp(account.get("pool_retry_at")) <= time.time():
            bucket = (self._normalize_account_type(account.get("type")),
                      self._normalize_source_type(account.get("source_type")),
                      int(bool(account.get("image_pool_hot"))),
                      int(account.get("image_score", 50)) // 10)
        old_bucket = self._bucket_for_token.get(token)
        if old_bucket is not None and old_bucket != bucket:
            self._bucket_for_token.pop(token, None)
            self._ready_buckets[old_bucket].pop(token, None)
            self._available_buckets[old_bucket].pop(token, None)
            if not self._ready_buckets[old_bucket]:
                self._ready_buckets.pop(old_bucket)
                self._available_buckets.pop(old_bucket)
        if bucket is None:
            return
        self._bucket_for_token[token] = bucket
        ready = self._ready_buckets.setdefault(bucket, OrderedDict())
        ready[token] = None
        ready.move_to_end(token)
        available = self._available_buckets.setdefault(bucket, OrderedDict())
        capacity = min(self._scheduler_concurrency, max(0, int(account.get("quota") or 0)))
        if self._image_inflight.get(token, 0) < capacity:
            available[token] = None
            available.move_to_end(token)
        else:
            available.pop(token, None)

    def _matching_buckets(self, plan_type, source_type, plan_types):
        for bucket, ready in self._ready_buckets.items():
            sample = {"type": bucket[0], "source_type": bucket[1]}
            if (self._account_matches_plan_type(sample, plan_type)
                    and self._account_matches_any_plan_type(sample, plan_types)
                    and self._account_matches_source_type(sample, source_type)):
                yield bucket, ready

    @staticmethod
    def _is_image_account_available(account: dict) -> bool:
        if not isinstance(account, dict):
            return False
        if account.get("status") in {"禁用", "限流", "异常"} or int(account.get("invalid_count") or 0) > 0:
            return False
        return int(account.get("quota") or 0) > 0

    @classmethod
    def _account_matches_plan_type(cls, account: dict, plan_type: str | None = None) -> bool:
        if not plan_type:
            return True
        normalized_plan = cls._normalize_account_type(plan_type)
        normalized_account = cls._normalize_account_type(account.get("type"))
        if not normalized_plan or not normalized_account:
            return False
        return normalized_plan.lower() == normalized_account.lower()

    @classmethod
    def _account_matches_source_type(cls, account: dict, source_type: str | None = None) -> bool:
        if not source_type:
            return True
        return cls._normalize_source_type(account.get("source_type")) == cls._normalize_source_type(source_type)

    @classmethod
    def _account_matches_any_plan_type(cls, account: dict, plan_types: set[str] | tuple[str, ...] | None = None) -> bool:
        if not plan_types:
            return True
        normalized_account = cls._normalize_account_type(account.get("type"))
        normalized_plans = {
            normalized
            for plan_type in plan_types
            if (normalized := cls._normalize_account_type(plan_type))
        }
        return bool(normalized_account and normalized_account in normalized_plans)

    @staticmethod
    def _normalize_source_type(value: object) -> str:
        return str(value or "web").strip().lower() or "web"

    @staticmethod
    def _normalize_account_type(value: object) -> str | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        key = raw.lower().replace("-", "_").replace(" ", "_")
        compact = key.replace("_", "")
        aliases = {
            "free": "free",
            "plus": "Plus",
            "pro": "Pro",
            "prolite": "ProLite",
            "team": "Team",
            "business": "Team",
            "enterprise": "Enterprise",
        }
        return aliases.get(compact) or aliases.get(key) or raw

    def _search_account_type(self, payload: object) -> str | None:
        if isinstance(payload, dict):
            for key in ("plan_type", "account_plan", "account_type", "subscription_type", "type"):
                plan = self._normalize_account_type(payload.get(key))
                if plan:
                    return plan
            for value in payload.values():
                plan = self._search_account_type(value)
                if plan:
                    return plan
        elif isinstance(payload, list):
            for value in payload:
                plan = self._search_account_type(value)
                if plan:
                    return plan
        return None

    def _normalize_account(self, item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None
        access_token = item.get("access_token") or item.get("accessToken") or ""
        if not access_token:
            return None
        normalized = dict(item)
        normalized.pop("accessToken", None)
        normalized["access_token"] = access_token
        if str(normalized.get("type") or "").strip().lower() == "codex":
            normalized["export_type"] = "codex"
            normalized.pop("type", None)
        normalized["type"] = normalized.get("type") or "free"
        normalized["status"] = normalized.get("status") or "正常"
        normalized["quota"] = max(0, int(normalized.get("quota") if normalized.get("quota") is not None else 0))
        normalized["email"] = normalized.get("email") or None
        normalized["user_id"] = normalized.get("user_id") or None
        normalized["proxy"] = str(normalized.get("proxy") or "").strip()
        source_type = normalized.get("source_type")
        if not source_type and str(normalized.get("export_type") or "").strip().lower() == "codex":
            source_type = "codex"
        normalized["source_type"] = self._normalize_source_type(source_type)
        limits_progress = normalized.get("limits_progress")
        normalized["limits_progress"] = limits_progress if isinstance(limits_progress, list) else []
        normalized["default_model_slug"] = normalized.get("default_model_slug") or None
        normalized["restore_at"] = normalized.get("restore_at") or None
        normalized["success"] = int(normalized.get("success") or 0)
        normalized["fail"] = int(normalized.get("fail") or 0)
        normalized["invalid_count"] = int(normalized.get("invalid_count") or 0)
        normalized["last_used_at"] = normalized.get("last_used_at")
        # 稳定身份随账号保存；AT轮换复制该字段，不以变化的AT作为刷新flight身份。
        normalized["pool_account_id"] = str(normalized.get("pool_account_id") or uuid.uuid5(uuid.NAMESPACE_OID, access_token))
        try:
            normalized["image_score"] = max(0, min(100, int(normalized.get("image_score", 50))))
        except (TypeError, ValueError, OverflowError):
            normalized["image_score"] = 50
        normalized["image_pool_hot"] = bool(normalized.get("image_pool_hot", False))
        if (normalized["status"] in {"禁用", "限流", "异常"}
                or normalized["quota"] <= 0 or normalized["invalid_count"] > 0):
            normalized["image_pool_hot"] = False
        for field in ("hot_entered_at", "last_image_started_at", "image_score_updated_at", "last_image_success_at",
                      "last_policy_confirmed_at", "verification_invalidated_at", "pool_retry_at"):
            normalized[field] = timestamp(normalized.get(field))
        normalized["last_invalid_at"] = normalized.get("last_invalid_at") or None
        normalized["last_refresh_error"] = normalized.get("last_refresh_error") or None
        normalized["last_refresh_error_at"] = normalized.get("last_refresh_error_at") or None
        normalized["last_token_refresh_at"] = normalized.get("last_token_refresh_at") or None
        normalized["last_token_refresh_error"] = normalized.get("last_token_refresh_error") or None
        normalized["last_token_refresh_error_at"] = normalized.get("last_token_refresh_error_at") or None
        normalized["refresh_token_status"] = self._refresh_token_status(normalized)
        normalized["created_at"] = normalized.get("created_at") or AccountService._now()
        return normalized

    @staticmethod
    def _jwt_exp(access_token: str) -> int:
        try:
            return int(AccountService._decode_jwt_payload(access_token).get("exp") or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _token_expires_in(cls, access_token: str) -> int | None:
        exp = cls._jwt_exp(access_token)
        if exp <= 0:
            return None
        return exp - int(time.time())

    @classmethod
    def _token_needs_refresh(cls, access_token: str, *, force: bool = False) -> bool:
        if force:
            return True
        remaining = cls._token_expires_in(access_token)
        return remaining is not None and remaining <= cls._ACCESS_TOKEN_REFRESH_SKEW_SECONDS

    @classmethod
    def _token_issued_at(cls, access_token: str) -> datetime | None:
        try:
            iat = int(cls._decode_jwt_payload(access_token).get("iat") or 0)
        except (TypeError, ValueError):
            return None
        if iat <= 0:
            return None
        return datetime.fromtimestamp(iat, tz=timezone.utc)

    @staticmethod
    def _safe_response_text(response: object, limit: int = 300) -> str:
        try:
            return str(getattr(response, "text", "") or "")[:limit]
        except Exception:
            return ""

    def _resolve_access_token_locked(self, access_token: str) -> str:
        token = str(access_token or "").strip()
        seen: set[str] = set()
        while token and token not in self._accounts and token in self._token_aliases and token not in seen:
            seen.add(token)
            token = self._token_aliases.get(token, token)
        return token

    def resolve_access_token(self, access_token: str) -> str:
        if not access_token:
            return ""
        with self._timed_account_lock("account_resolve_access_token_lock"):
            return self._resolve_access_token_locked(access_token)

    def _get_account_for_token(self, access_token: str) -> tuple[str, dict | None]:
        with self._timed_account_lock("account_get_account_for_token_lock"):
            resolved = self._resolve_access_token_locked(access_token)
            account = self._accounts.get(resolved)
            return resolved, dict(account) if account else None

    @persisted
    def _record_token_refresh_error(self, access_token: str, event: str, error: str,
                                    *, expected_refresh_token: str | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._timed_account_lock("account_record_token_refresh_error_lock"):
            resolved = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(resolved)
            if current is None:
                return
            if expected_refresh_token is not None and current.get("refresh_token") != expected_refresh_token:
                return  # 旧 RT 的远程结果不能污染并发导入的新凭证。
            next_item = dict(current)
            next_item["last_token_refresh_error"] = str(error or "refresh token failed")
            next_item["last_token_refresh_error_at"] = now
            account = self._normalize_account(next_item)
            if account is not None:
                self._accounts[resolved] = account
                self._save_account(account)
        self._log(
            LOG_TYPE_ACCOUNT,
            "refresh_token 刷新 access_token 失败",
            {"source": event, "token": anonymize_token(access_token), "error": str(error or "")},
        )

    def _recent_token_refresh_error(self, account: dict) -> bool:
        last_error_at = self._parse_time(account.get("last_token_refresh_error_at"))
        if last_error_at is None:
            return False
        return (datetime.now(timezone.utc) - last_error_at).total_seconds() < self._TOKEN_REFRESH_ERROR_BACKOFF_SECONDS

    def _recent_refresh_token_keepalive_error(self, account: dict, now: datetime) -> bool:
        last_error_at = self._parse_time(account.get("last_token_refresh_error_at"))
        if last_error_at is None:
            return False
        return (now - last_error_at).total_seconds() < self._REFRESH_TOKEN_KEEPALIVE_ERROR_BACKOFF_SECONDS

    def _refresh_token_keepalive_anchor(self, account: dict) -> datetime | None:
        return (
            self._parse_time(account.get("last_token_refresh_at"))
            or self._token_issued_at(str(account.get("access_token") or ""))
            or self._parse_time(account.get("created_at"))
        )

    def _refresh_token_keepalive_due_at(self, account: dict, now: datetime) -> datetime | None:
        if not str(account.get("refresh_token") or "").strip():
            return None
        if account.get("status") == "禁用" or self._refresh_token_status(account) == "invalid":
            return None
        if self._recent_refresh_token_keepalive_error(account, now):
            return None
        anchor = self._refresh_token_keepalive_anchor(account)
        if anchor is None:
            return now
        due_at = anchor + timedelta(seconds=self._REFRESH_TOKEN_KEEPALIVE_SECONDS)
        return due_at if due_at <= now else None

    def _request_access_token_refresh(self, refresh_token: str, account: dict | None = None) -> dict[str, str]:
        from curl_cffi import requests
        from services.proxy_service import proxy_settings

        session = requests.Session(**proxy_settings.build_session_kwargs(account=account, impersonate="chrome110", verify=True))
        try:
            response = session.post(
                self._OAUTH_TOKEN_URL,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "User-Agent": self._OAUTH_USER_AGENT,
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self._OAUTH_CLIENT_ID,
                },
                timeout=60,
            )
            data = response.json() if response.text else {}
            if response.status_code != 200 or not isinstance(data, dict) or not data.get("access_token"):
                detail = ""
                if isinstance(data, dict):
                    detail = str(data.get("error_description") or data.get("error") or data.get("message") or "")
                detail = detail or self._safe_response_text(response)
                raise RuntimeError(f"oauth_refresh_http_{response.status_code}{': ' + detail if detail else ''}")
            return {
                "access_token": str(data.get("access_token") or "").strip(),
                "refresh_token": str(data.get("refresh_token") or refresh_token).strip(),
                "id_token": str(data.get("id_token") or "").strip(),
            }
        finally:
            session.close()

    @persisted
    def _apply_refreshed_tokens(self, old_access_token: str, token_data: dict, event: str,
                                *, expected_refresh_token: str | None = None) -> str:
        now = datetime.now(timezone.utc).isoformat()
        with self._image_slot_condition:
            old_token = self._resolve_access_token_locked(old_access_token)
            current = self._accounts.get(old_token)
            if current is None:
                return old_token
            if expected_refresh_token is not None and current.get("refresh_token") != expected_refresh_token:
                return old_token
            new_token = str(token_data.get("access_token") or old_token).strip()
            if not new_token:
                return old_token

            next_item = dict(current)
            next_item["access_token"] = new_token
            if token_data.get("refresh_token"):
                next_item["refresh_token"] = str(token_data.get("refresh_token") or "").strip()
            if token_data.get("id_token"):
                next_item["id_token"] = str(token_data.get("id_token") or "").strip()
            if new_token != old_token:
                next_item["remote_verified_at"] = None
            next_item["last_token_refresh_at"] = now
            next_item["last_token_refresh_error"] = None
            next_item["last_token_refresh_error_at"] = None
            next_item["invalid_count"] = 0
            next_item["last_invalid_at"] = None
            next_item["last_refresh_error"] = None
            next_item["last_refresh_error_at"] = None

            account = self._normalize_account(next_item)
            if account is None:
                return old_token

            rotated = new_token != old_token
            if rotated:
                self._accounts.pop(old_token, None)
                self._token_aliases[old_token] = new_token
                old_inflight = int(self._image_inflight.pop(old_token, 0))
                if old_inflight:
                    self._image_inflight[new_token] = int(self._image_inflight.get(new_token, 0)) + old_inflight
            self._accounts[new_token] = account
            if rotated and self._accounts.get(old_token) is None:
                self._delete_account(old_token)
                self._save_account(account)
            else:
                self._save_account(account)
            self._image_slot_condition.notify_all()

        self._log(
            LOG_TYPE_ACCOUNT,
            "refresh_token 已刷新 access_token",
            {"source": event, "token": anonymize_token(new_token), "rotated": rotated},
        )
        return new_token

    @blocking
    def refresh_access_token(self, access_token: str, *, force: bool = False, event: str = "refresh_access_token",
                             retry_terminal: bool = False) -> str:
        if not access_token:
            return ""
        resolved_token, initial = self._get_account_for_token(access_token)
        if initial and not force:
            active = str(initial.get("access_token") or resolved_token)
            if (not initial.get("refresh_token") or not self._token_needs_refresh(active)
                    or self._refresh_token_status(initial) == "invalid"
                    or self._recent_token_refresh_error(initial)):
                return active
        # A stable account identity keeps token rotation on the same refresh lock.
        identity = str((initial or {}).get("id") or (initial or {}).get("email") or resolved_token)
        with self._token_refresh_locks.hold(identity):
            resolved_token, account = self._get_account_for_token(access_token)
            if not account:
                return access_token
            active_token = str(account.get("access_token") or resolved_token or access_token)
            if not self._token_needs_refresh(active_token, force=force):
                return active_token
            refresh_token = str(account.get("refresh_token") or "").strip()
            if not refresh_token:
                return active_token
            # 认证失败恢复的 force 也不重试已明确永久失效的 RT。
            # 仅显式维护调用可用 retry_terminal=True 重新验证同一 RT。
            if self._refresh_token_status(account) == "invalid" and not retry_terminal:
                return active_token
            if not force and self._recent_token_refresh_error(account):
                return active_token
            try:
                token_data = self._request_access_token_refresh(refresh_token, account)
            except Exception as exc:
                error_str = str(exc or "")
                self._record_token_refresh_error(active_token, event, error_str, expected_refresh_token=refresh_token)
                # 如果是 app_session_terminated 错误，尝试密码重新登录
                if "app_session_terminated" in error_str.lower():
                    # 获取账号信息（email, password）
                    email = str(account.get("email") or "").strip()
                    password = str(account.get("password") or "").strip()
                    if email and password:
                        # 创建新线程执行密码重新登录
                        t = Thread(
                            target=self._password_re_login_thread,
                            args=(active_token, email, password, event),
                            daemon=True,
                        )
                        t.start()
                return active_token
            return self._apply_refreshed_tokens(active_token, token_data, event, expected_refresh_token=refresh_token)

    def _password_re_login_thread(self, access_token: str, email: str, password: str, event: str, progress_id: str | None = None) -> None:
        """密码重新登录线程入口"""
        try:
            result = self._login_with_password(email, password)
            if result.get("ok"):
                # 登录成功，更新账号
                new_access_token = result.get("access_token", "")
                new_refresh_token = result.get("refresh_token", "")
                new_id_token = result.get("id_token", "")
                new_expires_at = result.get("expires_at")

                # 构建 token_data 供 _apply_refreshed_tokens 使用
                token_data = {
                    "access_token": new_access_token,
                    "refresh_token": new_refresh_token,
                    "id_token": new_id_token,
                }

                # 使用 _apply_refreshed_tokens 更新账号（处理 token 别名）
                new_token = self._apply_refreshed_tokens(access_token, token_data, f"{event}:password_relogin")

                # 额外更新 source_type 和 status（静默，避免重复日志）
                self.update_account(new_token, {
                    "source_type": result.get("source_type", "password"),
                    "status": "正常",
                }, quiet=True)

                self._log(
                    LOG_TYPE_ACCOUNT,
                    "更新账号",
                    {
                        "source": event,
                        "old_token": anonymize_token(access_token),
                        "new_token": anonymize_token(new_access_token),
                        "email": email,
                        "status": "成功",
                    },
                )
                if progress_id:
                    self.update_relogin_progress(progress_id, access_token, "成功")
            else:
                # 登录失败
                error_type = result.get("error", "")
                if error_type == "password_verify_failed_403" and isinstance(result.get("detail"), dict):
                    self._log(
                        LOG_TYPE_ACCOUNT,
                        "更新账号",
                        {
                            "source": event,
                            "token": anonymize_token(access_token),
                            "email": email,
                            "status": "失败",
                            "error": error_type,
                            "detail": result.get("detail", {}),
                        },
                    )
                    detail_error = result["detail"].get("error", {})
                    if isinstance(detail_error, dict) and detail_error.get("code") == "account_deactivated":
                        # 账号已删除/停用 → 标记为禁用
                        self.update_account(access_token, {"status": "禁用", "quota": 0}, quiet=True)
                        account = self.get_account(access_token) or {}
                        self._log(
                            LOG_TYPE_ACCOUNT,
                            "账号已停用-标记禁用",
                            {
                                "source": event,
                                "token": anonymize_token(access_token),
                                "email": email,
                                "detail": result.get("detail", {}),
                            },
                        )
                        if progress_id:
                            self.update_relogin_progress(progress_id, access_token, "禁用")
                    else:
                        # 永久故障：将账号标记为异常（或自动移除）
                        self.remove_invalid_token(access_token, f"{event}:password_relogin_failed", quiet=True)
                        if progress_id:
                            self.update_relogin_progress(progress_id, access_token, "异常", error_type)
                else:
                    self._log(
                        LOG_TYPE_ACCOUNT,
                        "更新账号",
                        {
                            "source": event,
                            "token": anonymize_token(access_token),
                            "email": email,
                            "status": "失败",
                            "error": error_type,
                            "detail": result.get("detail", {}),
                        },
                    )
                    # 永久故障：将账号标记为异常（或自动移除）
                    self.remove_invalid_token(access_token, f"{event}:password_relogin_failed", quiet=True)
                    if progress_id:
                        self.update_relogin_progress(progress_id, access_token, "异常", error_type)
        except Exception as exc:
            self._log(
                LOG_TYPE_ACCOUNT,
                "更新账号",
                {
                    "source": event,
                    "token": anonymize_token(access_token),
                    "email": email,
                    "status": "异常",
                    "error": str(exc),
                },
            )
            # 将账号标记为异常（或自动移除）
            self.remove_invalid_token(access_token, f"{event}:password_relogin_exception", quiet=True)
            if progress_id:
                self.update_relogin_progress(progress_id, access_token, "异常", str(exc))

    def _login_with_password(self, email: str, password: str) -> dict:
        """通过邮箱+密码登录，返回 {access_token, refresh_token, id_token, ...}"""
        from curl_cffi import requests
        
        # 常量
        auth_base = "https://auth.openai.com"
        platform_oauth_audience = "https://api.openai.com/v1"
        platform_auth0_client = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjEuMjEuMCJ9"
        platform_oauth_client_id = self._OAUTH_CLIENT_ID
        platform_oauth_redirect_uri = "https://platform.openai.com/auth/callback"
        user_agent = self._OAUTH_USER_AGENT
        
        # 创建 session
        session_kwargs = {"impersonate": "chrome110", "verify": False}
        proxy = config.get_proxy_settings()
        if proxy:
            session_kwargs["proxy"] = proxy
        session = requests.Session(**session_kwargs)
        
        try:
            device_id = str(uuid.uuid4())
            
            # ─── 方式2: OAuth authorize 流程 ──────────────────────────
            # 使用 Platform Client + PKCE
            
            from utils.pkce import generate_pkce
            code_verifier, code_challenge = generate_pkce()
            
            # ② 发起 OAuth authorize 请求 (使用 Platform Client + PKCE)
            session.cookies.set("oai-did", device_id, domain=".auth.openai.com")
            session.cookies.set("oai-did", device_id, domain="auth.openai.com")
            params = {
                "issuer": auth_base,
                "client_id": platform_oauth_client_id,
                "audience": platform_oauth_audience,
                "redirect_uri": platform_oauth_redirect_uri,
                "device_id": device_id,
                "screen_hint": "login_or_signup",
                "max_age": "0",
                "login_hint": email,
                "scope": "openid profile email offline_access",
                "response_type": "code",
                "response_mode": "query",
                "state": secrets.token_urlsafe(32),
                "nonce": secrets.token_urlsafe(32),
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
                "auth0Client": platform_auth0_client,
            }
            authorize_url = f"{auth_base}/api/accounts/authorize?{urlencode(params)}"
            resp = session.get(
                authorize_url,
                headers={
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "user-agent": user_agent,
                    "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "cross-site",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                    "referer": "https://platform.openai.com/",
                },
                allow_redirects=True,
                timeout=30,
            )
            
            if resp.status_code not in (200, 302):
                return {"ok": False, "error": f"authorize_failed_{resp.status_code}", "detail": {"url": resp.url, "text": resp.text[:500]}}
            
            # 检测最终 URL 是否指向错误页面
            final_url = str(resp.url)
            if "/error" in final_url and "payload=" in final_url:
                from urllib.parse import parse_qs, urlparse
                try:
                    parsed_query = parse_qs(urlparse(final_url).query)
                    error_payload_b64 = parsed_query.get("payload", [""])[0]
                    error_payload_b64 += "=" * ((4 - len(error_payload_b64) % 4) % 4)
                    error_payload = json.loads(base64.b64decode(error_payload_b64))
                    error_code = error_payload.get("errorCode", "")
                    if error_code == "rate_limit_exceeded":
                        return {"ok": False, "error": "rate_limit_exceeded", "detail": error_payload}
                    else:
                        return {"ok": False, "error": f"authorize_error_{error_code}", "detail": error_payload}
                except Exception as e:
                    return {"ok": False, "error": "authorize_redirect_error", "detail": {"url": final_url, "parse_error": str(e)}}
            
            # ③ 提交密码验证
            login_headers = {
                "accept": "application/json",
                "accept-language": "zh-CN,zh;q=0.9",
                "content-type": "application/json",
                "origin": auth_base,
                "priority": "u=1, i",
                "user-agent": user_agent,
                "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "referer": f"{auth_base}/email-verification",
                "oai-device-id": device_id,
            }
            
            # 添加 sentinel token
            try:
                from utils.sentinel import build_sentinel_token
                sentinel_val, oai_sc_val = build_sentinel_token(session, device_id, "password_verify")
                login_headers["openai-sentinel-token"] = sentinel_val
                if oai_sc_val:
                    session.cookies.set("oai-sc", oai_sc_val, domain=".openai.com")
            except Exception:
                pass
            
            login_resp = session.post(
                f"{auth_base}/api/accounts/password/verify",
                headers=login_headers,
                json={"password": password},
                timeout=30,
            )
            
            login_data = {}
            try:
                login_data = login_resp.json() if login_resp.text else {}
            except Exception:
                pass
            
            if login_resp.status_code != 200:
                error_code = login_data.get("error", {}).get("code", "")
                error_msg = login_data.get("error", {}).get("message", "")
                if error_code == "unsupported_country_region_territory":
                    return {"ok": False, "error": "unsupported_country_region_territory", "detail": login_data}
                elif error_code == "invalid_state":
                    return {"ok": False, "error": "invalid_state", "detail": login_data}
                elif "Invalid credentials" in error_msg or "wrong password" in error_msg.lower():
                    return {"ok": False, "error": "invalid_password", "detail": login_data}
                return {"ok": False, "error": f"password_verify_failed_{login_resp.status_code}", "detail": login_data}
            
            # 获取 authorization code
            continue_url = str(login_data.get("continue_url") or "").strip()
            auth_code = ""
            if continue_url:
                from urllib.parse import parse_qs, urlparse
                parsed_params = parse_qs(urlparse(continue_url).query)
                auth_code = str((parsed_params.get("code") or [""])[0]).strip()
            
            # ─── 处理邮箱 OTP 验证 ──────────────────────────
            if not auth_code:
                page_type = ""
                page_info = login_data.get("page")
                if isinstance(page_info, dict):
                    page_type = str(page_info.get("type") or "")
                
                if page_type == "email_otp_verification":
                    # 需要验证码才能登录，直接标记为账号异常
                    return {"ok": False, "error": "need_verification_code", "detail": login_data}
                else:
                    return {"ok": False, "error": "no_auth_code", "detail": login_data}
            
            # ④ 用 code 换 token (使用 Platform Client + code_verifier)
            platform_base = "https://platform.openai.com"
            token_resp = session.post(
                f"{auth_base}/api/accounts/oauth/token",
                headers={
                    "accept": "*/*",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "auth0-client": platform_auth0_client,
                    "cache-control": "no-cache",
                    "content-type": "application/json",
                    "origin": platform_base,
                    "pragma": "no-cache",
                    "priority": "u=1, i",
                    "referer": f"{platform_base}/",
                    "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": user_agent,
                },
                json={
                    "client_id": platform_oauth_client_id,
                    "code_verifier": code_verifier,
                    "grant_type": "authorization_code",
                    "code": auth_code,
                    "redirect_uri": platform_oauth_redirect_uri,
                },
                verify=False,
                timeout=60,
            )
            
            token_data = {}
            try:
                token_data = token_resp.json() if token_resp.text else {}
            except Exception:
                pass
            
            if token_resp.status_code != 200 or not token_data.get("access_token"):
                return {"ok": False, "error": "token_exchange_failed", "detail": token_data}
            
            access_token = str(token_data.get("access_token") or "").strip()
            refresh_token = str(token_data.get("refresh_token") or "").strip()
            id_token = str(token_data.get("id_token") or "").strip()
            
            # ⑤ 用 access_token 获取用户信息
            user_info = {}
            try:
                me_resp = session.get(
                    "https://chatgpt.com/backend-api/me",
                    headers={
                        "accept": "application/json",
                        "authorization": f"Bearer {access_token}",
                        "user-agent": user_agent,
                    },
                    timeout=30,
                )
                if me_resp.status_code == 200:
                    user_info = me_resp.json() if me_resp.text else {}
            except Exception:
                pass
            
            # 解析 JWT payload
            jwt_payload = self._decode_jwt_payload(access_token)
            
            email_from_jwt = str(jwt_payload.get("https://api.openai.com/profile", {}).get("email") or "").strip()
            account_id_from_jwt = str(
                jwt_payload.get("https://api.openai.com/auth", {}).get("chatgpt_account_id") or ""
            ).strip()
            
            account_info = user_info.get("account") if isinstance(user_info.get("account"), dict) else {}
            result = {
                "ok": True,
                "email": email_from_jwt or email,
                "account_id": account_id_from_jwt or account_info.get("account_id", ""),
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "expires_at": jwt_payload.get("exp"),
                "source_type": "password",
            }
            
            return result
        
        finally:
            session.close()

    def list_expiring_access_tokens(self) -> list[str]:
        with self._timed_account_lock("account_list_expiring_access_tokens_lock"):
            accounts = tuple(self._accounts.values())
        # 账号字典以替换方式更新；快照引用稳定，JWT/错误解析不占调度锁。
        return [
            token for account in accounts
            if str(account.get("refresh_token") or "").strip()
            and account.get("status") != "禁用"
            and self._refresh_token_status(account) != "invalid"
            and (token := str(account.get("access_token") or "").strip())
            and self._token_needs_refresh(token)
        ]

    def list_refresh_token_keepalive_tokens(self) -> list[str]:
        now = datetime.now(timezone.utc)
        due_items: list[tuple[datetime, str]] = []
        with self._timed_account_lock("account_list_refresh_token_keepalive_tokens_lock"):
            accounts = tuple(self._accounts.values())
        for account in accounts:
            due_at = self._refresh_token_keepalive_due_at(account, now)
            token = str(account.get("access_token") or "").strip()
            if due_at is not None and token:
                due_items.append((due_at, token))
        due_items.sort(key=lambda item: item[0])
        return [token for _, token in due_items[: self._REFRESH_TOKEN_KEEPALIVE_BATCH_SIZE]]

    def keepalive_refresh_tokens(self, access_tokens: list[str], *, include_items: bool = True,
                                 on_progress: Callable | None = None,
                                 on_failure: Callable | None = None) -> dict[str, Any]:
        access_tokens = list(dict.fromkeys(token for token in access_tokens if token))
        if not access_tokens:
            return {"refreshed": 0, "errors": [], **({"items": self.list_accounts()} if include_items else {})}

        refreshed = 0
        errors = []
        for access_token in access_tokens:
            success = False
            failure_account = self.get_account(access_token) if on_failure else None
            failure_reason = "保活未返回账号信息（账号可能已被移除）"
            if on_progress:
                on_progress("started")
            try:
                before = self.resolve_access_token(access_token)
                after = self.refresh_access_token(before, force=True, event="refresh_token_keepalive")
                account = self.get_account(after)
                if account and str(account.get("last_token_refresh_error") or "").strip():
                    failure_reason = str(account["last_token_refresh_error"])
                    errors.append({
                        "token": anonymize_token(before),
                        "error": str(account.get("last_token_refresh_error") or "refresh token failed"),
                    })
                elif account:
                    refreshed += 1
                    success = True
            except Exception as exc:
                failure_reason = str(exc) or type(exc).__name__
                raise
            finally:
                if not success and on_failure:
                    on_failure(refresh_failure_item(access_token, failure_account, failure_reason))
                if on_progress:
                    on_progress("finished", success)

        return {
            "refreshed": refreshed,
            "errors": errors,
            **({"items": self.list_accounts()} if include_items else {}),
            "relogined": 0,
        }

    def list_tokens(self) -> list[str]:
        with self._timed_account_lock("account_list_tokens_lock"):
            return list(self._accounts)

    def _list_ready_candidate_tokens(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        excluded = set(excluded_tokens or set())
        return [
            token
            for item in self._accounts.values()
            if self._is_image_account_available(item)
               and self._account_matches_plan_type(item, plan_type)
               and self._account_matches_any_plan_type(item, plan_types)
               and self._account_matches_source_type(item, source_type)
               and (token := item.get("access_token") or "")
               and token not in excluded
        ]

    def _list_available_candidate_tokens(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        max_concurrency = max(1, int(config.image_account_concurrency or 1))
        return [
            token
            for token in self._list_ready_candidate_tokens(excluded_tokens, plan_type, source_type, plan_types)
            if int(self._image_inflight.get(token, 0)) < max_concurrency
        ]

    @contextmanager
    def _timed_account_lock(self, name):
        # 不在持账号锁期间更新指标，避免指标锁或线程调度延长共享临界区。
        started = time.perf_counter()
        self._lock.acquire()
        acquired = time.perf_counter()
        cpu_started = time.thread_time()
        try:
            yield
        finally:
            held = time.perf_counter() - acquired
            cpu = time.thread_time() - cpu_started
            self._lock.release()
            metrics = current_metrics.get()
            for suffix, elapsed in (("_wait", acquired - started), ("_hold", held), ("_hold_cpu", cpu)):
                runtime_phases.observe(name + suffix, elapsed)
                if metrics is not None:
                    metrics.add(name + suffix, elapsed)

    def _acquire_next_candidate_token(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
            *, snapshot_out: dict | None = None,
    ) -> str:
        excluded = excluded_tokens or set()
        deadline = time.monotonic() + config.image_account_verification["wait_timeout_secs"]
        while True:
            if time.monotonic() >= deadline:
                raise RuntimeError("image account slot wait timed out")
            concurrency = max(1, int(config.image_account_concurrency or 1))
            with self._timed_account_lock("account_selection_lock"):
                if concurrency != self._scheduler_concurrency:
                    self._scheduler_concurrency = concurrency
                    for token in tuple(self._accounts):
                        self._sync_candidate_locked(token)
                buckets = list(self._matching_buckets(plan_type, source_type, plan_types))
                # 只排序有限数量的类别/热度/评分桶，不扫描或排序全池账号。
                # 每20次给低分/新账号一次机会，其余严格优先热池和高分档。
                explore = self._index % 20 == 19
                ranked = []
                for hot in (1, 0):
                    bands = sorted({bucket[3] for bucket, _ in buckets if bucket[2] == hot}, reverse=not explore)
                    for band in bands:
                        group = [item for item in buckets if item[0][2:] == (hot, band)]
                        offset = self._index % len(group)
                        ranked.extend(group[offset:] + group[:offset])
                for bucket, _ in ranked:
                    available = self._available_buckets[bucket]
                    token = next((t for t in available if t not in excluded), None)
                    if token is None:
                        continue
                    self._index += 1
                    self._image_inflight[token] = self._image_inflight.get(token, 0) + 1
                    self._sync_candidate_locked(token)
                    if snapshot_out is not None:
                        snapshot_out.update(self._accounts[token])
                    return token
                # Condition.wait 会释放锁；不把等待槽位时间算作持锁时间。
                if not any(len(ready) > len(excluded) or any(t not in excluded for t in ready)
                           for _, ready in buckets):
                    raise RuntimeError(
                        f"no available {plan_type or source_type or ''} image quota".replace("  ", " ").strip()
                        if plan_type or source_type else "no available image quota"
                    )
            from services.image_diagnostics import remaining_image_budget
            remaining_image_budget()
            with phase("account_slot_wait"):
                with self._image_slot_condition:
                    self._image_slot_condition.wait_for(
                        lambda: any(t not in excluded for bucket, _ in buckets
                                   for t in self._available_buckets.get(bucket, ())), timeout=1.0)

    def _release_image_slot_locked(self, access_token: str, *, sync_candidate=True) -> str:
        token = self._resolve_access_token_locked(access_token)
        current = self._image_inflight.get(token, 0)
        if current <= 1:
            self._image_inflight.pop(token, None)
        else:
            self._image_inflight[token] = current - 1
        if sync_candidate:
            self._sync_candidate_locked(token)
        # One released slot needs only one waiter, avoiding a thundering herd.
        self._image_slot_condition.notify(1)
        return token

    def release_image_slot(self, access_token: str) -> None:
        if access_token:
            with self._image_slot_condition:
                self._release_image_slot_locked(access_token)

    @blocking
    @measured("account_lookup")
    def get_available_access_token(self, plan_type=None, source_type=None, plan_types=None,
                                   *, budget: ImageAttemptBudget | None = None) -> str:
        from services.image_diagnostics import remaining_image_budget, ImageDeadlineExceeded, ImageRequestCancelled
        budget = budget or ImageAttemptBudget(config.image_account_selection_max_attempts)
        while len(budget.identities) < budget.limit:
            remaining_image_budget()
            snapshot = {}
            access_token = self._acquire_next_candidate_token(
                excluded_tokens=budget.tokens, plan_type=plan_type,
                source_type=source_type, plan_types=plan_types, snapshot_out=snapshot)
            try:
                if not budget.consume(snapshot, access_token):
                    self.release_image_slot(access_token)
                    continue
                account = self._image_account_with_verification(access_token, snapshot=snapshot)
                remaining_image_budget()
            except (ImageDeadlineExceeded, ImageRequestCancelled):
                self.release_image_slot(access_token)
                raise
            except Exception:
                self.release_image_slot(access_token)
                continue
            resolved = str((account or {}).get("access_token") or access_token)
            budget.tokens.add(resolved)
            if (self._is_image_account_available(account or {})
                    and self._account_matches_plan_type(account or {}, plan_type)
                    and self._account_matches_any_plan_type(account or {}, plan_types)
                    and self._account_matches_source_type(account or {}, source_type)):
                try:
                    self.mark_image_started(resolved)
                except BaseException:
                    self.release_image_slot(resolved)
                    raise
                return resolved
            self.release_image_slot(access_token)
        raise RuntimeError(f"no available image quota (tried {len(budget.identities)} accounts; shared attempt budget)")

    @persisted
    def mark_image_started(self, access_token):
        with self._image_slot_condition:
            token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(token)
            if not self._is_image_account_available(current):
                raise RuntimeError("image account became unavailable")
            if self._image_inflight.get(token, 0) > min(
                    self._scheduler_concurrency, int(current.get("quota") or 0)):
                raise RuntimeError("image quota changed while reserving account slot")
            now = time.time()
            entered = not current.get("image_pool_hot")
            account = {**current, "image_pool_hot": True, "last_image_started_at": now,
                       "hot_entered_at": now if entered else current.get("hot_entered_at")}
            if entered:
                self._pool_index.expanded += 1
                self._pool_index.last_reason = "request_expansion"
            self._accounts[token] = account
            self._save_account(account)

    def pool_snapshot(self):
        with self._timed_account_lock("account_pool_snapshot_lock"):
            return self._pool_index.snapshot()

    @staticmethod
    def _pool_schedule_signature():
        settings = config.image_account_verification
        return tuple(settings[key] for key in (
            "hot_idle_secs", "hot_refresh_interval_secs", "cold_refresh_interval_secs"))

    def reconfigure_pool(self):
        # 没变更维护间隔的保存不扫描账号；真正重建分小批持锁，避免挡住生图选号。
        with self._pool_reconfigure_lock:
            signature = self._pool_schedule_signature()
            if signature == self._pool_schedule_settings:
                return
            with self._image_slot_condition:
                tokens = tuple(self._accounts)
            for start in range(0, len(tokens), 128):
                with self._image_slot_condition:
                    for token in tokens[start:start + 128]:
                        self._pool_index.entries.pop(token, None)
                        self._sync_candidate_locked(token)
                    self._image_slot_condition.notify_all()
            self._pool_schedule_settings = signature

    @persisted
    def maintenance_candidates(self, lane, limit):
        now = time.time()
        tokens = []
        with self._image_slot_condition:
            pool = self._pool_index
            base = config.image_account_verification["hot_pool_size"]
            for _ in range(max(1, limit)):
                token = pool.cooldown.pop_due(now)
                if token is None:
                    break
                self._sync_candidate_locked(token)
            # 到期闲置账号逐个缩容，实际生成中的账号延后，不修改使用时间。
            for _ in range(max(1, limit)):
                if len(pool.hot) <= base:
                    break
                token = pool.idle.pop_due(now)
                if token is None:
                    break
                account = self._accounts.get(token)
                if not account:
                    continue
                if self._image_inflight.get(token, 0):
                    pool.idle.set(token, now + 60)
                    continue
                account = {**account, "image_pool_hot": False}
                pool.shrunk += 1
                pool.last_reason = "idle_shrink"
                self._accounts[token] = account
                self._save_account(account)
            if lane == "hot_fill":
                limit = min(limit, max(0, base - len(pool.hot)))
                for _ in range(limit):
                    token = pool.fill.pop_due(float("inf"))
                    if token is None:
                        break
                    tokens.append(token)
            else:
                for _ in range(limit):
                    token = pool.due[lane].pop_due(now)
                    if token is None:
                        break
                    tokens.append(token)
                    # 领取而非删除长期计划；异常/进程退出后仍可重试。
                    pool.reschedule(token, lane, now + config.image_account_verification["wait_timeout_secs"])
        return tokens

    @persisted
    def maintain_account(self, token, lane):
        active = self.resolve_access_token(token)
        current = self.get_account(active)
        if not current or current.get("status") == "禁用":
            return None
        settings = config.image_account_verification
        interval = settings["cache_ttl_secs"] if lane == "hot_fill" else settings[
            "hot_refresh_interval_secs" if current.get("image_pool_hot") else "cold_refresh_interval_secs"]
        # 信息新鲜也不跳过独立凭据预检。
        active = self.refresh_access_token(active, event=lane) or active
        current = self.get_account(active) or {}
        age = time.time() - availability_time(current)
        if not (self._is_image_account_available(current) and 0 <= age < interval):
            current = self._fetch_remote_info(active, lane)
        else:
            current = CachedAccountInfo(current)
        if current and lane == "hot_fill":
            with self._image_slot_condition:
                active = self._resolve_access_token_locked(active)
                latest = self._accounts.get(active)
                if (self._is_image_account_available(latest) and
                        len(self._pool_index.hot) < settings["hot_pool_size"]):
                    account = {**latest, "image_pool_hot": True, "hot_entered_at": time.time()}
                    self._pool_index.last_reason = "base_fill"
                    self._accounts[active] = account
                    self._save_account(account)
        return current

    @persisted
    def maintenance_finished(self, token, lane, *, failed=False):
        with self._image_slot_condition:
            token = self._resolve_access_token_locked(token)
            current = self._accounts.get(token)
            if not current:
                return
            if not failed and self._is_image_account_available(current):
                # 评分每小时向初始50恢复一档，不自动解除认证/额度硬状态。
                last_score_at = timestamp(current.get("image_score_updated_at")) or time.time()
                steps = int(max(0, time.time() - last_score_at) // 3600)
                score = int(current.get("image_score", 50))
                if steps and score != 50:
                    score += min(steps, 50 - score) if score < 50 else -min(steps, score - 50)
                    current = {**current, "image_score": score, "image_score_updated_at": time.time()}
                    self._accounts[token] = current
                    self._save_account(current)
            if not failed and lane == "hot_fill" and not current.get("image_pool_hot") and self._is_image_account_available(current) and (
                    len(self._pool_index.hot) < config.image_account_verification["hot_pool_size"]):
                current = {**current, "image_pool_hot": True, "hot_entered_at": time.time()}
                self._accounts[token] = current
                self._save_account(current)
            if failed:
                current = {**current, "pool_retry_at": time.time() + config.image_account_verification["failure_backoff_secs"]}
                self._accounts[token] = current
                self._save_account(current)
            # 出错时推迟维护，但不伪造成功时间；热/冷身份可能已改变。
            self._pool_index.entries.pop(token, None)
            self._sync_candidate_locked(token)
            actual_lane = "hot_maintenance" if current.get("image_pool_hot") else "cold_maintenance"
            due = max(time.time() + 1, availability_time(current) + config.image_account_verification[
                "hot_refresh_interval_secs" if current.get("image_pool_hot") else "cold_refresh_interval_secs"],
                timestamp(current.get("pool_retry_at")))
            self._pool_index.reschedule(token, actual_lane, due)

    def _image_account_with_verification(self, access_token: str, *, snapshot=None):
        settings = config.image_account_verification
        if snapshot is None:
            with self._timed_account_lock("account_cache_lock"):
                active = self._resolve_access_token_locked(access_token)
                account = self._accounts.get(active)
                if not account or not self._is_image_account_available(account):
                    return dict(account) if account else None
                snapshot = dict(account)
        else:
            active = str(snapshot.get("access_token") or access_token)
        # 凭据检查独立于信息缓存；沿用既有AT/RT续期路径。
        with phase("account_token_inspection"):
            remaining = self._token_expires_in(active)
        refresh_window = max(self._IMAGE_TOKEN_REFRESH_SKEW_SECONDS, config.image_poll_timeout_secs + 60)
        if (snapshot.get("refresh_token") and snapshot.get("refresh_token_status") != "invalid"
                and (remaining is None or remaining <= refresh_window)):
            with phase("account_token_refresh"):
                active = self.refresh_access_token(active, event="image_selection") or active
            snapshot = self.get_account(active) or {}
            remaining = self._token_expires_in(active)
        if remaining is not None and remaining <= 0:
            self.invalidate_verification(active)
            self.update_account(active, {"pool_retry_at": time.time() + settings["failure_backoff_secs"]}, quiet=True)
            raise RuntimeError("account access token expired")
        age = time.time() - availability_time(snapshot)
        if self._is_image_account_available(snapshot) and 0 <= age < settings["cache_ttl_secs"]:
            self.verification.cache_hit()
            return snapshot
        with phase("account_foreground_verification"):
            self.fetch_remote_info(active, "get_available_access_token")
        return self.get_account(active)

    @blocking
    @persisted
    def invalidate_verification(self, access_token: str) -> None:
        with self._timed_account_lock("account_invalidate_verification_lock"):
            active = self._resolve_access_token_locked(access_token)
            self._verified_at.pop(active, None)
            self._verification_versions[active] = self._verification_versions.get(active, 0) + 1
            current = self._accounts.get(active)
            if current:
                account = {**current, "remote_verified_at": None,
                           "verification_invalidated_at": time.time(), "image_pool_hot": False}
                self._accounts[active] = account
                self._save_account(account)

    def get_text_access_token(
            self,
            excluded_tokens: set[str] | None = None,
            model: str = "auto",
    ) -> str:
        excluded = set(excluded_tokens or set())
        requested_model = str(model or "auto").strip() or "auto"
        route = None
        if requested_model != "auto":
            from services.model_service import model_catalog_service

            route = model_catalog_service.route_for_model(requested_model)
        with self._timed_account_lock("account_get_text_access_token_lock"):
            candidates = [
                token
                for account in self._accounts.values()
                if account.get("status") not in {"禁用", "异常"}
                   and (
                       route is None
                       or self._normalize_account_type(account.get("type")) in route.account_types
                   )
                   and (token := account.get("access_token") or "")
                   and token not in excluded
            ]
            if not candidates:
                if route is None or route.allow_anonymous:
                    return ""
                from services.model_service import ModelUnavailableError

                raise ModelUnavailableError(
                    f"model {requested_model!r} is not available to any active account"
                )
            access_token = candidates[self._index % len(candidates)]
            self._index += 1
        return self.refresh_access_token(access_token, event="get_text_access_token") or access_token

    @persisted
    def mark_text_used(self, access_token: str) -> None:
        if not access_token:
            return
        with self._timed_account_lock("account_mark_text_used_lock"):
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            account = self._normalize_account(next_item)
            if account is None:
                return
            self._accounts[access_token] = account
            self._save_account(account)

    @blocking
    def remove_invalid_token(self, access_token: str, event: str, quiet: bool = False) -> bool:
        if not config.auto_remove_invalid_accounts:
            self.update_account(access_token, {"status": "异常", "quota": 0}, quiet=quiet)
            return False
        removed = bool(self.delete_accounts([access_token])["removed"])
        if removed:
            self._log(LOG_TYPE_ACCOUNT, "自动移除异常账号",
                            {"source": event, "token": anonymize_token(access_token)})
        elif access_token:
            self.update_account(access_token, {"status": "异常", "quota": 0}, quiet=quiet)
        return removed

    def get_account(self, access_token: str) -> dict | None:
        if not access_token:
            return None
        with self._timed_account_lock("account_read_lock"):
            access_token = self._resolve_access_token_locked(access_token)
            account = self._accounts.get(access_token)
            return dict(account) if account else None

    def list_accounts(self) -> list[dict]:
        """返回所有账号的副本，并为每个账号附加当前图片在途数 image_inflight。

        image_inflight 为内存态并发计数(账号正在生成、尚未结束的图片数)。号池空闲时
        若某账号该值持续 > 0，说明其并发槽位泄漏、已被静默排除出调度，可借此在 UI 上诊断。
        """
        with self._timed_account_lock("account_list_accounts_lock"):
            items = tuple(self._accounts.values())
            inflight = dict(self._image_inflight)
        return [{**item, "image_inflight": int(inflight.get(item.get("access_token") or "", 0))}
                for item in items]

    def list_accounts_page(
        self, page: int = 1, page_size: int = 20, query: str = "",
        account_type: str = "all", status: str = "all",
        source: str = "all", rt_status: str = "all", pool: str = "all",
        score_min: int | None = None, score_max: int | None = None,
        created_from: float | None = None, created_to: float | None = None,
    ) -> dict[str, Any]:
        """Return a bounded management page.

        Database backends query PostgreSQL/SQL indexes directly. Snapshot
        backends keep the existing in-memory compatibility path.
        """
        page_size = max(1, min(100, int(page_size)))
        filters = dict(source=source, rt_status=rt_status, pool=pool, score_min=score_min,
                       score_max=score_max, created_from=created_from, created_to=created_to)
        native_page = getattr(getattr(self, "storage", None), "list_accounts_page", None)
        if callable(native_page):
            result = native_page(page, page_size, query, account_type, status, **filters)
            if result is not None:
                return {
                    **result,
                    "items": [self._account_page_item(item) for item in result.get("items", [])],
                }

        with self._timed_account_lock("account_list_accounts_page_lock"):
            view = self._account_view_locked() if hasattr(self, "_account_view_locked") else None
            if view is None:
                snapshot = tuple(self._accounts.values())
                view = {"snapshot": snapshot, "buckets": {}, "summary": None, "types": ()}
        query = query.strip().lower()
        summary = view["summary"]
        if summary is None:
            snapshot = view["snapshot"]
            summary = dict(total=len(snapshot), active=0, limited=0, abnormal=0, disabled=0, quota=0)
            status_keys = {"正常": "active", "限流": "limited", "异常": "abnormal", "禁用": "disabled"}
            for item in snapshot:
                if key := status_keys.get(item.get("status")):
                    summary[key] += 1
                if item.get("status") == "正常":
                    summary["quota"] += max(0, int(item.get("quota") or 0))
        # Keep the fast cached-bucket path for unfiltered snapshot pages.
        candidates = view["snapshot"]
        buckets = view["buckets"]
        bucket_selected = bool(buckets and account_type != "all" and status != "all")
        if bucket_selected:
            candidates = buckets.get((account_type, status), ())
        extra_filters = (source != "all" or rt_status != "all" or pool != "all"
                         or any(value is not None for value in (score_min, score_max, created_from, created_to)))
        if not query and not extra_filters and (bucket_selected or (account_type == "all" and status == "all")):
            matched = candidates
        else:
            matched = [item for item in candidates
                       if (not query or query in str(item.get("email") or "").lower())
                       and (account_type == "all" or str(item.get("type") or "Free") == account_type)
                       and (status == "all" or item.get("status") == status)
                       and (not extra_filters or matches_account_filters(item, **filters))]
        total = len(matched)
        page = min(max(1, page), max(1, (total + page_size - 1) // page_size))
        start = (page - 1) * page_size
        with self._timed_account_lock("account_list_accounts_page_lock"):
            items = [self._account_page_item(item) for item in matched[start:start + page_size]]
        page_summary = {
            key: summary[key]
            for key in ("total", "active", "limited", "abnormal", "disabled", "quota")
        }
        return dict(items=items, total=total, page=page, page_size=page_size,
                    summary=page_summary, types=list(view["types"]),
                    sources=list(view.get("sources", ())))

    def _account_page_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Keep page responses bounded and free of account credentials."""
        fields = (
            "access_token", "email", "type", "status", "quota", "success", "fail", "proxy",
            "source_type", "restore_at", "created_at", "updated_at", "refresh_token_status",
            "image_pool_hot", "image_score", "remote_verified_at", "last_image_success_at",
            "last_policy_confirmed_at", "last_image_started_at", "image_score_updated_at",
            "last_token_refresh_at", "last_token_refresh_error", "last_token_refresh_error_at",
        )
        account = {key: item[key] for key in fields if key in item}
        account["has_refresh_token"] = bool(item.get("refresh_token") or item.get("has_refresh_token"))
        account["image_inflight"] = int(self._image_inflight.get(item.get("access_token"), 0))
        return account

    def list_account_tokens(self, status: str = "all") -> list[str]:
        """显式批量操作才读取全池 token，普通列表加载不调用。"""
        with self._timed_account_lock("account_list_account_tokens_lock"):
            return [token for token, item in self._accounts.items()
                    if status == "all" or item.get("status") == status]

    def list_limited_tokens(self) -> list[str]:
        with self._timed_account_lock("account_list_limited_tokens_lock"):
            accounts = tuple(self._accounts.values())
        return [
                token
                for item in accounts
                if item.get("status") == "限流"
                   and (token := item.get("access_token") or "")
            ]

    def list_normal_tokens(self) -> list[str]:
        with self._timed_account_lock("account_list_normal_tokens_lock"):
            accounts = tuple(self._accounts.values())
        return [
                token
                for item in accounts
                if item.get("status") == "正常"
                   and (token := item.get("access_token") or "")
            ]

    @staticmethod
    def _account_payload_token(item: dict) -> str:
        return str(item.get("access_token") or item.get("accessToken") or "").strip()

    @staticmethod
    def _prepare_account_payload(item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None
        access_token = AccountService._account_payload_token(item)
        if not access_token:
            return None
        payload = dict(item)
        payload.pop("accessToken", None)
        payload["access_token"] = access_token
        # CPA/Codex 导出文件里的 `type=codex` 是导出格式，不是号池套餐类型。
        if str(payload.get("type") or "").strip().lower() == "codex":
            payload["export_type"] = "codex"
            payload["source_type"] = "codex"
            payload.pop("type", None)
        if str(payload.get("export_type") or "").strip().lower() == "codex":
            payload["source_type"] = "codex"
        if payload.get("plan_type") and not payload.get("type"):
            payload["type"] = str(payload.get("plan_type") or "").strip()
        return payload

    def add_account_items(self, items: list[dict], *, include_items: bool = True) -> dict:
        payloads = [
            payload
            for item in items
            if (payload := self._prepare_account_payload(item)) is not None
        ]
        return self._add_account_payloads(payloads, include_items=include_items)

    def add_accounts(self, tokens: list[str], source_type: str = "web", *, include_items: bool = True) -> dict:
        tokens = list(dict.fromkeys(token for token in tokens if token))
        if not tokens:
            return {"added": 0, "skipped": 0, **({"items": self.list_accounts()} if include_items else {})}
        return self._add_account_payloads([
            {"access_token": token, "source_type": self._normalize_source_type(source_type)}
            for token in tokens
        ], include_items=include_items)

    @persisted
    def _add_account_payloads(self, payloads: list[dict], *, include_items: bool = True) -> dict:
        deduped: dict[str, dict] = {}
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            access_token = self._account_payload_token(payload)
            if not access_token:
                continue
            current = deduped.get(access_token, {})
            deduped[access_token] = {**current, **payload, "access_token": access_token}

        if not deduped:
            return {"added": 0, "skipped": 0, **({"items": self.list_accounts()} if include_items else {})}

        with self._timed_account_lock("account_add_account_payloads_lock"):
            added = 0
            skipped = 0
            for access_token, payload in deduped.items():
                current = self._accounts.get(access_token)
                if current is None:
                    added += 1
                    self._cumulative_total += 1
                    current = {"created_at": self._now()}
                else:
                    skipped += 1
                incoming = dict(payload)
                if not incoming.get("created_at"):
                    incoming.pop("created_at", None)
                if current is not None and "refresh_token" in incoming:
                    current_refresh_token = str(current.get("refresh_token") or "").strip()
                    incoming_refresh_token = str(incoming.get("refresh_token") or "").strip()
                    if current_refresh_token != incoming_refresh_token:
                        current = dict(current)
                        current["last_token_refresh_at"] = None
                        current["last_token_refresh_error"] = None
                        current["last_token_refresh_error_at"] = None
                account = self._normalize_account(
                    {
                        **current,
                        **incoming,
                        "access_token": access_token,
                        "type": str(incoming.get("type") or current.get("type") or "free"),
                    }
                )
                if account is not None:
                    self._accounts[access_token] = account
            self._write_context.after.append(self._save_cumulative_total)
            for token in deduped:
                self._sync_candidate_locked(token)
            if getattr(self.storage, "supports_incremental_account_writes", False):
                self._queue_write({token: dict(self._accounts[token]) for token in deduped if token in self._accounts})
            else:
                self._save_accounts()
            self._image_slot_condition.notify_all()
            items = [dict(item) for item in self._accounts.values()] if include_items else []
            self._log(LOG_TYPE_ACCOUNT, f"新增 {added} 个账号，跳过 {skipped} 个",
                            {"added": added, "skipped": skipped})
        return {"added": added, "skipped": skipped, **({"items": items} if include_items else {})}

    @persisted
    def delete_accounts(self, tokens: list[str], *, include_items: bool = True) -> dict:
        target_set = set(token for token in tokens if token)
        if not target_set:
            return {"removed": 0, **({"items": self.list_accounts()} if include_items else {})}
        with self._timed_account_lock("account_delete_accounts_lock"):
            target_set = {self._resolve_access_token_locked(token) for token in target_set if token}
            removed = sum(self._accounts.pop(token, None) is not None for token in target_set)
            for token in target_set:
                self._image_inflight.pop(token, None)
            self._token_aliases = {
                old: new
                for old, new in self._token_aliases.items()
                if old not in target_set and new not in target_set
            }
            if removed:
                if self._accounts:
                    self._index %= len(self._accounts)
                else:
                    self._index = 0
                for token in target_set:
                    previous = self._pool_index.entries.get(token)
                    self.verification.forget(previous[3] if previous else token)
                    self._sync_candidate_locked(token)
                    self._verified_at.pop(token, None)
                if getattr(self.storage, "supports_incremental_account_writes", False):
                    self._queue_write({token: None for token in target_set})
                else:
                    self._save_accounts()
                self._image_slot_condition.notify_all()
                self._log(LOG_TYPE_ACCOUNT, f"删除 {removed} 个账号", {"removed": removed})
            items = [dict(item) for item in self._accounts.values()] if include_items else []
        return {"removed": removed, **({"items": items} if include_items else {})}

    @persisted
    def update_account(self, access_token: str, updates: dict, quiet: bool = False, *, remote_success_before: int | None = None, remote_verification_version: int | None = None) -> dict | None:
        if not access_token:
            return None
        with self._timed_account_lock("account_update_account_lock"):
            resolved = self._resolve_access_token_locked(access_token)
            if remote_verification_version is not None and resolved != access_token:
                raise RuntimeError("account token rotated while verification was in progress")
            access_token = resolved
            current = self._accounts.get(access_token)
            if current is None:
                return None
            if remote_verification_version is not None and self._verification_versions.get(access_token, 0) != remote_verification_version:
                raise RuntimeError("account verification was invalidated while in progress")
            if remote_success_before is not None:
                # 后台校验期间可能有图片完成；保守扣除这段时间的本地成功量。
                consumed = max(0, int(current.get("success") or 0) - remote_success_before)
                updates = dict(updates)
                updates["quota"] = max(0, int(updates.get("quota") or 0) - consumed)
                if current.get("status") == "禁用":
                    updates["status"] = "禁用"
                elif updates["quota"] == 0:
                    updates["status"] = "限流"
            account = self._normalize_account({**current, **updates, "access_token": access_token})
            if account is None:
                return None
            if account.get("status") == "限流" and config.auto_remove_rate_limited_accounts:
                self._accounts.pop(access_token, None)
                self._delete_account(access_token)
                self._log(LOG_TYPE_ACCOUNT, "自动移除限流账号", {"token": anonymize_token(access_token)})
                return None
            self._accounts[access_token] = account
            if remote_verification_version is not None:
                self._verified_at[access_token] = time.monotonic()
            self._save_account(account)
            if not quiet:
                self._log(LOG_TYPE_ACCOUNT, "更新账号",
                                {"token": anonymize_token(access_token), "status": account.get("status")})
            return dict(account)
        return None

    def _record_refresh_success(self, access_token: str) -> None:
        with self._timed_account_lock("account_record_refresh_success_lock"):
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return
            next_item = dict(current)
            next_item["invalid_count"] = 0
            next_item["last_invalid_at"] = None
            next_item["last_refresh_error"] = None
            next_item["last_refresh_error_at"] = None
            account = self._normalize_account(next_item)
            if account is not None:
                self._accounts[access_token] = account

    def _should_defer_invalid_token(self, account: dict | None, now: datetime) -> bool:
        if not isinstance(account, dict):
            return False
        created_at = self._parse_time(account.get("created_at"))
        if created_at is not None and (now - created_at).total_seconds() < self._NEW_ACCOUNT_INVALID_GRACE_SECONDS:
            return True
        last_invalid_at = self._parse_time(account.get("last_invalid_at"))
        invalid_count = int(account.get("invalid_count") or 0)
        if invalid_count <= 1:
            return True
        if last_invalid_at is not None and (now - last_invalid_at).total_seconds() < self._INVALID_CONFIRM_SECONDS:
            return True
        return False

    @persisted
    def _record_invalid_token_seen(
        self,
        access_token: str,
        event: str,
        error: str,
        defer_invalid_removal: bool = True,
    ) -> bool:
        now = datetime.now(timezone.utc)
        with self._timed_account_lock("account_record_invalid_token_seen_lock"):
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return True
            should_defer = defer_invalid_removal and self._should_defer_invalid_token(current, now)
            next_item = dict(current)
            next_item["invalid_count"] = int(next_item.get("invalid_count") or 0) + 1
            self._verified_at.pop(access_token, None)
            self._verification_versions[access_token] = self._verification_versions.get(access_token, 0) + 1
            next_item["remote_verified_at"] = None
            next_item["verification_invalidated_at"] = time.time()
            next_item["image_pool_hot"] = False
            next_item["last_invalid_at"] = now.isoformat()
            next_item["last_refresh_error"] = str(error or "invalid access token")
            next_item["last_refresh_error_at"] = now.isoformat()
            account = self._normalize_account(next_item)
            if account is not None:
                self._accounts[access_token] = account
                self._save_account(account)
            if should_defer:
                self._log(
                    LOG_TYPE_ACCOUNT,
                    "暂缓标记异常账号",
                    {"source": event, "token": anonymize_token(access_token), "error": str(error or "")},
                )
                return False
        return True

    @blocking
    @persisted
    def mark_image_result(self, access_token: str, success: bool, *, outcome="neutral", attempt_started_at=None) -> dict | None:
        with self._image_slot_condition:
            access_token = self._release_image_slot_locked(access_token, sync_candidate=False)
            current = self._accounts.get(access_token)
            if current is None:
                return None
            now = time.time()
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if success or outcome in {"account_error", "no_image_generated"}:
                next_item["image_score_updated_at"] = now
            # 失效前启动的旧尝试不能在稍后完成时复活认证状态。
            can_confirm = (attempt_started_at is None or
                           attempt_started_at > timestamp(current.get("verification_invalidated_at")))
            if success:
                next_item["success"] = int(next_item.get("success") or 0) + 1
                next_item["quota"] = max(0, int(next_item.get("quota") or 0) - 1)
                next_item["image_score"] = min(100, int(next_item.get("image_score", 50)) + 1)
                next_item["image_soft_failures"] = 0
                if can_confirm:
                    next_item["last_image_success_at"] = now
                if next_item["quota"] == 0:
                    next_item["status"] = "限流"
                    next_item["restore_at"] = next_item.get("restore_at") or None
            elif outcome == "policy":
                if can_confirm:
                    next_item["last_policy_confirmed_at"] = now
            else:
                next_item["fail"] = int(next_item.get("fail") or 0) + 1
                if outcome in {"account_error", "no_image_generated"}:
                    # 上游正常返回但不出图同样降低选号优先级，不标记认证失效。
                    next_item["image_score"] = max(0, int(next_item.get("image_score", 50)) - 5)
                elif outcome == "upstream_timeout":
                    failures = int(next_item.get("image_soft_failures") or 0) + 1
                    next_item["image_soft_failures"] = failures
                    if failures >= 3:
                        next_item["pool_retry_at"] = now + min(300, 30 * (failures - 2))
            account = self._normalize_account(next_item)
            if account.get("status") == "限流" and config.auto_remove_rate_limited_accounts:
                self._accounts.pop(access_token, None)
                self._delete_account(access_token)
                return None
            self._accounts[access_token] = account
            self._save_account(account)
            return dict(account)

    def fetch_remote_info(
        self,
        access_token: str,
        event: str = "fetch_remote_info",
        defer_invalid_removal: bool = True,
        *, lane: str = "foreground",
    ) -> dict[str, Any] | None:
        if not access_token:
            raise ValueError("access_token is required")

        active = self.resolve_access_token(access_token)
        account = self.get_account(active) or {}
        key = str(account.get("pool_account_id") or active)
        for _ in range(3):
            result = self.verification.run(key, lambda: self._fetch_remote_info(
                self.resolve_access_token(active), event, defer_invalid_removal), lane=lane)
            if not isinstance(result, CachedAccountInfo):
                return result
            # 维护通道使用较长间隔，跳过查询的flight不能冒充手动/前台所需的真实刷新。
            # 上一个handler已结束，重试仍由同一稳定身份去重；重复碰撞有界退出。
        raise VerificationBackoff("joined maintenance repeatedly skipped required account query")

    def _fetch_remote_info(self, access_token: str, event: str, defer_invalid_removal: bool = True):
        active_token = self.refresh_access_token(access_token, event=f"{event}:preflight") or access_token

        def baseline(token):
            with self._timed_account_lock("account_fetch_remote_info_lock"):
                account = self._accounts.get(token) or {}
                return int(account.get("success") or 0), self._verification_versions.get(token, 0)

        success_before, verification_version = baseline(active_token)
        try:
            from services.openai_backend_api import InvalidAccessTokenError, OpenAIBackendAPI
            backend = OpenAIBackendAPI(active_token)
            try:
                result = backend.get_user_info()
            finally:
                backend.close()
        except InvalidAccessTokenError as exc:
            refreshed_token = self.refresh_access_token(active_token, force=True, event=f"{event}:invalid_access_token")
            if refreshed_token and refreshed_token != active_token:
                success_before, verification_version = baseline(refreshed_token)
                try:
                    backend = OpenAIBackendAPI(refreshed_token)
                    try:
                        result = backend.get_user_info()
                    finally:
                        backend.close()
                except InvalidAccessTokenError as retry_exc:
                    if self._record_invalid_token_seen(
                        refreshed_token,
                        event,
                        str(retry_exc),
                        defer_invalid_removal=defer_invalid_removal,
                    ):
                        self.remove_invalid_token(refreshed_token, event)
                    raise
                active_token = refreshed_token
            else:
                if self._record_invalid_token_seen(
                    active_token,
                    event,
                    str(exc),
                    defer_invalid_removal=defer_invalid_removal,
                ):
                    self.remove_invalid_token(active_token, event)
                raise
        result = {**result, "remote_verified_at": time.time(), "invalid_count": 0,
                  "last_invalid_at": None, "last_refresh_error": None, "last_refresh_error_at": None, "pool_retry_at": 0}
        account = self.update_account(active_token, result, quiet=True, remote_success_before=success_before,
                                      remote_verification_version=verification_version)
        return account

    # ---- 刷新进度追踪 ----

    def init_refresh_progress(self, progress_id: str, total: int, *, phase: str = "refreshing") -> None:
        """初始化刷新进度记录。"""
        with self._refresh_progress_lock:
            self._refresh_progress[progress_id] = {
                "total": total,
                "processed": 0,
                "done": False,
                "error": None,
                "status_counts": {"正常": 0, "限流": 0, "异常": 0, "禁用": 0},
                "total_quota": 0,
                "phase": phase,
            }

    def update_refresh_progress(self, progress_id: str, token: str) -> None:
        """刷新单个账号后，更新进度计数。"""
        account = self.get_account(token)
        status = str(account.get("status") or "正常").strip() if account else "正常"
        quota = max(0, int(account.get("quota") or 0)) if account else 0

        with self._refresh_progress_lock:
            progress = self._refresh_progress.get(progress_id)
            if progress is None:
                return
            progress["processed"] += 1
            progress["status_counts"][status] = progress["status_counts"].get(status, 0) + 1
            progress["total_quota"] += quota

    def finish_refresh_progress(self, progress_id: str, result: dict | None = None, error: str | None = None) -> None:
        """标记刷新完成。"""
        with self._refresh_progress_lock:
            progress = self._refresh_progress.get(progress_id)
            if progress is None:
                return
            if result is not None and result.get("refreshed") == 0 and not error:
                progress["processed"] = progress["total"]
            progress["done"] = True
            progress["result"] = result
            if error:
                progress["error"] = error

    def get_refresh_progress(self, progress_id: str) -> dict | None:
        """查询刷新进度。"""
        with self._refresh_progress_lock:
            progress = self._refresh_progress.get(progress_id)
            return dict(progress) if progress else None

    def clean_refresh_progress(self, progress_id: str) -> None:
        """清理过期进度记录。"""
        with self._refresh_progress_lock:
            self._refresh_progress.pop(progress_id, None)

    # ---- 重新登录进度追踪 ----

    def init_relogin_progress(self, progress_id: str, total: int) -> None:
        """初始化重新登录进度记录。"""
        with self._relogin_progress_lock:
            self._relogin_progress[progress_id] = {
                "total": total,
                "processed": 0,
                "done": False,
                "error": None,
                "results": [],
            }

    def update_relogin_progress(self, progress_id: str, token: str, status: str, error: str | None = None) -> None:
        """更新单个重新登录进度。当所有账号处理完毕时自动标记完成。"""
        with self._relogin_progress_lock:
            progress = self._relogin_progress.get(progress_id)
            if progress is None:
                return
            progress["processed"] += 1
            progress["results"].append({
                "token": anonymize_token(token),
                "status": status,
                "error": error,
            })
            if progress["processed"] >= progress["total"]:
                progress["done"] = True

    def finish_relogin_progress(self, progress_id: str, result: dict | None = None, error: str | None = None) -> None:
        """标记重新登录完成。"""
        with self._relogin_progress_lock:
            progress = self._relogin_progress.get(progress_id)
            if progress is None:
                return
            progress["done"] = True
            progress["result"] = result
            if error:
                progress["error"] = error

    def get_relogin_progress(self, progress_id: str) -> dict | None:
        """查询重新登录进度。"""
        with self._relogin_progress_lock:
            progress = self._relogin_progress.get(progress_id)
            return dict(progress) if progress else None

    def clean_relogin_progress(self, progress_id: str) -> None:
        """清理过期进度记录。"""
        with self._relogin_progress_lock:
            self._relogin_progress.pop(progress_id, None)

    def refresh_accounts(
        self,
        access_tokens: list[str],
        progress_id: str | None = None,
        defer_invalid_removal: bool = True,
        result_metadata: dict[str, Any] | None = None,
        include_items: bool = True,
        on_progress: Callable | None = None,
        on_failure: Callable | None = None,
    ) -> dict[str, Any]:
        with self._timed_account_lock("account_manual_refresh_plan_lock"):
            unique = {}
            for token in access_tokens:
                if not token:
                    continue
                active = self._resolve_access_token_locked(token)
                account = self._accounts.get(active) or {}
                unique.setdefault(str(account.get("pool_account_id") or active), active)
            access_tokens = list(unique.values())
        if not access_tokens:
            items = self.list_accounts() if include_items else []
            result = {**(result_metadata or {}), "refreshed": 0, "errors": [], **({"items": items} if include_items else {}), "relogined": 0}
            if progress_id:
                self.finish_refresh_progress(progress_id, result)
            return result

        refreshed = 0
        errors = []
        if progress_id:
            self.init_refresh_progress(progress_id, len(access_tokens))
        # 一个滑动窗口，不为整个账号列表创建Future；所有手动批次共享manual通道。
        remaining = iter(access_tokens)
        pending = {}
        exhausted = False
        while not exhausted or pending:
            while not exhausted and len(pending) < config.image_account_verification["workers"]:
                try:
                    token = next(remaining)
                except StopIteration:
                    exhausted = True
                    break
                snapshot = self.get_account(token) or {}
                key = str(snapshot.get("pool_account_id") or token)
                try:
                    future = self.verification.submit(key, lambda token=token: self._fetch_remote_info(
                        self.resolve_access_token(token), "refresh_accounts", defer_invalid_removal), lane="manual")
                    if future is None:
                        # 其他手动批次已占满窗口；等待有界，不无限积压。
                        future = Future()
                        try:
                            value = self.fetch_remote_info(token, "refresh_accounts", defer_invalid_removal, lane="manual")
                            future.set_result(value)
                        except Exception as exc:
                            future.set_exception(exc)
                except Exception as exc:
                    future = Future()
                    future.set_exception(exc)
                pending[future] = (token, snapshot, time.monotonic() + config.image_account_verification["wait_timeout_secs"])
                if on_progress:
                    on_progress("started")
            if not pending:
                continue
            nearest = min(row[2] for row in pending.values())
            done, _ = wait(pending, timeout=max(0, nearest - time.monotonic()), return_when=FIRST_COMPLETED)
            timed_out = {future for future, row in pending.items() if row[2] <= time.monotonic() and future not in done}
            for future in done | timed_out:
                token, snapshot, _ = pending.pop(future)
                success = False
                reason = "刷新未返回账号信息（账号可能已被移除）"
                try:
                    if future in timed_out:
                        raise VerificationBackoff("manual account refresh wait timed out; existing verification continues")
                    account = future.result()
                    if isinstance(account, CachedAccountInfo):
                        account = self.fetch_remote_info(token, "refresh_accounts:confirm", defer_invalid_removal, lane="manual")
                    success = account is not None
                    refreshed += int(success)
                except Exception as exc:
                    reason = str(exc) or type(exc).__name__
                    from services.protocol.conversation import is_tls_connection_error
                    if not is_tls_connection_error(reason):
                        errors.append({"token": anonymize_token(token), "error": reason})
                if not success and on_failure:
                    on_failure(refresh_failure_item(token, snapshot, reason))
                if on_progress:
                    on_progress("finished", success)
                if progress_id:
                    self.update_refresh_progress(progress_id, token)

        # 自动重新登录异常账号（仅当配置开启时）
        relogined = 0
        if config.auto_relogin_after_refresh:
            for token in access_tokens:
                account = self.get_account(token)
                if not account:
                    continue
                status = str(account.get("status") or "").strip()
                if status != "异常":
                    continue
                email = str(account.get("email") or "").strip()
                password = str(account.get("password") or "").strip()
                if not email or not password:
                    continue
                t = Thread(
                    target=self._password_re_login_thread,
                    args=(token, email, password, "auto_relogin_after_refresh"),
                    daemon=True,
                )
                t.start()
                relogined += 1

        result = {
            **(result_metadata or {}),
            "refreshed": refreshed,
            "errors": errors,
            **({"items": self.list_accounts()} if include_items else {}),
            "relogined": relogined,
        }

        if progress_id:
            self.finish_refresh_progress(progress_id, result)

        return result

    def re_login_accounts(self, access_tokens: list[str], progress_id: str | None = None, include_items: bool = True) -> dict[str, Any]:
        """对选中账号执行密码重新登录流程。

        仅对包含 email + password 的账号有效。
        登录成功后自动将状态设为"正常"。
        """
        access_tokens = list(dict.fromkeys(token for token in access_tokens if token))
        if not access_tokens:
            result = {"relogined": 0, "skipped": 0, "errors": [], **({"items": self.list_accounts()} if include_items else {})}
            if progress_id:
                self.finish_relogin_progress(progress_id, result)
            return result

        if progress_id:
            self.init_relogin_progress(progress_id, len(access_tokens))

        relogined = 0
        skipped = 0
        errors = []

        for token in access_tokens:
            account = self.get_account(token)
            if not account:
                errors.append({"token": anonymize_token(token), "error": "账号不存在"})
                if progress_id:
                    self.update_relogin_progress(progress_id, token, "跳过", "账号不存在")
                continue

            email = str(account.get("email") or "").strip()
            password = str(account.get("password") or "").strip()
            if not email or not password:
                skipped += 1
                if progress_id:
                    self.update_relogin_progress(progress_id, token, "跳过", "无邮箱密码")
                continue

            # 在新线程中执行密码重新登录
            t = Thread(
                target=self._password_re_login_thread,
                args=(token, email, password, "manual_relogin", progress_id),
                daemon=True,
            )
            t.start()
            relogined += 1

        result = {
            "relogined": relogined,
            "skipped": skipped,
            "errors": errors,
            **({"items": self.list_accounts()} if include_items else {}),
        }
        if progress_id:
            # 如果所有账号都已同步处理完毕（没有启动线程），直接标记完成
            if relogined == 0:
                self.finish_relogin_progress(progress_id, result)
            else:
                # 有线程在运行，等线程结束后再完成
                pass
        return result

    def build_export_items(self, access_tokens: list[str] | None = None) -> list[dict[str, str]]:
        target_tokens = set(token for token in (access_tokens or []) if token)
        with self._timed_account_lock("account_build_export_items_lock"):
            accounts = [
                dict(item)
                for item in self._accounts.values()
                if not target_tokens or str(item.get("access_token") or "") in target_tokens
            ]

        items: list[dict[str, str]] = []
        for account in accounts:
            access_token = str(account.get("access_token") or "").strip()
            refresh_token = str(account.get("refresh_token") or "").strip()
            id_token = str(account.get("id_token") or "").strip()
            if not access_token or not refresh_token or not id_token:
                continue

            access_payload = self._decode_jwt_payload(access_token)
            id_payload = self._decode_jwt_payload(id_token)
            auth_claim = access_payload.get("https://api.openai.com/auth")
            auth_claim = auth_claim if isinstance(auth_claim, dict) else {}
            profile_claim = access_payload.get("https://api.openai.com/profile")
            profile_claim = profile_claim if isinstance(profile_claim, dict) else {}

            email = (
                str(account.get("email") or "").strip()
                or str(profile_claim.get("email") or "").strip()
                or str(id_payload.get("email") or "").strip()
            )
            account_id = (
                str(account.get("account_id") or "").strip()
                or str(auth_claim.get("chatgpt_account_id") or "").strip()
                or str(account.get("user_id") or "").strip()
            )
            item = {
                "type": str(account.get("export_type") or "codex"),
                "email": email,
                "account_id": account_id,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "expired": self._timestamp_to_iso(access_payload.get("exp")),
                "last_refresh": self._timestamp_to_iso(access_payload.get("iat")),
            }
            password = str(account.get("password") or "").strip()
            if password:
                item["password"] = password
            items.append(item)
        return items

    def get_stats(self) -> dict:
        with self._timed_account_lock("account_get_stats_lock"):
            if hasattr(self, "_stats_summary"):
                summary = dict(self._stats_summary)
                by_type = dict(self._stats_by_type)
            else:
                # 兼容未调用构造器的旧测试替身。
                items = tuple(self._accounts.values())
                summary = dict(total=len(items), active=0, limited=0, abnormal=0, disabled=0,
                               quota=0, total_success=0, total_fail=0)
                by_type = {}
                for item in items:
                    for name, status in (("active", "正常"), ("limited", "限流"), ("abnormal", "异常"), ("disabled", "禁用")):
                        summary[name] += int(item.get("status") == status)
                    summary["quota"] += max(0, int(item.get("quota") or 0)) if item.get("status") == "正常" else 0
                    summary["total_success"] += int(item.get("success") or 0)
                    summary["total_fail"] += int(item.get("fail") or 0)
                    kind = item.get("type", "unknown")
                    by_type[kind] = by_type.get(kind, 0) + 1
        return {
            "total": summary["total"],
            "cumulative_total": self._cumulative_total,
            "active": summary["active"],
            "limited": summary["limited"],
            "abnormal": summary["abnormal"],
            "disabled": summary["disabled"],
            "total_quota": summary["quota"],
            "total_success": summary["total_success"],
            "total_fail": summary["total_fail"],
            "by_type": by_type,
        }

    def account_health(self) -> dict:
        stats = self.get_stats()
        return {
            "healthy": stats["active"] > 0,
            "status": "ok" if stats["active"] > 0 else "degraded",
            **stats,
        }


account_service = AccountService(config.get_storage_backend())
