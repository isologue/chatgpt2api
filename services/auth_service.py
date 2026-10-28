from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from datetime import datetime, timezone
from threading import Event, Lock, Thread
import time
from typing import Literal

from services.config import config
from services.storage.base import StorageBackend

AuthRole = Literal["admin", "user"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class AuthService:
    def __init__(self, storage: StorageBackend):
        self.storage = storage
        self._lock = Lock()
        self._items = self._load()
        # 授权索引只在管理变更后整体替换；鉴权不等待存储 I/O 或管理锁。
        self._usage_lock = Lock()
        self._usage: dict[str, str] = {}
        self._usage_dirty = False
        self._usage_event = Event()
        self._usage_thread = None
        self._usage_closed = False
        self._usage_flush_interval = 60.0
        self._usage_flush_at = 0.0
        self._publish_index()

    @staticmethod
    def _clean(value: object) -> str:
        return str(value or "").strip()

    @staticmethod
    def _default_name(role: object) -> str:
        return "管理员密钥" if str(role or "").strip().lower() == "admin" else "普通用户"

    def _normalize_item(self, raw: object) -> dict[str, object] | None:
        if not isinstance(raw, dict):
            return None
        role = self._clean(raw.get("role")).lower()
        if role not in {"admin", "user"}:
            return None
        key_hash = self._clean(raw.get("key_hash"))
        if not key_hash:
            return None
        item_id = self._clean(raw.get("id")) or uuid.uuid4().hex[:12]
        name = self._clean(raw.get("name")) or self._default_name(role)
        created_at = self._clean(raw.get("created_at")) or _now_iso()
        last_used_at = self._clean(raw.get("last_used_at")) or None
        return {
            "id": item_id,
            "name": name,
            "role": role,
            "key_hash": key_hash,
            "enabled": bool(raw.get("enabled", True)),
            "created_at": created_at,
            "last_used_at": last_used_at,
        }

    def _load(self) -> list[dict[str, object]]:
        try:
            items = self.storage.load_auth_keys()
        except Exception:
            return []
        if not isinstance(items, list):
            return []
        return [normalized for item in items if (normalized := self._normalize_item(item)) is not None]

    def _publish_index(self) -> None:
        self._auth_index = {str(item["key_hash"]): dict(item) for item in self._items if item.get("enabled")}
        live_ids = {str(item["id"]) for item in self._items}
        with self._usage_lock:
            self._usage = {key: value for key, value in self._usage.items() if key in live_ids}

    def _merge_usage(self) -> None:
        with self._usage_lock:
            usage = dict(self._usage)
        self._items = [{**item, "last_used_at": max(str(item.get("last_used_at") or ""), usage.get(str(item["id"]), "")) or None}
                       for item in self._items]

    def _save(self) -> None:
        self._merge_usage()
        self.storage.save_auth_keys(self._items)
        self._publish_index()

    def _reload_locked(self) -> None:
        self._items = self._load()
        self._merge_usage()
        self._publish_index()

    def _flush_usage(self) -> None:
        # 与增删改共用管理锁，旧统计快照绝不能覆盖密钥禁用/删除/轮换。
        with self._lock:
            with self._usage_lock:
                if not self._usage_dirty:
                    return
                self._usage_dirty = False
            try:
                self._save()
            except Exception:
                with self._usage_lock:
                    self._usage_dirty = True
            finally:
                self._usage_flush_at = time.monotonic()

    def _usage_worker(self) -> None:
        while True:
            self._usage_event.wait()
            self._usage_event.clear()
            while True:
                with self._usage_lock:
                    closed = self._usage_closed
                delay = self._usage_flush_interval - (time.monotonic() - self._usage_flush_at)
                if closed or delay <= 0:
                    break
                self._usage_event.wait(delay)
                self._usage_event.clear()
            self._flush_usage()
            with self._usage_lock:
                if self._usage_closed:
                    return
                if self._usage_dirty:
                    self._usage_event.set()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def start(self) -> None:
        # 支持应用 lifespan 在同一进程内重新进入。
        with self._usage_lock:
            if self._usage_closed:
                self._usage_closed = False
                self._usage_thread = None

    def close(self) -> None:
        with self._usage_lock:
            self._usage_closed = True
            thread = self._usage_thread
        self._usage_event.set()
        if thread is not None:
            thread.join()

    @staticmethod
    def _public_item(item: dict[str, object]) -> dict[str, object]:
        return {
            "id": item.get("id"),
            "name": item.get("name"),
            "role": item.get("role"),
            "enabled": bool(item.get("enabled", True)),
            "created_at": item.get("created_at"),
            "last_used_at": item.get("last_used_at"),
        }

    def list_keys(self, role: AuthRole | None = None) -> list[dict[str, object]]:
        with self._lock:
            self._reload_locked()
            items = [item for item in self._items if role is None or item.get("role") == role]
            return [self._public_item(item) for item in items]

    def _has_key_hash_locked(self, key_hash: str, *, exclude_id: str = "") -> bool:
        for item in self._items:
            item_id = self._clean(item.get("id"))
            if exclude_id and item_id == exclude_id:
                continue
            stored_hash = self._clean(item.get("key_hash"))
            if stored_hash and hmac.compare_digest(stored_hash, key_hash):
                return True
        return False

    def _build_key_hash_locked(self, raw_key: str, *, exclude_id: str = "") -> str:
        candidate = self._clean(raw_key)
        if not candidate:
            raise ValueError("请输入新的专用密钥")
        admin_key = self._clean(config.auth_key)
        if admin_key and hmac.compare_digest(candidate, admin_key):
            raise ValueError("这个密钥和管理员密钥冲突了，请换一个新的密钥")
        key_hash = _hash_key(candidate)
        if self._has_key_hash_locked(key_hash, exclude_id=exclude_id):
            raise ValueError("这个专用密钥已经存在，请换一个新的密钥")
        return key_hash

    def _has_name_locked(self, name: str, *, role: AuthRole | None = None, exclude_id: str = "") -> bool:
        candidate = self._clean(name)
        if not candidate:
            return False
        for item in self._items:
            item_id = self._clean(item.get("id"))
            if exclude_id and item_id == exclude_id:
                continue
            if role is not None and item.get("role") != role:
                continue
            if self._clean(item.get("name")) == candidate:
                return True
        return False

    def _build_default_name_locked(self, role: AuthRole, *, exclude_id: str = "") -> str:
        base_name = self._default_name(role)
        if not self._has_name_locked(base_name, role=role, exclude_id=exclude_id):
            return base_name
        suffix = 2
        while True:
            candidate = f"{base_name} {suffix}"
            if not self._has_name_locked(candidate, role=role, exclude_id=exclude_id):
                return candidate
            suffix += 1

    def _build_name_locked(self, name: str, *, role: AuthRole, exclude_id: str = "") -> str:
        candidate = self._clean(name)
        if not candidate:
            return self._build_default_name_locked(role, exclude_id=exclude_id)
        if self._has_name_locked(candidate, role=role, exclude_id=exclude_id):
            raise ValueError("这个名称已经在使用中了，换一个更容易区分的名称吧")
        return candidate

    def create_key(self, *, role: AuthRole, name: str = "") -> tuple[dict[str, object], str]:
        with self._lock:
            self._reload_locked()
            normalized_name = self._build_name_locked(name, role=role)
            while True:
                raw_key = f"sk-{secrets.token_urlsafe(24)}"
                try:
                    key_hash = self._build_key_hash_locked(raw_key)
                    break
                except ValueError:
                    continue
            item = {
                "id": uuid.uuid4().hex[:12],
                "name": normalized_name,
                "role": role,
                "key_hash": key_hash,
                "enabled": True,
                "created_at": _now_iso(),
                "last_used_at": None,
            }
            self._items.append(item)
            self._save()
            return self._public_item(item), raw_key

    def update_key(
        self,
        key_id: str,
        updates: dict[str, object],
        *,
        role: AuthRole | None = None,
    ) -> dict[str, object] | None:
        normalized_id = self._clean(key_id)
        if not normalized_id:
            return None
        with self._lock:
            self._reload_locked()
            for index, item in enumerate(self._items):
                if item.get("id") != normalized_id:
                    continue
                if role is not None and item.get("role") != role:
                    return None
                next_item = dict(item)
                next_role = "admin" if str(next_item.get("role") or "").strip().lower() == "admin" else "user"
                if "name" in updates and updates.get("name") is not None:
                    next_item["name"] = self._build_name_locked(
                        str(updates.get("name") or ""),
                        role=next_role,
                        exclude_id=normalized_id,
                    )
                if "enabled" in updates and updates.get("enabled") is not None:
                    next_item["enabled"] = bool(updates.get("enabled"))
                if "key" in updates and updates.get("key") is not None:
                    next_item["key_hash"] = self._build_key_hash_locked(str(updates.get("key") or ""), exclude_id=normalized_id)
                self._items[index] = next_item
                self._save()
                return self._public_item(next_item)
        return None

    def delete_key(self, key_id: str, *, role: AuthRole | None = None) -> bool:
        normalized_id = self._clean(key_id)
        if not normalized_id:
            return False
        with self._lock:
            self._reload_locked()
            before = len(self._items)
            self._items = [
                item
                for item in self._items
                if not (item.get("id") == normalized_id and (role is None or item.get("role") == role))
            ]
            if len(self._items) == before:
                return False
            self._save()
            return True

    def authenticate(self, raw_key: str) -> dict[str, object] | None:
        candidate = self._clean(raw_key)
        if not candidate:
            return None
        candidate_hash = _hash_key(candidate)
        item = self._auth_index.get(candidate_hash)
        if item is None or not hmac.compare_digest(str(item["key_hash"]), candidate_hash):
            return None
        now = _now_iso()
        wake = False
        with self._usage_lock:
            if not self._usage_closed:
                self._usage[str(item["id"])] = now
                wake = not self._usage_dirty
                self._usage_dirty = True
                if self._usage_thread is None:
                    self._usage_thread = Thread(target=self._usage_worker, name="auth-usage-writer", daemon=True)
                    try:
                        self._usage_thread.start()
                    except RuntimeError:
                        # 使用时间是尽力统计，后台线程启动失败也不能拒绝有效密钥。
                        self._usage_thread = None
        if wake:
            self._usage_event.set()
        return self._public_item({**item, "last_used_at": now})


auth_service = AuthService(config.get_storage_backend())
