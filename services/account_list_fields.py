"""Shared, deterministic management-list fields for SQL and snapshot backends."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def parse_account_time(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def refresh_error_is_terminal(error: object) -> bool:
    patterns = (
        r"\binvalid_grant\b", r"\binvalid_refresh_token\b",
        r"\brefresh_token_(?:reused|expired|revoked|invalid)\b",
        r"\brefresh[\s_-]*token\b.{0,24}\b(?:is|has\s+been|was)?\s*(?:expired|revoked|invalid|reused)\b",
        r"\b(?:expired|revoked|invalid|reused)\s+refresh[\s_-]*token\b",
    )
    return any(re.search(pattern, str(error or "").lower(), flags=re.DOTALL) for pattern in patterns)


def refresh_token_status(account: dict) -> str:
    if not str(account.get("refresh_token") or "").strip():
        return "missing"
    error = str(account.get("last_token_refresh_error") or "").strip()
    error_at = parse_account_time(account.get("last_token_refresh_error_at"))
    refreshed_at = parse_account_time(account.get("last_token_refresh_at"))
    if error and not (error_at and refreshed_at and refreshed_at >= error_at):
        return "invalid" if refresh_error_is_terminal(error) else "pending"
    return "valid" if refreshed_at is not None else "unverified"


def account_list_fields(account: dict[str, Any]) -> dict[str, Any]:
    try:
        score = max(0, min(100, int(account.get("image_score", 50))))
    except (TypeError, ValueError, OverflowError):
        score = 50
    source = account.get("source_type")
    if not source and str(account.get("export_type") or account.get("type") or "").strip().lower() == "codex":
        source = "codex"
    try:
        eligible = int(account.get("quota") or 0) > 0 and int(account.get("invalid_count") or 0) <= 0
    except (TypeError, ValueError, OverflowError):
        eligible = False
    hot = bool(account.get("image_pool_hot", False)) and eligible and account.get("status") not in {"禁用", "限流", "异常"}
    created = parse_account_time(account.get("created_at"))
    return {
        "source_type": str(source or "web").strip().lower() or "web",
        "refresh_token_status": refresh_token_status(account),
        "image_pool_hot": int(hot), "image_score": score,
        "created_at_epoch": created.timestamp() if created else None,
    }


def matches_account_filters(account: dict, *, source: str = "all", rt_status: str = "all",
                            pool: str = "all", score_min: int | None = None, score_max: int | None = None,
                            created_from: float | None = None, created_to: float | None = None) -> bool:
    fields = account_list_fields(account)
    created = fields["created_at_epoch"]
    return not (
        (source != "all" and fields["source_type"] != source)
        or (rt_status != "all" and fields["refresh_token_status"] != rt_status)
        or (pool != "all" and fields["image_pool_hot"] != int(pool == "hot"))
        or (score_min is not None and fields["image_score"] < score_min)
        or (score_max is not None and fields["image_score"] > score_max)
        or (created_from is not None and (created is None or created < created_from))
        or (created_to is not None and (created is None or created >= created_to))
    )
