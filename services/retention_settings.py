from __future__ import annotations

from datetime import datetime
import math

KINDS = ("images", "logs")
MAX_HOURS = 876000
MAX_INTERVAL_MINUTES = 43200


def timestamp(value: object) -> float | None:
    """兼容带时区和历史本地时间；无法识别的时间保留，不猜测删除。"""
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def normalize_retention(value: object, legacy_image_days: object = 30) -> dict:
    if value is not None and not isinstance(value, dict):
        raise ValueError("data_retention must be an object")
    try:
        image_hours = max(1, min(MAX_HOURS, int(legacy_image_days) * 24))
    except (ValueError, TypeError, OverflowError):
        image_hours = 720
    result = {}
    for kind, hours in (("images", image_hours), ("logs", 720)):
        policy = (value or {}).get(kind, {})
        if not isinstance(policy, dict):
            raise ValueError(f"data_retention.{kind} must be an object")
        enabled = policy.get("enabled", False)
        if not isinstance(enabled, bool):
            raise ValueError("enabled must be a boolean")
        result[kind] = {"enabled": enabled}
        for key, default, maximum in (("retention_hours", hours, MAX_HOURS),
                                       ("interval_minutes", 60, MAX_INTERVAL_MINUTES)):
            raw = policy.get(key, default)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw) or not 1 <= raw <= maximum or int(raw) != raw:
                raise ValueError(f"{key} must be an integer between 1 and {maximum}")
            result[kind][key] = int(raw)
    return result
