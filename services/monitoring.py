from __future__ import annotations

import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any

from services.config import DATA_DIR
from services.thread_pool import image_executor


def _read_proc_stat() -> tuple[int, int] | None:
    try:
        first = Path("/proc/stat").read_text(encoding="ascii").splitlines()[0].split()
        values = [int(value) for value in first[1:]]
        return sum(values), values[3] + (values[4] if len(values) > 4 else 0)
    except (OSError, IndexError, ValueError):
        return None


_cpu_state: tuple[float, int, int] | None = None
_cpu_lock = threading.Lock()
_network_state = None
_network_lock = threading.Lock()


def _network_totals():
    totals = dict.fromkeys(('received_bytes', 'sent_bytes', 'receive_errors',
        'send_errors', 'receive_drops', 'send_drops'), 0)
    for line in Path('/proc/net/dev').read_text(encoding='ascii').splitlines()[2:]:
        name, raw = line.split(':', 1)
        if name.strip() == 'lo':
            continue
        fields = raw.split()
        for key, index in (('received_bytes', 0), ('sent_bytes', 8),
                ('receive_errors', 2), ('send_errors', 10),
                ('receive_drops', 3), ('send_drops', 11)):
            totals[key] += int(fields[index])
    return totals


def _network():
    global _network_state
    try:
        current = _network_totals()
    except (OSError, IndexError, ValueError):
        return {'available': False}
    with _network_lock:
        now = time.monotonic()
        previous = _network_state
        _network_state = (now, current)
    span = now - previous[0] if previous is not None else 0
    def rate(key):
        return (round(max(0, current[key] - previous[1][key]) / span, 3)
            if span > 0 else None)
    return {**current, 'available': True, 'scope': 'process_network_namespace',
        'sample_seconds': round(span, 3), 'received_bytes_per_sec': rate('received_bytes'),
        'sent_bytes_per_sec': rate('sent_bytes')}


def _cpu_percent() -> float | None:
    global _cpu_state
    current = _read_proc_stat()
    if current is None:
        return None
    now = time.monotonic()
    with _cpu_lock:
        previous = _cpu_state
        _cpu_state = (now, current[0], current[1])
    if previous is None:
        return 0.0
    total_delta = current[0] - previous[1]
    idle_delta = current[1] - previous[2]
    return round(max(0.0, min(100.0, (total_delta - idle_delta) * 100 / total_delta)), 2) if total_delta else 0.0


def _memory() -> dict[str, int | float]:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            key, raw = line.split(":", 1)
            parts = raw.strip().split()
            if parts:
                values[key] = int(parts[0]) * 1024
    except (OSError, ValueError):
        return {"used_bytes": 0, "total_bytes": 0, "percent": 0.0}
    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", values.get("MemFree", 0))
    used = max(0, total - available)
    return {"used_bytes": used, "total_bytes": total, "percent": round(used * 100 / total, 2) if total else 0.0}


def snapshot(account_stats: dict[str, Any], image_stats: dict[str, Any]) -> dict[str, Any]:
    executor = image_executor.stats()
    disk = shutil.disk_usage(DATA_DIR)
    unavailable = int(account_stats.get("limited", 0)) + int(account_stats.get("abnormal", 0)) + int(account_stats.get("disabled", 0))
    return {
        "timestamp": int(time.time()),
        "accounts": {
            "total": int(account_stats.get("total", 0)),
            "available": int(account_stats.get("active", 0)),
            "unavailable": unavailable,
            "limited": int(account_stats.get("limited", 0)),
            "abnormal": int(account_stats.get("abnormal", 0)),
            "disabled": int(account_stats.get("disabled", 0)),
        },
        "image_executor": {
            **executor,
            "available_slots": int(executor.get("available", 0)),
            "remaining_queue_slots": max(0, int(executor.get("queue_size", 0)) - int(executor.get("queued", 0))),
        },
        "images": image_stats,
        "system": {
            "cpu_percent": _cpu_percent(),
            "network": _network(),
            "memory": _memory(),
            "disk": {
                "used_bytes": disk.total - disk.free,
                "total_bytes": disk.total,
                "free_bytes": disk.free,
                "percent": round((disk.total - disk.free) * 100 / disk.total, 2) if disk.total else 0.0,
            },
            "load_average": list(os.getloadavg()) if hasattr(os, "getloadavg") else [],
        },
    }
