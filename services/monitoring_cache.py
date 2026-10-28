from __future__ import annotations

import asyncio
import time
from collections import OrderedDict


class SnapshotCache:
    """有界合并采集；已知快照可限时回退，冷启动不伪造零值。"""
    def __init__(self, capacity=32, ttl=1.0, wait=0.35):
        self.capacity, self.ttl, self.wait = capacity, ttl, wait
        self.entries = OrderedDict()

    async def get(self, key, collect):
        key = (asyncio.get_running_loop(), key)
        entry = self.entries.get(key)
        if entry is None:
            if len(self.entries) >= self.capacity:
                expired = next((k for k, v in self.entries.items() if v["task"] is None), None)
                if expired is None:
                    from fastapi import HTTPException
                    raise HTTPException(status_code=503, detail="monitoring collection is busy", headers={"Retry-After": "1"})
                self.entries.pop(expired)
            entry = {"value": None, "at": 0.0, "time": None, "task": None, "error": False}
            self.entries[key] = entry
        self.entries.move_to_end(key)
        if entry["task"] is None and (entry["value"] is None or time.monotonic() - entry["at"] >= self.ttl):
            async def refresh():
                try:
                    value = await collect()
                    entry.update(value=value, at=time.monotonic(), time=time.time(), error=False)
                    return value
                except Exception:
                    entry["error"] = True
                    raise
                finally:
                    entry["task"] = None
            task = asyncio.create_task(refresh())
            # 请求超时/取消后仍消费异常，避免无人接收的任务异常。
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            entry["task"] = task
        task = entry["task"]
        if task is not None:
            try:
                if entry["value"] is None:
                    await asyncio.shield(task)
                else:
                    await asyncio.wait_for(asyncio.shield(task), timeout=self.wait)
            except Exception:
                if entry["value"] is None:
                    raise
        age = max(0.0, time.monotonic() - entry["at"])
        return entry["value"], {"collected_at": entry["time"], "age_ms": round(age * 1000, 1),
                                "stale": bool(entry["error"] or age >= self.ttl),
                                "refreshing": entry["task"] is not None, "refresh_failed": entry["error"]}
