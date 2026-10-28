import base64
import atexit
import time
from collections import deque
from threading import Condition, Thread, current_thread
import binascii
import json
import logging
import re
from typing import Any


class BufferedStreamHandler(logging.Handler):
    """运行诊断日志有界异步输出；与必须可靠落盘的调用日志完全分离。"""

    def __init__(self, sink=None, capacity=4096):
        super().__init__()
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._sink = sink if sink is not None else logging.StreamHandler()
        self._sink.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        self._condition = Condition()
        self._records = deque()
        self._capacity = capacity
        self._worker = None
        self._stopping = False
        self._accepted = self._completed = self._dropped = self._failed = 0
        atexit.register(self.close)

    def handle(self, record):
        # 不使用 Handler.handle 的输出锁；生产者只入队，不做终端/容器 IO。
        if not self.filter(record):
            return False
        with self._condition:
            if self._stopping or len(self._records) >= self._capacity:
                self._dropped += 1
                return False
            self._records.append(record)
            self._accepted += 1
            if self._worker is None:
                self._worker = Thread(target=self._run, name="runtime-log-writer", daemon=True)
                self._worker.start()
            self._condition.notify()
        return True

    def _run(self):
        while True:
            with self._condition:
                while not self._records and not self._stopping:
                    self._condition.wait()
                if not self._records:
                    return
                record = self._records.popleft()
            try:
                self._sink.handle(record)
            except Exception:
                with self._condition:
                    self._failed += 1
            finally:
                with self._condition:
                    self._completed += 1
                    self._condition.notify_all()

    def stats(self):
        with self._condition:
            return {"capacity": self._capacity, "queued": len(self._records),
                    "pending": self._accepted - self._completed,
                    "accepted": self._accepted, "completed": self._completed,
                    "dropped": self._dropped, "failed": self._failed}

    def flush(self, timeout=5.0):
        if current_thread() is self._worker:
            return False
        deadline = time.monotonic() + timeout
        with self._condition:
            target = self._accepted
            while self._completed < target:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
        return True

    def close(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        self.flush()
        if self._worker is not None and current_thread() is not self._worker:
            self._worker.join(timeout=0.1)
        atexit.unregister(self.close)
        super().close()


class Logger:
    _DATA_URL_RE = re.compile(r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+")
    _JSON_B64_RE = re.compile(r'("b64_json"\s*:\s*")([A-Za-z0-9+/=]+)(")')

    def __init__(self, name: str = "chatgpt2api") -> None:
        self._logger = logging.getLogger(name)
        if not self._logger.handlers:
            self._logger.addHandler(BufferedStreamHandler())
        self._logger.setLevel(logging.DEBUG)
        self._logger.propagate = False

    def stats(self):
        return [handler.stats() for handler in self._logger.handlers if isinstance(handler, BufferedStreamHandler)]

    def flush(self):
        return all(handler.flush() for handler in self._logger.handlers if isinstance(handler, BufferedStreamHandler))

    def _enabled(self, level: str) -> bool:
        try:
            from services.config import config
            levels = set(config.log_levels)
        except Exception:
            levels = set()
        return level in (levels or {"info", "warning", "error"})

    def _mask_string(self, value: str, keep: int = 10) -> str:
        if len(value) <= keep:
            return value
        return value[:keep] + "..."

    def _mask_base64(self, value: str) -> str:
        if value.startswith("data:") and ";base64," in value:
            header, _, data = value.partition(",")
            return f"{header},{self._mask_string(data, 24)} (base64 len={len(data)})"
        return f"{self._mask_string(value, 24)} (base64 len={len(value)})"

    def _is_base64_string(self, value: str) -> bool:
        if len(value) < 64 or len(value) % 4 != 0:
            return False
        if not any(char in value for char in "+/="):
            return False
        try:
            base64.b64decode(value, validate=True)
            return True
        except (binascii.Error, ValueError):
            return False

    def _sanitize_string(self, value: str) -> str:
        stripped = value.strip()
        if stripped.startswith("data:") and ";base64," in stripped:
            return self._mask_base64(stripped)
        if self._is_base64_string(stripped):
            return self._mask_base64(stripped)
        sanitized = self._DATA_URL_RE.sub(lambda match: self._mask_base64(match.group(0)), value)
        sanitized = self._JSON_B64_RE.sub(
            lambda match: f'{match.group(1)}{self._mask_base64(match.group(2))}{match.group(3)}',
            sanitized,
        )
        if sanitized != value:
            return sanitized
        return value

    def _sanitize(self, value: Any) -> Any:
        if isinstance(value, dict):
            sanitized = {}
            for key, item in value.items():
                lowered_key = key.lower()
                if isinstance(item, str) and ("token" in lowered_key or lowered_key == "dx"):
                    sanitized[key] = self._mask_string(item)
                elif isinstance(item, str) and ("base64" in lowered_key or lowered_key == "b64_json"):
                    sanitized[key] = self._mask_base64(item)
                else:
                    sanitized[key] = self._sanitize(item)
            return sanitized
        if isinstance(value, list):
            return [self._sanitize(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._sanitize(item) for item in value)
        if isinstance(value, str):
            return self._sanitize_string(value)
        return value

    def _message(self, value: Any) -> str:
        sanitized = self._sanitize(value)
        if isinstance(sanitized, str):
            return sanitized
        return json.dumps(sanitized, ensure_ascii=False, default=str)

    def debug(self, message: Any) -> None:
        if self._enabled("debug"):
            self._logger.debug(self._message(message))

    def info(self, message: Any) -> None:
        if self._enabled("info"):
            self._logger.info(self._message(message))

    def warning(self, message: Any) -> None:
        if self._enabled("warning"):
            self._logger.warning(self._message(message))

    def error(self, message: Any) -> None:
        if self._enabled("error"):
            self._logger.error(self._message(message))


logger = Logger()
