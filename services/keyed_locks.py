from contextlib import contextmanager
from threading import Lock


class KeyedLocks:
    """仅同一账号互斥；等待者持引用，最后一个调用结束后回收锁。"""

    def __init__(self):
        self._guard = Lock()
        self._entries = {}

    @contextmanager
    def hold(self, key):
        with self._guard:
            lock, users = self._entries.get(key, (Lock(), 0))
            self._entries[key] = (lock, users + 1)
        try:
            with lock:
                yield
        finally:
            with self._guard:
                _, users = self._entries[key]
                if users == 1:
                    del self._entries[key]
                else:
                    self._entries[key] = (lock, users - 1)
