from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from functools import wraps
from threading import Lock, Thread, local


def persisted(method):
    """Wait for durable writes after the caller has released the account lock."""
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        if getattr(self._write_context, "pending", None) is not None:
            return method(self, *args, **kwargs)
        pending = self._write_context.pending = []
        after = self._write_context.after = []
        try:
            return method(self, *args, **kwargs)
        finally:
            self._write_context.pending = None
            self._write_context.after = None
            if pending:
                self._writer.ensure_running()
            for future in pending:
                future.result()
            for callback in after:
                callback()
    return wrapped


class AccountWriter:
    """Ordered, coalesced writes. A failed commit reaches every waiting caller."""
    def __init__(self, storage):
        self.storage = storage
        self._lock = Lock()
        self._queue = deque()
        self._running = False

    def submit(self, changes=None, *, snapshot=None, deferred=False):
        future = Future()
        with self._lock:
            self._queue.append((changes, snapshot, future))
        if not deferred:
            self.ensure_running()
        return future

    def ensure_running(self):
        with self._lock:
            if self._running or not self._queue:
                return
            self._running = True
        try:
            Thread(target=self._run, name="account-writer", daemon=True).start()
        except BaseException as exc:
            with self._lock:
                self._running = False
                batch = list(self._queue)
                self._queue.clear()
            for _, _, future in batch:
                future.set_exception(exc)
            raise

    def _run(self):
        while True:
            with self._lock:
                if not self._queue:
                    self._running = False
                    return
                batch = list(self._queue)
                self._queue.clear()
            try:
                changes = {}
                snapshot = None
                for updates, full, _ in batch:
                    if full is not None:
                        snapshot = full
                    if updates:
                        changes.update(updates)
                if snapshot is not None:
                    self.storage.save_accounts(snapshot)
                elif changes:
                    upserts = [value for value in changes.values() if value is not None]
                    deletes = [key for key, value in changes.items() if value is None]
                    if callable(apply := getattr(self.storage, "apply_account_changes", None)):
                        apply(upserts, deletes)
                    else:
                        for token in deletes:
                            self.storage.delete_account(token)
                        for account in upserts:
                            self.storage.save_account(account)
            except Exception as exc:
                for _, _, future in batch:
                    future.set_exception(exc)
            else:
                for _, _, future in batch:
                    future.set_result(None)
