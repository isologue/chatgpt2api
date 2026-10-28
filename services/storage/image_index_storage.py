from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from services.storage.batch_writer import BatchWriter


class ImageIndexStorage:
    """图片元数据采用 WAL 快照读取、有界单写线程及批量事务。"""

    def __init__(self, legacy: Path):
        self.legacy = legacy
        self.path = legacy.with_suffix(".sqlite3")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_connection: sqlite3.Connection | None = None
        # 表结构及 WAL 仅在初始化时设置，移出生图完成路径。
        with sqlite3.connect(self.path, timeout=30) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("CREATE TABLE IF NOT EXISTS images (path TEXT PRIMARY KEY, data TEXT NOT NULL)")
            connection.execute("CREATE TABLE IF NOT EXISTS source (id INTEGER PRIMARY KEY, fingerprint TEXT)")
        connection.close()
        self._sync_legacy()
        self._writer = BatchWriter(self._write_batch, name="image-index-writer", close=self._close_connection)

    @contextmanager
    def connection(self, *, write: bool = False):
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write:
                connection.rollback()
            raise
        finally:
            connection.close()

    def _sync_legacy(self) -> None:
        with self.connection(write=True) as connection:
            fingerprint = "missing"
            if self.legacy.exists():
                stat = self.legacy.stat()
                fingerprint = f"{stat.st_size}:{stat.st_mtime_ns}"
            row = connection.execute("SELECT fingerprint FROM source WHERE id=1").fetchone()
            if row is None or row[0] != fingerprint:
                if self.legacy.exists():
                    raw = json.loads(self.legacy.read_text(encoding="utf-8"))
                    items = raw.get("items", {}) if isinstance(raw, dict) else {}
                    connection.execute("DELETE FROM images")
                    connection.executemany("INSERT INTO images VALUES (?, ?)", [
                        (key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))
                        for key, value in items.items() if isinstance(value, dict)
                    ])
                connection.execute("INSERT OR REPLACE INTO source VALUES (1, ?)", (fingerprint,))

    def _write_batch(self, items) -> None:
        if self._write_connection is None:
            self._write_connection = sqlite3.connect(self.path, timeout=30)
        connection = self._write_connection
        # 保留 SQLite 默认 FULL 同步级别，整批提交后才唤醒调用方。
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            for operation, path, value in items:
                if operation == "put":
                    connection.execute(
                        "INSERT INTO images VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET data=excluded.data",
                        (path, json.dumps(value, ensure_ascii=False, separators=(",", ":"))),
                    )
                elif operation == "delete":
                    connection.execute("DELETE FROM images WHERE path=?", (path,))
                elif operation == "reconcile":
                    before, after = value
                    for key in before.keys() | after.keys():
                        if before.get(key) == after.get(key):
                            continue
                        row = connection.execute("SELECT data FROM images WHERE path=?", (key,)).fetchone()
                        current = json.loads(row[0]) if row else None
                        # 扫描只更新未被并发生图改写的记录，保留新增和较新的元数据。
                        if current != before.get(key):
                            continue
                        if key not in after:
                            connection.execute("DELETE FROM images WHERE path=?", (key,))
                        else:
                            connection.execute("INSERT OR REPLACE INTO images VALUES (?, ?)",
                                               (key, json.dumps(after[key], ensure_ascii=False, separators=(",", ":"))))
                elif operation == "replace":
                    connection.execute("DELETE FROM images")
                    connection.executemany("INSERT INTO images VALUES (?, ?)", [
                        (key, json.dumps(item, ensure_ascii=False, separators=(",", ":")))
                        for key, item in value.items()
                    ])

    def _close_connection(self):
        if self._write_connection is not None:
            self._write_connection.close()
            self._write_connection = None

    def close(self):
        self._writer.close()

    def get(self, path):
        with self.connection() as connection:
            row = connection.execute("SELECT data FROM images WHERE path=?", (path,)).fetchone()
            return json.loads(row[0]) if row else {}

    def iter_items(self):
        with self.connection() as connection:
            for path, data in connection.execute("SELECT path, data FROM images"):
                yield path, json.loads(data)

    def all(self):
        return dict(self.iter_items())

    def put(self, path, value):
        self._writer.submit(("put", path, value)).result()

    def delete(self, path):
        self._writer.submit(("delete", path, None)).result()

    def replace(self, items):
        self._writer.submit(("replace", None, items)).result()

    def reconcile(self, before, after):
        self._writer.submit(("reconcile", None, (before, after))).result()
