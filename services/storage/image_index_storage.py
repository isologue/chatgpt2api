from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


class ImageIndexStorage:
    """Local image metadata only; incremental writes keep image saves constant-size."""
    def __init__(self, legacy: Path):
        self.legacy = legacy
        self.path = legacy.with_suffix(".sqlite3")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection(write=True):
            pass
        self._sync_legacy()

    @contextmanager
    def connection(self, *, write: bool = False):
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            connection.execute("PRAGMA busy_timeout=30000")
            if write:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("CREATE TABLE IF NOT EXISTS images (path TEXT PRIMARY KEY, data TEXT NOT NULL)")
                connection.execute("CREATE TABLE IF NOT EXISTS source (id INTEGER PRIMARY KEY, fingerprint TEXT)")
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
        """Migrate the old JSON index once at startup or before a write."""
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
                    connection.executemany(
                        "INSERT INTO images VALUES (?, ?)",
                        [
                            (key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))
                            for key, value in items.items()
                            if isinstance(value, dict)
                        ],
                    )
                connection.execute("INSERT OR REPLACE INTO source VALUES (1, ?)", (fingerprint,))

    def get(self, path):
        with self.connection() as connection:
            row = connection.execute("SELECT data FROM images WHERE path=?", (path,)).fetchone()
            return json.loads(row[0]) if row else {}

    def all(self):
        with self.connection() as connection:
            return {row[0]: json.loads(row[1]) for row in connection.execute("SELECT path, data FROM images")}

    def put(self, path, value):
        with self.connection(write=True) as connection:
            connection.execute("INSERT INTO images VALUES (?, ?) ON CONFLICT(path) DO UPDATE SET data=excluded.data",
                               (path, json.dumps(value, ensure_ascii=False, separators=(",", ":"))))

    def delete(self, path):
        with self.connection(write=True) as connection:
            connection.execute("DELETE FROM images WHERE path=?", (path,))

    def replace(self, items):
        with self.connection(write=True) as connection:
            connection.execute("DELETE FROM images")
            connection.executemany("INSERT INTO images VALUES (?, ?)",
                [(key, json.dumps(value, ensure_ascii=False, separators=(",", ":"))) for key, value in items.items()])
