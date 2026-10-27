from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime
from typing import Any

from sqlalchemy import Column, Float, Index, MetaData, String, Table, Text, create_engine, delete, select, update
from sqlalchemy.pool import NullPool

metadata = MetaData()
tasks = Table(
    "image_tasks", metadata,
    Column("key", String(64), primary_key=True),
    Column("owner_id", String(255), nullable=False),
    Column("status", String(16), nullable=False),
    Column("updated_ts", Float, nullable=False),
    Column("state", Text, nullable=False),
    Column("result", Text),
    Column("request_body", Text),
    Index("ix_image_tasks_owner_updated", "owner_id", "updated_ts"),
    Index("ix_image_tasks_status_updated", "status", "updated_ts"),
)
migrations = Table("image_task_migrations", metadata, Column("source", String(64), primary_key=True))


def _dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


class ImageTaskStorage:
    """Indexed per-task persistence; progress writes never read or rewrite image data."""
    def __init__(self, path: Path, *, engine=None):
        self._owns_engine = engine is None
        if engine is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine("sqlite:///" + path.with_suffix(".sqlite3").as_posix(),
                                   poolclass=NullPool, connect_args={"timeout": 30})
            with engine.connect() as connection:
                connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        self.engine = engine
        metadata.create_all(engine)

    def close(self) -> None:
        """Release a locally-owned engine without closing a shared app engine."""
        if self._owns_engine:
            self.engine.dispose()

    @staticmethod
    def _key(key):
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    @contextmanager
    def transaction(self):
        with self.engine.connect() as connection:
            if self.engine.dialect.name == "sqlite":
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            else:
                connection.begin()
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _insert(self, connection, values):
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            from sqlalchemy.dialects.mysql import insert
        statement = insert(tasks).values(**values)
        if dialect == "mysql":
            statement = statement.on_duplicate_key_update(key=statement.inserted.key)
        else:
            statement = statement.on_conflict_do_nothing(index_elements=[tasks.c.key])
        return connection.execute(statement).rowcount == 1

    def _values(self, key, task):
        state = {k: v for k, v in task.items() if k not in {"data", "request_body"}}
        timestamp = task.get("updated_ts") or task.get("created_ts")
        if not timestamp:
            try:
                timestamp = datetime.fromisoformat(task.get("updated_at") or task.get("created_at") or "").timestamp()
            except ValueError:
                timestamp = 0
        return dict(key=self._key(key), owner_id=task["owner_id"], status=task["status"],
                    updated_ts=float(timestamp),
                    state=_dump(state), result=_dump(task["data"]) if "data" in task else None,
                    request_body=_dump(task["request_body"]) if "request_body" in task else None)

    def create(self, key, task):
        with self.transaction() as connection:
            created = self._insert(connection, self._values(key, task))
        return created

    @staticmethod
    def _decode(row):
        if row is None:
            return None
        task = json.loads(row.state)
        if row.result is not None:
            task["data"] = json.loads(row.result)
        return task

    def get(self, key):
        with self.engine.connect() as connection:
            return self._decode(connection.execute(select(tasks.c.state, tasks.c.result)
                                .where(tasks.c.key == self._key(key))).first())

    def list_owner(self, owner):
        with self.engine.connect() as connection:
            return [self._decode(row) for row in connection.execute(
                select(tasks.c.state, tasks.c.result).where(tasks.c.owner_id == owner)
                .order_by(tasks.c.updated_ts.desc()))]

    def update(self, key, changes):
        with self.transaction() as connection:
            key_hash = self._key(key)
            row = connection.execute(select(tasks.c.state).where(tasks.c.key == key_hash).with_for_update()).first()
            if row is None:
                return
            state = json.loads(row.state)
            state.update({k: v for k, v in changes.items() if k != "data"})
            values = dict(state=_dump(state), status=state["status"], updated_ts=state.get("updated_ts", 0))
            if "data" in changes:
                values["result"] = _dump(changes["data"])
            connection.execute(update(tasks).where(tasks.c.key == key_hash).values(**values))

    def cleanup(self, cutoff):
        with self.engine.begin() as connection:
            connection.execute(delete(tasks).where(tasks.c.status.in_(["success", "error"]), tasks.c.updated_ts < cutoff))

    def recover(self, now, now_ts):
        with self.transaction() as connection:
            rows = connection.execute(select(tasks.c.key, tasks.c.state).where(tasks.c.status.in_(["queued", "running"]))).all()
            for row in rows:
                state = json.loads(row.state)
                state.update(status="error", error="服务已重启，未完成的图片任务已中断", updated_at=now, updated_ts=now_ts)
                connection.execute(update(tasks).where(tasks.c.key == row.key).values(
                    state=_dump(state), status="error", updated_ts=now_ts))

    def import_legacy(self, source: Path, loader):
        marker = hashlib.sha256(str(source.resolve()).encode()).hexdigest()
        with self.engine.connect() as connection:
            if connection.execute(select(migrations).where(migrations.c.source == marker)).first():
                return
        legacy = loader()
        with self.transaction() as connection:
            for key, task in legacy.items():
                self._insert(connection, self._values(key, task))
            # Migration runs during startup in the supported single-process deployment.
            connection.execute(migrations.insert().values(source=marker))

    def export(self):
        with self.engine.connect() as connection:
            result = []
            for row in connection.execute(select(tasks.c.state, tasks.c.result, tasks.c.request_body)):
                task = self._decode(row)
                if row.request_body is not None:
                    task["request_body"] = json.loads(row.request_body)
                result.append(task)
            return {"tasks": result}
