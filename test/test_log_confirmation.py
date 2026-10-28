import asyncio
import sqlite3
from unittest import mock

import pytest

from services.log_service import LogService


def test_sqlite_commit_failure_reaches_async_caller_and_rolls_back_body(tmp_path):
    fail_commit = False
    class Connection(sqlite3.Connection):
        def commit(self):
            nonlocal fail_commit
            if fail_commit:
                fail_commit = False
                raise sqlite3.OperationalError("commit failed")
            return super().commit()
    connect = sqlite3.connect
    def factory(*args, **kwargs):
        return connect(*args, factory=Connection, **kwargs)
    with mock.patch("services.log_service.sqlite3.connect", side_effect=factory):
        with LogService(tmp_path / "logs.jsonl") as service:
            service.add("call", summary="before")
            before = service.path.read_bytes()
            fail_commit = True
            async def scenario():
                with pytest.raises(sqlite3.OperationalError, match="commit failed"):
                    await service.add_async("call", summary="failed")
                assert service.path.read_bytes() == before
                await service.add_async("call", summary="after")
            asyncio.run(scenario())
            assert [item["summary"] for item in service.list()] == ["after", "before"]
            assert service._writer.stats()["failed"] == 1
            assert service._writer.stats()["pending"] == 0
