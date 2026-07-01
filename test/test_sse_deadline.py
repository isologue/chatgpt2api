import sys
import types

import pytest

if "fastapi" not in sys.modules:
    fastapi_stub = types.ModuleType("fastapi")

    class _HTTPException(Exception):
        pass

    fastapi_stub.HTTPException = _HTTPException
    sys.modules["fastapi"] = fastapi_stub

from utils import helper


class _FakeResponse:
    def __init__(self, lines: list[bytes | str]) -> None:
        self._lines = lines
        self.closed = False

    def iter_lines(self):
        for line in self._lines:
            yield line

    def close(self) -> None:
        self.closed = True


def test_iter_sse_payloads_without_deadline():
    response = _FakeResponse([b"data: first", b"", b"data: second"])

    assert list(helper.iter_sse_payloads(response)) == ["first", "second"]
    assert response.closed is False


def test_iter_sse_payloads_respects_deadline(monkeypatch):
    response = _FakeResponse([b"data: first", b"data: second"])
    times = iter([100.0, 101.2])
    monkeypatch.setattr(helper.time, "time", lambda: next(times))

    iterator = helper.iter_sse_payloads(response, deadline_ts=101.0)
    assert next(iterator) == "first"
    with pytest.raises(TimeoutError):
        next(iterator)
    assert response.closed is True
