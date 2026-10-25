import json
import tempfile
import unittest
from pathlib import Path

import anyio

from services.config import (
    DEFAULT_ANYIO_THREAD_LIMIT,
    MAX_ANYIO_THREAD_LIMIT,
    ConfigStore,
)
from services.thread_pool import apply_anyio_thread_limit


class AnyIOThreadPoolConfigTests(unittest.TestCase):
    def _make_store(self, initial: dict[str, object] | None = None) -> tuple[tempfile.TemporaryDirectory, ConfigStore]:
        tmp_dir = tempfile.TemporaryDirectory()
        path = Path(tmp_dir.name) / "config.json"
        data: dict[str, object] = {"auth-key": "test-auth"}
        if initial:
            data.update(initial)
        path.write_text(json.dumps(data), encoding="utf-8")
        return tmp_dir, ConfigStore(path)

    def test_default_is_anyio_default_capacity(self) -> None:
        tmp_dir, store = self._make_store()
        with tmp_dir:
            self.assertEqual(store.anyio_thread_limit, DEFAULT_ANYIO_THREAD_LIMIT)
            self.assertEqual(store.get()["anyio_thread_limit"], DEFAULT_ANYIO_THREAD_LIMIT)

    def test_update_persists_and_normalizes_capacity(self) -> None:
        tmp_dir, store = self._make_store()
        with tmp_dir:
            updated = store.update({"anyio_thread_limit": "128"})
            self.assertEqual(updated["anyio_thread_limit"], 128)
            saved = json.loads(store.path.read_text(encoding="utf-8"))
            self.assertEqual(saved["anyio_thread_limit"], 128)

            self.assertEqual(store.update({"anyio_thread_limit": 0})["anyio_thread_limit"], 1)
            self.assertEqual(
                store.update({"anyio_thread_limit": MAX_ANYIO_THREAD_LIMIT + 1})["anyio_thread_limit"],
                MAX_ANYIO_THREAD_LIMIT,
            )
            self.assertEqual(store.update({"anyio_thread_limit": "invalid"})["anyio_thread_limit"], DEFAULT_ANYIO_THREAD_LIMIT)

    def test_apply_updates_current_event_loop_limiter(self) -> None:
        async def check_limit() -> int:
            await apply_anyio_thread_limit(73)
            import anyio.to_thread

            return anyio.to_thread.current_default_thread_limiter().total_tokens

        self.assertEqual(anyio.run(check_limit), 73)


if __name__ == "__main__":
    unittest.main()
