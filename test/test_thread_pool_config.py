import json
import tempfile
import threading
import unittest
from pathlib import Path

import anyio
from fastapi import HTTPException

from services.config import (
    DEFAULT_ANYIO_THREAD_LIMIT,
    DEFAULT_IMAGE_QUEUE_SIZE,
    DEFAULT_IMAGE_WORKERS,
    DEFAULT_IMAGE_ACCOUNT_SELECTION_MAX_ATTEMPTS,
    DEFAULT_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES,
    DEFAULT_IMAGE_TEXT_REPLY_ACCOUNT_SWITCH_RETRIES,
    MAX_ANYIO_THREAD_LIMIT,
    MAX_IMAGE_QUEUE_SIZE,
    MAX_IMAGE_WORKERS,
    MAX_IMAGE_ACCOUNT_SELECTION_MAX_ATTEMPTS,
    MAX_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES,
    MAX_IMAGE_TEXT_REPLY_ACCOUNT_SWITCH_RETRIES,
    ConfigStore,
)
from services.thread_pool import BoundedImageExecutor, apply_anyio_thread_limit


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

    def test_obsolete_startup_full_refresh_is_not_exposed(self) -> None:
        tmp_dir, store = self._make_store()
        with tmp_dir:
            self.assertNotIn("account_refresh_startup_mode", store.get())
            self.assertNotIn("account_refresh_startup_mode", store.update({"account_refresh_startup_mode": "full"}))

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

    def test_image_concurrency_defaults_and_bounds(self) -> None:
        tmp_dir, store = self._make_store()
        with tmp_dir:
            self.assertEqual(store.image_workers, DEFAULT_IMAGE_WORKERS)
            self.assertEqual(store.image_queue_size, DEFAULT_IMAGE_QUEUE_SIZE)
            self.assertEqual(
                store.image_account_selection_max_attempts,
                DEFAULT_IMAGE_ACCOUNT_SELECTION_MAX_ATTEMPTS,
            )
            self.assertEqual(
                store.image_poll_timeout_account_switch_retries,
                DEFAULT_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES,
            )
            self.assertEqual(
                store.image_text_reply_account_switch_retries,
                DEFAULT_IMAGE_TEXT_REPLY_ACCOUNT_SWITCH_RETRIES,
            )

            updated = store.update({
                "image_workers": 1000,
                "image_queue_size": 0,
                "image_account_selection_max_attempts": 50,
                "image_poll_timeout_account_switch_retries": 0,
                "image_text_reply_account_switch_retries": 7,
            })
            self.assertEqual(updated["image_workers"], 1000)
            self.assertEqual(updated["image_queue_size"], 0)
            self.assertEqual(updated["image_account_selection_max_attempts"], 50)
            self.assertEqual(updated["image_poll_timeout_account_switch_retries"], 0)
            self.assertEqual(updated["image_text_reply_account_switch_retries"], 7)

            self.assertEqual(
                store.update({"image_workers": MAX_IMAGE_WORKERS + 1})["image_workers"],
                MAX_IMAGE_WORKERS,
            )
            self.assertEqual(
                store.update({"image_queue_size": MAX_IMAGE_QUEUE_SIZE + 1})["image_queue_size"],
                MAX_IMAGE_QUEUE_SIZE,
            )
            self.assertEqual(
                store.update({"image_account_selection_max_attempts": MAX_IMAGE_ACCOUNT_SELECTION_MAX_ATTEMPTS + 1})[
                    "image_account_selection_max_attempts"
                ],
                MAX_IMAGE_ACCOUNT_SELECTION_MAX_ATTEMPTS,
            )
            self.assertEqual(
                store.update({"image_poll_timeout_account_switch_retries": MAX_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES + 1})[
                    "image_poll_timeout_account_switch_retries"
                ],
                MAX_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES,
            )
            self.assertEqual(
                store.update({"image_text_reply_account_switch_retries": MAX_IMAGE_TEXT_REPLY_ACCOUNT_SWITCH_RETRIES + 1})[
                    "image_text_reply_account_switch_retries"
                ],
                MAX_IMAGE_TEXT_REPLY_ACCOUNT_SWITCH_RETRIES,
            )
            self.assertEqual(store.update({"image_account_selection_max_attempts": 0})["image_account_selection_max_attempts"], 1)
            self.assertEqual(store.update({"image_poll_timeout_account_switch_retries": "invalid"})[
                "image_poll_timeout_account_switch_retries"
            ], DEFAULT_IMAGE_POLL_TIMEOUT_ACCOUNT_SWITCH_RETRIES)

    def test_image_executor_rejects_when_capacity_is_full(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def blocking_job() -> None:
            started.set()
            release.wait(timeout=5)

        executor = BoundedImageExecutor(workers=1, queue_size=1)
        try:
            first = executor.submit(blocking_job)
            self.assertTrue(started.wait(timeout=1))
            second = executor.submit(lambda: None)

            with self.assertRaises(HTTPException) as context:
                executor.submit(lambda: None)
            self.assertEqual(context.exception.status_code, 429)
            self.assertEqual(executor.stats()["inflight"], 2)
            self.assertTrue(executor.stats()["full"])

            release.set()
            first.result(timeout=2)
            second.result(timeout=2)
            self.assertEqual(executor.stats()["inflight"], 0)
            self.assertEqual(executor.stats()["available"], 2)
        finally:
            release.set()
            executor.shutdown(wait=True)

    def test_image_executor_reconfigure_updates_capacity(self) -> None:
        executor = BoundedImageExecutor(workers=2, queue_size=3)
        try:
            self.assertEqual(executor.stats()["capacity"], 5)
            executor.reconfigure(1000, 100)
            stats = executor.stats()
            self.assertEqual(stats["workers"], 1000)
            self.assertEqual(stats["queue_size"], 100)
            self.assertEqual(stats["capacity"], 1100)
        finally:
            executor.shutdown(wait=True)


if __name__ == "__main__":
    unittest.main()
