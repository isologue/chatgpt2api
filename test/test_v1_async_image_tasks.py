from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
import api.image_tasks as image_tasks_module
from services.image_task_service import ImageTaskService

AUTH_HEADERS = {"Authorization": "Bearer test-token"}
IDENTITY = {"id": "test-user", "name": "Tester", "role": "user"}


class V1AsyncImageTaskApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.started = threading.Event()
        self.release = threading.Event()

        def handler(payload):
            prompt = payload["prompt"]
            if prompt == "fail case":
                raise RuntimeError("simulated upstream failure")
            if prompt == "running case":
                payload["progress_callback"]("generating")
                self.started.set()
                self.release.wait(timeout=5)
            return {"created": 1792650081, "data": [{"url": "https://images.example.test/result.png", "revised_prompt": "internal"}]}

        self.service = ImageTaskService(
            Path(self.temp_dir.name) / "image_tasks.json",
            generation_handler=handler,
        )
        self.service._log_call = lambda *_args, **_kwargs: None

        patchers = [
            mock.patch.object(ai_module, "image_task_service", self.service),
            mock.patch.object(image_tasks_module, "image_task_service", self.service),
            mock.patch.object(ai_module, "require_identity", return_value=IDENTITY),
            mock.patch.object(image_tasks_module, "require_identity", return_value=IDENTITY),
            mock.patch.object(ai_module, "check_request"),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

        app = FastAPI()
        app.include_router(ai_module.create_router())
        app.include_router(image_tasks_module.create_router())
        self.client = TestClient(app)

    def tearDown(self):
        self.release.set()
        self.service.close()
        self.temp_dir.cleanup()

    def create_task(self, task_id: str, prompt: str):
        return self.client.post(
            "/v1/images/generations",
            headers=AUTH_HEADERS,
            json={"async": True, "client_task_id": task_id, "prompt": prompt},
        )

    def wait_for_status(self, task_id: str, expected_status: str):
        deadline = time.monotonic() + 3
        last_response = None
        while time.monotonic() < deadline:
            last_response = self.client.get(f"/v1/images/tasks/{task_id}", headers=AUTH_HEADERS)
            if last_response.status_code == 200 and last_response.json()["status"] == expected_status:
                return last_response.json()
            time.sleep(0.01)
        self.fail(
            f"task {task_id} did not reach {expected_status}: "
            f"{last_response.text if last_response else 'no response'}"
        )

    def test_async_creation_and_query_cover_error_running_and_success(self):
        created_error = self.create_task("task-error", "fail case")
        self.assertEqual(created_error.status_code, 200, created_error.text)
        self.assertEqual(set(created_error.json()), {"created_at", "model", "status", "task_id"})
        self.assertEqual(created_error.json()["task_id"], "task-error")
        self.assertEqual(created_error.json()["status"], "queued")
        failed = self.wait_for_status("task-error", "failed")
        self.assertEqual(failed["error"]["message"], "simulated upstream failure")

        created_running = self.create_task("task-running", "running case")
        self.assertEqual(created_running.status_code, 200, created_running.text)
        self.assertTrue(self.started.wait(timeout=2))
        running_response = self.client.get("/v1/images/tasks/task-running", headers=AUTH_HEADERS)
        self.assertEqual(running_response.status_code, 200, running_response.text)
        self.assertEqual(running_response.json()["status"], "processing")
        self.assertEqual(running_response.json()["progress"], "42%")
        self.release.set()

        created_success = self.create_task("task-success", "success case")
        self.assertEqual(created_success.status_code, 200, created_success.text)
        succeeded = self.wait_for_status("task-success", "succeeded")
        self.assertEqual(succeeded["progress"], "100%")
        self.assertEqual(succeeded["data"]["created"], 1792650081)
        self.assertEqual(succeeded["data"]["data"][0]["url"], "https://images.example.test/result.png")
        self.assertNotIn("revised_prompt", succeeded["data"]["data"][0])


if __name__ == "__main__":
    unittest.main()
