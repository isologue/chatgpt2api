import threading
import time
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api.accounts as routes
from services.account_service import AccountService
from test.test_account_refresh_token_status import MemoryStorage


class AccountImportProgressTests(unittest.TestCase):
    def setUp(self):
        self.service = AccountService(MemoryStorage())
        self.addCleanup(self.service.verification.close)
        self.service._save_cumulative_total = lambda: None
        self.gate = threading.Event()
        self.addCleanup(self.gate.set)
        self.patchers = [
            patch.object(routes, "account_service", self.service),
            patch.object(routes, "require_admin", self.require_admin),
            patch("services.account_service.log_service.add"),
        ]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        self.headers = {"Authorization": "Bearer test-admin"}
        app = FastAPI()
        app.include_router(routes.create_router())
        self.client = TestClient(app)
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)
        # Release blocked worker threads before closing the client's event loop.
        self.addCleanup(self.gate.set)
        self.service._fetch_remote_info = lambda token, *_: self.service.get_account(token)

    @staticmethod
    def require_admin(authorization):
        if authorization != "Bearer test-admin":
            raise HTTPException(status_code=401)

    def progress(self, progress_id):
        response = self.client.get(f"/api/accounts/refresh/progress/{progress_id}", headers=self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def completed(self, progress_id):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            progress = self.progress(progress_id)
            if progress["done"]:
                return progress
            time.sleep(0.02)
        self.fail("Import did not finish")

    def test_saving_progress_exists_before_worker_finishes_and_final_result_is_sanitized(self):
        original = self.service.add_account_items
        def blocked_save(items):
            if not self.gate.wait(5):
                raise RuntimeError("test save timed out")
            return original(items)
        self.service.add_account_items = blocked_save
        response = self.client.post("/api/accounts/import", headers=self.headers, json={
            "tokens": [" at ", "at", "extra"],
            "accounts": [{"access_token": "at", "refresh_token": "rt", "id_token": "id", "password": "pw"}],
        })
        self.assertEqual(response.status_code, 200, response.text)
        progress_id = response.json()["progress_id"]
        initial = self.progress(progress_id)
        self.assertEqual(initial["phase"], "saving")
        self.assertEqual(initial["total"], 2)
        self.assertEqual(initial["processed"], 0)
        self.assertFalse(initial["done"])
        self.gate.set()
        done = self.completed(progress_id)
        self.assertEqual(done["processed"], 2)
        self.assertEqual(done["result"]["added"], 2)
        self.assertEqual(done["result"]["refreshed"], 2)
        account = next(item for item in done["result"]["items"] if item["access_token"] == "at")
        self.assertTrue(account["has_refresh_token"])
        for secret in ("refresh_token", "id_token", "password"):
            self.assertNotIn(secret, account)

    def test_live_progress_advances_per_completed_account(self):
        def refresh(token, *_):
            if token == "slow" and not self.gate.wait(5):
                raise RuntimeError("test refresh timed out")
            return self.service.get_account(token)
        self.service._fetch_remote_info = refresh
        response = self.client.post("/api/accounts/import", headers=self.headers, json={"tokens": ["fast", "slow"]})
        progress_id = response.json()["progress_id"]
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            progress = self.progress(progress_id)
            if progress["processed"] == 1:
                break
            time.sleep(0.02)
        self.assertEqual(progress["phase"], "refreshing")
        self.assertEqual(progress["processed"], 1)
        self.assertFalse(progress["done"])
        self.gate.set()
        self.assertEqual(self.completed(progress_id)["processed"], 2)

    def test_duplicate_import_counts_existing_accounts(self):
        self.service.add_accounts(["existing"])
        response = self.client.post("/api/accounts/import", headers=self.headers, json={"tokens": ["existing", "existing", "new"]})
        result = self.completed(response.json()["progress_id"])["result"]
        self.assertEqual(result["added"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["refreshed"], 2)

    def test_large_import_can_finish_without_upstream_refresh(self):
        calls = []
        self.service._fetch_remote_info = lambda token, *_: calls.append(token)
        tokens = [f"token-{index}" for index in range(100)]
        response = self.client.post(
            "/api/accounts/import",
            headers=self.headers,
            json={"tokens": tokens, "refresh_after_import": False},
        )
        self.assertEqual(response.status_code, 200, response.text)
        progress = self.completed(response.json()["progress_id"])
        self.assertEqual(progress["phase"], "importing")
        self.assertEqual(progress["processed"], 100)
        self.assertEqual(progress["result"]["added"], 100)
        self.assertEqual(progress["result"]["refreshed"], 0)
        self.assertEqual(calls, [])

    def test_compact_import_avoids_full_account_snapshot(self):
        with patch.object(self.service, "list_accounts", side_effect=AssertionError("full snapshot forbidden")):
            response = self.client.post("/api/accounts/import?include_items=false", headers=self.headers,
                                        json={"tokens": ["compact"], "refresh_after_import": False})
            done = self.completed(response.json()["progress_id"])
        self.assertEqual(done["result"]["added"], 1)
        self.assertNotIn("items", done["result"])
        self.assertIsNotNone(self.service.get_account("compact"))

    def test_refresh_all_compact_mode_includes_accounts_beyond_first_page(self):
        self.service.add_accounts([f"t-{i}" for i in range(35)])
        response = self.client.post("/api/accounts/refresh?include_items=false", headers=self.headers,
                                    json={"access_tokens": []})
        done = self.completed(response.json()["progress_id"])
        self.assertEqual(done["total"], 35)
        self.assertEqual(done["result"]["refreshed"], 35)
        self.assertNotIn("items", done["result"])

    def test_compact_update_delete_and_refresh_return_no_full_pool(self):
        self.service.add_accounts(["a", "b"])
        response = self.client.post("/api/accounts/update?include_items=false", headers=self.headers,
                                    json={"access_token": "a", "quota": 20})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("items", response.json())
        self.assertEqual(response.json()["item"]["quota"], 20)
        response = self.client.request("DELETE", "/api/accounts?include_items=false", headers=self.headers,
                                       json={"tokens": ["a"]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {"removed": 1})
        self.assertIsNotNone(self.service.get_account("b"))

    def test_save_failure_finishes_progress_with_error(self):
        def fail(_tokens):
            raise RuntimeError("storage write failed")
        self.service.add_accounts = fail
        response = self.client.post("/api/accounts/import", headers=self.headers, json={"tokens": ["at"]})
        result = self.completed(response.json()["progress_id"])
        self.assertEqual(result["error"], "storage write failed")
        self.assertIsNone(result["result"])

    def test_original_sync_import_remains_compatible(self):
        response = self.client.post("/api/accounts", headers=self.headers, json={"tokens": ["at"]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["added"], 1)
        self.assertEqual(response.json()["refreshed"], 1)
        self.assertNotIn("progress_id", response.json())

    def test_empty_input_and_unauthorized_requests_are_rejected(self):
        self.assertEqual(self.client.post("/api/accounts/import", headers=self.headers, json={"tokens": [" "]}).status_code, 400)
        self.assertEqual(self.client.post("/api/accounts/import", json={"tokens": ["at"]}).status_code, 401)
        self.assertEqual(self.client.get("/api/accounts/refresh/progress/unknown").status_code, 401)


if __name__ == "__main__":
    unittest.main()
