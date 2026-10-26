import json
import threading
import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import accounts as routes
from services.account_service import AccountService


class AccountPaginationTests(unittest.TestCase):
    def setUp(self):
        # 隔离内存号池，不接触本机账号、数据库或累计计数文件。
        self.service = AccountService.__new__(AccountService)
        self.service._lock = threading.Lock()
        self.service._image_inflight = {"token-0": 2}
        self.service._accounts = {
            f"token-{i}": {
                "access_token": f"token-{i}", "email": f"user{i:06d}@example.test",
                "type": "Plus" if i % 2 else "Free", "status": "异常" if i % 4 == 0 else "正常",
                "quota": 3, "refresh_token": "secret", "password": "secret", "id_token": "secret",
                "upstream_blob": "x" * 10000,
            }
            for i in range(3000)
        }
        self.patchers = [patch.object(routes, "account_service", self.service),
                         patch.object(routes, "require_admin", self.require_admin)]
        for patcher in self.patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        app = FastAPI()
        app.include_router(routes.create_router())
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer test-admin"}

    @staticmethod
    def require_admin(authorization):
        from fastapi import HTTPException
        if authorization != "Bearer test-admin":
            raise HTTPException(status_code=401)

    def get(self, query):
        response = self.client.get("/api/accounts?" + query, headers=self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def test_page_is_bounded_and_does_not_call_legacy_full_list(self):
        with patch.object(self.service, "list_accounts", side_effect=AssertionError("full scan copy")):
            data = self.get("page=2&page_size=10")
        self.assertEqual(data["total"], 3000)
        self.assertEqual(len(data["items"]), 10)
        self.assertEqual(data["items"][0]["access_token"], "token-10")
        self.assertEqual(data["summary"], dict(total=3000, active=2250, abnormal=750, limited=0, disabled=0, quota=6750))
        self.assertEqual(data["types"], ["Free", "Plus"])
        self.assertLess(len(json.dumps(data)), 10000)
        for item in data["items"]:
            self.assertTrue(item["has_refresh_token"])
            for secret in ("refresh_token", "password", "id_token", "upstream_blob"):
                self.assertNotIn(secret, item)
        self.assertEqual(self.get("page=1")["items"][0]["image_inflight"], 2)

    def test_server_search_and_filters_apply_to_whole_pool(self):
        data = self.get("page=1&page_size=10&query=USER002&account_type=Free&status=异常")
        self.assertEqual(data["total"], 250)
        self.assertEqual(data["summary"]["total"], 3000)
        for item in data["items"]:
            self.assertIn("user002", item["email"])
            self.assertEqual(item["type"], "Free")
            self.assertEqual(item["status"], "异常")

    def test_empty_and_out_of_range_pages(self):
        data = self.get("page=999&page_size=100")
        self.assertEqual(data["page"], 30)
        self.assertEqual(len(data["items"]), 100)
        empty = self.get("page=99&query=nonexistent")
        self.assertEqual(empty["items"], [])
        self.assertEqual(empty["page"], 1)

    def test_invalid_limits_and_authorization(self):
        for query in ("page=0", "page=1&page_size=101", "page=1&page_size=0"):
            self.assertEqual(self.client.get("/api/accounts?" + query, headers=self.headers).status_code, 422)
        self.assertEqual(self.client.get("/api/accounts?page=1").status_code, 401)
        self.assertEqual(self.client.get("/api/accounts/tokens").status_code, 401)

    def test_tokens_and_legacy_clients_preserve_whole_pool_semantics(self):
        response = self.client.get("/api/accounts/tokens?status=异常", headers=self.headers)
        self.assertEqual(len(response.json()["tokens"]), 750)
        data = self.client.get("/api/accounts", headers=self.headers).json()
        self.assertEqual(len(data["items"]), 3000)
        self.assertNotIn("password", data["items"][0])

    def test_compact_progress_does_not_mutate_stored_result(self):
        progress = {"done": True, "result": {"items": [{"access_token": "x"}], "refreshed": 1}}
        with patch.object(self.service, "get_refresh_progress", return_value=progress):
            data = self.client.get("/api/accounts/refresh/progress/test?include_items=false", headers=self.headers).json()
        self.assertEqual(data["result"], {"refreshed": 1})
        self.assertIn("items", progress["result"])


if __name__ == "__main__":
    unittest.main()
