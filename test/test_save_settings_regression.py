"""回归测试：保存设置接口不得因监控缓存失效调用而 500。

历史 bug：save_settings 末尾调用了已不存在的 monitoring_cache.clear()（监控缓存早已
拆分为 runtime_cache / image_stats_cache），线上保存设置时抛 NameError 返回 500。
本测试锁定修复：保存路径跑通返回 200，且 SnapshotCache.clear() 真正清空条目。
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import system as routes
from services.monitoring_cache import SnapshotCache


class SnapshotCacheClearTests(unittest.TestCase):
    def test_clear_empties_entries(self):
        cache = SnapshotCache(capacity=4)

        async def scenario():
            await cache.get("k", AsyncMock(return_value={"x": 1}))
            self.assertTrue(cache.entries)
            cache.clear()
            self.assertEqual(len(cache.entries), 0)
            # 清空后仍可正常采集
            await cache.get("k", AsyncMock(return_value={"x": 2}))
            self.assertTrue(cache.entries)

        asyncio.run(scenario())


class SaveSettingsRegressionTests(unittest.TestCase):
    def setUp(self):
        fake_config = MagicMock()
        fake_config.update.return_value = {"ok": True}
        self.patchers = [
            patch.object(routes, "require_admin", lambda authorization: None),
            patch.object(routes, "config", fake_config),
            patch.object(routes, "apply_anyio_thread_limit", AsyncMock()),
            patch.object(routes, "configure_image_executors", MagicMock()),
            patch.object(routes, "account_service", MagicMock()),
            patch.object(routes, "completion_executor", MagicMock()),
        ]
        for p in self.patchers:
            p.start()
            self.addCleanup(p.stop)
        app = FastAPI()
        app.include_router(routes.create_router("test"))
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def test_save_settings_returns_200_not_nameerror(self):
        resp = self.client.post("/api/settings", json={}, headers={"Authorization": "Bearer x"})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json(), {"config": {"ok": True}})


if __name__ == "__main__":
    unittest.main()
