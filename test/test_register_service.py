from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.register_service import RegisterService


class RegisterServiceTests(unittest.TestCase):
    def test_empty_top_level_proxy_clears_mail_proxy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "register.json"
            path.write_text(
                json.dumps(
                    {
                        "mail": {
                            "request_timeout": 30,
                            "wait_timeout": 30,
                            "wait_interval": 2,
                            "providers": [],
                            "proxy": "http://127.0.0.1:7890",
                        },
                        "proxy": "",
                        "total": 1,
                        "threads": 1,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            service = RegisterService(path)

            self.assertEqual(service.get()["proxy"], "")
            self.assertEqual(service.get()["mail"]["proxy"], "")

            service.update({"proxy": ""})

            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["mail"]["proxy"], "")

    def test_schedule_config_is_normalized_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "register.json"
            path.write_text(
                json.dumps(
                    {
                        "mail": {
                            "request_timeout": 30,
                            "wait_timeout": 30,
                            "wait_interval": 2,
                            "providers": [],
                        },
                        "schedule_enabled": True,
                        "schedule_interval_minutes": 0,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            service = RegisterService(path)
            config = service.get()

            self.assertTrue(config["schedule_enabled"])
            self.assertEqual(config["schedule_interval_minutes"], 1)
            self.assertIsNotNone(config["next_scheduled_at"])
            self.assertIsNotNone(config["schedule_started_at"])

            updated = service.update({
                "schedule_enabled": True,
                "schedule_interval_minutes": 7,
            })

            self.assertTrue(updated["schedule_enabled"])
            self.assertEqual(updated["schedule_interval_minutes"], 7)
            self.assertIsNotNone(updated["next_scheduled_at"])

            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(saved["schedule_enabled"])
            self.assertEqual(saved["schedule_interval_minutes"], 7)
            self.assertTrue(saved["next_scheduled_at"])


if __name__ == "__main__":
    unittest.main()
