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

if __name__ == "__main__":
    unittest.main()
