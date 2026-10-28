from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.log_service import LogService, json_safe


class LogServiceTests(unittest.TestCase):
    def test_list_omits_bodies_but_detail_loads_them(self):
        with tempfile.TemporaryDirectory() as temp_dir, LogService(Path(temp_dir) / "logs.jsonl") as service:
            service.add(
                "call",
                "test call",
                {"status": "success", "request_body": {"prompt": "cat"}, "response_body": {"data": "large"}},
            )

            listed = service.list()
            self.assertEqual(len(listed), 1)
            self.assertNotIn("request_body", listed[0]["detail"])
            self.assertNotIn("response_body", listed[0]["detail"])

            detailed = service.get(listed[0]["id"])
            self.assertEqual(detailed["detail"]["request_body"], {"prompt": "cat"})
            self.assertEqual(detailed["detail"]["response_body"], {"data": "large"})

    def test_json_safe_encodes_uploaded_bytes(self):
        self.assertEqual(json_safe({"file": b"image-bytes"}), {"file": "aW1hZ2UtYnl0ZXM="})


if __name__ == "__main__":
    unittest.main()
