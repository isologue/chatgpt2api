from __future__ import annotations

import unittest

from services.register.openai_register import _is_cloudflare_challenge


class _FakeResponse:
    def __init__(self, *, status_code: int, text: str = "", headers: dict | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}


class OpenAIRegisterTests(unittest.TestCase):
    def test_cloudflare_server_header_alone_is_not_treated_as_challenge(self) -> None:
        resp = _FakeResponse(
            status_code=403,
            text='{"error":{"message":"forbidden"}}',
            headers={"server": "cloudflare", "content-type": "application/json"},
        )

        self.assertFalse(_is_cloudflare_challenge(resp))

    def test_html_challenge_page_is_detected(self) -> None:
        resp = _FakeResponse(
            status_code=403,
            text="<html><title>Just a moment...</title></html>",
            headers={"server": "cloudflare", "content-type": "text/html"},
        )

        self.assertTrue(_is_cloudflare_challenge(resp))


if __name__ == "__main__":
    unittest.main()
