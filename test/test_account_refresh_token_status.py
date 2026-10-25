import unittest
from typing import Any

from api.accounts import _sanitize_account_response
from services.account_service import AccountService


class MemoryStorage:
    def __init__(self, accounts: list[dict[str, Any]] | None = None) -> None:
        self.accounts = list(accounts or [])

    def load_accounts(self) -> list[dict[str, Any]]:
        return list(self.accounts)

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        self.accounts = list(accounts)

    def load_auth_keys(self) -> list[dict[str, Any]]:
        return []

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        pass

    def health_check(self) -> dict[str, Any]:
        return {"ok": True}

    def get_backend_info(self) -> dict[str, Any]:
        return {"type": "memory"}


class RefreshTokenStatusTests(unittest.TestCase):
    def test_refresh_token_status_distinguishes_unverified_success_and_failures(self) -> None:
        def status(**fields: Any) -> str:
            service = AccountService(MemoryStorage([{"access_token": "at", **fields}]))
            return service.list_accounts()[0]["refresh_token_status"]

        self.assertEqual(status(), "missing")
        self.assertEqual(status(refresh_token="rt"), "unverified")
        self.assertEqual(
            status(refresh_token="rt", last_token_refresh_at="2026-10-25T01:02:03+00:00"),
            "valid",
        )
        self.assertEqual(
            status(refresh_token="rt", last_token_refresh_error="oauth_refresh_http_400: invalid_grant"),
            "invalid",
        )
        self.assertEqual(
            status(refresh_token="rt", last_token_refresh_error="oauth_refresh_http_502: upstream unavailable"),
            "pending",
        )
        self.assertEqual(
            status(refresh_token="rt", last_token_refresh_error="The refresh token has been revoked"),
            "invalid",
        )

    def test_replacing_refresh_token_clears_old_verification_history(self) -> None:
        service = AccountService(
            MemoryStorage(
                [
                    {
                        "access_token": "at",
                        "refresh_token": "old_rt",
                        "last_token_refresh_at": "2026-10-24T01:02:03+00:00",
                    }
                ]
            )
        )

        service.add_account_items([{"access_token": "at", "refresh_token": "new_rt"}])
        account = service.get_account("at")

        self.assertIsNotNone(account)
        self.assertEqual(account["refresh_token_status"], "unverified")
        self.assertIsNone(account["last_token_refresh_at"])
        self.assertIsNone(account["last_token_refresh_error"])
        self.assertEqual(account["refresh_token"], "new_rt")

    def test_account_api_sanitizer_removes_oauth_secrets_and_keeps_status(self) -> None:
        response = {
            "result": {
                "items": [
                    {
                        "access_token": "at",
                        "refresh_token": "rt",
                        "id_token": "id",
                        "password": "pw",
                        "last_token_refresh_error": "invalid_grant",
                        "refresh_token_status": "invalid",
                    }
                ]
            }
        }

        sanitized = _sanitize_account_response(response)
        item = sanitized["result"]["items"][0]

        self.assertEqual(item["access_token"], "at")
        self.assertTrue(item["has_refresh_token"])
        self.assertEqual(item["refresh_token_status"], "invalid")
        self.assertEqual(item["last_token_refresh_error"], "invalid_grant")
        self.assertNotIn("refresh_token", item)
        self.assertNotIn("id_token", item)
        self.assertNotIn("password", item)


if __name__ == "__main__":
    unittest.main()
