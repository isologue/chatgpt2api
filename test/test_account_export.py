import base64
import json
import unittest
from typing import Any

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


def make_jwt(payload: dict[str, Any]) -> str:
    def encode(value: dict[str, Any]) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    return f'{encode({"alg": "none", "typ": "JWT"})}.{encode(payload)}.sig'


class AccountExportTests(unittest.TestCase):
    def test_build_export_items_uses_codex_shape_and_jwt_claims(self) -> None:
        access_token = make_jwt(
            {
                "exp": 0,
                "iat": 3600,
                "https://api.openai.com/auth": {"chatgpt_account_id": "acct_123"},
                "https://api.openai.com/profile": {"email": "test@example.com"},
            }
        )
        id_token = make_jwt({"email": "fallback@example.com"})
        service = AccountService(
            MemoryStorage(
                [
                    {
                        "access_token": access_token,
                        "id_token": id_token,
                        "refresh_token": "rt_test",
                    }
                ]
            )
        )

        [item] = service.build_export_items([access_token])

        self.assertEqual(item["type"], "codex")
        self.assertEqual(item["email"], "test@example.com")
        self.assertEqual(item["expired"], "1970-01-01T08:00:00+08:00")
        self.assertEqual(item["account_id"], "acct_123")
        self.assertEqual(item["access_token"], access_token)
        self.assertEqual(item["last_refresh"], "1970-01-01T09:00:00+08:00")
        self.assertEqual(item["id_token"], id_token)
        self.assertEqual(item["refresh_token"], "rt_test")

    def test_build_export_items_skips_accounts_missing_complete_tokens(self) -> None:
        complete_access_token = make_jwt({"exp": 0})
        complete_id_token = make_jwt({"email": "complete@example.com"})
        service = AccountService(
            MemoryStorage(
                [
                    {"access_token": "only_access"},
                    {"access_token": "missing_id", "refresh_token": "rt_missing_id"},
                    {"access_token": complete_access_token, "id_token": complete_id_token, "refresh_token": "rt_complete"},
                ]
            )
        )

        items = service.build_export_items()

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["access_token"], complete_access_token)
        self.assertEqual(items[0]["id_token"], complete_id_token)
        self.assertEqual(items[0]["refresh_token"], "rt_complete")

    def test_build_sub2api_export_payload_matches_sample_shape_and_filters_selection(self) -> None:
        access_token = make_jwt(
            {
                "exp": 1800000000,
                "iat": 1700000000,
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": "chatgpt-account",
                    "chatgpt_user_id": "chatgpt-user",
                    "chatgpt_plan_type": "plus",
                },
                "https://api.openai.com/profile": {"email": "sub@example.com"},
            }
        )
        id_token = make_jwt({"email": "sub@example.com"})
        service = AccountService(
            MemoryStorage(
                [
                    {
                        "access_token": access_token,
                        "id_token": id_token,
                        "refresh_token": "rt_sub",
                        "session_token": "session_sub",
                        "account_id": "stored-account",
                        "email": "sub@example.com",
                        "type": "Plus",
                        "source_type": "codex",
                    },
                    {"access_token": "incomplete"},
                    {
                        "access_token": make_jwt({"exp": 1800000000}),
                        "id_token": id_token,
                        "refresh_token": "rt_unselected",
                    },
                ]
            )
        )

        payload = service.build_sub2api_export_payload([access_token])

        self.assertEqual(set(payload), {"exported_at", "proxies", "accounts"})
        self.assertTrue(payload["exported_at"].endswith("Z"))
        self.assertEqual(payload["proxies"], [])
        self.assertEqual(len(payload["accounts"]), 1)
        account = payload["accounts"][0]
        self.assertEqual(
            set(account),
            {
                "name", "platform", "type", "account_id", "chatgpt_account_id", "chatgpt_user_id",
                "session_token", "credentials", "extra", "concurrency", "priority", "rate_multiplier",
                "auto_pause_on_expired",
            },
        )
        self.assertEqual(account["platform"], "openai")
        self.assertEqual(account["type"], "oauth")
        self.assertEqual(account["account_id"], "stored-account")
        self.assertEqual(account["chatgpt_account_id"], "chatgpt-account")
        self.assertEqual(account["chatgpt_user_id"], "chatgpt-user")
        self.assertEqual(account["session_token"], "session_sub")
        self.assertEqual(account["credentials"]["access_token"], access_token)
        self.assertEqual(account["credentials"]["refresh_token"], "rt_sub")
        self.assertEqual(account["credentials"]["id_token"], id_token)
        self.assertEqual(account["credentials"]["session_token"], "session_sub")
        self.assertEqual(
            set(account["credentials"]),
            {
                "access_token", "id_token", "session_token", "chatgpt_account_id", "chatgpt_user_id",
                "client_id", "email", "expires_at", "expires_in", "model_mapping", "organization_id",
                "plan_type", "refresh_token",
            },
        )
        self.assertEqual(account["credentials"]["organization_id"], "chatgpt-account")
        self.assertEqual(account["extra"]["email"], "sub@example.com")
        self.assertEqual(set(account["extra"]), {"email", "auth_provider", "source", "plan_type"})
        self.assertEqual(account["extra"]["source"], "codex")
        self.assertEqual(account["concurrency"], 10)
        self.assertEqual(account["priority"], 1)
        self.assertEqual(account["rate_multiplier"], 1)
        self.assertTrue(account["auto_pause_on_expired"])

    def test_build_sub2api_export_payload_skips_accounts_without_refresh_or_id_token(self) -> None:
        service = AccountService(
            MemoryStorage(
                [
                    {"access_token": make_jwt({"exp": 1800000000}), "refresh_token": "rt_missing_id"},
                    {"access_token": "only_access"},
                ]
            )
        )

        self.assertEqual(service.build_sub2api_export_payload()["accounts"], [])

    def test_add_account_items_preserves_export_fields_without_overwriting_plan_type(self) -> None:
        service = AccountService(MemoryStorage())

        result = service.add_account_items(
            [
                {
                    "type": "codex",
                    "access_token": "access_token_test",
                    "refresh_token": "rt_test",
                    "account_id": "acct_123",
                }
            ]
        )

        account = service.get_account("access_token_test")
        self.assertEqual(result["added"], 1)
        self.assertIsNotNone(account)
        self.assertEqual(account["type"], "free")
        self.assertEqual(account["export_type"], "codex")
        self.assertEqual(account["refresh_token"], "rt_test")
        self.assertEqual(account["account_id"], "acct_123")


if __name__ == "__main__":
    unittest.main()
