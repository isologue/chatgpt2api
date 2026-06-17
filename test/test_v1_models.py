from __future__ import annotations

import json
import unittest
from unittest import mock

import requests

from services.protocol import openai_v1_models
from utils.helper import standardize_model_name, upstream_text_model_name


AUTH_KEY = "chatgpt2api"
BASE_URL = "http://localhost:8000"


class ModelListTests(unittest.TestCase):
    def test_list_models_standardizes_gpt_5_5_names(self):
        with (
            mock.patch.object(
                openai_v1_models.OpenAIBackendAPI,
                "list_models",
                return_value={
                    "object": "list",
                    "data": [
                        {"id": "gpt-5-3", "object": "model", "created": 0, "owned_by": "chatgpt", "permission": [], "root": "gpt-5-3", "parent": None},
                        {"id": "gpt-5-5", "object": "model", "created": 0, "owned_by": "chatgpt", "permission": [], "root": "gpt-5-5", "parent": None},
                        {"id": "gpt-5-6", "object": "model", "created": 0, "owned_by": "chatgpt", "permission": [], "root": "gpt-5-6", "parent": None},
                        {"id": "gpt-5-8", "object": "model", "created": 0, "owned_by": "chatgpt", "permission": [], "root": "gpt-5-8", "parent": None},
                        {"id": "gpt-5-5-thinking", "object": "model", "created": 0, "owned_by": "chatgpt", "permission": [], "root": "gpt-5-5-thinking", "parent": None},
                    ],
                },
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-5.3", ids)
        self.assertIn("gpt-5.5", ids)
        self.assertIn("gpt-5.6", ids)
        self.assertIn("gpt-5.8", ids)
        self.assertIn("gpt-5.5-thinking", ids)
        self.assertNotIn("gpt-5-3", ids)
        self.assertNotIn("gpt-5-5", ids)
        self.assertNotIn("gpt-5-6", ids)
        self.assertNotIn("gpt-5-8", ids)
        self.assertNotIn("gpt-5-5-thinking", ids)

    def test_standardize_model_name_helper(self):
        self.assertEqual(standardize_model_name("gpt-5-3"), "gpt-5.3")
        self.assertEqual(standardize_model_name("gpt-5-5"), "gpt-5.5")
        self.assertEqual(standardize_model_name("gpt-5-6"), "gpt-5.6")
        self.assertEqual(standardize_model_name("gpt-5-8"), "gpt-5.8")
        self.assertEqual(standardize_model_name("gpt-5-5-thinking"), "gpt-5.5-thinking")
        self.assertEqual(standardize_model_name("gpt-image-2"), "gpt-image-2")

    def test_upstream_text_model_name_helper(self):
        self.assertEqual(upstream_text_model_name("gpt-5.3"), "gpt-5-3")
        self.assertEqual(upstream_text_model_name("gpt-5.5"), "gpt-5-5")
        self.assertEqual(upstream_text_model_name("gpt-5.6"), "gpt-5-6")
        self.assertEqual(upstream_text_model_name("gpt-5.8"), "gpt-5-8")
        self.assertEqual(upstream_text_model_name("gpt-5.5-thinking"), "gpt-5-5-thinking")
        self.assertEqual(upstream_text_model_name("gpt-image-2"), "gpt-image-2")

    def test_list_models_only_returns_image_models_backed_by_account_types(self):
        with (
            mock.patch.object(
                openai_v1_models.OpenAIBackendAPI,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "token-free", "type": "free"},
                    {"access_token": "token-web-team", "type": "Team", "source_type": "web"},
                    {"access_token": "token-codex-team", "type": "Team", "source_type": "codex"},
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-image-2", ids)
        self.assertIn("codex-gpt-image-2", ids)
        self.assertIn("team-codex-gpt-image-2", ids)
        self.assertNotIn("plus-codex-gpt-image-2", ids)
        self.assertNotIn("pro-codex-gpt-image-2", ids)

    def test_list_models_does_not_return_codex_models_for_web_plus_accounts(self):
        with (
            mock.patch.object(
                openai_v1_models.OpenAIBackendAPI,
                "list_models",
                return_value={"object": "list", "data": []},
            ),
            mock.patch.object(
                openai_v1_models.account_service,
                "list_accounts",
                return_value=[
                    {"access_token": "token-web-plus", "type": "Plus", "source_type": "web"},
                ],
            ),
        ):
            result = openai_v1_models.list_models()

        ids = {item["id"] for item in result["data"]}
        self.assertIn("gpt-image-2", ids)
        self.assertNotIn("codex-gpt-image-2", ids)
        self.assertNotIn("plus-codex-gpt-image-2", ids)

    def test_list_models_function(self):
        """测试直接调用服务层获取模型列表。"""
        result = openai_v1_models.list_models()
        print("function result:")
        print(json.dumps(result, ensure_ascii=False, indent=2))

    def test_list_models_http(self):
        """测试通过 HTTP 接口获取模型列表。"""
        response = requests.get(
            f"{BASE_URL}/v1/models",
            headers={"Authorization": f"Bearer {AUTH_KEY}"},
            timeout=30,
        )
        print("http status:")
        print(response.status_code)
        print("http result:")
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
