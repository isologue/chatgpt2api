from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StorageBackend(ABC):
    """抽象存储后端基类"""

    @abstractmethod
    def load_accounts(self) -> list[dict[str, Any]]:
        """加载所有账号数据"""
        pass

    @abstractmethod
    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存所有账号数据"""
        pass

    supports_incremental_account_writes = False

    def list_accounts_page(
        self,
        page: int = 1,
        page_size: int = 20,
        query: str = "",
        account_type: str = "all",
        status: str = "all",
        source: str = "all", rt_status: str = "all", pool: str = "all",
        score_min: int | None = None, score_max: int | None = None,
        created_from: float | None = None, created_to: float | None = None,
    ) -> dict[str, Any] | None:
        """Return a database-backed account page when the backend supports it.

        Backends that store accounts as a JSON snapshot return ``None`` and let
        AccountService use its compatibility implementation.
        """
        return None

    def save_account(self, account: dict[str, Any]) -> None:
        """Optional single-account persistence for backends that support it."""
        raise NotImplementedError

    def delete_account(self, access_token: str) -> None:
        """Optional single-account deletion for backends that support it."""
        raise NotImplementedError

    @abstractmethod
    def load_auth_keys(self) -> list[dict[str, Any]]:
        """加载所有鉴权密钥数据"""
        pass

    @abstractmethod
    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存所有鉴权密钥数据"""
        pass

    @abstractmethod
    def health_check(self) -> dict[str, Any]:
        """健康检查，返回存储后端状态"""
        pass

    @abstractmethod
    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        pass
