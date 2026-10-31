from __future__ import annotations

import json
import os
from typing import Any

from sqlalchemy import (
    Column,
    Integer,
    Float,
    Index,
    String,
    Text,
    create_engine,
    delete,
    func,
    inspect,
    or_,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from services.storage.base import StorageBackend
from services.account_list_fields import account_list_fields

Base = declarative_base()


class AccountModel(Base):
    """账号数据模型"""
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    access_token = Column(String(2048), unique=True, nullable=False, index=True)
    email = Column(String(320), nullable=False, default="", index=True)
    account_type = Column("type", String(64), nullable=False, default="Free", index=True)
    status = Column(String(32), nullable=False, default="", index=True)
    quota = Column(Integer, nullable=False, default=0, index=True)
    source_type = Column(String(64), nullable=False, default="web", index=True)
    refresh_token_status = Column(String(16), nullable=False, default="missing", index=True)
    image_pool_hot = Column(Integer, nullable=False, default=0, index=True)
    image_score = Column(Integer, nullable=False, default=50, index=True)
    created_at_epoch = Column(Float, nullable=True, index=True)
    list_index_version = Column(Integer, nullable=False, default=0, index=True)
    data = Column(Text, nullable=False)  # JSON 格式存储完整账号数据

    __table_args__ = (
        Index("ix_accounts_email_type_status", "email", "type", "status"),
    )


class AuthKeyModel(Base):
    """鉴权密钥数据模型"""
    __tablename__ = "auth_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String(255), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)


class DatabaseStorageBackend(StorageBackend):
    """数据库存储后端（支持 SQLite、PostgreSQL、MySQL 等）"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        engine_options = {
            "pool_pre_ping": True,  # 自动检测连接是否有效
            "pool_recycle": 3600,   # 1小时回收连接
        }
        # The default SQLAlchemy pool is small for the image worker pool: every
        # completion updates account state and task state independently. Keep
        # SQLite on its native pool, while allowing PostgreSQL/MySQL to serve
        # concurrent image requests without queueing behind five connections.
        if not database_url.lower().startswith("sqlite"):
            try:
                pool_size = max(5, int(os.getenv("CHATGPT2API_DB_POOL_SIZE", "20")))
            except ValueError:
                pool_size = 20
            try:
                max_overflow = max(0, int(os.getenv("CHATGPT2API_DB_MAX_OVERFLOW", "40")))
            except ValueError:
                max_overflow = 40
            engine_options.update({
                "pool_size": pool_size,
                "max_overflow": max_overflow,
                "pool_timeout": 30,
            })
        self.engine = create_engine(database_url, **engine_options)
        Base.metadata.create_all(self.engine)
        self._ensure_account_schema()
        self.Session = sessionmaker(bind=self.engine)

    @staticmethod
    def _account_index_values(account: dict[str, Any]) -> dict[str, Any]:
        """Extract the fields used by the management list indexes."""
        try:
            quota = max(0, int(account.get("quota") or 0))
        except (TypeError, ValueError):
            quota = 0
        return {
            "email": str(account.get("email") or "").strip(),
            "account_type": str(account.get("type") or "Free").strip() or "Free",
            "status": str(account.get("status") or "").strip(),
            "quota": quota,
            **account_list_fields(account),
            "list_index_version": 1,
        }

    def _ensure_account_schema(self) -> None:
        """Add indexed columns to databases created by older releases."""
        inspector = inspect(self.engine)
        existing = {column["name"] for column in inspector.get_columns("accounts")}
        definitions = {
            "email": 'VARCHAR(320) NOT NULL DEFAULT \'\'',
            "type": 'VARCHAR(64) NOT NULL DEFAULT \'Free\'',
            "status": 'VARCHAR(32) NOT NULL DEFAULT \'\'',
            "quota": "INTEGER NOT NULL DEFAULT 0",
            "source_type": "VARCHAR(64) NOT NULL DEFAULT 'web'",
            "refresh_token_status": "VARCHAR(16) NOT NULL DEFAULT 'missing'",
            "image_pool_hot": "INTEGER NOT NULL DEFAULT 0",
            "image_score": "INTEGER NOT NULL DEFAULT 50",
            "created_at_epoch": "DOUBLE PRECISION",
            "list_index_version": "INTEGER NOT NULL DEFAULT 0",
        }
        with self.engine.begin() as connection:
            for name, definition in definitions.items():
                if name not in existing:
                    connection.execute(text(f'ALTER TABLE accounts ADD COLUMN "{name}" {definition}'))

        # create_all() does not add indexes to an already existing table.
        for index in AccountModel.__table__.indexes:
            index.create(self.engine, checkfirst=True)

        # PostgreSQL's normal B-tree email index helps prefix lookups, while
        # the UI deliberately uses contains/fuzzy matching. Use trigram search
        # when the database role can install the standard extension.
        if self.engine.dialect.name == "postgresql":
            try:
                with self.engine.begin() as connection:
                    connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    connection.execute(text(
                        "CREATE INDEX IF NOT EXISTS ix_accounts_email_trgm "
                        "ON accounts USING gin (email gin_trgm_ops)"
                    ))
            except Exception:
                # A managed PostgreSQL instance may deny extension creation;
                # the ordinary email index and all other indexes remain valid.
                pass

        # Existing installations have the account metadata only in JSON. The
        # one-time backfill makes subsequent list/search requests index-only.
        session = self.Session() if hasattr(self, "Session") else sessionmaker(bind=self.engine)()
        try:
            # Keyset batches keep memory bounded and never commit an open
            # server-side cursor. A version marker resumes interrupted backfills.
            last_id = 0
            while True:
                rows = session.query(AccountModel).filter(
                    AccountModel.id > last_id,
                    or_(AccountModel.list_index_version.is_(None), AccountModel.list_index_version < 1,
                        AccountModel.email.is_(None), AccountModel.account_type.is_(None),
                        AccountModel.status.is_(None), AccountModel.quota.is_(None)),
                ).order_by(AccountModel.id).limit(500).all()
                if not rows:
                    break
                last_id = rows[-1].id
                for row in rows:
                    try:
                        account = json.loads(row.data)
                    except (TypeError, json.JSONDecodeError):
                        account = {}
                    values = self._account_index_values(account if isinstance(account, dict) else {})
                    for key, value in values.items():
                        setattr(row, key, value)
                session.commit()
        finally:
            session.close()

    def load_accounts(self) -> list[dict[str, Any]]:
        """从数据库加载账号数据"""
        session = self.Session()
        try:
            accounts = []
            for row in session.query(AccountModel.data).yield_per(500):
                try:
                    account_data = json.loads(row.data)
                    if isinstance(account_data, dict):
                        accounts.append(account_data)
                except json.JSONDecodeError:
                    continue
            return accounts
        finally:
            session.close()

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到数据库"""
        self._save_rows(AccountModel, accounts, "access_token")

    supports_incremental_account_writes = True

    def save_account(self, account: dict[str, Any]) -> None:
        self.save_account_batch([account])

    def save_account_batch(self, accounts: list[dict[str, Any]]) -> None:
        self.apply_account_changes(accounts, [])

    def apply_account_changes(self, accounts: list[dict[str, Any]], access_tokens: list[str]) -> None:
        """Commit ordered account changes in one transaction, without read-before-write."""
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        elif dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert
        else:
            raise RuntimeError(f"unsupported account upsert dialect: {dialect}")
        rows = [{
            "access_token": item["access_token"],
            "data": json.dumps(item, ensure_ascii=False, separators=(",", ":")),
            **self._account_index_values(item),
        }
                for item in accounts if item.get("access_token")]
        with self.Session.begin() as session:
            for start in range(0, len(access_tokens), 500):
                session.execute(delete(AccountModel).where(AccountModel.access_token.in_(access_tokens[start:start + 500])))
            for start in range(0, len(rows), 500):
                statement = insert(AccountModel).values(rows[start:start + 500])
                index_keys = self._account_index_values({}).keys()
                if dialect == "mysql":
                    statement = statement.on_duplicate_key_update(**{
                        "data": statement.inserted.data,
                        **{key: statement.inserted["type" if key == "account_type" else key] for key in index_keys},
                    })
                else:
                    statement = statement.on_conflict_do_update(
                        index_elements=[AccountModel.access_token],
                        set_={"data": statement.excluded.data, **{
                            "type" if key == "account_type" else key:
                            statement.excluded["type" if key == "account_type" else key] for key in index_keys
                        }},
                    )
                session.execute(statement)

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
    ) -> dict[str, Any]:
        """Query the bounded management page directly from indexed columns."""
        page = max(1, int(page))
        page_size = min(100, max(1, int(page_size)))
        query_text = str(query or "").strip()
        session = self.Session()
        try:
            filtered = session.query(AccountModel)
            if query_text:
                filtered = filtered.filter(AccountModel.email.ilike(f"%{query_text}%"))
            if account_type and account_type != "all":
                filtered = filtered.filter(AccountModel.account_type == account_type)
            if status and status != "all":
                filtered = filtered.filter(AccountModel.status == status)

            if source != "all":
                filtered = filtered.filter(AccountModel.source_type == source)
            if rt_status != "all":
                filtered = filtered.filter(AccountModel.refresh_token_status == rt_status)
            if pool != "all":
                filtered = filtered.filter(AccountModel.image_pool_hot == int(pool == "hot"))
            if score_min is not None:
                filtered = filtered.filter(AccountModel.image_score >= score_min)
            if score_max is not None:
                filtered = filtered.filter(AccountModel.image_score <= score_max)
            if created_from is not None:
                filtered = filtered.filter(AccountModel.created_at_epoch >= created_from)
            if created_to is not None:
                filtered = filtered.filter(AccountModel.created_at_epoch < created_to)

            total = filtered.with_entities(func.count(AccountModel.id)).scalar() or 0
            page_count = max(1, (total + page_size - 1) // page_size)
            page = min(page, page_count)
            rows = (
                filtered.order_by(AccountModel.id)
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            )

            summary_rows = (
                session.query(AccountModel.status, func.count(AccountModel.id))
                .group_by(AccountModel.status)
                .all()
            )
            summary = {
                "total": int(session.query(func.count(AccountModel.id)).scalar() or 0),
                "active": 0,
                "limited": 0,
                "abnormal": 0,
                "disabled": 0,
                "quota": int(
                    session.query(func.coalesce(func.sum(AccountModel.quota), 0))
                    .filter(AccountModel.status == "正常")
                    .scalar()
                    or 0
                ),
            }
            status_keys = {"正常": "active", "限流": "limited", "异常": "abnormal", "禁用": "disabled"}
            for item_status, count in summary_rows:
                if key := status_keys.get(item_status):
                    summary[key] = int(count)

            types = [
                item_type
                for (item_type,) in session.query(AccountModel.account_type)
                .distinct()
                .order_by(AccountModel.account_type)
                .all()
                if item_type
            ]
            items = []
            for row in rows:
                try:
                    account = json.loads(row.data)
                except (TypeError, json.JSONDecodeError):
                    account = {}
                if not isinstance(account, dict):
                    account = {}
                account.setdefault("access_token", row.access_token)
                account["email"] = row.email or str(account.get("email") or "")
                account["type"] = row.account_type or str(account.get("type") or "Free")
                account["status"] = row.status or str(account.get("status") or "")
                account["quota"] = int(row.quota or 0)
                account.update({key: getattr(row, key) for key in (
                    "source_type", "refresh_token_status", "image_pool_hot", "image_score")})
                account["image_pool_hot"] = bool(row.image_pool_hot)
                items.append(account)
            return {
                "items": items,
                "total": int(total),
                "page": int(page),
                "page_size": page_size,
                "summary": summary,
                "types": types,
                "sources": [value for (value,) in session.query(AccountModel.source_type).distinct().order_by(AccountModel.source_type).all() if value],
            }
        finally:
            session.close()

    def delete_account_batch(self, access_tokens: list[str]) -> None:
        with self.Session.begin() as session:
            for start in range(0, len(access_tokens), 500):
                session.execute(delete(AccountModel).where(AccountModel.access_token.in_(access_tokens[start:start + 500])))

    def delete_account(self, access_token: str) -> None:
        session = self.Session()
        try:
            session.execute(delete(AccountModel).where(AccountModel.access_token == access_token))
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从数据库加载鉴权密钥数据"""
        return self._load_rows(AuthKeyModel)

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到数据库"""
        self._save_rows(AuthKeyModel, auth_keys, "id", "key_id")

    def _load_rows(self, model: type[AccountModel] | type[AuthKeyModel]) -> list[dict[str, Any]]:
        session = self.Session()
        try:
            items = []
            for row in session.query(model).all():
                try:
                    item_data = json.loads(row.data)
                    if isinstance(item_data, dict):
                        items.append(item_data)
                except json.JSONDecodeError:
                    continue
            return items
        finally:
            session.close()

    def _save_rows(
        self,
        model: type[AccountModel] | type[AuthKeyModel],
        items: list[dict[str, Any]],
        source_key: str,
        target_key: str | None = None,
    ) -> None:
        session = self.Session()
        try:
            key_column = target_key or source_key
            existing_rows = {
                str(getattr(row, key_column)): row
                for row in session.query(model).all()
            }
            incoming_keys: set[str] = set()

            for item in items:
                if not isinstance(item, dict):
                    continue
                key_value = str(item.get(source_key) or "").strip()
                if not key_value:
                    continue
                if key_value in incoming_keys:
                    raise ValueError(f"Duplicate {source_key} in storage snapshot")

                incoming_keys.add(key_value)
                serialized_data = json.dumps(item, ensure_ascii=False)
                existing_row = existing_rows.get(key_value)
                if existing_row is None:
                    session.add(
                        model(
                            **{key_column: key_value},
                            data=serialized_data,
                            **(self._account_index_values(item) if model is AccountModel else {}),
                        )
                    )
                elif existing_row.data != serialized_data or model is AccountModel:
                    existing_row.data = serialized_data
                    if model is AccountModel:
                        values = self._account_index_values(item)
                        for key, value in values.items():
                            setattr(existing_row, key, value)

            for key_value, row in existing_rows.items():
                if key_value not in incoming_keys:
                    session.delete(row)

            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            session = self.Session()
            try:
                # 尝试执行简单查询
                session.execute(text("SELECT 1"))
                count = session.query(AccountModel).count()
                auth_key_count = session.query(AuthKeyModel).count()
                return {
                    "status": "healthy",
                    "backend": "database",
                    "database_url": self._mask_password(self.database_url),
                    "account_count": count,
                    "auth_key_count": auth_key_count,
                }
            finally:
                session.close()
        except Exception as e:
            return {
                "status": "unhealthy",
                "backend": "database",
                "error": str(e),
            }

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        db_type = "unknown"
        if "sqlite" in self.database_url:
            db_type = "sqlite"
        elif "postgresql" in self.database_url or "postgres" in self.database_url:
            db_type = "postgresql"
        elif "mysql" in self.database_url:
            db_type = "mysql"
        
        return {
            "type": "database",
            "db_type": db_type,
            "description": f"数据库存储 ({db_type})",
            "database_url": self._mask_password(self.database_url),
        }

    @staticmethod
    def _mask_password(url: str) -> str:
        """隐藏数据库连接字符串中的密码"""
        if "://" not in url:
            return url
        try:
            protocol, rest = url.split("://", 1)
            if "@" in rest:
                credentials, host = rest.split("@", 1)
                if ":" in credentials:
                    username, _ = credentials.split(":", 1)
                    return f"{protocol}://{username}:****@{host}"
            return url
        except Exception:
            return url
