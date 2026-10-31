"""Indexed account management filtering, pagination and legacy upgrade regressions."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text

import api.accounts as routes
from services.account_list_fields import account_list_fields
from services.account_service import AccountService
from services.storage.database_storage import AccountModel, DatabaseStorageBackend
from test.test_account_refresh_token_status import MemoryStorage


@pytest.fixture
def accounts():
    items = []
    for i in range(160):
        item = dict(access_token=f"test-token-{i:06d}", email=f"user{i:03d}@example.test",
                    type="Plus" if i % 2 else "Free", source_type="codex" if i % 3 == 0 else "web",
                    status="正常" if i % 4 else "限流", quota=10,
                    image_score=i % 101, image_pool_hot=bool(i % 3),
                    created_at=(datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(hours=i)).isoformat())
        if i % 5:
            item["refresh_token"] = "test-rt"
        if i % 5 == 1:
            item["last_token_refresh_at"] = "2026-10-01T10:00:00Z"
        elif i % 5 == 2:
            item["last_token_refresh_error"] = "invalid_grant"
        elif i % 5 == 3:
            item["last_token_refresh_error"] = "upstream_http_502"
        items.append(item)
    return items


@pytest.fixture
def stores(tmp_path, accounts, monkeypatch):
    monkeypatch.setattr(AccountService, "_load_cumulative_total", lambda self: 0)
    memory = AccountService(MemoryStorage(accounts))
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'filters.db'}")
    # Persist the same normalized metadata as the live account writer.
    backend.save_account_batch(list(memory._accounts.values()))
    sql = AccountService(backend)
    yield memory, sql, backend
    memory.verification.close()
    sql.verification.close()
    backend.engine.dispose()


@pytest.mark.parametrize("filters", [
    {}, {"source": "codex"}, {"source": "not-found"}, {"account_type": "Plus"}, {"status": "限流"},
    *[{"rt_status": status} for status in ("valid", "invalid", "pending", "unverified", "missing")],
    {"pool": "hot"}, {"pool": "cold"}, {"score_min": 0, "score_max": 0},
    {"score_min": 80}, {"score_max": 30},
    {"created_from": 1790784000, "created_to": 1790870400},
    {"source": "web", "pool": "hot", "score_min": 20, "score_max": 90, "status": "正常", "query": "USER"},
])
def test_sql_snapshot_filters_and_bounded_pages_match(stores, filters):
    memory, sql, backend = stores
    expected = memory.list_accounts_page(page_size=100, **filters)
    collected = []
    for number in range(1, max(1, (expected["total"] + 6) // 7) + 1):
        page = sql.list_accounts_page(page=number, page_size=7, **filters)
        assert page["total"] == expected["total"]
        assert len(page["items"]) <= 7
        assert page["summary"]["total"] == 160  # Global cards must not become filtered counts.
        assert page["sources"] == ["codex", "web"]
        collected.extend(item["access_token"] for item in page["items"])
    memory_tokens = []
    for number in range(1, max(1, (expected["total"] + 99) // 100) + 1):
        memory_tokens.extend(item["access_token"] for item in memory.list_accounts_page(page=number, page_size=100, **filters)["items"])
    assert collected == memory_tokens


def test_page_includes_real_score_hot_status_but_no_oauth_secrets(stores):
    memory, sql, backend = stores
    for service in (memory, sql):
        page = service.list_accounts_page(pool="hot", score_min=80)
        assert page["total"] > 0
        for item in page["items"]:
            assert item["image_pool_hot"]
            assert item["image_score"] >= 80
            assert "refresh_token" not in item
            assert "password" not in item
            assert "id_token" not in item


def test_update_and_rotation_keep_all_indexes_in_sync(stores):
    memory, sql, backend = stores
    item = dict(memory.get_account("test-token-000001"), source_type="custom", image_score=93,
                image_pool_hot=True, refresh_token="rt", last_token_refresh_at="2026-10-30 00:00:00")
    backend.save_account_batch([item])
    match = dict(source="custom", rt_status="valid", pool="hot", score_min=90)
    assert backend.list_accounts_page(**match)["total"] == 1
    backend.save_accounts([{**item, "image_score": 4, "image_pool_hot": False}])
    assert backend.list_accounts_page(**match)["total"] == 0
    backend.apply_account_changes([{**item, "access_token": "rotated"}], [item["access_token"]])
    page = backend.list_accounts_page(**match)
    assert page["total"] == 1 and page["items"][0]["access_token"] == "rotated"


def test_legacy_backfill_multiple_batches_and_restart(tmp_path):
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    backend = DatabaseStorageBackend(url)
    with backend.engine.begin() as connection:
        connection.execute(text("DROP TABLE accounts"))
        connection.execute(text("CREATE TABLE accounts (id INTEGER PRIMARY KEY, access_token VARCHAR(2048) NOT NULL UNIQUE, data TEXT NOT NULL)"))
        connection.execute(text("INSERT INTO accounts (id, access_token, data) VALUES (:id, :token, :data)"), [
            {"id": i + 1, "token": f"legacy-{i}", "data": json.dumps({
                "access_token": f"legacy-{i}", "type": "codex", "quota": 10, "status": "正常",
                "image_pool_hot": True, "image_score": 88, "refresh_token": "rt", "refresh_token_status": "valid",
                "last_token_refresh_error": "invalid_grant", "created_at": "2026-10-30T08:00:00+08:00",
            })} for i in range(1103)
        ])
    backend.engine.dispose()
    upgraded = DatabaseStorageBackend(url)
    filters = dict(source="codex", rt_status="invalid", pool="hot", score_min=88,
                   created_from=1793318400, created_to=1793404800)
    assert upgraded.list_accounts_page(**filters)["total"] == 1103
    with upgraded.engine.begin() as connection:
        connection.execute(text("UPDATE accounts SET list_index_version=0, image_score=0 WHERE id=1001"))
    upgraded.engine.dispose()
    restarted = DatabaseStorageBackend(url)
    assert restarted.list_accounts_page(**filters)["total"] == 1103
    names = {column["column_names"][0] for column in inspect(restarted.engine).get_indexes("accounts")}
    assert {"source_type", "refresh_token_status", "image_pool_hot", "image_score", "created_at_epoch"} <= names
    restarted.engine.dispose()


def test_date_range_is_utc_with_exclusive_end_and_ignores_missing_dates(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'dates.db'}")
    backend.save_account_batch([
        {"access_token": "start", "created_at": "2026-10-30T00:00:00+08:00"},
        {"access_token": "last-second", "created_at": "2026-10-30T15:59:59Z"},
        {"access_token": "end", "created_at": "2026-10-30T16:00:00Z"},
        {"access_token": "unknown"},
    ])
    start = datetime(2026, 10, 29, 16, tzinfo=timezone.utc).timestamp()
    end = datetime(2026, 10, 30, 16, tzinfo=timezone.utc).timestamp()
    page = backend.list_accounts_page(created_from=start, created_to=end)
    assert [item["access_token"] for item in page["items"]] == ["start", "last-second"]
    backend.engine.dispose()


def test_api_filters_and_validation(stores, monkeypatch):
    memory, sql, backend = stores
    monkeypatch.setattr(routes, "account_service", sql)
    monkeypatch.setattr(routes, "require_admin", Mock())
    app = FastAPI()
    app.include_router(routes.create_router())
    with TestClient(app) as client:
        response = client.get("/api/accounts", params={"page": 1, "page_size": 3, "pool": "hot", "rt_status": "valid", "score_min": 20})
        assert response.status_code == 200
        assert response.json()["total"] == sql.list_accounts_page(pool="hot", rt_status="valid", score_min=20)["total"]
        for params in ({"score_min": -1}, {"score_max": 101}, {"score_min": 70, "score_max": 30},
                       {"pool": "bad"}, {"rt_status": "bad"}, {"created_from": 20, "created_to": 10},
                       {"created_from": "NaN"}, {"created_to": "Infinity"}):
            assert client.get("/api/accounts", params={"page": 1, **params}).status_code == 422
