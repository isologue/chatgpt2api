from __future__ import annotations

import json
from datetime import date
from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.image_statistics import image_result_counts, is_content_policy_rejection
from services.log_service import LogService


def log_item(id, day, status="success", **detail):
    return {"id": id, "type": "call", "time": day + " 12:00:00", "detail": {"endpoint": "/v1/images/generations", "status": status, **detail}}


def test_daily_statistics_persist_filter_and_never_rescan_on_refresh(tmp_path):
    path = tmp_path / "logs.jsonl"
    records = [
        log_item("before", "2026-10-19"),
        log_item("two-images", "2026-10-20", response_body={"data": [{"url": "a"}, {"b64_json": "abc"}]}),
        log_item("policy", "2026-10-20", "failed", error="This request violates our content policy"),
        log_item("timeout", "2026-10-21", "failed", error="upstream timed out"),
        log_item("review-down", "2026-10-21", "failed", error="AI 审核服务暂时不可用，请稍后重试"),
        log_item("sensitive", "2026-10-21", "failed", error="检测到敏感词，拒绝本次任务"),
        log_item("review-denied", "2026-10-21", "failed", error="AI 审核未通过，拒绝本次任务"),
        log_item("async", "2026-10-21", is_async=True, response_body={"status": "succeeded", "data": {"data": [{"url": "a"}, {"url": "b"}, {"url": "c"}]}}),
        log_item("prompt-not-error", "2026-10-21", "failed", request_body={"prompt": "content_policy_violation"}, error="network error"),
        {"id": "other", "type": "account", "time": "2026-10-21", "detail": {"status": "failed"}},
    ]
    path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records), encoding="utf-8")
    service = LogService(path)
    stats = service.image_stats("2026-10-20", "2026-10-21")
    assert stats == {"success": 2, "failed": 3, "excluded": 3, "success_images": 5, "total": 5, "success_rate": 40.0, "start_date": "2026-10-20", "end_date": "2026-10-21", "duration_samples": 0, "duration_p50_ms": None, "duration_p95_ms": None}
    assert service.image_stats("2026-10-20", "2026-10-20")["success_images"] == 2
    assert service.image_stats("2026-10-22", "2026-10-22")["total"] == 0

    restarted = LogService(path)
    # 即便换一个服务实例，刷新只读取汇总，不再次解析历史请求/大 Base64。
    with mock.patch.object(restarted, "_parse_line", side_effect=AssertionError("unexpected full scan")):
        assert restarted.image_stats("2026-10-20", "2026-10-21") == stats
        restarted.add("call", detail={"endpoint": "/v1/images/edits", "status": "success", "response_body": {"data": [{"url": "new"}]}})
        assert restarted.image_stats(date.today().isoformat(), date.today().isoformat())["success"] == 1

    service.delete(["two-images"])
    updated = service.image_stats("2026-10-20", "2026-10-21")
    assert updated["success"] == 1
    assert updated["success_images"] == 3
    assert updated["failed"] == 3
    assert updated["excluded"] == 3


def test_existing_index_without_statistics_is_backfilled_once(tmp_path):
    service = LogService(tmp_path / "logs.jsonl")
    service.add("call", detail={"endpoint": "/v1/images/generations", "status": "failed", "error": "content_policy_violation"})
    with service._index(write=True) as connection:
        connection.execute("DROP TABLE image_stats_daily")
        connection.execute("DELETE FROM image_stats_meta")
    day = date.today().isoformat()
    restarted = LogService(service.path)
    with mock.patch.object(restarted, "_parse_line", side_effect=AssertionError("migration must reuse summaries")):
        assert restarted.image_stats(day, day)["excluded"] == 1


@pytest.mark.parametrize("message", [
    "content_policy_violation", "This request violates our content policy.",
    "Blocked by our safety system", "安全审查未通过", "请求违反了内容政策", "检测到敏感词，拒绝本次任务",
    "AI 审核未通过，拒绝本次任务",
])
def test_clear_content_rejections_are_excluded(message):
    assert is_content_policy_rejection(message)
    assert image_result_counts(log_item("x", "2026-10-27", "failed", error=message)) == (0, 0, 1, 0)


@pytest.mark.parametrize("message", [
    "AI 审核服务暂时不可用，请稍后重试", "moderation service timed out", "content filter unavailable",
    "unable to generate image due to network error", "policy service unavailable", "account blocked", "无法生成图片：连接超时",
])
def test_technical_errors_are_failures(message):
    from services.openai_backend_api import _is_content_policy_error
    assert not _is_content_policy_error(message)
    assert image_result_counts(log_item("x", "2026-10-27", "failed", error=message)) == (0, 1, 0, 0)


def test_error_codes_and_internal_filter_failures_are_counted():
    item = log_item("x", "2026-10-27", "failed", error="refused", error_code="content_policy_violation")
    assert image_result_counts(item) == (0, 0, 1, 0)
    item["detail"].update(endpoint="/api/image-tasks/generations", error_code="", error="AI 审核未通过，拒绝本次任务")
    assert image_result_counts(item) == (0, 0, 1, 0)
    item["detail"]["endpoint"] = "/v1/chat/completions"
    assert image_result_counts(item) == (0, 0, 0, 0)


def test_monitoring_date_validation_and_range(tmp_path, monkeypatch):
    import api.system as system
    service = LogService(tmp_path / "logs.jsonl")
    monkeypatch.setattr(system, "require_admin", lambda _: None)
    monkeypatch.setattr(system, "log_service", service)
    monkeypatch.setattr(system.account_service, "get_stats", lambda: {})
    monkeypatch.setattr(system, "monitoring_snapshot", lambda accounts, images: {"images": images})
    app = FastAPI()
    app.include_router(system.create_router("test"))
    with TestClient(app) as client:
        for query in ("start_date=2026-10-28&end_date=2026-10-27", "start_date=invalid", "start_date=2026-02-30"):
            assert client.get("/api/monitoring?" + query).status_code == 400
        result = client.get("/api/monitoring?start_date=2026-10-01&end_date=2026-10-27")
        assert result.status_code == 200
        assert result.json()["images"]["start_date"] == "2026-10-01"
        assert result.json()["images"]["end_date"] == "2026-10-27"
        assert client.get("/api/monitoring").json()["images"]["start_date"] == date.today().isoformat()
