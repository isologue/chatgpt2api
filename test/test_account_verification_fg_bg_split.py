"""验证前后台校验额度分离：后台热池维护占满自身额度时，前台生图校验仍能立即通过，
不再被后台挤进长队列。对应优化 account_verification_queue 尾部排队。"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from services.account_verification import AccountVerification
from services.config import config, DEFAULT_IMAGE_ACCOUNT_VERIFICATION


@pytest.fixture
def settings(monkeypatch):
    # C=2：前台和后台各自完整使用2个槽位，无后台减半或启动速率限制。
    value = {**DEFAULT_IMAGE_ACCOUNT_VERIFICATION, "workers": 2}
    monkeypatch.setitem(config.data, "image_account_verification", value)
    return value


@pytest.fixture
def verifier(settings):
    service = AccountVerification(lambda: settings)
    yield service
    service.close()


def _wait_for(predicate, timeout=3):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition timed out"
        time.sleep(0.002)


def test_foreground_not_blocked_by_saturated_background(verifier):
    """后台把它的 2 个额度占满并阻塞住，前台校验仍应立即执行、不排队等后台。"""
    release = Event()

    def blocking_bg():
        assert release.wait(3)

    try:
        # 占满后台额度（background_workers=2），并让它们挂住。
        assert verifier.run("bg-1", blocking_bg, background=True)
        assert verifier.run("bg-2", blocking_bg, background=True)
        _wait_for(lambda: verifier.stats()["active_background"] == 2)

        # 再提交第 3 个后台任务：应被后台额度挡住、不占前台。
        assert verifier.run("bg-3", blocking_bg, background=True)
        _wait_for(lambda: verifier.stats()["background_pending"] == 3)

        # 关键断言：此刻前台校验立即返回，不被后台占用阻塞。
        done_at = []
        start = time.monotonic()

        def fg_fetch():
            return {"quota": 42}

        with ThreadPoolExecutor(2) as pool:
            futs = [pool.submit(verifier.run, f"fg-{i}", fg_fetch) for i in range(2)]
            results = [f.result(2) for f in futs]
            done_at.append(time.monotonic() - start)

        assert results == [{"quota": 42}] * 2
        # 前台 2 个在前台额度(2)内并发完成，应很快（远小于 2 秒；后台仍被 release 挂着）。
        assert done_at[0] < 1.5
        # 后台仍然占着自己的额度，未被前台挪用。
        assert verifier.stats()["active_background"] == 2
    finally:
        release.set()
    _wait_for(lambda: verifier.stats()["background_pending"] == 0)
    assert verifier.stats()["active"] == 0


def test_foreground_concurrency_uses_full_workers(verifier):
    """前台应独享完整 workers 并发额度，不受后台占用影响。"""
    release = Event()
    entered = []

    def bg_block():
        assert release.wait(3)

    def fg_hold():
        entered.append(time.monotonic())
        assert release.wait(3)

    try:
        # 后台占满自身 2 个额度
        assert verifier.run("bg-1", bg_block, background=True)
        assert verifier.run("bg-2", bg_block, background=True)
        _wait_for(lambda: verifier.stats()["active_background"] == 2)

        # 前台应能同时跑满 workers(=2) 个，而不是被后台占用后只剩 1 个
        with ThreadPoolExecutor(2) as pool:
            for i in range(2):
                pool.submit(verifier.run, f"fg-{i}", fg_hold)
            _wait_for(lambda: verifier.stats()["active_foreground"] == 2)
            assert verifier.stats()["active_foreground"] == 2
            release.set()
    finally:
        release.set()
    _wait_for(lambda: verifier.stats()["active"] == 0)
