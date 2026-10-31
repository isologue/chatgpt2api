"""完成通知的取消竞态与批量投递回归，不访问网络。"""
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from contextvars import ContextVar
from threading import Event, get_ident
from unittest.mock import patch

import pytest

from services import async_completion as completions
from services.image_async import run_blocking


async def until(predicate):
    async with asyncio.timeout(3):
        while not predicate():
            await asyncio.sleep(.001)


def test_result_is_attached_in_submit_thread_without_intermediate_loop_round_trip():
    context = ContextVar('handoff-request', default=None)
    entered, release = Event(), Event()
    submit_thread = []
    async def scenario():
        loop_thread = get_ident()
        future = Future()
        def submit():
            submit_thread.append(get_ident())
            assert context.get() == 'request-a'
            entered.set()
            assert release.wait(3)
            future.set_result('done')
            return future
        token = context.set('request-a')
        task = asyncio.create_task(completions.await_submitted(submit))
        context.reset(token)
        try:
            await until(entered.is_set)
            bridge = asyncio.get_running_loop()._cg2api_completions
            with patch.object(bridge, 'enqueue', wraps=bridge.enqueue) as enqueue:
                release.set()
                assert await task == 'done'
                assert enqueue.call_count == 1
            assert submit_thread[0] != loop_thread
        finally:
            release.set()
    asyncio.run(scenario())


@pytest.mark.parametrize('when', ['before_submit', 'during_submit', 'waiting_result'])
def test_cancel_covers_dispatch_queue_submission_and_waiting_result(monkeypatch, when):
    submitted = Event()
    release = Event()
    calls = []
    worker = Future()
    async def scenario():
        # 固定单个测试分发线程，可靠复现尚未提交时的取消。
        dispatcher = ThreadPoolExecutor(max_workers=1)
        class Dispatcher:
            def submit(self, submit, args):
                return dispatcher.submit(submit, *args)
        monkeypatch.setattr(completions, '_submission_dispatcher', Dispatcher())
        def submit():
            calls.append(1)
            submitted.set()
            if when == 'during_submit':
                assert release.wait(3)
            return worker
        try:
            if when == 'before_submit':
                dispatcher.submit(release.wait, 3)
            task = asyncio.create_task(completions.await_submitted(submit))
            await asyncio.sleep(0)
            if when != 'before_submit':
                await until(submitted.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            if when == 'before_submit':
                await asyncio.to_thread(dispatcher.shutdown)
                assert not calls
            else:
                await until(worker.cancelled)
        finally:
            release.set()
            dispatcher.shutdown(wait=True)
    asyncio.run(scenario())


def test_running_work_naturally_finishes_after_waiter_cancel_and_errors_propagate():
    async def scenario():
        worker = Future()
        worker.set_running_or_notify_cancel()
        attached = Event()
        def submit():
            attached.set()
            return worker
        task = asyncio.create_task(completions.await_submitted(submit))
        await until(attached.is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert worker.running()
        worker.set_exception(ValueError('late business error'))
        for origin in ('submit', 'work'):
            def fail():
                if origin == 'submit':
                    raise RuntimeError('business error')
                future = Future()
                future.set_exception(ValueError('business error'))
                return future
            with pytest.raises(RuntimeError if origin == 'submit' else ValueError, match='business error'):
                await completions.await_submitted(fail)
        canceled = Future()
        canceled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await completions.await_submitted(lambda: canceled)
    asyncio.run(scenario())


def test_blocking_result_is_reclaimed_after_multiple_cancels(monkeypatch):
    from services import thread_pool
    entered, release = Event(), Event()
    identity = object()
    def work():
        entered.set()
        assert release.wait(3)
        return identity
    async def scenario():
        task = asyncio.create_task(run_blocking(work))
        try:
            await until(entered.is_set)
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
            release.set()
            assert await task is identity
        finally:
            release.set()
    asyncio.run(scenario())


def test_thousands_of_mixed_completions_have_one_result_each():
    async def scenario():
        def submit(index):
            future = Future()
            if index % 17 == 0:
                future.set_exception(ValueError(str(index)))
            elif index % 19 == 0:
                future.cancel()
            else:
                future.set_result(index)
            return future
        results = await asyncio.gather(*(completions.await_submitted(submit, i)
                                        for i in range(2000)), return_exceptions=True)
        for i, result in enumerate(results):
            if i % 17 == 0:
                assert isinstance(result, ValueError) and str(result) == str(i)
            elif i % 19 == 0:
                assert isinstance(result, asyncio.CancelledError)
            else:
                assert result == i
    asyncio.run(scenario())
