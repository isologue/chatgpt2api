"""图片 curl 原生调用边界；验证 ABI、回退和错误传播，不请求上游。"""
import asyncio
import ctypes
import sys

import pytest
from curl_cffi import AsyncCurl

from services import image_curl


def test_native_binding_is_limited_to_supported_runtime(monkeypatch):
    monkeypatch.setattr(image_curl.sys, 'platform', 'unsupported')
    assert image_curl._bind_socket_action() is None
    monkeypatch.setattr(image_curl.sys, 'platform', 'linux')
    monkeypatch.setattr(image_curl.sys, '_is_gil_enabled', lambda: False, raising=False)
    assert image_curl._bind_socket_action() is None


def test_linux_binding_retains_python_execution_permission():
    binding = image_curl._bind_socket_action()
    if sys.platform != 'linux':
        assert binding is None
        return
    if not getattr(sys, '_is_gil_enabled', lambda: True)():
        assert binding is None
        return
    assert binding is not None
    assert binding._flags_ & ctypes._FUNCFLAG_PYTHONAPI


def test_unsupported_abi_and_missing_symbol_use_library_path(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(image_curl.sys, 'platform', 'linux')
    monkeypatch.setattr(image_curl.sys, '_is_gil_enabled', lambda: True, raising=False)
    class ChangedABI:
        def addressof(self, *args):
            return object()
        def typeof(self, address):
            return SimpleNamespace(result=SimpleNamespace(cname='int'),
                args=[SimpleNamespace(cname=name) for name in ('void *', 'long', 'int', 'int *')])
    monkeypatch.setattr(image_curl, 'ffi', ChangedABI())
    assert image_curl._bind_socket_action() is None
    def missing(*args):
        raise AttributeError('missing native entrypoint')
    monkeypatch.setattr(image_curl.ffi, 'addressof', missing)
    assert image_curl._bind_socket_action() is None


@pytest.mark.parametrize('native', [False, True])
def test_native_and_fallback_return_handle_count_and_propagate_errors(monkeypatch, native):
    async def scenario():
        calls = []
        error = [0]
        def bound(handle, fd, mask, running):
            calls.append((handle, fd, mask))
            ctypes.cast(running, ctypes.POINTER(ctypes.c_int))[0] = 7
            return error[0]
        def fallback(self, fd, mask):
            calls.append((self, fd, mask))
            return 9
        monkeypatch.setattr(image_curl, '_native_socket_action', bound if native else None)
        monkeypatch.setattr(AsyncCurl, 'socket_action', fallback)
        driver = image_curl.ImageAsyncCurl()
        try:
            assert driver.socket_action(-1, 0) == (7 if native else 9)
            assert len(calls) == 1 and calls[0][1:] == (-1, 0)
            if native:
                assert calls[0][0] == int(image_curl.ffi.cast('uintptr_t', driver._curlm))
                def check(code):
                    if code:
                        raise RuntimeError('expected native failure')
                monkeypatch.setattr(driver, '_check_error', check)
                error[0] = 2
                with pytest.raises(RuntimeError, match='expected native failure'):
                    driver.socket_action(-1, 0)
                assert len(calls) == 2  # 失败不能再驱动一次 fallback。
        finally:
            await driver.close()
    asyncio.run(scenario())
