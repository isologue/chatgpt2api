"""为生图 HTTP 传输设置真正的 libcurl 总时限，不依赖流式 low-speed timeout。"""
from __future__ import annotations

import math
import time

from curl_cffi import CurlOpt
from curl_cffi.requests import Session


class ImageBackendSession(Session):
    def __new__(cls, *args, **kwargs):
        from services.image_async import cooperative
        if cooperative():
            from services.image_async_transport import AsyncImageSession
            return AsyncImageSession(**kwargs)
        return super().__new__(cls)

    @property
    def executor(self):
        # Session 不拥有此共享池；关闭单个账号会话不会停止其他请求的流。
        from services.thread_pool import image_stream_executor
        return image_stream_executor

    def request(self, method, url, **kwargs):
        # 延迟导入，保持 metrics -> diagnostics 的依赖无环。
        from services.image_metrics import current_metrics

        from services.image_diagnostics import (
            ImageStreamHardTimeoutError, detached_budget, remaining_poll_budget,
        )
        metrics = current_metrics.get()
        remaining = None if metrics is None or detached_budget.get() else metrics.diagnostics.remaining()
        remaining = remaining_poll_budget(remaining)
        stream_cap = kwargs.pop("image_stream_timeout", None)
        stream_deadline = None
        if stream_cap is not None:
            stream_deadline = time.monotonic() + float(stream_cap)
            remaining = min(remaining, float(stream_cap)) if remaining is not None else float(stream_cap)
        if remaining is None:
            return super().request(method, url, **kwargs)

        requested = kwargs.get('timeout', self.timeout)
        if isinstance(requested, tuple):
            stage_cap = sum(float(part) for part in requested)
        elif isinstance(requested, (float, int)) and requested > 0:
            stage_cap = float(requested)
        else:
            stage_cap = remaining
        total = max(0.001, min(remaining, stage_cap))
        kwargs['timeout'] = total

        # curl_cffi 在 stream=True 时把普通 timeout 转成 LOW_SPEED_TIME；
        # TIMEOUT_MS 必须作为最后覆盖项显式设置，才包含首字节和正文传输。
        # 每个 backend 独享 Session。set_curl_options 在 request 返回前已执行，
        # 恢复字典不会撤销已复制给流式 curl handle 的超时选项。
        original = self.curl_options
        self.curl_options = {**original, CurlOpt.TIMEOUT_MS: max(1, math.ceil(total * 1000))}
        try:
            response = super().request(method, url, **kwargs)
            if stream_deadline is not None:
                response._image_stream_deadline = stream_deadline
            try:
                if metrics is not None and not detached_budget.get():
                    metrics.diagnostics.remaining()
                remaining_poll_budget()
            except BaseException:
                response.close()
                raise
            return response
        except Exception as exc:
            # 总预算或调用方取消优先于 curl 的泛化错误，防止进入换号循环。
            if metrics is not None and not detached_budget.get():
                metrics.diagnostics.remaining()
            remaining_poll_budget()
            # 首字节之前也可能达到流时限，此时尚未进入 SSE 消费生成器。
            if stream_deadline is not None and time.monotonic() >= stream_deadline:
                raise ImageStreamHardTimeoutError(
                    f"图片生成请求已超过硬上限 {float(stream_cap):g} 秒，已强制中断（等待响应头或生成流）"
                ) from exc
            raise
        finally:
            self.curl_options = original
