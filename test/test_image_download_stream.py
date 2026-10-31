"""Regression: curl_cffi streaming content is not an eagerly populated body."""
import io
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest import mock

import pytest
from PIL import Image
from curl_cffi import requests
from services.openai_backend_api import OpenAIBackendAPI
from services.image_metrics import ImageMetrics, current_metrics
from services.image_storage_service import ImageStorageService, ImageStorageError


def png_bytes():
    buf = io.BytesIO()
    Image.new("RGB", (3, 3), "red").save(buf, format="PNG")
    return buf.getvalue()


@pytest.fixture
def image_server():
    payload = png_bytes()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
        def log_message(self, *_args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/image.png", payload
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_real_stream_download_reads_complete_valid_image(image_server):
    url, expected = image_server
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        with requests.Session(trust_env=False) as session:
            backend.session = session
            actual = backend.download_image_bytes([url])
        assert actual == [expected]
        with Image.open(io.BytesIO(actual[0])) as image:
            image.verify()
        assert metrics.snapshot()["image_download_bytes"] == len(expected)
    finally:
        current_metrics.reset(token)


@pytest.mark.parametrize("payload", [b"", b"<html>upstream error</html>", png_bytes()[:30]])
def test_rejects_empty_invalid_and_truncated_image_and_closes(payload):
    response = SimpleNamespace(status_code=200, headers={}, elapsed=None,
                               iter_content=lambda **kwargs: iter([payload]), close=mock.Mock())
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.session = SimpleNamespace(get=lambda *args, **kwargs: response)
    metrics = ImageMetrics()
    token = current_metrics.set(metrics)
    try:
        with pytest.raises(RuntimeError, match="image download"):
            backend.download_image_bytes(["https://example.test/image.png"])
        assert metrics.snapshot()["image_download_failures"] == 1
        assert metrics.snapshot().get("image_download_count", 0) == 0
        # 下载失败会按配置重试同一个结果地址；每次尝试都必须关闭响应。
        assert response.close.call_count == 2
    finally:
        current_metrics.reset(token)


def test_stream_read_error_propagates_and_closes():
    def chunks(**kwargs):
        yield png_bytes()[:10]
        raise RuntimeError("connection interrupted")
    response = SimpleNamespace(status_code=200, headers={}, elapsed=None,
                               iter_content=chunks, close=mock.Mock())
    backend = OpenAIBackendAPI.__new__(OpenAIBackendAPI)
    backend.session = SimpleNamespace(get=lambda *args, **kwargs: response)
    with pytest.raises(RuntimeError, match="connection interrupted"):
        backend.download_image_bytes(["https://example.test/image.png"])
    assert response.close.call_count == 2


def test_storage_rejects_empty_before_any_disk_write(tmp_path):
    service = ImageStorageService(tmp_path / "index.json")
    with mock.patch.object(service, "make_relative_path") as make_path:
        with pytest.raises(ImageStorageError, match="empty"):
            service.save(b"", "http://example.test")
        make_path.assert_not_called()
