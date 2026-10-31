import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from services.account_info_queries import AccountInfoQueries
from services.openai_backend_api import OpenAIBackendAPI, InvalidAccessTokenError


class Server(ThreadingHTTPServer):
    request_queue_size = 256
    daemon_threads = True


@pytest.fixture
def upstream():
    state = {"requests": [], "active": 0, "peak": 0, "blocked": False, "bad_me": False}
    lock, release, all_started = threading.Lock(), threading.Event(), threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            self.do_GET()
        def do_GET(self):
            raw = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            with lock:
                state["requests"].append((self.path, self.headers.get("Authorization"), self.headers.get("Cookie"), raw))
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
                if state["active"] >= 36:
                    all_started.set()
            try:
                if state["blocked"]:
                    assert release.wait(5)
                path = self.path.split("?", 1)[0]
                payload = ({"email": "sandbox@example.test", "id": "u1"} if path.endswith("/me") else
                           {"limits_progress": [], "default_model_slug": "auto"} if path.endswith("/init") else
                           {"accounts": {"default": {"account": {"plan_type": "plus"}}}})
                body = json.dumps(payload).encode()
                self.send_response(401 if state["bad_me"] and path.endswith("/me") else 200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Set-Cookie", "query-isolation=true")
                self.end_headers()
                self.wfile.write(body)
            finally:
                with lock:
                    state["active"] -= 1
    server = Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state, release, all_started
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(3)


def test_real_http_queries_exceed_old_32_cap_and_keep_sessions_isolated(upstream):
    url, state, release, all_started = upstream
    queries = AccountInfoQueries()
    state["blocked"] = True
    specs = [{"method": "GET", "url": url + path} for path in (
        "/backend-api/me", "/backend-api/conversation/init", "/backend-api/accounts/check")]
    try:
        with ThreadPoolExecutor(12) as workers:
            futures = [workers.submit(queries.run, {"proxy": "", "verify": True},
                {"Authorization": "Bearer sandbox-test"}, specs) for _ in range(12)]
            try:
                assert all_started.wait(5), f"only {state['peak']} requests entered simultaneously"
                assert state["peak"] >= 36
            finally:
                release.set()
            assert all(all(response.status_code == 200 for response in f.result(8)) for f in futures)
        assert len(state["requests"]) == 36
        assert all(row[1] == "Bearer sandbox-test" and row[2] is None for row in state["requests"])
    finally:
        queries.close()
    # 生命周期关闭后可以重新启动，而不是保留失效event loop。
    assert len(queries.run({"proxy": ""}, {}, specs)) == 3
    queries.close()


def test_get_user_info_keeps_request_contract_and_401_mapping(upstream, monkeypatch):
    import services.openai_backend_api as module
    url, state, _, _ = upstream
    queries = AccountInfoQueries()
    monkeypatch.setattr(module, "account_info_queries", queries)
    monkeypatch.setattr(module.proxy_settings, "build_session_kwargs", lambda **kw: {"proxy": "", "verify": True})
    backend = OpenAIBackendAPI("sandbox-token")
    backend.base_url = url
    try:
        result = backend.get_user_info()
        assert result["email"] == "sandbox@example.test" and result["type"] == "plus"
        paths = {row[0]: row for row in state["requests"]}
        body = json.loads(paths["/backend-api/conversation/init"][3])
        assert body["timezone_offset_min"] == -480
        assert any("timezone_offset_min=-480" in path for path in paths)
        state["bad_me"] = True
        with pytest.raises(InvalidAccessTokenError, match="token invalidated"):
            backend.get_user_info()
        assert state["active"] == 0
    finally:
        backend.close()
        queries.close()
