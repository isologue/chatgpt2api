from __future__ import annotations

from pathlib import Path
from threading import Event, Thread
import time

from fastapi import HTTPException, Request

from services.account_service import account_service
from services.account_refresh_monitor import account_refresh_monitor
from services.auth_service import auth_service
from services.config import config

BASE_DIR = Path(__file__).resolve().parents[1]
WEB_DIST_DIR = BASE_DIR / "web_dist"


def extract_bearer_token(authorization: str | None) -> str:
    scheme, _, value = str(authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return ""
    return value.strip()


def _legacy_admin_identity(token: str) -> dict[str, object] | None:
    auth_key = str(config.auth_key or "").strip()
    if auth_key and token == auth_key:
        return {"id": "admin", "name": "管理员", "role": "admin"}
    return None


def require_identity(authorization: str | None) -> dict[str, object]:
    token = extract_bearer_token(authorization)
    identity = _legacy_admin_identity(token) or auth_service.authenticate(token)
    if identity is None:
        raise HTTPException(status_code=401, detail={"error": "密钥无效或已失效，请重新登录"})
    return identity


def require_auth_key(authorization: str | None) -> None:
    require_identity(authorization)


def require_admin(authorization: str | None) -> dict[str, object]:
    identity = require_identity(authorization)
    if identity.get("role") != "admin":
        raise HTTPException(status_code=403, detail={"error": "需要管理员权限才能执行这个操作"})
    return identity


def resolve_image_base_url(request: Request) -> str:
    return config.base_url or f"{request.url.scheme}://{request.headers.get('host', request.url.netloc)}"


def raise_image_quota_error(exc: Exception) -> None:
    message = str(exc)
    if "no available image quota" in message.lower():
        raise HTTPException(status_code=429, detail={"error": "no available image quota"}) from exc
    raise HTTPException(status_code=502, detail={"error": message}) from exc


def sanitize_cpa_pool(pool: dict | None) -> dict | None:
    if not isinstance(pool, dict):
        return None
    return {key: value for key, value in pool.items() if key != "secret_key"}


def sanitize_cpa_pools(pools: list[dict]) -> list[dict]:
    return [sanitized for pool in pools if (sanitized := sanitize_cpa_pool(pool)) is not None]


def sanitize_sub2api_server(server: dict | None) -> dict | None:
    if not isinstance(server, dict):
        return None
    sanitized = {key: value for key, value in server.items() if key not in {"password", "api_key"}}
    sanitized["has_api_key"] = bool(str(server.get("api_key") or "").strip())
    return sanitized


def sanitize_sub2api_servers(servers: list[dict]) -> list[dict]:
    return [sanitized for server in servers if (sanitized := sanitize_sub2api_server(server)) is not None]


def start_limited_account_watcher(stop_event: Event) -> Thread:
    from services.account_pool_maintenance import pool_maintenance

    def worker():
        maintenance = Thread(target=pool_maintenance.run, args=(stop_event,),
                             name="account-pool-maintenance", daemon=True)
        maintenance.start()
        try:
            while not stop_event.is_set():
                # 信息刷新由热/冷到期队列负责；这里仅保留既有AT/RT维护。
                interval = max(1, config.refresh_account_interval_minute) * 60
                round_id = account_refresh_monitor.start(interval)
                error = None
                try:
                    expiring = account_service.list_expiring_access_tokens()
                    keepalive = account_service.list_refresh_token_keepalive_tokens()
                    expiring_set = set(expiring)
                    keepalive = [t for t in keepalive if t not in expiring_set]
                    account_refresh_monitor.plan(round_id, len(expiring), len(keepalive))
                    account_refresh_monitor.set_phase(round_id, "refresh")
                    for token in expiring:
                        account_refresh_monitor.update(round_id, "refresh", "started")
                        success = False
                        try:
                            active = account_service.refresh_access_token(token, event="credential_maintenance")
                            account = account_service.get_account(active or token) or {}
                            success = bool(account) and not account.get("last_token_refresh_error")
                        finally:
                            account_refresh_monitor.update(round_id, "refresh", "finished", success)
                    if keepalive:
                        account_refresh_monitor.set_phase(round_id, "keepalive")
                        account_service.keepalive_refresh_tokens(keepalive, include_items=False,
                            on_progress=lambda event, success=None: account_refresh_monitor.update(
                                round_id, "keepalive", event, success),
                            on_failure=lambda item: account_refresh_monitor.add_failure(round_id, "keepalive", item))
                except Exception as exc:
                    error = f"凭据维护中断（{type(exc).__name__}），请检查运行日志"
                finally:
                    account_refresh_monitor.finish(round_id, error, full_refresh=False)
                stop_event.wait(interval)
        finally:
            pool_maintenance.wake.set()
            maintenance.join()

    thread = Thread(target=worker, name="account-watcher", daemon=True)
    thread.start()
    return thread


def resolve_web_asset(requested_path: str) -> Path | None:
    if not WEB_DIST_DIR.exists():
        return None
    clean_path = requested_path.strip("/")
    base_dir = WEB_DIST_DIR.resolve()
    candidates = [base_dir / "index.html"] if not clean_path else [
        base_dir / Path(clean_path),
        base_dir / clean_path / "index.html",
        base_dir / f"{clean_path}.html",
    ]
    for candidate in candidates:
        try:
            candidate.resolve().relative_to(base_dir)
        except ValueError:
            continue
        if candidate.is_file():
            return candidate
    return None
