from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from threading import Event, Thread

from fastapi import HTTPException, Request

from services.account_service import account_service
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


def _watcher_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _watcher_elapsed(start_perf: float) -> str:
    return f"{time.perf_counter() - start_perf:.2f}s"


def _watcher_tokens_preview(tokens: list[str], limit: int = 5) -> str:
    items = [str(token or "").strip()[:12] for token in tokens if str(token or "").strip()]
    if not items:
        return "-"
    preview = items[:limit]
    suffix = f" ...(+{len(items) - limit})" if len(items) > limit else ""
    return ", ".join(preview) + suffix


def start_limited_account_watcher(stop_event: Event) -> Thread:
    def _refresh_probe_tokens(tokens: list[str], *, defer_invalid_removal: bool) -> tuple[list[str], list[str], bool]:
        before = {token: account_service.get_account(token) for token in tokens}
        account_service.refresh_accounts(tokens, defer_invalid_removal=defer_invalid_removal)
        dead_found = False
        removed_tokens: list[str] = []
        changed_tokens: list[str] = []
        for token in tokens:
            after = account_service.get_account(token)
            after_status = str((after or {}).get("status") or "").strip()
            before_status = str((before.get(token) or {}).get("status") or "").strip()
            if after is None:
                removed_tokens.append(token)
                dead_found = True
                continue
            if after_status != before_status:
                changed_tokens.append(f"{token[:12]}:{before_status}->{after_status}")
            if bool(after.get("suspect")):
                dead_found = True
                continue
            if after_status in {"\u5f02\u5e38", "\u7981\u7528", "\u9650\u6d41"}:
                dead_found = True
                continue
            if before_status == "\u6b63\u5e38" and after_status != "\u6b63\u5e38":
                dead_found = True
        return removed_tokens, changed_tokens, dead_found

    def _run_full_cycle(interval_seconds: int) -> None:
        cycle_perf = time.perf_counter()
        try:
            print(f"[account-watcher] cycle start at {_watcher_now()}, interval={interval_seconds}s")
            limited_tokens = account_service.list_limited_tokens()
            expiring_tokens = account_service.list_expiring_access_tokens()
            keepalive_tokens = account_service.list_refresh_token_keepalive_tokens()
            tokens = list(dict.fromkeys([*limited_tokens, *expiring_tokens]))
            expiring_token_set = set(expiring_tokens)
            keepalive_tokens = [token for token in keepalive_tokens if token not in expiring_token_set]
            if tokens:
                check_perf = time.perf_counter()
                print(
                    "[account-watcher] checking start "
                    f"at {_watcher_now()}, "
                    f"limited={len(limited_tokens)}, expiring={len(expiring_tokens)}, "
                    f"tokens={_watcher_tokens_preview(tokens)}"
                )
                account_service.refresh_accounts(tokens)
                print(f"[account-watcher] checking end at {_watcher_now()}, elapsed={_watcher_elapsed(check_perf)}")
            probe_seen = set(tokens)
            probe_round = 0
            while True:
                probe_tokens = account_service.list_probe_candidate_tokens(limit=5, excluded_tokens=probe_seen)
                if not probe_tokens:
                    break
                probe_round += 1
                probe_seen.update(probe_tokens)
                probe_perf = time.perf_counter()
                print(
                    "[account-watcher] probe start "
                    f"at {_watcher_now()}, round={probe_round}, count={len(probe_tokens)}, "
                    f"tokens={_watcher_tokens_preview(probe_tokens)}"
                )
                removed_tokens, changed_tokens, dead_found = _refresh_probe_tokens(
                    probe_tokens,
                    defer_invalid_removal=False,
                )
                print(
                    "[account-watcher] probe end "
                    f"at {_watcher_now()}, round={probe_round}, elapsed={_watcher_elapsed(probe_perf)}, "
                    f"removed={_watcher_tokens_preview(removed_tokens)}, "
                    f"changed={'; '.join(changed_tokens) if changed_tokens else '-'}, "
                    f"continue={'yes' if dead_found else 'no'}"
                )
                if not dead_found:
                    break
            if keepalive_tokens:
                keepalive_perf = time.perf_counter()
                print(
                    "[account-watcher] keepalive start "
                    f"at {_watcher_now()}, count={len(keepalive_tokens)}, "
                    f"tokens={_watcher_tokens_preview(keepalive_tokens)}"
                )
                result = account_service.keepalive_refresh_tokens(keepalive_tokens)
                if result.get("errors"):
                    print(f"[account-watcher] keepalive errors: {result['errors']}")
                print(
                    "[account-watcher] keepalive end "
                    f"at {_watcher_now()}, elapsed={_watcher_elapsed(keepalive_perf)}, "
                    f"refreshed={result.get('refreshed', 0)}, errors={len(result.get('errors') or [])}"
                )
        except Exception as exc:
            print(f"[account-watcher] fail at {_watcher_now()}: {exc}")
        finally:
            print(f"[account-watcher] cycle end at {_watcher_now()}, elapsed={_watcher_elapsed(cycle_perf)}")

    def _run_suspect_probe() -> None:
        suspect_tokens = account_service.list_suspect_tokens(limit=3)
        if not suspect_tokens:
            return
        suspect_perf = time.perf_counter()
        print(
            "[account-watcher] suspect probe start "
            f"at {_watcher_now()}, count={len(suspect_tokens)}, "
            f"tokens={_watcher_tokens_preview(suspect_tokens)}"
        )
        try:
            removed_tokens, changed_tokens, dead_found = _refresh_probe_tokens(
                suspect_tokens,
                defer_invalid_removal=False,
            )
            print(
                "[account-watcher] suspect probe end "
                f"at {_watcher_now()}, elapsed={_watcher_elapsed(suspect_perf)}, "
                f"removed={_watcher_tokens_preview(removed_tokens)}, "
                f"changed={'; '.join(changed_tokens) if changed_tokens else '-'}, "
                f"continue={'yes' if dead_found else 'no'}"
            )
        except Exception as exc:
            print(f"[account-watcher] suspect probe fail at {_watcher_now()}: {exc}")

    def worker() -> None:
        last_full_cycle_started: float | None = None
        while not stop_event.is_set():
            interval_seconds = max(60, int(config.refresh_account_interval_minute or 1) * 60)
            suspect_probe_interval_seconds = max(1.0, float(config.suspect_account_probe_interval_secs or 60))
            now = time.time()
            should_run_full = last_full_cycle_started is None or (now - last_full_cycle_started) >= interval_seconds
            if should_run_full:
                last_full_cycle_started = now
                _run_full_cycle(interval_seconds)
            else:
                _run_suspect_probe()
            elapsed_since_full = 0.0 if last_full_cycle_started is None else max(0.0, time.time() - last_full_cycle_started)
            next_full_in = max(1.0, interval_seconds - elapsed_since_full)
            stop_event.wait(min(suspect_probe_interval_seconds, next_full_in))

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
