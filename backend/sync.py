"""
Auto-sync: fetches new CS2 share codes via Steam Web API,
resolves each to a demo URL, downloads, decompresses, and processes.
"""
from __future__ import annotations

import asyncio
import bz2
import shutil
from pathlib import Path

import asyncpg
import httpx

from backend import config
from backend.processing import process_demo

_SHARE_CODE_API = (
    "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1/"
)


# ── DB helpers (standalone connections — runs in a thread pool) ────────────────

async def _get_user(steam_id: str) -> dict | None:
    conn = await asyncpg.connect(dsn=config.DATABASE_URL)
    try:
        row = await conn.fetchrow("SELECT * FROM users WHERE steam_id = $1", steam_id)
        return dict(row) if row else None
    finally:
        await conn.close()


async def _update_share_code(steam_id: str, code: str) -> None:
    conn = await asyncpg.connect(dsn=config.DATABASE_URL)
    try:
        await conn.execute(
            "UPDATE users SET last_share_code = $2, updated_at = NOW() WHERE steam_id = $1",
            steam_id, code,
        )
    finally:
        await conn.close()


# ── Share code walking ─────────────────────────────────────────────────────────

def _next_share_codes(steam_id: str, auth_code: str, last_code: str) -> list[str]:
    codes: list[str] = []
    cursor = last_code

    while len(codes) < 10:
        try:
            r = httpx.get(
                _SHARE_CODE_API,
                params={
                    "key":        config.STEAM_API_KEY,
                    "steamid":    steam_id,
                    "steamidkey": auth_code,
                    "knowncode":  cursor,
                },
                timeout=10,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429 and codes:
                break
            raise RuntimeError(
                f"Steam API error {exc.response.status_code}: {exc.response.text}"
            ) from exc

        next_code = r.json().get("result", {}).get("nextcode", "n/a")
        if next_code in ("n/a", cursor):
            break

        codes.append(next_code)
        cursor = next_code

    return codes


def _resolve_demo_url(share_code: str) -> str:
    r = httpx.post(
        f"{config.RESOLVER_URL}/resolve",
        json={"shareCode": share_code},
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Resolver: {data['error']}")
    return data["demoUrl"]


def _download_and_decompress(demo_url: str, dem_path: Path) -> None:
    bz2_path = dem_path.with_suffix(".dem.bz2")
    with httpx.stream("GET", demo_url, timeout=120, follow_redirects=True) as r:
        r.raise_for_status()
        with bz2_path.open("wb") as fh:
            for chunk in r.iter_bytes(chunk_size=65536):
                fh.write(chunk)
    with bz2.open(bz2_path, "rb") as src, dem_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    bz2_path.unlink()


# ── Process a single share code ───────────────────────────────────────────────

def process_share_code(steam_id: str, share_code: str) -> dict:
    """Resolve, download, and process a single share code. Returns summary dict."""
    slug     = share_code.replace("CSGO-", "").replace("-", "_")
    demo_id  = f"user_{steam_id}_{slug}.dem"
    dem_path = config.DEMOS_USER_DIR / steam_id / demo_id
    dem_path.parent.mkdir(parents=True, exist_ok=True)

    demo_url = _resolve_demo_url(share_code)
    _download_and_decompress(demo_url, dem_path)
    return process_demo(dem_path, steam_id, demo_id, share_code=share_code)


# ── Main entry point ───────────────────────────────────────────────────────────

def sync_user(steam_id: str) -> dict:
    """
    Full sync for one user, guarded by a PostgreSQL advisory lock so that
    web and worker (separate processes) can't double-download. The lock is
    held on a dedicated connection for the duration of the sync.

      1. Read cursor from DB
      2. Poll GetNextMatchSharingCode for new codes
      3. resolve → download → decompress → process each
      4. Advance cursor in DB after each successful match
    """
    sid = int(steam_id)
    lock_loop = asyncio.new_event_loop()
    try:
        conn = lock_loop.run_until_complete(asyncpg.connect(dsn=config.DATABASE_URL))
        try:
            acquired = lock_loop.run_until_complete(
                conn.fetchval("SELECT pg_try_advisory_lock($1::bigint)", sid)
            )
            if not acquired:
                return {"new_matches": 0, "message": "Sync already in progress for this user"}
            try:
                return _sync_user_locked(steam_id)
            finally:
                lock_loop.run_until_complete(
                    conn.fetchval("SELECT pg_advisory_unlock($1::bigint)", sid)
                )
        finally:
            lock_loop.run_until_complete(conn.close())
    finally:
        lock_loop.close()


def _sync_user_locked(steam_id: str) -> dict:
    if not config.STEAM_API_KEY:
        return {"error": "STEAM_API_KEY not set"}

    user = asyncio.run(_get_user(steam_id))

    if not user:
        return {"error": "User not registered — call /api/setup first"}
    if not user.get("match_auth_code"):
        return {"error": "No match_auth_code stored"}
    if not user.get("last_share_code"):
        return {"error": "No starting share code — provide one at /api/setup"}

    try:
        new_codes = _next_share_codes(
            steam_id, user["match_auth_code"], user["last_share_code"]
        )
    except Exception as exc:
        return {"error": f"GetNextMatchSharingCode failed: {exc}"}

    if not new_codes:
        return {"new_matches": 0, "message": "No new matches since last sync"}

    processed = 0
    errors: list[dict] = []

    for share_code in new_codes:
        slug     = share_code.replace("CSGO-", "").replace("-", "_")
        demo_id  = f"user_{steam_id}_{slug}.dem"
        dem_path = config.DEMOS_USER_DIR / steam_id / demo_id
        dem_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            demo_url = _resolve_demo_url(share_code)
            _download_and_decompress(demo_url, dem_path)
            process_demo(dem_path, steam_id, demo_id, share_code=share_code)
            processed += 1
        except Exception as exc:
            err_str = str(exc)
            errors.append({"share_code": share_code, "error": err_str})
            is_transient = any(code in err_str for code in ("502", "503", "504"))
            if not is_transient:
                asyncio.run(_update_share_code(steam_id, share_code))
            continue

        asyncio.run(_update_share_code(steam_id, share_code))

    return {"new_matches": processed, "errors": errors}
