"""
Auto-sync: fetches new CS2 Premier share codes via Steam Web API,
resolves each to a demo URL via the Node sharecode-resolver,
then downloads, decompresses, and processes each demo.
"""

import bz2
import shutil
from pathlib import Path

import httpx

from backend.config import DB_PATH, DEMOS_USER_DIR, STEAM_API_KEY, RESOLVER_URL
from backend.db import connect, init_schema, get_user, upsert_user
from backend.processing import process_demo

_SHARE_CODE_API = (
    "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1/"
)


def _next_share_codes(steam_id: str, auth_code: str, last_code: str) -> list[str]:
    """
    Walk GetNextMatchSharingCode starting from last_code.
    Returns new codes oldest→newest, up to 10.
    Stops on 'n/a' or 429 (rate-limited after partial results).
    """
    codes: list[str] = []
    cursor = last_code

    while len(codes) < 10:
        try:
            r = httpx.get(
                _SHARE_CODE_API,
                params={
                    "key": STEAM_API_KEY,
                    "steamid": steam_id,
                    "steamidkey": auth_code,
                    "knowncode": cursor,
                },
                timeout=10,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 429 and codes:
                break  # got some codes already; process them, retry remainder next sync
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
        f"{RESOLVER_URL}/resolve",
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


def sync_user(steam_id: str) -> dict:
    """
    Full sync for one user:
      1. Read cursor from DB
      2. Poll GetNextMatchSharingCode for new codes
      3. resolve → download → decompress → process each
      4. Advance cursor in DB after each successful match

    Returns summary dict.
    """
    if not STEAM_API_KEY:
        return {"error": "STEAM_API_KEY not set"}

    conn = connect(DB_PATH)
    init_schema(conn)
    user = get_user(conn, steam_id)
    conn.close()

    if not user:
        return {"error": "User not registered — call /api/setup first"}
    if not user["match_auth_code"]:
        return {"error": "No match_auth_code stored"}
    if not user["last_share_code"]:
        return {"error": "No starting share code — provide one at /api/setup"}

    try:
        new_codes = _next_share_codes(steam_id, user["match_auth_code"], user["last_share_code"])
    except Exception as exc:
        return {"error": f"GetNextMatchSharingCode failed: {exc}"}

    if not new_codes:
        return {"new_matches": 0, "message": "No new matches since last sync"}

    processed = 0
    errors: list[dict] = []

    for share_code in new_codes:
        slug     = share_code.replace("CSGO-", "").replace("-", "_")
        demo_id  = f"user_{steam_id}_{slug}"
        dem_path = DEMOS_USER_DIR / f"{demo_id}.dem"

        try:
            demo_url = _resolve_demo_url(share_code)
            _download_and_decompress(demo_url, dem_path)
            process_demo(dem_path, steam_id, demo_id)
            processed += 1
        except Exception as exc:
            err_str = str(exc)
            errors.append({"share_code": share_code, "error": err_str})
            # Advance cursor past permanently-expired replays so they don't block future syncs
            if "502" in err_str or "404" in err_str:
                conn = connect(DB_PATH)
                upsert_user(conn, steam_id, last_share_code=share_code)
                conn.close()
            continue

        conn = connect(DB_PATH)
        upsert_user(conn, steam_id, last_share_code=share_code)
        conn.close()

    return {"new_matches": processed, "errors": errors}
