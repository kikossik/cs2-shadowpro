"""
Auto-sync: fetches new CS2 Premier match share codes via the Steam Web API,
resolves each to a demo URL via the Node sharecode-resolver microservice,
then downloads and processes each demo through the existing pipeline.
"""

import bz2
import os
import shutil
import sqlite3
import time
from pathlib import Path

import httpx

DB_PATH = Path(__file__).resolve().parent.parent / "situations.db"
DEMOS_USER_DIR = Path(__file__).resolve().parent.parent / "demos_user"
DEMOS_USER_DIR.mkdir(exist_ok=True)

SHARE_CODE_API = "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1/"


def _db_connect() -> sqlite3.Connection:
    from backend.db_users import init_users_table
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    init_users_table(conn)
    return conn


def _get_next_share_codes(
    steam_id: str,
    auth_code: str,
    last_share_code: str,
    api_key: str,
) -> list[str]:
    """
    Walk the GetNextMatchSharingCode chain starting from last_share_code.
    Returns new share codes in chronological order (oldest → newest).
    Stops when the API returns 'n/a' or after 10 codes (safety limit).
    """
    codes: list[str] = []
    cursor = last_share_code

    while len(codes) < 10:
        try:
            r = httpx.get(
                SHARE_CODE_API,
                params={
                    "key": api_key,
                    "steamid": steam_id,
                    "steamidkey": auth_code,
                    "knownmatchid": cursor,
                },
                timeout=10,
            )
            r.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Steam API error {exc.response.status_code}: {exc.response.text}"
            ) from exc

        next_code = r.json().get("result", {}).get("nextcode", "n/a")
        if next_code == "n/a" or next_code == cursor:
            break

        codes.append(next_code)
        cursor = next_code

    return codes


def _resolve_demo_url(share_code: str, resolver_url: str) -> str:
    """Call the Node microservice to convert a share code into a demo URL."""
    r = httpx.post(
        f"{resolver_url}/resolve",
        json={"shareCode": share_code},
        timeout=25,
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Resolver error: {data['error']}")
    return data["demoUrl"]


def _download_demo(demo_url: str, bz2_path: Path) -> None:
    """Stream-download a .dem.bz2 from Valve's replay servers."""
    with httpx.stream("GET", demo_url, timeout=120, follow_redirects=True) as r:
        r.raise_for_status()
        with bz2_path.open("wb") as fh:
            for chunk in r.iter_bytes(chunk_size=65536):
                fh.write(chunk)


def _decompress_demo(bz2_path: Path, dem_path: Path) -> None:
    """Decompress .dem.bz2 → .dem in place."""
    with bz2.open(bz2_path, "rb") as src, dem_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    bz2_path.unlink()


def sync_user(steam_id: str) -> dict:
    """
    Full sync for one user:
      1. Read their cursor (last_share_code) from DB
      2. Poll GetNextMatchSharingCode for new codes
      3. For each new code: resolve → download → decompress → process
      4. Advance cursor in DB after each successful match

    Returns a summary dict.
    """
    from backend.db_users import get_user, upsert_user
    from backend.processing import process_demo

    api_key = os.getenv("STEAM_API_KEY", "")
    resolver_url = os.getenv("RESOLVER_URL", "http://127.0.0.1:3001")

    if not api_key:
        return {"error": "STEAM_API_KEY not set"}

    conn = _db_connect()
    user = get_user(conn, steam_id)
    conn.close()

    if not user:
        return {"error": "User not registered — call /api/setup first"}
    if not user["match_auth_code"]:
        return {"error": "No match_auth_code stored for this user"}
    if not user["last_share_code"]:
        return {"error": "No starting share code — user must provide one at setup"}

    auth_code = user["match_auth_code"]
    last_code = user["last_share_code"]

    try:
        new_codes = _get_next_share_codes(steam_id, auth_code, last_code, api_key)
    except Exception as exc:
        return {"error": f"GetNextMatchSharingCode failed: {exc}"}

    if not new_codes:
        return {"new_matches": 0, "message": "No new matches since last sync"}

    processed = 0
    errors = []

    for share_code in new_codes:
        try:
            demo_url = _resolve_demo_url(share_code, resolver_url)

            slug = share_code.replace("CSGO-", "").replace("-", "_")
            demo_id = f"user_{steam_id}_{slug}"
            dem_path = DEMOS_USER_DIR / f"{demo_id}.dem"
            bz2_path = DEMOS_USER_DIR / f"{demo_id}.dem.bz2"

            _download_demo(demo_url, bz2_path)
            _decompress_demo(bz2_path, dem_path)
            process_demo(dem_path, steam_id, demo_id)

            processed += 1

            # Advance cursor only after each successful process
            conn = _db_connect()
            upsert_user(conn, steam_id, last_share_code=share_code)
            conn.close()

        except Exception as exc:
            errors.append({"share_code": share_code, "error": str(exc)})

    return {
        "new_matches": processed,
        "errors": errors,
    }
