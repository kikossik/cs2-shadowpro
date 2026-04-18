"""
CS2 ShadowPro — FastAPI backend.

Run (development):
    cd /home/tomyan/Code/cs2-shadowpro
    uvicorn backend.main:app --reload --port 8000

Endpoints:
    GET  /api/profile/{steam_id}       Real Steam profile (name, avatar)
    GET  /api/matches/{steam_id}       User's processed matches from DB
    POST /api/import                   Upload a .dem file, process in background
    GET  /api/import/{job_id}          Poll processing status
    POST /api/setup                    Save user's Match Auth Code + starting share code
    POST /api/sync/{steam_id}          Trigger an immediate sync for a user
"""

import os
import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from backend.steam_api import fetch_profile  # noqa: E402
from backend.processing import process_demo  # noqa: E402
from backend.db_users import init_users_table, upsert_user  # noqa: E402
from backend.user_matches import sync_user  # noqa: E402

STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
DB_PATH = Path(__file__).resolve().parent.parent / "situations.db"
DEMOS_USER_DIR = Path(__file__).resolve().parent.parent / "demos_user"
DEMOS_USER_DIR.mkdir(exist_ok=True)

app = FastAPI(title="CS2 ShadowPro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job registry.  Good enough for single-server; replace with DB at scale.
_jobs: dict[str, dict] = {}
_executor = ThreadPoolExecutor(max_workers=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _map_display(map_name: Optional[str]) -> dict:
    """Convert 'de_mirage' → {key, name, display}."""
    name = map_name or "unknown"
    display_map = {
        "de_mirage": "Mirage", "de_inferno": "Inferno", "de_dust2": "Dust II",
        "de_ancient": "Ancient", "de_nuke": "Nuke", "de_anubis": "Anubis",
        "de_vertigo": "Vertigo", "de_overpass": "Overpass", "de_cache": "Cache",
    }
    display = display_map.get(name, name.replace("de_", "").title())
    key = name.replace("de_", "")
    return {"key": key, "name": name, "display": display}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/api/profile/{steam_id}")
async def get_profile(steam_id: str):
    if not STEAM_API_KEY:
        # Graceful fallback: return Steam ID only, no name/avatar
        return {"steam_id": steam_id, "personaname": None, "avatar": None}
    try:
        return await fetch_profile(steam_id, STEAM_API_KEY)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/matches/{steam_id}")
def get_matches(steam_id: str):
    conn = _db_connect()
    try:
        rows = conn.execute(
            """
            SELECT demo_id, map, date_ts, round_count, score_ct, score_t,
                   user_side_first, user_result, kills, deaths, assists,
                   hs_pct, situations_count
            FROM matches
            WHERE source = 'user' AND steam_id = ?
            ORDER BY date_ts DESC
            LIMIT 30
            """,
            (steam_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        # matches table doesn't exist yet (fresh DB with no user demos)
        rows = []
    finally:
        conn.close()

    matches = []
    for r in rows:
        score = None
        if r["score_ct"] is not None and r["score_t"] is not None:
            score = {"ct": r["score_ct"], "t": r["score_t"]}

        stats = None
        if r["kills"] is not None:
            k = r["kills"] or 0
            d = r["deaths"] or 1
            stats = {
                "k": k,
                "d": r["deaths"] or 0,
                "a": r["assists"] or 0,
                "kd": f"{k/d:.2f}",
                "hs_pct": r["hs_pct"] or 0,
            }

        matches.append({
            "id": r["demo_id"],
            "map": _map_display(r["map"]),
            "date": r["date_ts"],
            "result": r["user_result"],
            "user_side_first": r["user_side_first"],
            "score": score,
            "round_count": r["round_count"],
            "stats": stats,
            "situations": r["situations_count"] or 0,
        })

    return matches


@app.post("/api/import")
async def import_demo(
    background_tasks: BackgroundTasks,
    steam_id: str = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename or not file.filename.endswith(".dem"):
        raise HTTPException(status_code=400, detail="Only .dem files are accepted.")

    job_id = str(uuid.uuid4())
    safe_name = f"{int(time.time())}_{file.filename}"
    dest = DEMOS_USER_DIR / safe_name

    # Save uploaded file synchronously (fast, small enough)
    with dest.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)

    _jobs[job_id] = {"status": "processing", "demo_id": safe_name, "steam_id": steam_id}

    def _run():
        try:
            result = process_demo(dest, steam_id, safe_name)
            _jobs[job_id].update({"status": "done", **result})
        except Exception as exc:
            _jobs[job_id].update({"status": "error", "error": str(exc)})

    background_tasks.add_task(_executor.submit, _run)
    return {"job_id": job_id, "demo_id": safe_name}


@app.get("/api/import/{job_id}")
def import_status(job_id: str):
    job = _jobs.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job ID")
    return JSONResponse(job)


@app.post("/api/setup")
def setup_user(
    steam_id: str = Form(...),
    match_auth_code: str = Form(...),
    last_share_code: str = Form(...),
):
    """
    Save a user's Match Auth Code and most recent share code.
    Both are obtained from steamcommunity.com/my/gcpd/730.

    After saving, triggers an immediate background sync.
    """
    conn = _db_connect()
    try:
        init_users_table(conn)
        upsert_user(conn, steam_id, match_auth_code=match_auth_code, last_share_code=last_share_code)
    finally:
        conn.close()

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run():
        result = sync_user(steam_id)
        _jobs[job_id].update({"status": "done", **result})

    _executor.submit(_run)
    return {"job_id": job_id, "message": "Setup saved, sync started"}


@app.post("/api/sync/{steam_id}")
def trigger_sync(steam_id: str, background_tasks: BackgroundTasks):
    """Trigger an immediate sync for the given user. Returns a job ID to poll."""
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run():
        result = sync_user(steam_id)
        _jobs[job_id].update({"status": "done", **result})

    background_tasks.add_task(_executor.submit, _run)
    return {"job_id": job_id}
