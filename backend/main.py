"""
CS2 ShadowPro — FastAPI backend.

Run (development):
    uvicorn backend.main:app --reload --port 8000

Endpoints:
    GET  /api/user/{steam_id}           Registration status (200 = registered, 404 = needs setup)
    GET  /api/profile/{steam_id}        Steam profile (name, avatar)
    GET  /api/matches/{steam_id}        User's processed matches from DB
    POST /api/setup                     Save Match Auth Code + starting share code; triggers sync
    POST /api/sync/{steam_id}           Trigger an immediate sync; returns job_id to poll
    POST /api/import                    Upload a .dem file, process in background
    GET  /api/import/{job_id}           Poll processing / sync job status
"""

import shutil
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from backend.config import DB_PATH, DEMOS_USER_DIR, STEAM_API_KEY
from backend.db import connect, init_schema, get_user, upsert_user
from backend.processing import process_demo
from backend.steam_api import fetch_profile
from backend.sync import sync_user

app = FastAPI(title="CS2 ShadowPro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job registry — fine for single-server; swap for DB at scale.
_jobs: dict[str, dict] = {}
_executor = ThreadPoolExecutor(max_workers=2)


def _map_display(map_name: Optional[str]) -> dict:
    name = map_name or "unknown"
    display_map = {
        "de_mirage": "Mirage", "de_inferno": "Inferno", "de_dust2": "Dust II",
        "de_ancient": "Ancient", "de_nuke": "Nuke", "de_anubis": "Anubis",
        "de_vertigo": "Vertigo", "de_overpass": "Overpass", "de_cache": "Cache",
    }
    display = display_map.get(name, name.replace("de_", "").title())
    key = name.replace("de_", "")
    return {"key": key, "name": name, "display": display}


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/api/user/{steam_id}")
def get_user_status(steam_id: str):
    """200 if registered with an auth code, 404 otherwise."""
    conn = connect(DB_PATH)
    try:
        init_schema(conn)
        user = get_user(conn, steam_id)
    finally:
        conn.close()
    if not user or not user["match_auth_code"]:
        raise HTTPException(status_code=404, detail="Not registered")
    return {"registered": True}


@app.get("/api/profile/{steam_id}")
async def get_profile(steam_id: str):
    if not STEAM_API_KEY:
        return {"steam_id": steam_id, "personaname": None, "avatar": None}
    try:
        return await fetch_profile(steam_id, STEAM_API_KEY)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/matches/{steam_id}")
def get_matches(steam_id: str):
    conn = connect(DB_PATH)
    try:
        init_schema(conn)
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
    finally:
        conn.close()

    matches = []
    for r in rows:
        score = (
            {"ct": r["score_ct"], "t": r["score_t"]}
            if r["score_ct"] is not None else None
        )
        k = r["kills"] or 0
        d = r["deaths"] or 1
        stats = (
            {"k": k, "d": r["deaths"] or 0, "a": r["assists"] or 0,
             "kd": f"{k/d:.2f}", "hs_pct": r["hs_pct"] or 0}
            if r["kills"] is not None else None
        )
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


@app.post("/api/setup")
def setup_user(
    steam_id: str = Form(...),
    match_auth_code: str = Form(...),
    last_share_code: str = Form(...),
):
    """Save Match Auth Code + starting share code, then trigger an immediate sync."""
    conn = connect(DB_PATH)
    try:
        init_schema(conn)
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
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run():
        result = sync_user(steam_id)
        _jobs[job_id].update({"status": "done", **result})

    background_tasks.add_task(_executor.submit, _run)
    return {"job_id": job_id}


@app.post("/api/import")
async def import_demo(
    background_tasks: BackgroundTasks,
    steam_id: str = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename or not file.filename.endswith(".dem"):
        raise HTTPException(status_code=400, detail="Only .dem files are accepted.")

    job_id   = str(uuid.uuid4())
    safe_name = f"{int(time.time())}_{file.filename}"
    dest     = DEMOS_USER_DIR / safe_name

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


# Map overview bounds (pos_x, pos_y, scale) for coordinate normalisation.
# Source: CS2 map overview files. Radar image is 1024×1024 px.
_MAP_BOUNDS: dict[str, tuple[float, float, float]] = {
    "de_mirage":   (-3230.0,  1713.0, 5.00),
    "de_inferno":  (-2087.0,  3870.0, 4.90),
    "de_dust2":    (-2476.0,  3239.0, 4.40),
    "de_ancient":  (-2953.0,  2164.0, 5.00),
    "de_nuke":     (-3453.0,  2887.0, 7.00),
    "de_anubis":   (-2796.0,  3328.0, 5.22),
    "de_vertigo":  (-3168.0,  1762.0, 4.00),
    "de_overpass": (-4831.0,  1781.0, 5.20),
}


def _normalize_xy(x: float, y: float, map_name: str) -> tuple[float, float]:
    pos_x, pos_y, scale = _MAP_BOUNDS.get(map_name, (-3230.0, 1830.0, 5.0))
    norm_x = (x - pos_x) / (scale * 1024)
    norm_y = (pos_y - y) / (scale * 1024)
    return max(0.0, min(1.0, norm_x)), max(0.0, min(1.0, norm_y))


@app.get("/api/situations/{demo_id}")
def get_match_situations(demo_id: str):
    conn = connect(DB_PATH)
    try:
        init_schema(conn)
        match_row = conn.execute(
            "SELECT * FROM matches WHERE demo_id = ?", (demo_id,)
        ).fetchone()
        rows = conn.execute(
            """
            SELECT id, round_num, tick, source_event, player_steamid, player_name,
                   player_side, player_place, player_x, player_y,
                   economy_bucket, alive_ct, alive_t, phase,
                   time_remaining_s, smokes_active, mollies_active,
                   clip_start_tick, clip_end_tick
            FROM situations
            WHERE source = 'user' AND demo_id = ?
            ORDER BY round_num, tick, player_steamid
            """,
            (demo_id,),
        ).fetchall()
    finally:
        conn.close()

    match_info = dict(match_row) if match_row else {}
    map_name = match_info.get("map") or "de_mirage"
    steam_id = match_info.get("steam_id") or ""

    all_sits = [dict(r) for r in rows]

    # Keep only the user's own situations (by their SteamID64).
    if steam_id:
        uid = int(steam_id)
        user_sits = [s for s in all_sits if s["player_steamid"] == uid]
        if not user_sits:
            user_sits = all_sits  # fallback: demo didn't match steam_id
    else:
        user_sits = all_sits

    situations = []
    for s in user_sits:
        nx, ny = _normalize_xy(s.get("player_x") or 0.0, s.get("player_y") or 0.0, map_name)
        situations.append({
            "id": s["id"],
            "round_num": s["round_num"],
            "tick": s["tick"],
            "clip_start_tick": s["clip_start_tick"],
            "clip_end_tick": s["clip_end_tick"],
            "player_side": s["player_side"],
            "player_place": s["player_place"],
            "player_name": s["player_name"],
            "economy_bucket": s["economy_bucket"],
            "alive_ct": s["alive_ct"],
            "alive_t": s["alive_t"],
            "phase": s["phase"],
            "time_remaining_s": s["time_remaining_s"],
            "smokes_active": s["smokes_active"] or 0,
            "mollies_active": s["mollies_active"] or 0,
            "player_x_norm": nx,
            "player_y_norm": ny,
        })

    return {
        "match": {
            "demo_id": demo_id,
            "map": _map_display(map_name),
            "score_ct": match_info.get("score_ct"),
            "score_t": match_info.get("score_t"),
            "round_count": match_info.get("round_count"),
        },
        "situations": situations,
    }


@app.get("/api/round/{demo_id}/{round_num}")
def get_round_detail(demo_id: str, round_num: int):
    """All player position samples for a round — drives animated playback."""
    conn = connect(DB_PATH)
    try:
        init_schema(conn)
        match_row = conn.execute(
            "SELECT * FROM matches WHERE demo_id = ?", (demo_id,)
        ).fetchone()
        rows = conn.execute(
            """
            SELECT player_steamid, player_name, player_side,
                   player_x, player_y, tick, source_event
            FROM situations
            WHERE source = 'user' AND demo_id = ? AND round_num = ?
            ORDER BY tick, player_steamid
            """,
            (demo_id, round_num),
        ).fetchall()
    finally:
        conn.close()

    match_info = dict(match_row) if match_row else {}
    map_name   = match_info.get("map") or "de_mirage"
    steam_id   = match_info.get("steam_id") or ""

    tracks: dict[str, dict] = {}
    all_ticks: list[int] = []
    events: list[dict] = []

    for r in rows:
        sid = str(r["player_steamid"])
        nx, ny = _normalize_xy(r["player_x"] or 0.0, r["player_y"] or 0.0, map_name)
        if sid not in tracks:
            tracks[sid] = {
                "steamid": sid,
                "name": r["player_name"] or sid,
                "side": (r["player_side"] or "ct").lower(),
                "is_focal": sid == steam_id,
                "samples": [],
            }
        tracks[sid]["samples"].append({"tick": r["tick"], "x": nx, "y": ny})
        all_ticks.append(r["tick"])
        if r["source_event"] in ("kill", "bomb"):
            events.append({
                "tick": r["tick"],
                "type": r["source_event"],
                "steamid": sid,
                "side": (r["player_side"] or "ct").lower(),
            })

    return {
        "round_num": round_num,
        "tick_start": min(all_ticks) if all_ticks else 0,
        "tick_end":   max(all_ticks) if all_ticks else 0,
        "players": list(tracks.values()),
        "events": events,
    }
