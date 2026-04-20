"""CS2 ShadowPro — FastAPI backend (pure PostgreSQL).

Run:
    uvicorn backend.main:app --reload --port 8000
"""
from __future__ import annotations

import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from backend import config, db

app = FastAPI(title="CS2 ShadowPro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job dict — fine for single-server; backed by job_runs table for audit.
_jobs: dict[str, dict] = {}
_executor = ThreadPoolExecutor(max_workers=2)


# ── Lifecycle ──────────────────────────────────────────────────────────────────

@app.on_event("shutdown")
async def _shutdown() -> None:
    await db.close_pool()


# ── Maps ───────────────────────────────────────────────────────────────────────

_MAPS_DIR = Path(__file__).parent.parent / "web" / "public" / "maps"


@app.get("/api/radar/{map_name}")
def get_radar_image(map_name: str):
    """Serve a radar PNG from web/public/maps (bundled in the Docker image)."""
    path = _MAPS_DIR / f"{map_name}.png"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Radar not found: {map_name}")
    return FileResponse(str(path), media_type="image/png")


@app.get("/api/maps")
async def get_maps():
    """All competitive map configs — frontend uses pos_x/pos_y/scale for coordinate transform."""
    rows = await db.get_maps()
    return [
        {
            "name":              r["map_name"],
            "display_name":      r["display_name"],
            "pos_x":             r["pos_x"],
            "pos_y":             r["pos_y"],
            "scale":             r["map_scale"],
            "has_lower_level":   r["has_lower_level"],
            "lower_level_max_z": r["lower_level_max_z"],
        }
        for r in rows
    ]


# ── Users ──────────────────────────────────────────────────────────────────────

@app.get("/api/user/{steam_id}")
async def get_user_status(steam_id: str):
    """200 if registered with an auth code, 404 otherwise."""
    user = await db.get_user(steam_id)
    if not user or not user.get("match_auth_code"):
        raise HTTPException(status_code=404, detail="Not registered")
    return {"registered": True}


@app.get("/api/profile/{steam_id}")
async def get_profile(steam_id: str):
    if not config.STEAM_API_KEY:
        return {"steam_id": steam_id, "personaname": None, "avatar": None}
    import httpx
    url = (
        "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
        f"?key={config.STEAM_API_KEY}&steamids={steam_id}"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
    resp.raise_for_status()
    players = resp.json().get("response", {}).get("players", [])
    if not players:
        raise HTTPException(status_code=404, detail="Steam profile not found")
    p = players[0]
    return {"steam_id": steam_id, "personaname": p.get("personaname"), "avatar": p.get("avatarfull")}


# ── Matches ────────────────────────────────────────────────────────────────────

_MAP_DISPLAY: dict[str, str] = {
    "de_ancient": "Ancient", "de_anubis": "Anubis", "de_dust2": "Dust 2",
    "de_inferno": "Inferno", "de_mirage": "Mirage", "de_nuke": "Nuke",
    "de_overpass": "Overpass",
}


def _map_display(map_name: str | None) -> dict:
    name    = map_name or "unknown"
    display = _MAP_DISPLAY.get(name, name.replace("de_", "").title())
    return {"key": name.replace("de_", ""), "name": name, "display": display}


def _read_round_replay_payload(demo_id: str, round_num: int, parquet_dir: str, map_name: str) -> dict:
    base = Path(parquet_dir)
    stem = demo_id

    def _read(field: str) -> pl.DataFrame:
        return pl.read_parquet(base / f"{stem}_{field}.parquet")

    ticks_all = _read("ticks")
    rounds_all = _read("rounds")
    shots_all = _read("shots")
    smokes_all = _read("smokes")
    infernos_all = _read("infernos")
    flashes_all = _read("flashes")
    grens_all = _read("grenade_paths")

    rn = round_num
    r_rounds = rounds_all.filter(pl.col('round_num') == rn)

    # Trim the pre-freeze idle (~8s of buy time) so user/pro replays align on
    # the action start. tick_list[0] becomes freeze_end.
    raw_ticks = ticks_all.filter(pl.col('round_num') == rn)
    freeze_end = (
        int(r_rounds['freeze_end'][0]) if r_rounds.height > 0 and 'freeze_end' in r_rounds.columns
        else (int(raw_ticks['tick'].min()) if raw_ticks.height > 0 else 0)
    )

    ticks = raw_ticks.filter(pl.col('tick') >= freeze_end).sort('tick')
    shots = shots_all.filter((pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end))
    smokes = smokes_all.filter(
        (pl.col('round_num') == rn)
        & ((pl.col('end_tick').is_null()) | (pl.col('end_tick') >= freeze_end))
    )
    infernos = infernos_all.filter(
        (pl.col('round_num') == rn)
        & ((pl.col('end_tick').is_null()) | (pl.col('end_tick') >= freeze_end))
    )
    flashes = flashes_all.filter((pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end))
    grens = grens_all.filter(
        (pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end)
    ).sort('tick')

    tick_list = sorted(ticks['tick'].unique().to_list())

    ticks_by_tick: dict[int, list[dict]] = {t: [] for t in tick_list}
    cols = set(ticks.columns)
    for row in ticks.iter_rows(named=True):
        t = int(row['tick'])
        ticks_by_tick[t].append({
            "steamid":        str(row.get('steamid') or ''),
            "name":           row.get('name') or '',
            "side":           (row.get('side') or 'ct').lower(),
            "x":              float(row.get('X') or 0.0),
            "y":              float(row.get('Y') or 0.0),
            "z":              float(row.get('Z') or 0.0),
            "yaw":            float(row.get('yaw') or 0.0) if 'yaw' in cols else 0.0,
            "health":         int(row.get('health') or 0),
            "inventory":      list(row.get('inventory') or []),
            "flash_duration": float(row.get('flash_duration') or 0.0),
        })
    ticks_payload = [{"tick": t, "players": ticks_by_tick[t]} for t in tick_list]

    shots_payload = []
    for row in shots.iter_rows(named=True):
        shots_payload.append({
            "tick":           int(row['tick']),
            "player_steamid": str(row.get('player_steamid') or ''),
            "weapon":         str(row.get('weapon') or ''),
        })

    # Utility can outlive a round; end_tick is null when the round ends first.
    round_last_tick = tick_list[-1] if tick_list else 0

    smokes_payload = []
    for row in smokes.iter_rows(named=True):
        end = row['end_tick'] if row['end_tick'] is not None else round_last_tick
        smokes_payload.append({
            "start_tick":    max(int(row['start_tick']), freeze_end),
            "end_tick":      int(end),
            "x":             float(row.get('X') or 0.0),
            "y":             float(row.get('Y') or 0.0),
            "thrower_name":  row.get('thrower_name') or '',
        })

    infernos_payload = []
    for row in infernos.iter_rows(named=True):
        end = row['end_tick'] if row['end_tick'] is not None else round_last_tick
        infernos_payload.append({
            "start_tick": max(int(row['start_tick']), freeze_end),
            "end_tick":   int(end),
            "x":          float(row.get('X') or 0.0),
            "y":          float(row.get('Y') or 0.0),
        })

    flashes_payload = []
    for row in flashes.iter_rows(named=True):
        flashes_payload.append({
            "tick": int(row['tick']),
            "x":    float(row.get('X') or 0.0),
            "y":    float(row.get('Y') or 0.0),
        })

    gren_by_entity: dict[int, dict] = {}
    for row in grens.iter_rows(named=True):
        if row.get('X') is None or row.get('Y') is None:
            continue
        eid = int(row['entity_id'])
        if eid not in gren_by_entity:
            gren_by_entity[eid] = {
                "entity_id":    eid,
                "grenade_type": row['grenade_type'],
                "path":         [],
            }
        gren_by_entity[eid]["path"].append({
            "tick": int(row['tick']),
            "x":    float(row['X']),
            "y":    float(row['Y']),
        })
    grenades_payload = list(gren_by_entity.values())

    return {
        "map":              map_name,
        "round_num":        round_num,
        "freeze_end_tick":  freeze_end,
        "tick_list":        tick_list,
        "ticks":            ticks_payload,
        "shots":            shots_payload,
        "smokes":           smokes_payload,
        "infernos":         infernos_payload,
        "flashes":          flashes_payload,
        "grenade_paths":    grenades_payload,
    }


@app.get("/api/matches/{steam_id}")
async def get_matches(steam_id: str):
    rows = await db.get_user_matches(steam_id, limit=30)
    result = []
    for r in rows:
        k = r.get("kills") or 0
        d = r.get("deaths") or 1
        result.append({
            "id":              r["demo_id"],
            "map":             _map_display(r["map_name"]),
            "date":            int(r["match_date"].timestamp()) if r["match_date"] else None,
            "rounds":          None,
            "result":          r["user_result"],
            "user_side_first": r["user_side_first"],
            "score":           {"ct": r["score_ct"], "t": r["score_t"]}
                               if r["score_ct"] is not None else None,
            "round_count":     r["round_count"],
            "stats":           {
                "k": k, "d": r.get("deaths") or 0, "a": r.get("assists") or 0,
                "kd": f"{k / max(d, 1):.2f}", "hs_pct": r.get("hs_pct") or 0,
            } if r.get("kills") is not None else None,
            "situations": 0,
        })
    return result


# ── Setup / Sync ───────────────────────────────────────────────────────────────

@app.post("/api/setup")
async def setup_user(
    steam_id:        str = Form(...),
    match_auth_code: str = Form(...),
    last_share_code: str = Form(...),
):
    await db.upsert_user(steam_id,
                         match_auth_code=match_auth_code,
                         last_share_code=last_share_code)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run() -> None:
        from backend.sync import process_share_code, sync_user
        results = []
        try:
            r = process_share_code(steam_id, last_share_code)
            results.append(r)
        except Exception as exc:
            results.append({"error": str(exc)})
        sync_result = sync_user(steam_id)
        results.append(sync_result)
        new_matches = sum(1 for r in results if "demo_id" in r)
        new_matches += sync_result.get("new_matches", 0)
        _jobs[job_id].update({"status": "done", "new_matches": new_matches})

    _executor.submit(_run)
    return {"job_id": job_id, "message": "Setup saved, sync started"}


@app.post("/api/sync/{steam_id}")
async def trigger_sync(steam_id: str, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run() -> None:
        from backend.sync import sync_user
        result = sync_user(steam_id)
        _jobs[job_id].update({"status": "done", **result})

    background_tasks.add_task(_executor.submit, _run)
    return {"job_id": job_id}


# ── Demo import ────────────────────────────────────────────────────────────────

@app.post("/api/import")
async def import_demo(
    background_tasks: BackgroundTasks,
    steam_id: str    = Form(...),
    file: UploadFile = File(...),
):
    if not file.filename or not file.filename.endswith(".dem"):
        raise HTTPException(status_code=400, detail="Only .dem files are accepted.")

    job_id    = str(uuid.uuid4())
    safe_name = f"{int(time.time())}_{file.filename}"
    dest      = config.DEMOS_USER_DIR / safe_name

    with dest.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)

    _jobs[job_id] = {"status": "processing", "demo_id": safe_name, "steam_id": steam_id}

    def _run() -> None:
        try:
            from backend.processing import process_demo
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


# ── Round replay ───────────────────────────────────────────────────────────────

@app.get("/api/round-replay/{demo_id}/{round_num}")
async def get_round_replay(demo_id: str, round_num: int):
    """Full tick-by-tick round data for the 2D viewer. Reads Parquet directly."""
    row = await db.get_match_parquet_dir(demo_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Demo not found or not yet processed")

    try:
        parquet_dir, map_name = row
        return _read_round_replay_payload(demo_id, round_num, parquet_dir, map_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Parquet file missing: {exc.filename}") from exc


@app.get("/api/similarity-map/{demo_id}/{round_num}/{anchor_tick}")
async def get_similarity_map(demo_id: str, round_num: int, anchor_tick: int):
    """Return the highest-scoring pro mapping for the requested user event anchor."""
    from backend.retrieval import get_best_pro_mapping

    mapping = await get_best_pro_mapping(demo_id, round_num, anchor_tick)
    best = mapping.get("best_match")
    if not best:
        return {"query": mapping["query"], "best_match": None}

    best["map"] = _map_display(best.get("map_name"))
    return mapping
