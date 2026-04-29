"""CS2 ShadowPro — FastAPI backend.

Run:
    uvicorn backend.main:app --reload --port 8000
"""
from __future__ import annotations

import json
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
from backend.log import get_logger
from backend.round_analysis_service import (
    compute_and_cache_round,
    normalize_round_analysis_result,
    _map_display,
)

log = get_logger("API")

app = FastAPI(title="CS2 ShadowPro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_executor = ThreadPoolExecutor(max_workers=2)


# ── Lifecycle ──────────────────────────────────────────────────────────────────

@app.on_event("shutdown")
async def _shutdown() -> None:
    await db.close_pool()
    _executor.shutdown(wait=False, cancel_futures=True)


# ── Maps ───────────────────────────────────────────────────────────────────────

_MAPS_DIR = Path(__file__).parent.parent / "web" / "public" / "maps"


@app.get("/api/radar/{map_name}")
def get_radar_image(map_name: str):
    """Serve a radar PNG from web/public/maps."""
    path = _MAPS_DIR / f"{map_name}.png"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Radar not found: {map_name}")
    return FileResponse(str(path), media_type="image/png")


@app.get("/api/maps")
async def get_maps():
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
            "match_type":      r.get("match_type") or "unknown",
            "date":            int(r["match_date"].timestamp()) if r["match_date"] else None,
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

    def _run() -> None:
        from backend.sync import process_share_code, sync_user
        try:
            process_share_code(steam_id, last_share_code)
        except Exception as exc:
            log.error("setup share-code processing failed for %s: %s", steam_id, exc, exc_info=True)
        try:
            sync_user(steam_id)
        except Exception as exc:
            log.error("sync failed for %s: %s", steam_id, exc, exc_info=True)

    _executor.submit(_run)
    return {"message": "Setup saved, sync started"}


@app.post("/api/sync/{steam_id}")
async def trigger_sync(steam_id: str, background_tasks: BackgroundTasks):
    def _run() -> None:
        from backend.sync import sync_user
        sync_user(steam_id)

    background_tasks.add_task(_executor.submit, _run)
    return {"message": "Sync started"}


# ── Demo import ────────────────────────────────────────────────────────────────

@app.post("/api/import")
async def import_demo(
    steam_id:   str        = Form(...),
    match_type: str        = Form("unknown"),
    file:       UploadFile = File(...),
):
    if not file.filename or not file.filename.endswith(".dem"):
        raise HTTPException(status_code=400, detail="Only .dem files are accepted.")

    await db.upsert_user(steam_id)

    job_id    = str(uuid.uuid4())
    safe_name = f"{int(time.time())}_{file.filename}"
    dest      = config.DEMOS_USER_DIR / steam_id / safe_name
    dest.parent.mkdir(parents=True, exist_ok=True)

    with dest.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)

    await db.create_demo_job(
        job_id=job_id,
        steam_id=steam_id,
        demo_path=str(dest),
        demo_id=safe_name,
        match_type=match_type or "unknown",
    )
    return {"job_id": job_id, "demo_id": safe_name}


@app.get("/api/import/{job_id}")
async def import_status(job_id: str):
    job = await db.get_demo_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Unknown job ID")
    payload: dict = {
        "status":   job["status"],
        "demo_id":  job["demo_id"],
        "steam_id": job["steam_id"],
        "error":    job.get("error"),
    }
    if job.get("result_json"):
        payload.update(job["result_json"])
    return JSONResponse(payload)


# ── Round replay ───────────────────────────────────────────────────────────────

def _empty_utility_frame(kind: str) -> pl.DataFrame:
    schemas = {
        "smokes": {"round_num": pl.Int64, "start_tick": pl.Int64, "end_tick": pl.Int64, "X": pl.Float64, "Y": pl.Float64, "thrower_name": pl.Utf8},
        "infernos": {"round_num": pl.Int64, "start_tick": pl.Int64, "end_tick": pl.Int64, "X": pl.Float64, "Y": pl.Float64},
        "flashes": {"round_num": pl.Int64, "tick": pl.Int64, "X": pl.Float64, "Y": pl.Float64},
        "grenades": {"round_num": pl.Int64, "tick": pl.Int64, "entity_id": pl.Int64, "grenade_type": pl.Utf8, "X": pl.Float64, "Y": pl.Float64},
    }
    return pl.DataFrame(schema=schemas[kind])


def _read_round_replay_payload(
    demo_id: str,
    round_num: int,
    parquet_dir: str,
    map_name: str,
    tick_rate: int | None,
) -> dict:
    base = Path(parquet_dir)
    stem = demo_id
    effective_tick_rate = int(tick_rate or 128)

    def _read(field: str, *, optional: bool = False) -> pl.DataFrame:
        for candidate in (
            base / f"{stem}_{field}.parquet",
            base / f"{Path(stem).stem}_{field}.parquet",
            base / f"{field}.parquet",
        ):
            if candidate.exists():
                return pl.read_parquet(candidate)
        if optional:
            return pl.DataFrame()
        raise FileNotFoundError(str(base / f"{stem}_{field}.parquet"))

    ticks_all   = _read("ticks")
    rounds_all  = _read("rounds")
    shots_all   = _read("shots", optional=True)
    grenades_all = _read("grenades", optional=True)
    smokes_all   = _empty_utility_frame("smokes")
    infernos_all = _empty_utility_frame("infernos")
    flashes_all  = _empty_utility_frame("flashes")
    grens_all = (
        grenades_all
        if {"round_num", "tick", "entity_id", "grenade_type", "X", "Y"}.issubset(grenades_all.columns)
        else _empty_utility_frame("grenades")
    )

    rn = round_num
    r_rounds = rounds_all.filter(pl.col('round_num') == rn)

    raw_ticks = ticks_all.filter(pl.col('round_num') == rn)
    freeze_end = (
        int(r_rounds['freeze_end'][0]) if r_rounds.height > 0 and 'freeze_end' in r_rounds.columns
        else (int(raw_ticks['tick'].min()) if raw_ticks.height > 0 else 0)
    )

    round_meta = _build_round_meta(rounds_all, r_rounds, rn, freeze_end, effective_tick_rate)

    ticks    = raw_ticks.filter(pl.col('tick') >= freeze_end).sort('tick')
    shots = (
        shots_all.filter((pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end))
        if {"round_num", "tick"}.issubset(shots_all.columns)
        else pl.DataFrame()
    )
    smokes   = smokes_all.filter(
        (pl.col('round_num') == rn)
        & ((pl.col('end_tick').is_null()) | (pl.col('end_tick') >= freeze_end))
    )
    infernos = infernos_all.filter(
        (pl.col('round_num') == rn)
        & ((pl.col('end_tick').is_null()) | (pl.col('end_tick') >= freeze_end))
    )
    flashes  = flashes_all.filter((pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end))
    grens    = grens_all.filter(
        (pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end)
    ).sort('tick')

    tick_list = sorted(ticks['tick'].unique().to_list())
    round_last_tick = tick_list[-1] if tick_list else 0

    cols = set(ticks.columns)
    ticks_by_tick: dict[int, list[dict]] = {t: [] for t in tick_list}
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

    shots_payload = [
        {
            "tick":           int(row['tick']),
            "player_steamid": str(row.get('player_steamid') or row.get('steamid') or ''),
            "weapon":         str(row.get('weapon') or ''),
        }
        for row in shots.iter_rows(named=True)
    ]

    smokes_payload = [
        {
            "start_tick":   max(int(row['start_tick']), freeze_end),
            "end_tick":     int(row['end_tick'] if row['end_tick'] is not None else round_last_tick),
            "x":            float(row.get('X') or 0.0),
            "y":            float(row.get('Y') or 0.0),
            "thrower_name": row.get('thrower_name') or '',
        }
        for row in smokes.iter_rows(named=True)
    ]

    infernos_payload = [
        {
            "start_tick": max(int(row['start_tick']), freeze_end),
            "end_tick":   int(row['end_tick'] if row['end_tick'] is not None else round_last_tick),
            "x":          float(row.get('X') or 0.0),
            "y":          float(row.get('Y') or 0.0),
        }
        for row in infernos.iter_rows(named=True)
    ]

    flashes_payload = [
        {"tick": int(row['tick']), "x": float(row.get('X') or 0.0), "y": float(row.get('Y') or 0.0)}
        for row in flashes.iter_rows(named=True)
    ]

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
        gren_by_entity[eid]["path"].append(
            {"tick": int(row['tick']), "x": float(row['X']), "y": float(row['Y'])}
        )

    return {
        "map":             map_name,
        "round_num":       round_num,
        "freeze_end_tick": freeze_end,
        "tick_list":       tick_list,
        "ticks":           ticks_payload,
        "shots":           shots_payload,
        "smokes":          smokes_payload,
        "infernos":        infernos_payload,
        "flashes":         flashes_payload,
        "grenade_paths":   list(gren_by_entity.values()),
        "round_meta":      round_meta,
    }


def _build_round_meta(
    rounds_all: pl.DataFrame,
    r_rounds: pl.DataFrame,
    round_num: int,
    freeze_end: int,
    tick_rate: int,
) -> dict:
    """Per-round metadata: scoreline going-in, outcome, bomb plant."""
    cols = set(rounds_all.columns)

    score_ct = score_t = 0
    if "winner" in cols:
        prior = rounds_all.filter(pl.col("round_num") < round_num)
        score_ct = int((prior["winner"] == "ct").sum())
        score_t  = int((prior["winner"] == "t").sum())

    outcome: dict = {"winner_side": None, "reason": None, "bomb_site": None,
                     "bomb_plant_tick": None, "bomb_plant_offset_s": None,
                     "official_end_tick": None}
    if r_rounds.height > 0:
        row = r_rounds.row(0, named=True)
        outcome["winner_side"] = row.get("winner")
        outcome["reason"]      = row.get("reason")
        outcome["bomb_site"]   = row.get("bomb_site")

        plant = row.get("bomb_plant")
        if plant is not None:
            plant_tick = int(plant)
            outcome["bomb_plant_tick"] = plant_tick
            outcome["bomb_plant_offset_s"] = round(max(0, plant_tick - freeze_end) / tick_rate, 1)

        for key in ("official_end", "end"):
            if key in cols and row.get(key) is not None:
                outcome["official_end_tick"] = int(row[key])
                break

    return {
        "score_before": {"ct": score_ct, "t": score_t},
        "outcome":      outcome,
    }


@app.get("/api/round-replay/{demo_id}/{round_num}")
async def get_round_replay(demo_id: str, round_num: int):
    """Full tick-by-tick round data for the 2D viewer."""
    row = await db.get_match_parquet_dir(demo_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Demo not found or not yet processed")
    try:
        parquet_dir, map_name, tick_rate = row
        return _read_round_replay_payload(demo_id, round_num, parquet_dir, map_name, tick_rate)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Parquet file missing: {exc}") from exc


# ── Round analysis ─────────────────────────────────────────────────────────────

def _decode_result_json(value) -> dict | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    return json.loads(value)


@app.get("/api/round-analysis/{demo_id}/{round_num}")
async def get_round_analysis(demo_id: str, round_num: int):
    """Return the user→pro mapping for one round.

    Cached per (game_id, round_num). On cache miss we compute synchronously
    with the in-memory original + nav matchers. The worker also precomputes
    after imports, so user-facing requests usually hit a fresh cache entry."""
    record = await db.get_match_source_record(demo_id)
    if record is None or not record.get("parquet_dir"):
        raise HTTPException(status_code=404, detail="Demo not found or not yet processed")

    state = await db.get_round_analysis_result_state(demo_id=demo_id, round_num=round_num)
    if state["cache_status"] == "fresh" and state["result"] is not None:
        row = state["result"]
        return {
            "status": row["status"],
            "result": normalize_round_analysis_result(_decode_result_json(row.get("result_json"))),
            "error":  row.get("error_message"),
        }

    try:
        payload = await compute_and_cache_round(demo_id, round_num)
        return {
            "status": "done",
            "result": normalize_round_analysis_result(payload),
            "error":  None,
        }
    except Exception as exc:
        log.error("round analysis compute failed for %s/%s: %s", demo_id, round_num, exc, exc_info=True)
        await db.upsert_round_analysis_result(
            demo_id=demo_id, round_num=round_num, status="error", error_message=str(exc),
        )
        return {"status": "error", "result": None, "error": str(exc)}
