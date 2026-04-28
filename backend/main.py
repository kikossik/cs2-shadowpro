"""CS2 ShadowPro — FastAPI backend.

Run:
    uvicorn backend.main:app --reload --port 8000
"""
from __future__ import annotations

import asyncio
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
from pipeline.features.featurize_windows import TICK_RATE
from backend.round_analysis_service import (
    MATCHER_VERSION as _ROUND_ANALYSIS_MATCHER_VERSION,
    PRO_CORPUS_VERSION as _ROUND_ANALYSIS_PRO_CORPUS_VERSION,
    _load_round_artifact,
    compute_and_cache_round,
    map_display as _map_display,
    normalize_round_analysis_result as _normalize_round_analysis_result,
)

log = get_logger("API")

app = FastAPI(title="CS2 ShadowPro")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:4173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_jobs: dict[str, dict] = {}
_executor = ThreadPoolExecutor(max_workers=2)
_round_analysis_jobs: dict[str, asyncio.Task[None]] = {}
_round_analysis_jobs_lock = asyncio.Lock()

_ROUND_ANALYSIS_LOGICS = {"nav", "original", "both"}
_ROUND_ANALYSIS_JOB_NAME = "round_analysis"


# ── Lifecycle ──────────────────────────────────────────────────────────────────

@app.on_event("shutdown")
async def _shutdown() -> None:
    tasks = list(_round_analysis_jobs.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
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
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {"status": "syncing", "steam_id": steam_id}

    def _run() -> None:
        from backend.sync import process_share_code, sync_user
        results = []
        try:
            r = process_share_code(steam_id, last_share_code)
            results.append(r)
        except Exception as exc:
            log.error("setup share-code processing failed for %s: %s", steam_id, exc, exc_info=True)
            results.append({"error": str(exc)})
        try:
            sync_result = sync_user(steam_id)
        except Exception as exc:
            log.error("sync failed for %s: %s", steam_id, exc, exc_info=True)
            sync_result = {"new_matches": 0, "errors": [str(exc)]}
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
    steam_id:   str        = Form(...),
    match_type: str        = Form("unknown"),
    file:       UploadFile = File(...),
):
    if not file.filename or not file.filename.endswith(".dem"):
        raise HTTPException(status_code=400, detail="Only .dem files are accepted.")

    await db.upsert_user(steam_id)

    job_id    = str(uuid.uuid4())
    safe_name = f"{int(time.time())}_{file.filename}"
    dest      = config.DEMOS_USER_DIR / safe_name

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

def _read_round_replay_payload(demo_id: str, round_num: int, parquet_dir: str, map_name: str) -> dict:
    base = Path(parquet_dir)
    stem = demo_id

    def _read(field: str) -> pl.DataFrame:
        return pl.read_parquet(base / f"{stem}_{field}.parquet")

    ticks_all   = _read("ticks")
    rounds_all  = _read("rounds")
    shots_all   = _read("shots")
    smokes_all  = _read("smokes")
    infernos_all = _read("infernos")
    flashes_all = _read("flashes")
    grens_all   = _read("grenade_paths")

    rn = round_num
    r_rounds = rounds_all.filter(pl.col('round_num') == rn)

    raw_ticks = ticks_all.filter(pl.col('round_num') == rn)
    freeze_end = (
        int(r_rounds['freeze_end'][0]) if r_rounds.height > 0 and 'freeze_end' in r_rounds.columns
        else (int(raw_ticks['tick'].min()) if raw_ticks.height > 0 else 0)
    )

    round_meta = _build_round_meta(rounds_all, r_rounds, rn, freeze_end)

    ticks    = raw_ticks.filter(pl.col('tick') >= freeze_end).sort('tick')
    shots    = shots_all.filter((pl.col('round_num') == rn) & (pl.col('tick') >= freeze_end))
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
            "player_steamid": str(row.get('player_steamid') or ''),
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
            outcome["bomb_plant_offset_s"] = round(max(0, plant_tick - freeze_end) / TICK_RATE, 1)

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
        parquet_dir, map_name = row
        return _read_round_replay_payload(demo_id, round_num, parquet_dir, map_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Parquet file missing: {exc.filename}") from exc


# ── Round analysis ─────────────────────────────────────────────────────────────

def _decode_json_blob(value: str | None) -> dict | None:
    if value is None:
        return None
    return json.loads(value)


def _round_analysis_meta(
    *,
    demo_id: str,
    round_num: int,
    logic: str,
    cache_key: str,
    cache_status: str,
    resolved_from_cache: bool,
) -> dict:
    return {
        "demo_id": demo_id,
        "round_num": round_num,
        "logic": logic,
        "cache_key": cache_key,
        "cache_status": cache_status,
        "resolved_from_cache": resolved_from_cache,
        "matcher_version": _ROUND_ANALYSIS_MATCHER_VERSION,
        "pro_corpus_version": _ROUND_ANALYSIS_PRO_CORPUS_VERSION,
    }


async def _run_round_analysis_job(
    *,
    cache_key: str,
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> None:
    run_id: int | None = None
    try:
        run_id = await db.start_job_run(_ROUND_ANALYSIS_JOB_NAME)
        await compute_and_cache_round(demo_id, round_num, logic, match_record)
        if run_id is not None:
            await db.finish_job_run(run_id, status="done", items_processed=1,
                                     stats={"cache_key": cache_key, "logic": logic,
                                            "round_num": round_num, "demo_id": demo_id})
    except (asyncio.CancelledError, Exception) as exc:
        err_msg = "Cancelled" if isinstance(exc, asyncio.CancelledError) else str(exc)
        if not isinstance(exc, asyncio.CancelledError):
            await db.upsert_round_analysis_result(
                cache_key=cache_key, demo_id=demo_id, round_num=round_num, logic=logic,
                matcher_version=_ROUND_ANALYSIS_MATCHER_VERSION,
                pro_corpus_version=_ROUND_ANALYSIS_PRO_CORPUS_VERSION,
                status="error", error_message=err_msg,
            )
        if run_id is not None:
            await db.finish_job_run(run_id, status="error", items_processed=0,
                                     error_message=err_msg,
                                     stats={"cache_key": cache_key, "logic": logic,
                                            "round_num": round_num, "demo_id": demo_id})
        if isinstance(exc, asyncio.CancelledError):
            raise
    finally:
        async with _round_analysis_jobs_lock:
            current = _round_analysis_jobs.get(cache_key)
            if current is asyncio.current_task():
                _round_analysis_jobs.pop(cache_key, None)


async def _enqueue_round_analysis_job(
    *,
    cache_key: str,
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> bool:
    async with _round_analysis_jobs_lock:
        existing = _round_analysis_jobs.get(cache_key)
        if existing is not None and not existing.done():
            return False
        task = asyncio.create_task(
            _run_round_analysis_job(
                cache_key=cache_key, demo_id=demo_id, round_num=round_num,
                logic=logic, match_record=match_record,
            )
        )
        _round_analysis_jobs[cache_key] = task
        return True


def _round_analysis_response_from_row(
    *,
    row: dict,
    demo_id: str,
    round_num: int,
    logic: str,
    cache_key: str,
    cache_status: str,
    resolved_from_cache: bool,
) -> dict:
    return {
        "status": row["status"],
        "analysis": _round_analysis_meta(
            demo_id=demo_id, round_num=round_num, logic=logic,
            cache_key=cache_key, cache_status=cache_status,
            resolved_from_cache=resolved_from_cache,
        ),
        "result": _normalize_round_analysis_result(_decode_json_blob(row.get("result_json"))),
        "error":  row.get("error_message"),
    }


async def _build_round_analysis_response(
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> dict:
    cache_state = await db.get_round_analysis_result_state(
        demo_id=demo_id,
        round_num=round_num,
        logic=logic,
        matcher_version=_ROUND_ANALYSIS_MATCHER_VERSION,
        pro_corpus_version=_ROUND_ANALYSIS_PRO_CORPUS_VERSION,
    )
    cache_key    = cache_state["cache_key"]
    cache_status = cache_state["cache_status"]
    exact_row    = cache_state.get("result")
    stale_row    = cache_state.get("stale_result")

    if exact_row is not None:
        if exact_row.get("status") == "pending":
            await _enqueue_round_analysis_job(
                cache_key=cache_key, demo_id=demo_id, round_num=round_num,
                logic=logic, match_record=match_record,
            )
        return _round_analysis_response_from_row(
            row=exact_row, demo_id=demo_id, round_num=round_num, logic=logic,
            cache_key=cache_key, cache_status=cache_status, resolved_from_cache=True,
        )

    await db.upsert_round_analysis_result(
        cache_key=cache_key, demo_id=demo_id, round_num=round_num, logic=logic,
        matcher_version=_ROUND_ANALYSIS_MATCHER_VERSION,
        pro_corpus_version=_ROUND_ANALYSIS_PRO_CORPUS_VERSION,
        status="pending",
    )
    await _enqueue_round_analysis_job(
        cache_key=cache_key, demo_id=demo_id, round_num=round_num,
        logic=logic, match_record=match_record,
    )

    if stale_row is not None and stale_row.get("status") == "done":
        return _round_analysis_response_from_row(
            row=stale_row, demo_id=demo_id, round_num=round_num, logic=logic,
            cache_key=cache_key, cache_status="stale", resolved_from_cache=True,
        )

    return {
        "status":   "pending",
        "analysis": _round_analysis_meta(
            demo_id=demo_id, round_num=round_num, logic=logic,
            cache_key=cache_key, cache_status=cache_status, resolved_from_cache=False,
        ),
        "result": None,
        "error":  None,
    }


@app.get("/api/round-analysis/{demo_id}/{round_num}")
async def get_round_analysis(demo_id: str, round_num: int, logic: str = "nav"):
    """Round analysis: Stage 1 retrieval + Stage 2 deep matching."""
    if logic not in _ROUND_ANALYSIS_LOGICS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported logic '{logic}'. Expected: {', '.join(sorted(_ROUND_ANALYSIS_LOGICS))}",
        )

    record = await db.get_match_source_record(demo_id)
    if record is None or not record.get("parquet_dir"):
        raise HTTPException(status_code=404, detail="Demo not found or not yet processed")

    try:
        if record.get("source_type") == "user":
            await _load_round_artifact(record, round_num)
        return await _build_round_analysis_response(demo_id, round_num, logic, record)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Parquet file missing: {exc.filename}") from exc
