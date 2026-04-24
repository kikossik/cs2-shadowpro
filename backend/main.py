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

_ROUND_ANALYSIS_MATCHER_VERSION = "clean-v2"
_ROUND_ANALYSIS_PRO_CORPUS_VERSION = "event-windows-v1"
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

_MAP_DISPLAY: dict[str, str] = {
    "de_ancient": "Ancient", "de_anubis": "Anubis", "de_dust2": "Dust 2",
    "de_inferno": "Inferno", "de_mirage": "Mirage", "de_nuke": "Nuke",
    "de_overpass": "Overpass",
}


def _map_display(map_name: str | None) -> dict:
    name    = map_name or "unknown"
    display = _MAP_DISPLAY.get(name, name.replace("de_", "").title())
    return {"key": name.replace("de_", ""), "name": name, "display": display}


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
    background_tasks: BackgroundTasks,
    steam_id: str    = Form(...),
    match_type: str  = Form("unknown"),
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
            result = process_demo(dest, steam_id, safe_name, match_type=match_type)
            _jobs[job_id].update({"status": "done", **result})
        except Exception as exc:
            log.error("import failed for %s: %s", safe_name, exc, exc_info=True)
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

def _read_json_file(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _artifact_is_current(path: str, expected_version: str) -> bool:
    try:
        payload = _read_json_file(path)
    except (OSError, json.JSONDecodeError):
        return False
    return payload.get("artifact_version") == expected_version


async def _load_round_artifact(match_record: dict, round_num: int) -> dict:
    """Load the round-level artifact dict from the match artifact file.

    If the artifact is missing or stale, rebuilds it on the fly.
    """
    from pipeline.steps.build_artifact import ARTIFACT_VERSION, build_match_artifact

    artifact_path = match_record.get("artifact_path")

    if (
        not artifact_path
        or not Path(artifact_path).exists()
        or not _artifact_is_current(artifact_path, ARTIFACT_VERSION)
    ):
        parquet_dir = match_record.get("parquet_dir")
        if not parquet_dir:
            raise ValueError(f"No parquet_dir for {match_record.get('source_match_id')}")
        artifact_path = build_match_artifact(
            source_type=match_record["source_type"],
            source_match_id=match_record["source_match_id"],
            parquet_dir=Path(parquet_dir),
            stem=match_record["source_match_id"],
            map_name=match_record["map_name"],
            steam_id=match_record.get("steam_id"),
        )
        await db.set_match_artifact_path(
            match_record["source_type"],
            match_record["source_match_id"],
            artifact_path,
        )

    match_artifact = _read_json_file(artifact_path)
    rounds = match_artifact.get("rounds", {})
    round_artifact = rounds.get(str(round_num)) or rounds.get(round_num)
    if round_artifact is None:
        raise ValueError(
            f"Round {round_num} not found in artifact for {match_record.get('source_match_id')}"
        )
    return round_artifact


def _decode_json_blob(value: str | None) -> dict | None:
    if value is None:
        return None
    return json.loads(value)


def _inject_map(item: dict) -> dict:
    out = dict(item)
    if out.get("map") is None:
        out["map"] = _map_display(out.get("map_name"))
    return out


def _normalize_round_analysis_result(result: dict | None) -> dict | None:
    if result is None:
        return None
    payload = dict(result)
    if payload.get("best_match") is not None:
        payload["best_match"] = _inject_map(payload["best_match"])
    payload["shortlist"] = [_inject_map(c) for c in payload.get("shortlist") or []]
    payload["matches"]   = [_inject_map(m) for m in payload.get("matches") or []]
    if payload.get("selected_match") is not None:
        payload["selected_match"] = _inject_map(payload["selected_match"])
    return payload


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


def _build_round_analysis_payload(
    *,
    logic: str,
    shortlist_result: dict,
    enriched_shortlist: list[dict],
    deep_analysis: dict,
) -> dict:
    matches = deep_analysis.get("matches", [])
    selected_match = deep_analysis.get("selected_match")
    best = None
    if selected_match:
        top_window = dict(selected_match["top_window"])
        best = {
            **top_window,
            "score":                  selected_match["deep_score"],
            "map_name":               selected_match.get("map_name"),
            "event_name":             selected_match.get("event_name"),
            "team1_name":              selected_match.get("team1_name"),
            "team2_name":              selected_match.get("team2_name"),
            "team_ct":                selected_match.get("team_ct"),
            "team_t":                 selected_match.get("team_t"),
            "match_date":             selected_match.get("match_date"),
            "candidate_score":        selected_match["deep_score"],
            "retrieval_score":        selected_match["score"],
            "coverage":               selected_match["coverage"],
            "matched_query_windows":  selected_match["matched_query_windows"],
            "supporting_window_hits": selected_match["supporting_window_hits"],
            "shortlist_rank":         selected_match["shortlist_rank"],
            "logic":                  logic,
            "break_event":            selected_match.get("break_event"),
            "shared_prefix":          selected_match.get("shared_prefix"),
            "divergence":             selected_match.get("divergence"),
            "timeline_sync":          selected_match.get("timeline_sync"),
            "pro_time_offset_s":      selected_match.get("pro_time_offset_s"),
            "divergence_start_sec":   selected_match.get("divergence_start_sec"),
            "divergence_end_sec":     selected_match.get("divergence_end_sec"),
            "summary":                selected_match.get("summary"),
        }

    return _normalize_round_analysis_result({
        "query":         shortlist_result["query"],
        "best_match":    best,
        "shortlist":     enriched_shortlist,
        "retrieval": {
            "query_window_count":    len(shortlist_result.get("query_windows", [])),
            "window_hit_count":      len(shortlist_result.get("window_hits", [])),
            "candidate_round_count": len(enriched_shortlist),
            "stage":                 "event_windows_shortlist",
        },
        "logic":          logic,
        "matches":        matches,
        "selected_match": selected_match,
    })


async def _compute_round_analysis_payload(
    demo_id: str,
    round_num: int,
    logic: str,
    match_record: dict,
) -> dict:
    from backend.retrieval import build_pro_round_shortlist
    from backend.round_analysis import analyze_shortlisted_rounds

    shortlist_result = await build_pro_round_shortlist(demo_id, round_num)
    shortlist = shortlist_result.get("shortlist", [])

    user_artifact_coro = _load_round_artifact(match_record, round_num)
    candidate_records, user_artifact = await asyncio.gather(
        asyncio.gather(*[db.get_match_source_record(c["source_match_id"]) for c in shortlist]),
        user_artifact_coro,
    )

    valid = [
        (idx + 1, candidate, record)
        for idx, (candidate, record) in enumerate(zip(shortlist, candidate_records))
        if record is not None and record.get("parquet_dir")
    ]

    pro_artifacts = await asyncio.gather(
        *[_load_round_artifact(record, int(candidate["round_num"]))
          for _, candidate, record in valid]
    )

    enriched_shortlist: list[dict] = []
    for (idx, candidate, candidate_record), pro_artifact in zip(valid, pro_artifacts):
        enriched_shortlist.append({
            "source_match_id":        candidate["source_match_id"],
            "round_num":              candidate["round_num"],
            "map_name":               candidate_record.get("map_name"),
            "event_name":             candidate_record.get("event_name"),
            "team1_name":              candidate_record.get("team1_name"),
            "team2_name":              candidate_record.get("team2_name"),
            "team_ct":                candidate_record.get("team_ct"),
            "team_t":                 candidate_record.get("team_t"),
            "match_date":             (
                candidate_record["match_date"].isoformat()
                if candidate_record.get("match_date") else None
            ),
            "score":                  candidate["score"],
            "best_window_score":      candidate["best_window_score"],
            "coverage":               candidate["coverage"],
            "supporting_window_hits": candidate["supporting_window_hits"],
            "matched_query_windows":  candidate["matched_query_windows"],
            "query_anchor_kinds":     candidate.get("query_anchor_kinds", []),
            "shortlist_rank":         idx,
            "top_window":             dict(candidate["top_window"]),
            "window_hits":            candidate.get("window_hits", []),
            "artifact":               pro_artifact,
        })

    deep_analysis = analyze_shortlisted_rounds(
        query=shortlist_result.get("query"),
        user_artifact=user_artifact,
        candidates=enriched_shortlist,
        logic=logic,
    )
    shortlist_without_artifacts = [
        {k: v for k, v in c.items() if k != "artifact"}
        for c in enriched_shortlist
    ]
    return _build_round_analysis_payload(
        logic=logic,
        shortlist_result=shortlist_result,
        enriched_shortlist=shortlist_without_artifacts,
        deep_analysis=deep_analysis,
    )


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
        result_payload = await _compute_round_analysis_payload(
            demo_id=demo_id,
            round_num=round_num,
            logic=logic,
            match_record=match_record,
        )
        await db.upsert_round_analysis_result(
            cache_key=cache_key,
            demo_id=demo_id,
            round_num=round_num,
            logic=logic,
            matcher_version=_ROUND_ANALYSIS_MATCHER_VERSION,
            pro_corpus_version=_ROUND_ANALYSIS_PRO_CORPUS_VERSION,
            status="done",
            result_payload=result_payload,
        )
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
