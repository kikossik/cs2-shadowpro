"""Parse a pro demo and write Parquet files + flat DB records.

Ingest flow:
  1. _parse_ingest_sync runs in a ProcessPoolExecutor (off the event loop):
       parse demo → write 7 parquets → build artifact → extract event windows
  2. ingest_pro_demo receives the plain-dict result and does async DB upserts.

Keeping CPU-heavy work in a subprocess means:
  - The asyncio event loop is never blocked during parsing.
  - awpy + polars memory is fully released when the subprocess task finishes.
"""
from __future__ import annotations

import asyncio
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import date
from pathlib import Path

import awpy
import polars as pl
from awpy import Demo

from backend import config, db
from backend.log import get_logger
from backend.processing import PLAYER_PROPS, _map_name, _write_parquets

log = get_logger("INGEST")
from pipeline.features.extract_windows import extract_match_event_windows
from pipeline.features.featurize_windows import FEATURE_VERSION, TICK_RATE
from pipeline.steps.build_artifact import ARTIFACT_VERSION, build_match_artifact

VALID_MAPS = {
    "de_ancient", "de_anubis", "de_dust2",
    "de_inferno", "de_mirage", "de_nuke", "de_overpass",
}

_AWPY_VERSION = getattr(awpy, "__version__", "unknown")


def _parse_ingest_sync(demo_path: str, parquet_dir: str, match_id: str) -> dict:
    """All CPU-heavy work for one pro demo. Runs in a ProcessPoolExecutor.

    Returns a plain dict of picklable values — the async caller uses these
    for DB upserts without touching awpy or polars itself.
    """
    parquet_dir_path = Path(parquet_dir)

    dem = Demo(path=demo_path)
    dem.parse(player_props=PLAYER_PROPS)

    map_name = _map_name(dem)
    if map_name not in VALID_MAPS:
        raise ValueError(f"map {map_name!r} not in competitive pool — skipping")

    _write_parquets(dem, parquet_dir_path, match_id)
    del dem  # free before reading parquets

    rounds_df = pl.read_parquet(parquet_dir_path / f"{match_id}_rounds.parquet")
    ct_round_wins = t_round_wins = None
    if "winner" in rounds_df.columns:
        ct_round_wins = int((rounds_df["winner"] == "ct").sum())
        t_round_wins  = int((rounds_df["winner"] == "t").sum())
    round_count = rounds_df.height
    del rounds_df

    artifact_path = build_match_artifact(
        source_type="pro",
        source_match_id=match_id,
        parquet_dir=parquet_dir_path,
        stem=match_id,
        map_name=map_name,
    )

    windows = extract_match_event_windows(
        source_type="pro",
        source_match_id=match_id,
        parquet_dir=parquet_dir_path,
        stem=match_id,
        map_name=map_name,
    )

    return {
        "map_name":       map_name,
        "round_count":    round_count,
        "ct_round_wins":  ct_round_wins,
        "t_round_wins":   t_round_wins,
        "artifact_path":  str(artifact_path),
        "windows":        windows,
    }


async def ingest_pro_demo(
    demo_path: Path,
    match_id: str,
    *,
    executor: ProcessPoolExecutor | None = None,
    **meta,
) -> dict:
    """Parse demo, write Parquets, build artifact, upsert flat game records.

    Pass a shared ProcessPoolExecutor to avoid spawning a new process per call.
    If omitted, a temporary single-use executor is created automatically.
    """
    parquet_dir = config.PARQUET_PRO_DIR / match_id
    log.info("parsing %s", demo_path.name)

    own_executor = executor is None
    if own_executor:
        executor = ProcessPoolExecutor(max_workers=1)

    loop = asyncio.get_event_loop()
    try:
        parsed = await loop.run_in_executor(
            executor,
            _parse_ingest_sync,
            str(demo_path),
            str(parquet_dir),
            match_id,
        )
    except Exception:
        shutil.rmtree(parquet_dir, ignore_errors=True)
        raise
    finally:
        if own_executor:
            executor.shutdown(wait=False)

    map_name      = parsed["map_name"]
    round_count   = parsed["round_count"]
    artifact_path = parsed["artifact_path"]
    windows       = parsed["windows"]

    log.info("%s map=%s rounds=%d windows=%d", match_id, map_name, round_count, len(windows))

    match_date = meta.get("match_date")
    if isinstance(match_date, str):
        match_date = date.fromisoformat(match_date)

    hltv_match_id = meta.get("hltv_match_id") or match_id.split("_", 1)[0]
    team1_name = meta.get("team1_name") or meta.get("team1") or meta.get("team_ct")
    team2_name = meta.get("team2_name") or meta.get("team2") or meta.get("team_t")

    try:
        await db.upsert_pro_game(
            game_id=match_id,
            map_name=map_name,
            hltv_match_id=hltv_match_id,
            hltv_url=meta.get("hltv_url"),
            source_slug=meta.get("source_slug"),
            event_name=meta.get("event_name"),
            team1_name=team1_name,
            team2_name=team2_name,
            match_date=match_date,
            parquet_dir=str(parquet_dir),
            artifact_path=artifact_path,
            ct_round_wins=parsed["ct_round_wins"],
            t_round_wins=parsed["t_round_wins"],
            round_count=round_count,
            demo_path=str(demo_path),
            map_number=meta.get("map_number"),
            tick_rate=TICK_RATE,
            parser_version=_AWPY_VERSION,
            artifact_version=ARTIFACT_VERSION,
            window_feature_version=FEATURE_VERSION,
        )
        await db.upsert_event_windows_batch(windows)
    except Exception:
        shutil.rmtree(parquet_dir, ignore_errors=True)
        raise

    log.info("done %s: artifact=%s", match_id, artifact_path)
    return {
        "match_id":      match_id,
        "map":           map_name,
        "parquet_dir":   str(parquet_dir),
        "artifact_path": artifact_path,
        "windows":       len(windows),
    }
