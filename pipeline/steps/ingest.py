"""Parse a pro demo and write Parquet files + dimensional DB records.

Ingest flow:
  1. Parse demo with awpy
  2. Write 7 Parquet files to PARQUET_PRO_DIR/{match_id}/
  3. Build match artifact JSON (all rounds, including nav sequences)
  4. Extract event windows and store embeddings
  5. Upsert matches/games/rounds metadata and retrieval windows
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

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


def _safe_int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _round_fact_rows(rounds_df: pl.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for row in rounds_df.iter_rows(named=True):
        start_tick = _safe_int(row.get("start"))
        freeze_end_tick = _safe_int(row.get("freeze_end"))
        end_tick = _safe_int(row.get("end"))
        official_end_tick = _safe_int(row.get("official_end")) or end_tick
        origin_tick = freeze_end_tick if freeze_end_tick is not None else start_tick
        duration_ticks = (
            official_end_tick - origin_tick
            if official_end_tick is not None and origin_tick is not None
            else None
        )
        rows.append({
            "round_num": _safe_int(row.get("round_num")),
            "start_tick": start_tick,
            "freeze_end_tick": freeze_end_tick,
            "end_tick": end_tick,
            "official_end_tick": official_end_tick,
            "winner_side": row.get("winner"),
            "reason": row.get("reason"),
            "bomb_plant_tick": _safe_int(row.get("bomb_plant")),
            "bomb_site": row.get("bomb_site"),
            "duration_ticks": duration_ticks,
        })
    return rows


async def ingest_pro_demo(demo_path: Path, match_id: str, **meta) -> dict:
    """Parse demo, write Parquets, build artifact, upsert dimensional records."""
    log.info("parsing %s", demo_path.name)
    dem = Demo(path=str(demo_path))
    dem.parse(player_props=PLAYER_PROPS)

    map_name = _map_name(dem)
    if map_name not in VALID_MAPS:
        raise ValueError(f"map {map_name!r} not in competitive pool — skipping")
    log.info("%s map=%s", match_id, map_name)

    parquet_dir = config.PARQUET_PRO_DIR / match_id
    _write_parquets(dem, parquet_dir, match_id)

    try:
        rounds_path = parquet_dir / f"{match_id}_rounds.parquet"
        rounds_df = pl.read_parquet(rounds_path)
        round_count = rounds_df.height
        round_rows = _round_fact_rows(rounds_df)
        if "winner" in rounds_df.columns:
            ct_round_wins = int((rounds_df["winner"] == "ct").sum())
            t_round_wins  = int((rounds_df["winner"] == "t").sum())
        else:
            ct_round_wins = t_round_wins = None

        match_date = meta.get("match_date")
        if isinstance(match_date, str):
            match_date = date.fromisoformat(match_date)

        hltv_match_id = meta.get("hltv_match_id")
        if not hltv_match_id:
            hltv_match_id = match_id.split("_", 1)[0]
        team1_name = meta.get("team1_name") or meta.get("team1") or meta.get("team_ct")
        team2_name = meta.get("team2_name") or meta.get("team2") or meta.get("team_t")

        artifact_path = build_match_artifact(
            source_type="pro",
            source_match_id=match_id,
            parquet_dir=parquet_dir,
            stem=match_id,
            map_name=map_name,
        )

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
            ct_round_wins=ct_round_wins,
            t_round_wins=t_round_wins,
            round_count=round_count,
            demo_path=str(demo_path),
            map_number=meta.get("map_number"),
            tick_rate=TICK_RATE,
            artifact_version=ARTIFACT_VERSION,
            window_feature_version=FEATURE_VERSION,
        )
        await db.upsert_rounds(match_id, round_rows)

        # Keep the legacy table populated, but with explicit non-side labels.
        await db.upsert_pro_match(
            match_id,
            hltv_match_id = str(hltv_match_id) if hltv_match_id else None,
            map_name      = map_name,
            match_type    = "hltv",
            parquet_dir   = str(parquet_dir),
            artifact_path = artifact_path,
            team1_name    = team1_name,
            team2_name    = team2_name,
            team_ct       = team1_name,
            team_t        = team2_name,
            ct_round_wins = ct_round_wins,
            t_round_wins  = t_round_wins,
            score_ct      = ct_round_wins,
            score_t       = t_round_wins,
            round_count   = round_count,
            match_date    = match_date,
            hltv_url      = meta.get("hltv_url"),
            event_name    = meta.get("event_name"),
        )

        windows = extract_match_event_windows(
            source_type="pro",
            source_match_id=match_id,
            parquet_dir=parquet_dir,
            stem=match_id,
            map_name=map_name,
        )
        for window in windows:
            window_id = window.pop("window_id")
            await db.upsert_event_window(window_id, **window)

    except Exception:
        shutil.rmtree(parquet_dir, ignore_errors=True)
        raise

    log.info("done %s: %d rounds, %d windows, artifact=%s", match_id, round_count, len(windows), artifact_path)
    return {
        "match_id":     match_id,
        "map":          map_name,
        "parquet_dir":  str(parquet_dir),
        "artifact_path": artifact_path,
        "windows":      len(windows),
    }
