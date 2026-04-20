"""Parse a pro demo and write Parquet files + insert pro_matches DB record.

Ingestion flow:
  1. Parse demo with awpy
  2. Write 7 Parquet files to PARQUET_PRO_DIR/{match_id}/
  3. Derive per-side scores from rounds parquet
  4. Upsert pro_matches row
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl
from awpy import Demo

from backend import config, db
from backend.processing import PLAYER_PROPS, _map_name, _write_parquets
from pipeline.features.extract_windows import extract_match_event_windows

VALID_MAPS = {
    "de_ancient", "de_anubis", "de_dust2",
    "de_inferno", "de_mirage", "de_nuke", "de_overpass",
}


async def ingest_pro_demo(demo_path: Path, match_id: str, **meta) -> dict:
    """Parse demo, write Parquets, upsert pro_matches row. Returns summary dict."""
    dem = Demo(path=str(demo_path))
    dem.parse(player_props=PLAYER_PROPS)

    map_name = _map_name(dem)
    if map_name not in VALID_MAPS:
        raise ValueError(f"map {map_name!r} not in competitive pool — skipping")

    parquet_dir = config.PARQUET_PRO_DIR / match_id
    _write_parquets(dem, parquet_dir, match_id)

    try:
        rounds_path = parquet_dir / f"{match_id}_rounds.parquet"
        rounds_df = pl.read_parquet(rounds_path)
        if "winner" in rounds_df.columns:
            score_ct = int((rounds_df["winner"] == "ct").sum())
            score_t  = int((rounds_df["winner"] == "t").sum())
        else:
            score_ct = score_t = None

        match_date = meta.get("match_date")
        if isinstance(match_date, str):
            match_date = date.fromisoformat(match_date)

        await db.upsert_pro_match(
            match_id,
            map_name    = map_name,
            parquet_dir = str(parquet_dir),
            score_ct    = score_ct,
            score_t     = score_t,
            match_date  = match_date,
            **{k: meta.get(k) for k in ("hltv_url", "event_name", "team_ct", "team_t")},
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

    return {
        "match_id":    match_id,
        "map":         map_name,
        "parquet_dir": str(parquet_dir),
        "windows":     len(windows),
    }
