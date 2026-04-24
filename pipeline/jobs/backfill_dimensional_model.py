"""Backfill dimensional match/game/round tables from legacy rows and parquet.

Run after db/migrations/20260424_dimensional_match_game.sql on an existing DB.
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import polars as pl

from backend import config, db
from backend.log import get_logger

log = get_logger("BACKFILL")


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


async def _backfill_rounds(limit: int | None = None) -> dict:
    pool = await db.get_pool()
    query = """
        SELECT game_id, demo_stem, parquet_dir
        FROM games
        WHERE parquet_dir IS NOT NULL
        ORDER BY source_type, game_id
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT $1"
        params = (limit,)

    rows = await pool.fetch(query, *params)
    stats = {"games_seen": len(rows), "games_backfilled": 0, "rounds": 0, "missing": 0}

    for row in rows:
        parquet_dir = Path(config.resolve_managed_path(row["parquet_dir"]))
        stem = row["demo_stem"] or row["game_id"]
        rounds_path = parquet_dir / f"{stem}_rounds.parquet"
        if not rounds_path.exists():
            stats["missing"] += 1
            continue

        rounds_df = pl.read_parquet(rounds_path)
        round_rows = _round_fact_rows(rounds_df)
        await db.upsert_rounds(row["game_id"], round_rows)
        await pool.execute(
            """
            UPDATE games
            SET round_count = COALESCE($2, round_count),
                updated_at = NOW()
            WHERE game_id = $1
            """,
            row["game_id"],
            rounds_df.height,
        )
        stats["games_backfilled"] += 1
        stats["rounds"] += len(round_rows)

    return stats


async def run(limit: int | None = None) -> dict:
    try:
        return await _backfill_rounds(limit=limit)
    finally:
        await db.close_pool()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    stats = asyncio.run(run(limit=args.limit))
    log.info("done: %s", stats)


if __name__ == "__main__":
    main()
