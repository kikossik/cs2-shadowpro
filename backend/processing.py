"""
Demo processing pipeline for user-uploaded demos.
Runs in a thread pool (awpy/polars are CPU-bound).
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
import shutil

import awpy.parsers.rounds
import awpy.parsers.utils
import polars as pl
from awpy import Demo

from backend import config, db
from backend.round_mapper import DEFAULT_EVENTS, FOCUSED_PLAYER_PROPS, FOCUSED_WORLD_PROPS


def _map_name(dem: Demo) -> str:
    try:
        return dem.header.get("map_name", "unknown") or "unknown"
    except Exception:
        return "unknown"


def _match_stats(dem: Demo, steam_id: str) -> dict:
    uid = int(steam_id)
    stats: dict = {}

    rounds = dem.rounds
    if rounds is not None and rounds.height > 0:
        sorted_rounds = rounds.sort("round_num")
        stats["round_count"] = sorted_rounds.height

        ticks = dem.ticks
        user_round_sides: pl.DataFrame | None = None
        if ticks is not None:
            rename_map = {}
            if "player_steamid" in ticks.columns and "steamid" not in ticks.columns:
                rename_map["player_steamid"] = "steamid"
            if rename_map:
                ticks = ticks.rename(rename_map)
            user_round_sides = (
                ticks
                .filter(pl.col("steamid") == uid)
                .select(["round_num", "tick", "side"])
                .sort(["round_num", "tick"])
                .group_by("round_num", maintain_order=True)
                .first()
                .sort("round_num")
            )
            if user_round_sides.height > 0:
                side_val = user_round_sides["side"][0]
                if side_val:
                    stats["user_side_first"] = str(side_val).lower()

        if user_round_sides is not None and user_round_sides.height > 0:
            round_results = (
                user_round_sides
                .select(["round_num", "side"])
                .join(
                    sorted_rounds.select(["round_num", "winner"]),
                    on="round_num",
                    how="inner",
                )
                .with_columns(
                    (pl.col("side") == pl.col("winner")).alias("user_won_round")
                )
            )
            wins = int(round_results["user_won_round"].sum())
            losses = round_results.height - wins
            # The matches UI renders these as "my team : opponent", not raw CT/T totals.
            stats["score_ct"] = wins
            stats["score_t"] = losses
            if wins > losses:
                stats["user_result"] = "win"
            elif wins == losses:
                stats["user_result"] = "draw"
            else:
                stats["user_result"] = "loss"
        else:
            # Fallback when the user's per-round side can't be determined.
            stats["score_ct"] = int((sorted_rounds["winner"] == "ct").sum())
            stats["score_t"] = int((sorted_rounds["winner"] == "t").sum())

    kills = dem.kills
    if kills is not None and kills.height > 0:
        k_rows = kills.filter(pl.col("attacker_steamid") == uid)
        d_rows = kills.filter(pl.col("victim_steamid") == uid)
        stats["kills"]  = k_rows.height
        stats["deaths"] = d_rows.height
        for col in ("assister_steamid",):
            if col in kills.columns:
                stats["assists"] = kills.filter(pl.col(col) == uid).height
                break
        else:
            stats["assists"] = 0
        if k_rows.height > 0 and "headshot" in k_rows.columns:
            hs = k_rows.filter(pl.col("headshot")).height
            stats["hs_pct"] = int(100 * hs / k_rows.height)
        else:
            stats["hs_pct"] = 0

    return stats


_MATCH_TYPES = {"unknown", "premier", "competitive", "faceit"}


def _tick_rate_from_header(header: dict) -> int | None:
    for key in ("tick_rate", "tickrate", "network_protocol_tickrate"):
        value = header.get(key)
        if value:
            try:
                parsed = int(float(value))
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
    return None


def _frame_or_empty(value) -> pl.DataFrame:
    return value if isinstance(value, pl.DataFrame) else pl.DataFrame()


def _parse_groundup_ticks(dem: Demo) -> pl.DataFrame:
    ticks = dem.parse_ticks(
        player_props=FOCUSED_PLAYER_PROPS,
        other_props=FOCUSED_WORLD_PROPS,
    )
    ticks = awpy.parsers.utils.fix_common_names(ticks)
    ticks = ticks.join(
        pl.DataFrame({"tick": dem.in_play_ticks}),
        on="tick",
        how="semi",
    )
    return awpy.parsers.rounds.apply_round_num(
        df=ticks,
        rounds_df=dem.rounds,
        tick_col="tick",
    ).filter(pl.col("round_num").is_not_null())


def _write_parquets(dem: Demo, parquet_dir: Path, demo_id: str) -> None:
    parquet_dir.mkdir(parents=True, exist_ok=True)

    def _w(df: pl.DataFrame, field: str) -> None:
        df.write_parquet(parquet_dir / f"{demo_id}_{field}.parquet")

    _w(_parse_groundup_ticks(dem), "ticks")
    _w(_frame_or_empty(dem.rounds), "rounds")
    _w(_frame_or_empty(dem.kills), "kills")
    _w(_frame_or_empty(dem.damages), "damages")
    _w(_frame_or_empty(dem.shots), "shots")
    _w(_frame_or_empty(dem.bomb), "bomb")
    _w(_frame_or_empty(dem.grenades), "grenades")
    with (parquet_dir / f"{demo_id}_header.json").open("w", encoding="utf-8") as fh:
        json.dump(dem.header, fh, default=str)


async def _save_match(demo_id: str, game_kwargs: dict) -> None:
    # process_demo runs in a ThreadPoolExecutor via asyncio.run(), which creates a new
    # event loop. The global db._pool was created in FastAPI's main event loop and
    # cannot be used from a different loop, so we create a fresh pool here.
    import asyncpg as _asyncpg
    pool = await _asyncpg.create_pool(dsn=config.DATABASE_URL, min_size=1, max_size=2)
    orig_pool = db._pool
    db._pool = pool
    try:
        await db.upsert_user_game(game_id=demo_id, **game_kwargs)
    finally:
        db._pool = orig_pool
        await pool.close()


def process_demo(
    demo_path: Path,
    steam_id: str,
    demo_id: str,
    share_code: str | None = None,
    match_type: str = "unknown",
) -> dict:
    """Parse a demo, write groundup-compatible Parquet files, upsert the user game row."""
    match_type = (match_type or "unknown").strip().lower()
    if match_type not in _MATCH_TYPES:
        match_type = "unknown"

    dem = Demo(path=str(demo_path))
    dem.parse(
        events=DEFAULT_EVENTS,
        player_props=FOCUSED_PLAYER_PROPS,
        other_props=FOCUSED_WORLD_PROPS,
    )

    parquet_dir = config.PARQUET_USER_DIR / steam_id / demo_id
    _write_parquets(dem, parquet_dir, demo_id)

    map_name = _map_name(dem)
    match_date = datetime.fromtimestamp(demo_path.stat().st_mtime, tz=timezone.utc)
    stats = _match_stats(dem, steam_id)
    rounds = dem.rounds if dem.rounds is not None else pl.DataFrame()

    try:
        ct_round_wins = int((rounds["winner"] == "ct").sum()) if rounds.height > 0 and "winner" in rounds.columns else None
        t_round_wins  = int((rounds["winner"] == "t").sum())  if rounds.height > 0 and "winner" in rounds.columns else None

        game_kwargs = {
            "steam_id":               steam_id,
            "map_name":               map_name,
            "match_type":             match_type or "unknown",
            "share_code":             share_code,
            "match_date":             match_date,
            "parquet_dir":            str(parquet_dir),
            "demo_path":              str(demo_path),
            "round_count":            stats.get("round_count"),
            "ct_round_wins":          ct_round_wins,
            "t_round_wins":           t_round_wins,
            "tick_rate":              _tick_rate_from_header(dem.header),
            "user_side_first":        stats.get("user_side_first"),
            "user_rounds_won":        stats.get("score_ct"),
            "user_rounds_lost":       stats.get("score_t"),
            "user_result":            stats.get("user_result"),
            "user_kills":             stats.get("kills"),
            "user_deaths":            stats.get("deaths"),
            "user_assists":           stats.get("assists"),
            "user_hs_pct":            stats.get("hs_pct"),
        }
        asyncio.run(_save_match(demo_id, game_kwargs))
    except Exception:
        shutil.rmtree(parquet_dir, ignore_errors=True)
        raise

    return {
        "demo_id":       demo_id,
        "map":           map_name,
        "parquet_dir":   str(parquet_dir),
        **{k: v for k, v in stats.items()},
    }
