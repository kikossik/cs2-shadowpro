"""
Demo processing pipeline for user-uploaded demos.
Runs in a thread pool (awpy/polars are CPU-bound).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import polars as pl
from awpy import Demo

from backend import config, db
from pipeline.features.featurize_windows import FEATURE_VERSION, TICK_RATE
from pipeline.steps.build_artifact import ARTIFACT_VERSION, build_match_artifact

PLAYER_PROPS = [
    'balance', 'armor_value', 'has_defuser', 'flash_duration',
    'inventory', 'yaw', 'pitch', 'zoom_lvl',
]

_TICKS_KEEP = [
    'round_num', 'tick', 'steamid', 'name', 'side',
    'X', 'Y', 'Z', 'health', 'place',
    'yaw', 'pitch', 'inventory', 'flash_duration',
    'armor_value', 'has_defuser', 'balance', 'zoom_lvl',
]


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


_PARQUET_FIELDS = (
    "ticks", "rounds", "shots", "smokes", "infernos", "flashes", "grenade_paths",
)
_MATCH_TYPES = {"unknown", "premier", "competitive", "faceit"}


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


def _delete_parquets(parquet_dir: Path, demo_id: str) -> None:
    for field in _PARQUET_FIELDS:
        (parquet_dir / f"{demo_id}_{field}.parquet").unlink(missing_ok=True)


def _write_parquets(dem: Demo, parquet_dir: Path, demo_id: str) -> None:
    parquet_dir.mkdir(parents=True, exist_ok=True)

    def _w(df: pl.DataFrame, field: str) -> None:
        df.write_parquet(parquet_dir / f"{demo_id}_{field}.parquet")

    keep = [c for c in _TICKS_KEEP if c in dem.ticks.columns]
    _w(dem.ticks.select(keep), "ticks")
    _w(dem.rounds, "rounds")

    shots_keep = [c for c in ['round_num', 'tick', 'player_steamid', 'weapon']
                  if c in dem.shots.columns]
    _w(dem.shots.select(shots_keep), "shots")

    smokes_keep = [c for c in ['round_num', 'start_tick', 'end_tick', 'X', 'Y', 'thrower_name']
                   if c in dem.smokes.columns]
    _w(dem.smokes.select(smokes_keep), "smokes")

    infernos_keep = [c for c in ['round_num', 'start_tick', 'end_tick', 'X', 'Y']
                     if c in dem.infernos.columns]
    _w(dem.infernos.select(infernos_keep), "infernos")

    gren_keep = [c for c in ['round_num', 'tick', 'entity_id', 'grenade_type', 'X', 'Y']
                 if c in dem.grenades.columns]
    grenade_paths = dem.grenades.filter(
        pl.col('grenade_type') != 'CDecoyProjectile'
    ).select(gren_keep)
    _w(grenade_paths, "grenade_paths")

    flash_raw = dem.grenades.filter(pl.col('grenade_type') == 'CFlashbangProjectile')
    if flash_raw.height > 0:
        flashes = (
            flash_raw
            .sort('tick')
            .group_by('entity_id')
            .last()
            .select([c for c in ['round_num', 'tick', 'X', 'Y'] if c in flash_raw.columns])
        )
    else:
        flashes = pl.DataFrame(schema={
            'round_num': pl.UInt32, 'tick': pl.Int32,
            'X': pl.Float32, 'Y': pl.Float32,
        })
    _w(flashes, "flashes")


async def _save_match(demo_id: str, kwargs: dict, game_kwargs: dict, round_rows: list[dict]) -> None:
    await db.upsert_user_match(demo_id, **kwargs)
    await db.upsert_user_game(game_id=demo_id, **game_kwargs)
    await db.upsert_rounds(demo_id, round_rows)


def process_demo(
    demo_path: Path,
    steam_id: str,
    demo_id: str,
    share_code: str | None = None,
    match_type: str = "unknown",
) -> dict:
    """Parse a demo, write Parquet files, build artifact, upsert user_matches row."""
    match_type = (match_type or "unknown").strip().lower()
    if match_type not in _MATCH_TYPES:
        match_type = "unknown"

    dem = Demo(path=str(demo_path))
    dem.parse(player_props=PLAYER_PROPS)

    parquet_dir = config.PARQUET_USER_DIR
    _write_parquets(dem, parquet_dir, demo_id)

    map_name = _map_name(dem)
    match_date = datetime.fromtimestamp(demo_path.stat().st_mtime, tz=timezone.utc)
    stats = _match_stats(dem, steam_id)
    rounds = dem.rounds if dem.rounds is not None else pl.DataFrame()
    round_rows = _round_fact_rows(rounds) if rounds.height > 0 else []

    try:
        artifact_path = build_match_artifact(
            source_type="user",
            source_match_id=demo_id,
            parquet_dir=parquet_dir,
            stem=demo_id,
            map_name=map_name,
            steam_id=steam_id,
        )
        db_kwargs = {
            "steam_id":        steam_id,
            "map_name":        map_name,
            "match_type":      match_type or "unknown",
            "match_date":      match_date,
            "parquet_dir":     str(parquet_dir),
            "artifact_path":   artifact_path,
            "share_code":      share_code,
            **{k: stats.get(k) for k in (
                "score_ct", "score_t", "user_side_first", "user_result",
                "kills", "deaths", "assists", "hs_pct", "round_count",
            )},
        }
        game_kwargs = {
            "steam_id":                 steam_id,
            "map_name":                 map_name,
            "match_type":               match_type or "unknown",
            "share_code":               share_code,
            "match_date":               match_date,
            "parquet_dir":              str(parquet_dir),
            "artifact_path":            artifact_path,
            "round_count":              stats.get("round_count"),
            "tick_rate":                TICK_RATE,
            "artifact_version":         ARTIFACT_VERSION,
            "window_feature_version":   FEATURE_VERSION,
        }
        asyncio.run(_save_match(demo_id, db_kwargs, game_kwargs, round_rows))
    except Exception:
        _delete_parquets(parquet_dir, demo_id)
        raise

    return {
        "demo_id":       demo_id,
        "map":           map_name,
        "artifact_path": artifact_path,
        **{k: v for k, v in stats.items()},
    }
