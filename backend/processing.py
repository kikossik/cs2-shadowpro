"""
Background demo processing pipeline for user-uploaded demos.

Runs in a thread pool (not the async event loop) because awpy/polars are
CPU-bound.  Writes situations + a match record into situations.db.
"""

import sys
import time
from pathlib import Path

# Allow importing from the pipeline package without installing it.
_PIPELINE = Path(__file__).resolve().parent.parent / "pipeline"
sys.path.insert(0, str(_PIPELINE))

from awpy import Demo  # type: ignore
import polars as pl

from extract_situations import extract
from situations_db import connect, init_schema, insert_dataframe, upsert_match

DB_PATH = Path(__file__).resolve().parent.parent / "situations.db"
PLAYER_PROPS = [
    "balance", "active_weapon", "armor_value",
    "has_defuser", "flash_duration", "inventory",
]
TICKRATE = 64


def _map_from_demo(dem: Demo) -> str:
    """Best-effort map name from the demo header."""
    try:
        return dem.header.get("map_name", "unknown")
    except Exception:
        return "unknown"


def _compute_match_stats(dem: Demo, user_steam_id: str) -> dict:
    """
    Derive win/loss, score, K/D/A/HS from the parsed demo tables.
    Returns a dict suitable for upsert_match kwargs.
    """
    uid = int(user_steam_id)
    stats: dict = {}

    # --- score & user side from rounds table ---
    rounds = dem.rounds  # polars DataFrame
    if rounds is not None and rounds.height > 0:
        last = rounds.sort("round_num").tail(1).row(0, named=True)
        stats["score_ct"] = last.get("ct_score") or last.get("ct_score_after", 0)
        stats["score_t"] = last.get("t_score") or last.get("t_score_after", 0)
        stats["round_count"] = rounds.height

        # Determine user's side in round 1 (first half starting side)
        ticks = dem.ticks
        if ticks is not None:
            round1 = rounds.sort("round_num").head(1).row(0, named=True)
            fe = round1.get("freeze_end")
            if fe:
                snap = (
                    ticks
                    .filter(pl.col("steamid") == uid)
                    .filter(pl.col("tick") == fe)
                )
                if snap.height > 0:
                    side_val = snap["side"][0]
                    stats["user_side_first"] = str(side_val).lower() if side_val else None

        # Determine result: count rounds won by user's team
        user_side_first = stats.get("user_side_first")
        if user_side_first:
            n_rounds = rounds.height
            wins = 0
            for r in rounds.iter_rows(named=True):
                rn = r["round_num"]
                half = 0 if rn <= 12 else 1
                my_side = user_side_first if half == 0 else ("t" if user_side_first == "ct" else "ct")
                winner = str(r.get("winner", "") or "").lower()
                if winner == my_side:
                    wins += 1
            losses = n_rounds - wins
            stats["user_result"] = "win" if wins > losses else ("draw" if wins == losses else "loss")

    # --- K/D/A/HS from kills table ---
    kills = dem.kills
    if kills is not None and kills.height > 0:
        k_rows = kills.filter(pl.col("attacker_steamid") == uid)
        d_rows = kills.filter(pl.col("victim_steamid") == uid)
        stats["kills"] = k_rows.height
        stats["deaths"] = d_rows.height
        # assists: column name varies by awpy version
        for col in ("assister_steamid", "assist_steamid"):
            if col in kills.columns:
                stats["assists"] = kills.filter(pl.col(col) == uid).height
                break
        else:
            stats["assists"] = 0
        if k_rows.height > 0 and "headshot" in k_rows.columns:
            hs = k_rows.filter(pl.col("headshot") == True).height  # noqa: E712
            stats["hs_pct"] = int(100 * hs / k_rows.height)
        else:
            stats["hs_pct"] = 0

    return stats


def process_demo(demo_path: Path, user_steam_id: str, demo_id: str) -> dict:
    """
    Parse, extract, and store a user demo.  Returns a summary dict.
    Raises on any error (caller should catch and update job state).
    """
    dem = Demo(path=str(demo_path))
    dem.parse(player_props=PLAYER_PROPS)

    situations = extract(
        dem.rounds, dem.ticks, dem.kills,
        dem.bomb, dem.smokes, dem.infernos,
        source="user", demo_id=demo_id,
    )

    map_name = _map_from_demo(dem)
    mtime = int(demo_path.stat().st_mtime)
    match_stats = _compute_match_stats(dem, user_steam_id)

    conn = connect(DB_PATH)
    init_schema(conn)
    insert_dataframe(conn, situations)
    upsert_match(
        conn,
        demo_id=demo_id,
        source="user",
        steam_id=user_steam_id,
        map=map_name,
        date_ts=mtime,
        situations_count=situations.height,
        **match_stats,
    )
    conn.close()

    return {
        "demo_id": demo_id,
        "situations": situations.height,
        "map": map_name,
        **match_stats,
    }
