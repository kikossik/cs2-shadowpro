"""
Demo processing pipeline for user-uploaded / auto-synced demos.
Runs in a thread pool (awpy/polars are CPU-bound).
"""

from pathlib import Path

import polars as pl
from awpy import Demo

from backend.config import DB_PATH
from backend.db import connect, init_schema, insert_situations, upsert_match
from pipeline.extract_situations import extract

PLAYER_PROPS = [
    "balance", "active_weapon", "armor_value",
    "has_defuser", "flash_duration", "inventory",
]


def _map_name(dem: Demo) -> str:
    try:
        return dem.header.get("map_name", "unknown") or "unknown"
    except Exception:
        return "unknown"


def _match_stats(dem: Demo, steam_id: str) -> dict:
    """
    Derive score, result, K/D/A/HS from the parsed demo tables.

    awpy rounds schema: round_num, start, freeze_end, end, winner (str "ct"/"t"),
                        reason, bomb_plant, bomb_site
    — no cumulative score columns; we compute them by counting winners.
    """
    uid = int(steam_id)  # awpy stores steamids as UInt64; polars handles int comparison
    stats: dict = {}

    rounds = dem.rounds
    if rounds is not None and rounds.height > 0:
        sorted_rounds = rounds.sort("round_num")
        stats["round_count"] = sorted_rounds.height

        ct_wins = (sorted_rounds["winner"] == "ct").sum()
        t_wins  = (sorted_rounds["winner"] == "t").sum()
        stats["score_ct"] = int(ct_wins)
        stats["score_t"]  = int(t_wins)

        # User's starting side from ticks at the round-1 freeze-end tick
        ticks = dem.ticks
        if ticks is not None:
            round1 = sorted_rounds.head(1).row(0, named=True)
            fe = round1.get("freeze_end")
            if fe is not None:
                snap = ticks.filter(
                    (pl.col("steamid") == uid) & (pl.col("tick") == fe)
                )
                if snap.height > 0:
                    side_val = snap["side"][0]
                    if side_val:
                        stats["user_side_first"] = str(side_val).lower()

        # Result: count rounds won by the user's side
        user_side_first = stats.get("user_side_first")
        if user_side_first:
            wins = 0
            for r in sorted_rounds.iter_rows(named=True):
                rn = r["round_num"]
                # Sides swap at round 13; OT: swap every 3 rounds after round 24
                if rn <= 12:
                    my_side = user_side_first
                elif rn <= 24:
                    my_side = "t" if user_side_first == "ct" else "ct"
                else:
                    ot_round = rn - 25          # 0-based within OT
                    swap = (ot_round // 3) % 2
                    my_side = (user_side_first if swap == 0
                               else ("t" if user_side_first == "ct" else "ct"))
                if str(r.get("winner", "") or "").lower() == my_side:
                    wins += 1
            losses = stats["round_count"] - wins
            if wins > losses:
                stats["user_result"] = "win"
            elif wins == losses:
                stats["user_result"] = "draw"
            else:
                stats["user_result"] = "loss"

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


def process_demo(demo_path: Path, steam_id: str, demo_id: str) -> dict:
    """Parse, extract situations, and write match record to DB. Returns summary dict."""
    dem = Demo(path=str(demo_path))
    dem.parse(player_props=PLAYER_PROPS)

    situations = extract(
        dem.rounds, dem.ticks, dem.kills,
        dem.bomb, dem.smokes, dem.infernos,
        source="user", demo_id=demo_id,
    )

    map_name   = _map_name(dem)
    date_ts    = int(demo_path.stat().st_mtime)
    stats      = _match_stats(dem, steam_id)

    conn = connect(DB_PATH)
    init_schema(conn)
    insert_situations(conn, situations)
    upsert_match(
        conn,
        demo_id=demo_id,
        source="user",
        steam_id=steam_id,
        map=map_name,
        date_ts=date_ts,
        situations_count=situations.height,
        **stats,
    )
    conn.close()

    return {"demo_id": demo_id, "situations": situations.height, "map": map_name, **stats}
