#!/usr/bin/env python3.13
"""
match_situation.py — given a situation, return top-5 pro matches with scoring.

Usage:
    # by DB id
    python match_situation.py --id 12345

    # by selector (demo_id:round_num:tick:steamid)
    python match_situation.py --selector "2393046_astralis-vs-fut:5:7850:12345678"

    # by raw JSON
    python match_situation.py --json '{
        "player_place":"Mid","player_side":"ct",
        "alive_ct":2,"alive_t":2,"phase":"post_plant",
        "economy_bucket":"full","time_remaining_s":25.0,
        "player_x":280.0,"player_y":150.0,"player_z":0.0,
        "smokes_active":1,"mollies_active":0
    }'

    # with radar PNG
    python match_situation.py --id 12345 --radar radar.png
"""

import argparse
import json
import math
import sys
from pathlib import Path

from situations_db import connect

# ── Scoring weights (PLAN.md) ──────────────────────────────────────────────────
W_FEATURE = 0.4   # categorical match (always 1.0; all SQL-filtered)
W_SPATIAL  = 0.3  # 2D euclidean proximity
W_UTILITY  = 0.2  # smokes + mollies overlap
W_TIME     = 0.1  # time_remaining closeness

SPATIAL_SCALE = 1000.0   # half-score at 1000 game-units
TIME_WINDOW   = 20.0     # must match the WHERE clause ±window

# ── SQL ────────────────────────────────────────────────────────────────────────
CANDIDATE_QUERY = """
SELECT id, demo_id, round_num, tick, source_event,
       player_steamid, player_name, player_side, player_place,
       player_x, player_y, player_z,
       balance, economy_bucket,
       alive_ct, alive_t, phase, time_remaining_s,
       smokes_active, mollies_active,
       clip_start_tick, clip_end_tick
FROM situations
WHERE source = 'pro'
  AND player_place   = :place
  AND player_side    = :side
  AND alive_ct       = :alive_ct
  AND alive_t        = :alive_t
  AND phase          = :phase
  AND economy_bucket = :economy
  AND abs(time_remaining_s - :time) < :time_window
ORDER BY
  ((player_x - :x) * (player_x - :x)
 + (player_y - :y) * (player_y - :y)) ASC
LIMIT :limit;
"""

LOOKUP_BY_ID = """
SELECT id, demo_id, round_num, tick, source_event,
       player_steamid, player_name, player_side, player_place,
       player_x, player_y, player_z,
       balance, economy_bucket,
       alive_ct, alive_t, phase, time_remaining_s,
       smokes_active, mollies_active,
       clip_start_tick, clip_end_tick
FROM situations WHERE id = ?;
"""

LOOKUP_BY_SELECTOR = """
SELECT id, demo_id, round_num, tick, source_event,
       player_steamid, player_name, player_side, player_place,
       player_x, player_y, player_z,
       balance, economy_bucket,
       alive_ct, alive_t, phase, time_remaining_s,
       smokes_active, mollies_active,
       clip_start_tick, clip_end_tick
FROM situations
WHERE demo_id = ? AND round_num = ? AND tick = ? AND player_steamid = ?
LIMIT 1;
"""

_COLS = [
    "id", "demo_id", "round_num", "tick", "source_event",
    "player_steamid", "player_name", "player_side", "player_place",
    "player_x", "player_y", "player_z",
    "balance", "economy_bucket",
    "alive_ct", "alive_t", "phase", "time_remaining_s",
    "smokes_active", "mollies_active",
    "clip_start_tick", "clip_end_tick",
]


def _row(row: tuple) -> dict:
    return dict(zip(_COLS, row))


# ── Scoring ────────────────────────────────────────────────────────────────────

def score_match(query: dict, cand: dict) -> tuple[float, dict]:
    """Return (total_score, breakdown_dict)."""
    qx = query.get("player_x") or 0.0
    qy = query.get("player_y") or 0.0
    cx = cand.get("player_x") or 0.0
    cy = cand.get("player_y") or 0.0
    dist = math.sqrt((cx - qx) ** 2 + (cy - qy) ** 2)
    s_spatial = 1.0 / (1.0 + dist / SPATIAL_SCALE)

    q_smokes  = query.get("smokes_active") or 0
    q_mollies = query.get("mollies_active") or 0
    c_smokes  = cand.get("smokes_active") or 0
    c_mollies = cand.get("mollies_active") or 0
    s_utility = max(0.0, 1.0 - 0.25 * (abs(c_smokes - q_smokes) + abs(c_mollies - q_mollies)))

    time_diff = abs((cand.get("time_remaining_s") or 0) - (query.get("time_remaining_s") or 0))
    s_time = max(0.0, 1.0 - time_diff / TIME_WINDOW)

    total = W_FEATURE + W_SPATIAL * s_spatial + W_UTILITY * s_utility + W_TIME * s_time
    return total, {
        "feature": round(W_FEATURE, 3),
        "spatial": round(W_SPATIAL * s_spatial, 3),
        "utility": round(W_UTILITY * s_utility, 3),
        "time":    round(W_TIME * s_time, 3),
        "_dist":   round(dist, 1),
        "_time_diff": round(time_diff, 2),
    }


# ── DB helpers ─────────────────────────────────────────────────────────────────

def lookup_query(conn, args: argparse.Namespace) -> dict:
    if args.id is not None:
        row = conn.execute(LOOKUP_BY_ID, (args.id,)).fetchone()
        if row is None:
            sys.exit(f"error: no situation with id={args.id}")
        return _row(row)
    if args.selector is not None:
        parts = args.selector.split(":")
        if len(parts) != 4:
            sys.exit("error: --selector must be 'demo_id:round_num:tick:steamid'")
        demo_id, round_num, tick, steamid = parts
        row = conn.execute(
            LOOKUP_BY_SELECTOR, (demo_id, int(round_num), int(tick), int(steamid))
        ).fetchone()
        if row is None:
            sys.exit(f"error: no situation found for selector '{args.selector}'")
        return _row(row)
    # --json
    try:
        return json.loads(args.json)
    except json.JSONDecodeError as e:
        sys.exit(f"error: invalid JSON — {e}")


def find_candidates(conn, query: dict, sql_limit: int) -> list[dict]:
    params = {
        "place":       query.get("player_place", ""),
        "side":        query.get("player_side", ""),
        "alive_ct":    query.get("alive_ct", 0),
        "alive_t":     query.get("alive_t", 0),
        "phase":       query.get("phase", ""),
        "economy":     query.get("economy_bucket", ""),
        "time":        query.get("time_remaining_s") or 0.0,
        "x":           query.get("player_x") or 0.0,
        "y":           query.get("player_y") or 0.0,
        "time_window": TIME_WINDOW,
        "limit":       sql_limit,
    }
    rows = conn.execute(CANDIDATE_QUERY, params).fetchall()
    return [_row(r) for r in rows]


# ── Output ─────────────────────────────────────────────────────────────────────

def print_results(query: dict, ranked: list[tuple[float, dict, dict]], top_n: int) -> None:
    SEP = "─" * 60

    print(f"\n{SEP}")
    print("  QUERY SITUATION")
    print(SEP)
    qid = query.get("id", "–")
    print(f"  id={qid}  demo={query.get('demo_id', '–')}  "
          f"round={query.get('round_num', '–')}  tick={query.get('tick', '–')}")
    print(f"  place={query.get('player_place')}  side={query.get('player_side')}  "
          f"{query.get('alive_ct')}v{query.get('alive_t')}  "
          f"phase={query.get('phase')}  econ={query.get('economy_bucket')}")
    tr = query.get("time_remaining_s")
    print(f"  time_remaining={tr:.1f}s  "
          f"smokes={query.get('smokes_active', 0)}  mollies={query.get('mollies_active', 0)}")
    px, py = query.get("player_x"), query.get("player_y")
    if px is not None and py is not None:
        print(f"  pos=({px:.0f}, {py:.0f})")

    if not ranked:
        print("\n  No matches found.")
        return

    print(f"\n{SEP}")
    print(f"  TOP {min(top_n, len(ranked))} MATCHES")
    print(SEP)

    for rank, (score, cand, bd) in enumerate(ranked[:top_n], 1):
        name = cand.get("player_name") or "Unknown"
        print(f"\n  [{rank}] {name}  score={score:.4f}")
        print(f"       demo={cand['demo_id']}  "
              f"round={cand['round_num']}  tick={cand['tick']}")
        print(f"       place={cand['player_place']}  side={cand['player_side']}  "
              f"{cand['alive_ct']}v{cand['alive_t']}  "
              f"phase={cand['phase']}  econ={cand['economy_bucket']}")
        print(f"       time_remaining={cand['time_remaining_s']:.1f}s  "
              f"smokes={cand['smokes_active']}  mollies={cand['mollies_active']}")
        print(f"       pos=({cand['player_x']:.0f}, {cand['player_y']:.0f})  "
              f"dist={bd['_dist']:.0f}u  Δtime={bd['_time_diff']:.1f}s")
        print(f"       score: feature={bd['feature']}  spatial={bd['spatial']}  "
              f"utility={bd['utility']}  time={bd['time']}")
        print(f"       clip ticks: {cand['clip_start_tick']}–{cand['clip_end_tick']}")


# ── Radar ──────────────────────────────────────────────────────────────────────

def render_radar(
    query: dict,
    ranked: list[tuple[float, dict, dict]],
    out_path: Path,
    top_n: int,
) -> None:
    try:
        import matplotlib.pyplot as plt
        from awpy.plot.plot import PointSettings, plot
    except ImportError as e:
        print(f"warning: radar skipped — {e}", file=sys.stderr)
        return

    shown = ranked[:top_n]
    points: list[tuple[float, float, float]] = []
    settings: list[PointSettings] = []

    # Query position — yellow star
    qx = query.get("player_x") or 0.0
    qy = query.get("player_y") or 0.0
    qz = query.get("player_z") or 0.0
    points.append((qx, qy, qz))
    settings.append(PointSettings(marker="*", color="yellow", size=20, label="Query"))

    # Matched pro positions
    colors = ["cyan", "lime", "orange", "magenta", "red"]
    for i, (score, cand, _) in enumerate(shown):
        cx = cand.get("player_x") or 0.0
        cy = cand.get("player_y") or 0.0
        cz = cand.get("player_z") or 0.0
        name = cand.get("player_name") or f"Match {i + 1}"
        points.append((cx, cy, cz))
        settings.append(PointSettings(
            marker="o",
            color=colors[i % len(colors)],
            size=12,
            label=f"[{i + 1}] {name} ({score:.3f})",
        ))

    fig, ax = plot("de_mirage", points=points, point_settings=settings)

    # awpy doesn't propagate PointSettings.label to matplotlib artists,
    # so build legend patches manually.
    import matplotlib.patches as mpatches
    legend_handles = [
        mpatches.Patch(color=s.color, label=s.label)
        for s in settings
        if s.label
    ]
    if legend_handles:
        ax.legend(handles=legend_handles, loc="upper right",
                  fontsize=8, framealpha=0.7, facecolor="#111")

    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"\nRadar saved to {out_path}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Match a CS2 situation against the pro situations DB."
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--id", type=int, metavar="ID",
                     help="Look up a situation by DB id")
    src.add_argument("--selector", metavar="DEMO:ROUND:TICK:STEAMID",
                     help="Look up by 'demo_id:round_num:tick:steamid'")
    src.add_argument("--json", metavar="JSON",
                     help="Raw situation JSON with matching fields")
    p.add_argument("--top", type=int, default=5, metavar="N",
                   help="Show top N matches (default: 5)")
    p.add_argument("--limit", type=int, default=50, metavar="N",
                   help="SQL candidate pool before rescoring (default: 50)")
    p.add_argument("--radar", metavar="FILE",
                   help="Render positions to a radar PNG")
    p.add_argument("--db", default="situations.db", metavar="FILE",
                   help="Path to situations.db (default: situations.db)")
    return p


def main() -> None:
    args = build_parser().parse_args()
    conn = connect(Path(args.db))

    query = lookup_query(conn, args)
    candidates = find_candidates(conn, query, sql_limit=args.limit)

    # Exclude self-match when the query came from the DB.
    query_id = query.get("id")
    if query_id is not None:
        candidates = [c for c in candidates if c["id"] != query_id]

    # Score all candidates, sort descending.
    scored = [(score, cand, bd) for cand in candidates
              for score, bd in [score_match(query, cand)]]
    scored.sort(key=lambda t: t[0], reverse=True)

    print_results(query, scored, top_n=args.top)

    if args.radar:
        render_radar(query, scored, Path(args.radar), top_n=args.top)

    conn.close()


if __name__ == "__main__":
    main()
