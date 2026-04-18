#!/usr/bin/env python3.13
"""
Verify index query latency against situations.db.

Runs the matching query shape from PLAN.md (categorical filter + spatial
tiebreaker) against a handful of sampled situations and prints timings.

Usage:
    python check_query_latency.py
"""

import random
import time

from backend.db import connect

MATCH_QUERY = """
SELECT id, demo_id, round_num, tick, player_steamid,
       player_x, player_y
FROM situations
WHERE source = 'pro'
  AND player_place   = :place
  AND player_side    = :side
  AND alive_ct       = :alive_ct
  AND alive_t        = :alive_t
  AND phase          = :phase
  AND economy_bucket = :economy
  AND abs(time_remaining_s - :time) < 20
ORDER BY
  ((player_x - :x) * (player_x - :x)
 + (player_y - :y) * (player_y - :y)) ASC
LIMIT 5;
"""


def main() -> None:
    conn = connect()
    total = conn.execute("SELECT COUNT(*) FROM situations").fetchone()[0]
    print(f"situations in db: {total}")

    # Sample 20 real situations to use as query inputs.
    probes = conn.execute(
        """
        SELECT player_place, player_side, alive_ct, alive_t, phase,
               economy_bucket, time_remaining_s, player_x, player_y
        FROM situations
        WHERE source = 'pro'
        ORDER BY RANDOM()
        LIMIT 20
        """
    ).fetchall()

    random.seed(0)
    timings_ms = []
    for row in probes:
        params = {
            "place": row[0], "side": row[1],
            "alive_ct": row[2], "alive_t": row[3],
            "phase": row[4], "economy": row[5],
            "time": row[6], "x": row[7], "y": row[8],
        }
        t0 = time.perf_counter()
        res = conn.execute(MATCH_QUERY, params).fetchall()
        dt = (time.perf_counter() - t0) * 1000
        timings_ms.append(dt)
        print(f"  place={row[0]:<12} side={row[1]} {row[2]}v{row[3]} "
              f"phase={row[4]:<10} econ={row[5]:<5} "
              f"→ {len(res)} rows  {dt:6.2f}ms")

    timings_ms.sort()
    print()
    print(f"min  : {timings_ms[0]:6.2f}ms")
    print(f"p50  : {timings_ms[len(timings_ms) // 2]:6.2f}ms")
    print(f"p90  : {timings_ms[int(len(timings_ms) * 0.9)]:6.2f}ms")
    print(f"max  : {timings_ms[-1]:6.2f}ms")


if __name__ == "__main__":
    main()
