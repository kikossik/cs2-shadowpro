#!/usr/bin/env python3.13
"""
Parse every demo in demos_decompressed/ and load situations into situations.db.

Resume-safe: demos already present in the DB (by `demo_id` = filename) are
skipped. Run this whenever the corpus grows.

Usage:
    python batch_extract.py
"""

import time
import traceback
from pathlib import Path

from awpy import Demo

from backend.config import DB_PATH, DEMOS_PRO_DECOMPRESSED_DIR
from backend.db import connect, init_schema, processed_demos, insert_situations
from pipeline.extract_situations import extract

DEMO_DIR = DEMOS_PRO_DECOMPRESSED_DIR

# Kept in sync with parse_one_demo.py — matching player_props is required for
# the economy bucket (inventory) and any future equipment features.
PLAYER_PROPS = [
    "balance",
    "active_weapon",
    "armor_value",
    "has_defuser",
    "flash_duration",
    "inventory",
]


def parse_demo(path: Path):
    dem = Demo(path=str(path))
    dem.parse(player_props=PLAYER_PROPS)
    return dem


def main() -> None:
    demos = sorted(DEMO_DIR.glob("*.dem"))
    if not demos:
        print(f"No demos in {DEMO_DIR}/")
        return

    conn = connect()
    init_schema(conn)
    done = processed_demos(conn)
    todo = [d for d in demos if d.name not in done]
    print(f"demos: {len(demos)} total, {len(done)} already ingested, {len(todo)} to process")

    total_added = 0
    for i, path in enumerate(todo, 1):
        demo_id = path.name
        t0 = time.monotonic()
        print(f"[{i}/{len(todo)}] {demo_id}", flush=True)
        try:
            dem = parse_demo(path)
            situations = extract(
                dem.rounds, dem.ticks, dem.kills,
                dem.bomb, dem.smokes, dem.infernos,
                source="pro", demo_id=demo_id,
            )
            insert_situations(conn, situations)
            total_added += situations.height
            elapsed = int(time.monotonic() - t0)
            print(f"  → {situations.height} situations ({elapsed}s)")
        except Exception as e:
            print(f"  ERROR: {e}")
            traceback.print_exc()

    total = conn.execute("SELECT COUNT(*) FROM situations").fetchone()[0]
    demos_in_db = conn.execute(
        "SELECT COUNT(DISTINCT demo_id) FROM situations"
    ).fetchone()[0]
    print(f"\nadded this run: {total_added}")
    print(f"db total: {total} situations across {demos_in_db} demos")


if __name__ == "__main__":
    main()
