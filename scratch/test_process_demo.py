"""
Run the full user-demo processing pipeline locally and print every DB field.
Connects to the local Postgres (port 5432) via DATABASE_URL.

Usage (from repo root):
    DATABASE_URL=postgresql://cs2shadowpro:cs2shadowpro@localhost/cs2shadowpro \
    python scratch/test_process_demo.py demos_user/user_76561198857367828_QB4Y6_LmikM_4kibz_txOU2_2zYUN.dem
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

# ── make repo root importable ─────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://cs2shadowpro:cs2shadowpro@localhost/cs2shadowpro",
)

from backend.processing import process_demo
from backend import db

STEAM_ID = "76561198857367828"

# ─────────────────────────────────────────────────────────────────────────────

async def fetch_all(demo_id: str) -> None:
    pool = await db.get_pool()

    print("\n─── user_matches ───────────────────────────────────────────────")
    rows = await pool.fetch("SELECT * FROM user_matches WHERE demo_id = $1", demo_id)
    for r in rows:
        for k, v in dict(r).items():
            print(f"  {k:30s} = {v}")

    print("\n─── games ───────────────────────────────────────────────────────")
    rows = await pool.fetch("SELECT * FROM games WHERE game_id = $1", demo_id)
    for r in rows:
        for k, v in dict(r).items():
            print(f"  {k:30s} = {v}")

    print("\n─── matches ─────────────────────────────────────────────────────")
    match_id = f"user_{demo_id}"
    rows = await pool.fetch("SELECT * FROM matches WHERE match_id = $1", match_id)
    for r in rows:
        for k, v in dict(r).items():
            print(f"  {k:30s} = {v}")

    print("\n─── game_teams ──────────────────────────────────────────────────")
    rows = await pool.fetch("SELECT * FROM game_teams WHERE game_id = $1", demo_id)
    if rows:
        for r in rows:
            print(f"  {dict(r)}")
    else:
        print("  (no rows)")

    print("\n─── game_artifacts ──────────────────────────────────────────────")
    rows = await pool.fetch("SELECT * FROM game_artifacts WHERE game_id = $1", demo_id)
    if rows:
        for r in rows:
            for k, v in dict(r).items():
                val = v[:80] if isinstance(v, str) and len(v) > 80 else v
                print(f"  {k:30s} = {val}")
    else:
        print("  (no rows)")

    print("\n─── rounds (count) ──────────────────────────────────────────────")
    row = await pool.fetchrow("SELECT COUNT(*) as n FROM rounds WHERE game_id = $1", demo_id)
    print(f"  rounds: {row['n']}")

    print("\n─── event_windows (count) ───────────────────────────────────────")
    row = await pool.fetchrow(
        "SELECT COUNT(*) as n, COUNT(embedding) as with_emb FROM event_windows WHERE source_match_id = $1",
        demo_id,
    )
    print(f"  windows: {row['n']}  with embedding: {row['with_emb']}")

    await db.close_pool()


def main() -> None:
    demo_path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    if demo_path is None or not demo_path.exists():
        print(f"Usage: python {sys.argv[0]} <path/to/demo.dem>")
        sys.exit(1)

    demo_id = demo_path.name
    print(f"\n>>> Processing {demo_id}")
    print(f">>> steam_id={STEAM_ID}\n")

    try:
        result = process_demo(demo_path, STEAM_ID, demo_id)
        print(f"\n>>> process_demo returned: {json.dumps(result, default=str, indent=2)}")
    except Exception as exc:
        print(f"\n>>> ERROR in process_demo: {exc}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    print("\n>>> Fetching DB rows …")
    asyncio.run(fetch_all(demo_id))


if __name__ == "__main__":
    main()
