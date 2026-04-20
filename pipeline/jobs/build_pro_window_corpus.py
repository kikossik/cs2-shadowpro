"""Backfill event-window features for ingested pro matches."""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from backend import db
from pipeline.features.extract_windows import extract_match_event_windows


async def build_pro_window_corpus(limit: int | None = None) -> dict:
    matches = await db.get_pro_matches(limit=limit)

    processed_matches = 0
    stored_windows = 0
    errors: list[dict] = []

    for match in matches:
        parquet_dir = match.get("parquet_dir")
        if not parquet_dir:
            continue

        try:
            windows = extract_match_event_windows(
                source_type="pro",
                source_match_id=match["match_id"],
                parquet_dir=Path(parquet_dir),
                stem=match["match_id"],
                map_name=match["map_name"],
            )
            for window in windows:
                window_id = window.pop("window_id")
                await db.upsert_event_window(window_id, **window)
            processed_matches += 1
            stored_windows += len(windows)
            print(f"[event-windows] {match['match_id']} -> {len(windows)} windows")
        except Exception as exc:
            errors.append({
                "match_id": match["match_id"],
                "error": str(exc),
            })
            print(f"[event-windows] ERROR {match['match_id']}: {exc}")

    return {
        "processed_matches": processed_matches,
        "stored_windows": stored_windows,
        "errors": errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    summary = asyncio.run(build_pro_window_corpus(limit=args.limit))
    print(f"[event-windows] done: {summary}")


if __name__ == "__main__":
    main()
