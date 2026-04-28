"""Ingest a directory of local .dem files into the pro corpus.

Usage:
    python -m pipeline.jobs.ingest_local_demos --dir /path/to/demos
    python -m pipeline.jobs.ingest_local_demos --dir /path/to/demos --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from backend import db
from backend.log import get_logger
from pipeline.steps.ingest import ingest_pro_demo

log = get_logger("INGEST_LOCAL")

_MAP_SUFFIXES = {"ancient", "anubis", "dust2", "inferno", "mirage", "nuke", "overpass"}


def _parse_slug_meta(slug: str) -> dict:
    """Extract team names and map number from a slug like 'fnatic-vs-eyeballers-m2-mirage'.

    Handles slugs produced by manual file naming when HLTV metadata is unavailable.
    Returns an empty dict if the slug doesn't match the expected pattern.
    """
    parts = slug.split("-vs-", 1)
    if len(parts) != 2:
        return {}

    team1_slug, rest = parts

    # Strip known map suffix (longest match first to avoid partial matches)
    for m in sorted(_MAP_SUFFIXES, key=len, reverse=True):
        if rest.endswith(f"-{m}"):
            rest = rest[: -len(m) - 1]
            break
        if rest == m:
            rest = ""
            break

    # Strip optional map number suffix, e.g. -m3
    map_number: int | None = None
    mn = re.search(r"-m(\d+)$", rest)
    if mn:
        map_number = int(mn.group(1))
        rest = rest[: mn.start()]

    team2_slug = rest

    def _slug_to_name(s: str) -> str:
        return " ".join(w.capitalize() for w in s.split("-") if w)

    result: dict = {}
    if team1_slug:
        result["team1_name"] = _slug_to_name(team1_slug)
    if team2_slug:
        result["team2_name"] = _slug_to_name(team2_slug)
    if map_number is not None:
        result["map_number"] = map_number
    return result


async def ingest_local_demos(demos_dir: Path, dry_run: bool = False, force: bool = False) -> dict:
    dem_files = sorted(demos_dir.glob("*.dem"))
    if not dem_files:
        log.warning("no .dem files found in %s", demos_dir)
        return {"found": 0, "ingested": 0, "skipped": 0, "errors": []}

    log.info("found %d .dem files in %s", len(dem_files), demos_dir)

    already_ingested = await db.get_ingested_pro_match_ids()
    summary = {"found": len(dem_files), "ingested": 0, "skipped": 0, "errors": []}

    if dry_run:
        for dem in dem_files:
            status = "SKIP (already ingested)" if dem.stem in already_ingested else "WOULD INGEST"
            log.info("[dry-run] %s -> %s", dem.name, status)
        return summary

    with ProcessPoolExecutor(max_workers=1) as executor:
        for dem in dem_files:
            match_id = dem.stem
            if match_id in already_ingested and not force:
                log.info("skip %s (already ingested)", dem.name)
                summary["skipped"] += 1
                continue

            try:
                slug_meta = _parse_slug_meta(match_id)
                await ingest_pro_demo(dem, match_id, executor=executor, **slug_meta)
                summary["ingested"] += 1
                log.info("ingested %s", dem.name)
            except Exception as exc:
                summary["errors"].append({"dem": dem.name, "error": str(exc)})
                log.error("ERROR %s: %s", dem.name, exc)

    log.info(
        "done: found=%d ingested=%d skipped=%d errors=%d",
        summary["found"], summary["ingested"], summary["skipped"], len(summary["errors"]),
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest local .dem files into the pro corpus.")
    parser.add_argument("--dir", required=True, type=Path, help="Directory containing .dem files")
    parser.add_argument("--dry-run", action="store_true", help="List what would be ingested without doing anything")
    parser.add_argument("--force", action="store_true", help="Re-ingest even if already in DB")
    args = parser.parse_args()

    if not args.dir.is_dir():
        parser.error(f"{args.dir} is not a directory")

    asyncio.run(ingest_local_demos(args.dir, dry_run=args.dry_run, force=args.force))


if __name__ == "__main__":
    main()
