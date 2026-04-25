"""One-time job: scrape HLTV and ingest N pro demos per map.

Scrapes a per-map filtered results page (e.g. /results?map=de_mirage) so that
every page visited is likely to contain a demo for the target map. Archives often
contain multiple maps, so a single download can satisfy several maps at once.

Usage:
    python -m pipeline.jobs.seed_corpus                       # 5 per map (default)
    python -m pipeline.jobs.seed_corpus --matches-per-map 1  # test: 1 per map
    python -m pipeline.jobs.seed_corpus --limit 30           # max pages per map
"""
from __future__ import annotations

import argparse
import asyncio
import os
import traceback
from pathlib import Path

os.environ.setdefault("PLAYWRIGHT_HEADLESS", "1")

from backend import config, db
from backend.log import get_logger
from pipeline.steps.decompress import extract_all_dems
from pipeline.steps.download import download_archive
from pipeline.steps.ingest import ingest_pro_demo
from pipeline.steps.scrape import scrape_pro_matches

log = get_logger("SEED")

JOB_NAME = "seed_corpus"

_TAG_TO_MAP = {
    "ancient":  "de_ancient",
    "anubis":   "de_anubis",
    "dust2":    "de_dust2",
    "inferno":  "de_inferno",
    "mirage":   "de_mirage",
    "nuke":     "de_nuke",
    "overpass": "de_overpass",
}

_HLTV_RESULTS = "https://www.hltv.org/results?map={map_name}"


async def _per_map_counts() -> dict[str, int]:
    pool = await db.get_pool()
    rows = await pool.fetch(
        "SELECT map_name, COUNT(*) AS cnt "
        "FROM games "
        "WHERE source_type = 'pro' AND ingest_status = 'ready' "
        "GROUP BY map_name"
    )
    return {r["map_name"]: r["cnt"] for r in rows}


async def _process_archive(
    match: dict,
    needed: dict[str, int],
    already_ingested: set[str],
    summary: dict,
) -> None:
    archive: Path | None = None
    try:
        archive = await download_archive(match, config.DEMOS_PRO_DIR)
        dems = extract_all_dems(archive, config.DEMOS_PRO_DIR / "decompressed")

        for map_number, dem in enumerate(dems, start=1):
            tag = dem.stem.rsplit("_", 1)[-1]
            dem_map = _TAG_TO_MAP.get(tag)

            if not dem_map:
                dem.unlink(missing_ok=True)
                continue

            if dem.stem in already_ingested:
                summary["skipped"] += 1
                dem.unlink(missing_ok=True)
                continue

            if needed.get(dem_map, 0) <= 0:
                dem.unlink(missing_ok=True)
                continue

            try:
                await ingest_pro_demo(
                    dem,
                    dem.stem,
                    hltv_match_id=match.get("match_id"),
                    hltv_url=match.get("match_url"),
                    source_slug=match.get("slug"),
                    event_name=match.get("event_name"),
                    team1_name=match.get("team1"),
                    team2_name=match.get("team2"),
                    match_date=match.get("match_date"),
                    map_number=map_number,
                )
                already_ingested.add(dem.stem)
                needed[dem_map] -= 1
                summary["maps_ingested"] += 1
                dem.unlink(missing_ok=True)
                log.info("ingested %s (%d more needed for %s)", dem.name, needed[dem_map], dem_map)
            except Exception as exc:
                summary["errors"].append({"dem": dem.name, "error": str(exc)})
                log.error("INGEST ERROR %s: %s", dem.name, exc)
                traceback.print_exc()

    except Exception as exc:
        summary["errors"].append({"match_id": match.get("match_id"), "error": str(exc)})
        log.error("MATCH ERROR %s: %s", match.get("match_id"), exc)
        traceback.print_exc()
    finally:
        if archive is not None:
            archive.unlink(missing_ok=True)


async def seed_corpus(matches_per_map: int = 5, limit: int = 30) -> dict:
    run_id = await db.start_job_run(JOB_NAME)
    summary: dict = {"maps_ingested": 0, "skipped": 0, "errors": []}
    status = "done"
    error_message = None

    try:
        all_maps = {m["map_name"] for m in await db.get_maps()}
        counts = await _per_map_counts()
        needed: dict[str, int] = {
            m: max(0, matches_per_map - counts.get(m, 0)) for m in all_maps
        }
        log.info(
            "target=%d per map; needed: %s",
            matches_per_map,
            {k: v for k, v in needed.items() if v > 0} or "none",
        )

        if all(v == 0 for v in needed.values()):
            log.info("all maps already have %d games — done", matches_per_map)
            await db.finish_job_run(run_id, status="done", items_processed=0)
            return summary

        already_ingested = await db.get_ingested_pro_match_ids()
        # Track which HLTV match IDs we've already downloaded to avoid
        # re-downloading the same archive when it appears on multiple maps' pages.
        processed_match_ids: set[str] = set()

        # Iterate maps sorted most-needed first so we always make progress on
        # the most starved maps before spending time on already-near-full ones.
        for map_name in sorted(needed, key=lambda m: -needed[m]):
            if needed[map_name] <= 0:
                continue

            results_url = _HLTV_RESULTS.format(map_name=map_name)
            log.info("scraping %s (need %d) from %s", map_name, needed[map_name], results_url)

            matches = await scrape_pro_matches(limit=limit, results_url=results_url)
            log.info("got %d match pages for %s", len(matches), map_name)

            for match in matches:
                if needed[map_name] <= 0:
                    break

                match_id = match.get("match_id")
                if match_id in processed_match_ids:
                    continue

                if not match.get("demo_url"):
                    summary["skipped"] += 1
                    continue

                processed_match_ids.add(match_id)
                await _process_archive(match, needed, already_ingested, summary)

    except Exception as exc:
        status = "error"
        error_message = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()

    await db.finish_job_run(
        run_id,
        status=status,
        items_processed=summary["maps_ingested"],
        error_message=error_message,
        stats={
            "maps_ingested": summary["maps_ingested"],
            "skipped": summary["skipped"],
            "errors": len(summary["errors"]),
        },
    )
    log.info(
        "done: maps_ingested=%d skipped=%d errors=%d",
        summary["maps_ingested"], summary["skipped"], len(summary["errors"]),
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed pro corpus: N demos per map.")
    parser.add_argument(
        "--matches-per-map", type=int, default=5,
        help="Target ingested pro games per map (default: 5; use 1 to smoke-test)",
    )
    parser.add_argument(
        "--limit", type=int, default=30,
        help="Max HLTV match pages to scrape per map (default: 30)",
    )
    args = parser.parse_args()
    asyncio.run(seed_corpus(matches_per_map=args.matches_per_map, limit=args.limit))


if __name__ == "__main__":
    main()
