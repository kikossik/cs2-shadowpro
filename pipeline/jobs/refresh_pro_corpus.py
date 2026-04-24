"""End-to-end pro-corpus refresh job.

Chain:
  scrape HLTV -> download archive -> extract map demos -> ingest pro demo

Each successful ingest writes parquets, builds the match artifact, and indexes
event windows through pipeline.steps.ingest.ingest_pro_demo().
"""
from __future__ import annotations

import argparse
import asyncio
import traceback
from pathlib import Path

from backend import config, db
from backend.log import get_logger

log = get_logger("REFRESH")
from pipeline.steps.decompress import KNOWN_MAPS, extract_all_dems
from pipeline.steps.download import download_archive
from pipeline.steps.ingest import ingest_pro_demo
from pipeline.steps.scrape import scrape_pro_matches

JOB_NAME = "refresh_pro_corpus"


async def refresh_pro_corpus(limit: int = 50, results_url: str | None = None) -> dict:
    """Scrape recent pro matches and ingest every competitive-map demo found."""
    run_id = await db.start_job_run(JOB_NAME)
    summary: dict = {
        "scraped": 0,
        "skipped": 0,
        "attempted": 0,
        "succeeded": 0,
        "maps_ingested": 0,
        "errors": [],
    }
    status = "done"
    error_message = None

    scrape_kwargs: dict = {"limit": limit}
    if results_url:
        scrape_kwargs["results_url"] = results_url

    try:
        matches = await scrape_pro_matches(**scrape_kwargs)
        summary["scraped"] = len(matches)
        done_ids = await db.get_ingested_pro_match_ids()

        for match in matches:
            if not match.get("demo_url"):
                summary["skipped"] += 1
                continue

            summary["attempted"] += 1
            archive: Path | None = None
            try:
                archive = await download_archive(match, config.DEMOS_PRO_DIR)
                dems = extract_all_dems(archive, config.DEMOS_PRO_DIR / "decompressed")

                for map_number, dem in enumerate(dems, start=1):
                    map_tag = dem.stem.rsplit("_", 1)[-1]
                    if map_tag not in KNOWN_MAPS:
                        dem.unlink(missing_ok=True)
                        continue
                    if dem.stem in done_ids:
                        summary["skipped"] += 1
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
                        done_ids.add(dem.stem)
                        summary["maps_ingested"] += 1
                        dem.unlink(missing_ok=True)
                        log.info("ingested: %s", dem.name)
                    except Exception as exc:
                        summary["errors"].append({"dem": dem.name, "error": str(exc)})
                        log.error("INGEST ERROR %s: %s", dem.name, exc)
                        traceback.print_exc()

                summary["succeeded"] += 1

            except Exception as exc:
                summary["errors"].append({"match_id": match.get("match_id"), "error": str(exc)})
                log.error("MATCH ERROR %s: %s", match.get("match_id"), exc)
                traceback.print_exc()
            finally:
                if archive is not None:
                    archive.unlink(missing_ok=True)

    except Exception as exc:
        status = "error"
        error_message = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()

    await db.finish_job_run(
        run_id,
        status=status,
        items_processed=summary["maps_ingested"],
        error_message=error_message,
        stats={key: value for key, value in summary.items() if key != "errors"},
    )
    log.info(
        "done: scraped=%d skipped=%d maps_ingested=%d errors=%d",
        summary["scraped"], summary["skipped"], summary["maps_ingested"], len(summary["errors"]),
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--results-url", default=None)
    args = parser.parse_args()
    asyncio.run(refresh_pro_corpus(limit=args.limit, results_url=args.results_url))


if __name__ == "__main__":
    main()
