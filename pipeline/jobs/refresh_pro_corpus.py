"""Daily pro-corpus refresh job.

Chain: scrape HLTV → filter already-ingested → download archive →
       extract per-map .dem files → ingest each → delete files.

Idempotency: a match whose match_id (dem stem) is already in pro_matches is skipped.
Cleanup: archive deleted after all maps processed; individual .dem deleted after ingest.
On per-dem failure the .dem is kept for debugging; job continues with other maps.
"""
from __future__ import annotations

import asyncio
import traceback
from pathlib import Path

from backend import config, db
from pipeline.steps.decompress import KNOWN_MAPS, extract_all_dems
from pipeline.steps.download import download_archive
from pipeline.steps.ingest import ingest_pro_demo
from pipeline.steps.scrape import scrape_pro_matches

JOB_NAME = "refresh_pro_corpus"


async def refresh_pro_corpus(limit: int = 50, results_url: str | None = None) -> dict:
    """Entry point for the scheduled daily job. Returns summary stats."""
    run_id = await db.start_job_run(JOB_NAME)

    summary: dict = {
        "scraped":       0,
        "skipped":       0,
        "attempted":     0,
        "succeeded":     0,
        "maps_ingested": 0,
        "errors":        [],
    }
    status        = "done"
    error_message = None

    scrape_kwargs = {"limit": limit}
    if results_url:
        scrape_kwargs["results_url"] = results_url

    try:
        matches = await scrape_pro_matches(**scrape_kwargs)
        summary["scraped"] = len(matches)

        done_ids = await db.get_ingested_pro_match_ids()

        for match in matches:
            if not match.get("demo_url"):
                continue

            # Idempotency: skip if HLTV match_id already has any ingested maps.
            # Each extracted dem gets match_id = "<hltv_id>_<slug>_<map_tag>" so
            # checking whether any done_id starts with "<hltv_id>_" is sufficient.
            prefix = f"{match['match_id']}_"
            if any(d.startswith(prefix) for d in done_ids):
                summary["skipped"] += 1
                continue

            summary["attempted"] += 1
            archive: Path | None = None
            try:
                archive = await download_archive(match, config.DEMOS_PRO_DIR)
                decomp_dir = config.DEMOS_PRO_DIR / "decompressed"
                dems = extract_all_dems(archive, decomp_dir)

                for dem in dems:
                    map_tag = dem.stem.rsplit("_", 1)[-1]
                    if map_tag not in KNOWN_MAPS:
                        dem.unlink(missing_ok=True)
                        continue

                    try:
                        await ingest_pro_demo(
                            dem,
                            dem.stem,
                            hltv_url   = match["match_url"],
                            event_name = match.get("event_name"),
                            team_ct    = match.get("team1"),
                            team_t     = match.get("team2"),
                            match_date = match.get("match_date"),
                        )
                        done_ids.add(dem.stem)
                        summary["maps_ingested"] += 1
                        dem.unlink(missing_ok=True)
                        print(f"[refresh] ingested+cleaned: {dem.name}")
                    except Exception as e:
                        summary["errors"].append({"dem": dem.name, "error": str(e)})
                        print(f"[refresh] INGEST ERROR {dem.name}: {e}")
                        traceback.print_exc()

                if archive:
                    archive.unlink(missing_ok=True)
                    print(f"[refresh] cleaned archive: {archive.name}")

                summary["succeeded"] += 1

            except Exception as e:
                summary["errors"].append({"match_id": match["match_id"], "error": str(e)})
                print(f"[refresh] MATCH ERROR {match['match_id']}: {e}")
                traceback.print_exc()
                if archive:
                    archive.unlink(missing_ok=True)

    except Exception as e:
        status        = "error"
        error_message = f"{type(e).__name__}: {e}"
        traceback.print_exc()

    loggable = {k: v for k, v in summary.items() if k != "errors"}
    await db.finish_job_run(
        run_id,
        status          = status,
        items_processed = summary["maps_ingested"],
        error_message   = error_message,
        stats           = loggable,
    )
    print(f"[refresh] done: {loggable}")
    return summary


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--results-url", default=None)
    args = p.parse_args()
    asyncio.run(refresh_pro_corpus(limit=args.limit, results_url=args.results_url))
