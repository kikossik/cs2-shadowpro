"""Daily pro match scraper worker.

Runs refresh_pro_corpus() once per PRO_SCRAPER_INTERVAL_SECONDS (default 24h).

Run:
    python -m backend.pro_scraper
"""
from __future__ import annotations

import asyncio
import logging
import os
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pro-scraper] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True,
)
log = logging.getLogger(__name__)

_INTERVAL = int(os.getenv("PRO_SCRAPER_INTERVAL_SECONDS", "86400"))


def main() -> None:
    log.info("Booting pro scraper process")
    try:
        from pipeline.jobs.refresh_pro_corpus import refresh_pro_corpus
    except Exception:
        log.exception("Failed to import refresh_pro_corpus")
        raise

    log.info("Pro scraper started — running every %ds", _INTERVAL)
    while True:
        try:
            log.info("Starting scrape cycle")
            summary = asyncio.run(refresh_pro_corpus(limit=2))
            log.info(
                "Cycle done — scraped=%d skipped=%d ingested=%d errors=%d",
                summary["scraped"],
                summary["skipped"],
                summary["maps_ingested"],
                len(summary["errors"]),
            )
            for err in summary["errors"]:
                log.warning("  error: %s", err)
        except Exception:
            log.exception("Unexpected error in pro scraper cycle")
        time.sleep(_INTERVAL)


if __name__ == "__main__":
    main()
