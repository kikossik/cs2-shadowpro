"""Scheduled pro-corpus scraper process.

Run:
    python -m backend.pro_scraper
"""
from __future__ import annotations

import asyncio
import os
import time

from backend.log import get_logger
from pipeline.jobs.refresh_pro_corpus import refresh_pro_corpus

log = get_logger("SCRAPER")


def _interval_seconds() -> int:
    return int(os.getenv("PRO_SCRAPER_INTERVAL_SECONDS", "86400"))


def _limit() -> int:
    return int(os.getenv("PRO_SCRAPER_LIMIT", "2"))


def _run_once() -> None:
    summary = asyncio.run(
        refresh_pro_corpus(
            limit=_limit(),
            results_url=os.getenv("PRO_SCRAPER_RESULTS_URL") or None,
        )
    )
    log.info(
        "Cycle done: scraped=%s skipped=%s attempted=%s maps_ingested=%s errors=%s",
        summary.get("scraped", 0),
        summary.get("skipped", 0),
        summary.get("attempted", 0),
        summary.get("maps_ingested", 0),
        len(summary.get("errors", [])),
    )
    for error in summary.get("errors", []):
        log.warning("error: %s", error)


def main() -> None:
    interval = _interval_seconds()
    log.info("Pro scraper started; running every %ss", interval)
    while True:
        try:
            _run_once()
        except Exception:
            log.exception("Unexpected error in pro scraper cycle")
        if os.getenv("PRO_SCRAPER_RUN_ONCE") == "1":
            return
        time.sleep(interval)


if __name__ == "__main__":
    main()
