"""Periodic sync worker — polls Steam for new matches for all registered users.

Run:
    python -m backend.worker
"""
from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from backend import config
from backend.sync import sync_user

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


async def _get_syncable_users() -> list[str]:
    conn = await asyncpg.connect(dsn=config.DATABASE_URL)
    try:
        rows = await conn.fetch(
            "SELECT steam_id FROM users "
            "WHERE match_auth_code IS NOT NULL AND last_share_code IS NOT NULL"
        )
        return [r["steam_id"] for r in rows]
    finally:
        await conn.close()


def _run_once() -> None:
    steam_ids = asyncio.run(_get_syncable_users())
    if not steam_ids:
        log.info("No syncable users")
        return
    for steam_id in steam_ids:
        log.info("Syncing %s", steam_id)
        result = sync_user(steam_id)
        if result.get("error"):
            log.warning("Sync failed for %s: %s", steam_id, result["error"])
            continue
        new  = result.get("new_matches", 0)
        errs = result.get("errors", [])
        log.info("Synced %s — %d new match(es), %d error(s)", steam_id, new, len(errs))
        for e in errs:
            log.warning("  %s: %s", e["share_code"], e["error"])


def main() -> None:
    interval = config.SYNC_INTERVAL_SECONDS
    log.info("Worker started — syncing every %ds", interval)
    while True:
        try:
            _run_once()
        except Exception:
            log.exception("Unexpected error in sync cycle")
        time.sleep(interval)


if __name__ == "__main__":
    main()
