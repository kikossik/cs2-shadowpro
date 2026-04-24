"""Background worker: demo processing queue + periodic user sync.

Single async event loop. Blocking work (process_demo, sync_user) runs in the
default ThreadPoolExecutor via run_in_executor so the loop stays responsive.

  Demo jobs  — drains demo_jobs every DEMO_POLL_INTERVAL_SECONDS (default 5s).
               Claims one pending job at a time with SELECT FOR UPDATE SKIP LOCKED.

  User sync  — runs every SYNC_INTERVAL_SECONDS (default 300s).
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from backend import config, db
from backend.log import get_logger
from backend.sync import sync_user

log = get_logger("WORKER")

DEMO_POLL_INTERVAL: int = int(os.getenv("DEMO_POLL_INTERVAL_SECONDS", "5"))


# ── Demo job processing ────────────────────────────────────────────────────────

def _process_one_demo(job: dict) -> None:
    """Full demo pipeline for one claimed job. Runs in a thread."""
    from backend.processing import process_demo

    job_id     = job["job_id"]
    demo_path  = Path(config.resolve_managed_path(job["demo_path"]) or job["demo_path"])
    demo_id    = job["demo_id"]
    steam_id   = job["steam_id"]
    match_type = job["match_type"]

    log.info("processing demo job %s (%s)", job_id, demo_id)
    try:
        result = process_demo(demo_path, steam_id, demo_id, match_type=match_type)
        asyncio.run(db.finish_demo_job(job_id, result=result))
        log.info("demo job %s done: map=%s", job_id, result.get("map"))
    except Exception as exc:
        log.error("demo job %s failed: %s", job_id, exc, exc_info=True)
        asyncio.run(db.finish_demo_job(job_id, error=str(exc)))


async def _claim_and_process() -> bool:
    """Claim and process one pending demo job. Returns True if a job was found."""
    job = await db.claim_demo_job()
    if job is None:
        return False
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _process_one_demo, job)
    return True


# ── User sync ─────────────────────────────────────────────────────────────────

async def _get_syncable_users() -> list[str]:
    pool = await db.get_pool()
    rows = await pool.fetch(
        "SELECT steam_id FROM users "
        "WHERE match_auth_code IS NOT NULL AND last_share_code IS NOT NULL"
    )
    return [str(row["steam_id"]) for row in rows]


async def _run_sync() -> None:
    steam_ids = await _get_syncable_users()
    if not steam_ids:
        log.info("No syncable users")
        return

    loop = asyncio.get_event_loop()
    for steam_id in steam_ids:
        log.info("Syncing %s", steam_id)
        try:
            result = await loop.run_in_executor(None, sync_user, steam_id)
        except Exception as exc:
            log.warning("Sync failed for %s: %s", steam_id, exc)
            continue
        errors = result.get("errors", [])
        log.info(
            "Synced %s: %s new match(es), %s error(s)",
            steam_id, result.get("new_matches", 0), len(errors),
        )
        for error in errors:
            log.warning("%s: %s", error.get("share_code"), error.get("error"))


# ── Main loop ─────────────────────────────────────────────────────────────────

async def _run_loop() -> None:
    sync_interval = config.SYNC_INTERVAL_SECONDS
    last_sync = -sync_interval  # run sync on first iteration

    log.info(
        "Worker started — demo poll every %ss, sync every %ss",
        DEMO_POLL_INTERVAL, sync_interval,
    )

    while True:
        # Drain the demo job queue first.
        while await _claim_and_process():
            pass

        # Sync on its own cadence.
        if time.monotonic() - last_sync >= sync_interval:
            try:
                await _run_sync()
            except Exception:
                log.exception("Unexpected error in sync cycle")
            last_sync = time.monotonic()

        await asyncio.sleep(DEMO_POLL_INTERVAL)


def main() -> None:
    asyncio.run(_run_loop())


if __name__ == "__main__":
    main()
