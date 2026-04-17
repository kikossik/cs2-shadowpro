#!/usr/bin/env python3.13
"""
Download HLTV demo files listed in mirage_demos.json.

Ctrl+C is handled gracefully: the current download finishes before the
program exits. Already-downloaded files are skipped (resume-safe).

Usage:
    python download_demos.py

Output:
    demos/<match_id>_<slug>.dem.bz2
"""

import asyncio
import json
import shutil
import signal
import time
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

INPUT = Path("mirage_demos.json")
OUTPUT_DIR = Path("demos")

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_stealth = Stealth()


async def _ticker(start: float) -> None:
    """Print elapsed time every 5 s so the terminal never looks frozen."""
    try:
        while True:
            await asyncio.sleep(5)
            print(f"\r  ... {int(time.monotonic() - start)}s", end="", flush=True)
    except asyncio.CancelledError:
        print()


async def download_demo(page, match: dict, dest: Path) -> None:
    dest_part = dest.with_name(dest.name + ".part")
    dest_part.unlink(missing_ok=True)

    # goto throws "Download is starting" on download URLs — that's expected.
    # expect_download still captures the download object regardless.
    # timeout=60_000: if the download event never fires (dead page), raise after 60s.
    async with page.expect_download(timeout=60_000) as dl_info:
        try:
            await page.goto(match["demo_url"], wait_until="commit", timeout=30_000)
        except Exception:
            pass  # "Download is starting" is the normal signal
    download = await dl_info.value
    print(f"  started: {download.suggested_filename}", flush=True)

    start = time.monotonic()
    ticker = asyncio.create_task(_ticker(start))
    try:
        # path() internally calls wait_for_finished() — blocks until complete
        tmp = await download.path()
    finally:
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)

    if tmp is None:
        raise RuntimeError("download failed — path() returned None")

    shutil.copy2(tmp, dest_part)
    await download.delete()  # free Playwright's temp copy immediately
    dest_part.rename(dest)
    elapsed = int(time.monotonic() - start)
    size_mb = dest.stat().st_size / 1_048_576
    print(f"  → {dest.name} ({size_mb:.1f} MB, {elapsed}s)")


async def main() -> None:
    data = json.loads(INPUT.read_text())
    matches = [m for m in data["matches"] if m["demo_url"]]
    OUTPUT_DIR.mkdir(exist_ok=True)

    stop = False

    def _request_stop() -> None:
        nonlocal stop
        stop = True
        print("\n[Ctrl+C] finishing current download then stopping…")

    pw = await async_playwright().start()

    async def new_browser_page():
        browser = await pw.chromium.launch(headless=False)
        ctx = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            accept_downloads=True,
        )
        page = await ctx.new_page()
        await _stealth.apply_stealth_async(page)
        return browser, page

    browser, page = await new_browser_page()
    asyncio.get_running_loop().add_signal_handler(signal.SIGINT, _request_stop)

    ok = 0
    for i, match in enumerate(matches, 1):
        if stop:
            print("Stopped.")
            break

        slug = match["slug"]
        mid = match["match_id"]
        dest = OUTPUT_DIR / f"{mid}_{slug}.dem.bz2"

        if dest.exists():
            print(f"[{i:2}/{len(matches)}] SKIP (exists): {dest.name}")
            ok += 1
            continue

        print(f"[{i:2}/{len(matches)}] {slug}", flush=True)
        for attempt in range(1, 4):
            try:
                await download_demo(page, match, dest)
                ok += 1
                break
            except Exception as e:
                print(f"  ERROR (attempt {attempt}/3): {e}")
                if attempt < 3:
                    print("  Restarting browser…")
                    try:
                        await browser.close()
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    browser, page = await new_browser_page()

    try:
        await browser.close()
    except Exception:
        pass
    await pw.stop()

    print(f"\nDone: {ok}/{len(matches)} demos saved to {OUTPUT_DIR}/")


asyncio.run(main())
