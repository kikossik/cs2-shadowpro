"""Download a single HLTV demo archive (.dem.bz2 — actually a RAR multi-map bundle).

Uses Playwright's download API because HLTV download links are Cloudflare-protected.
"""
from __future__ import annotations

import asyncio
import os
import shutil
import time
from pathlib import Path

from playwright.async_api import async_playwright
from playwright_stealth import Stealth

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_stealth = Stealth()


def _headless() -> bool:
    return os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"


def _browser_launch_kwargs() -> dict:
    return {
        "headless": _headless(),
        "args": [
            "--disable-dev-shm-usage",
            "--disable-setuid-sandbox",
            "--no-sandbox",
        ],
    }


def archive_path(match: dict, dest_dir: Path) -> Path:
    return dest_dir / f"{match['match_id']}_{match['slug']}.dem.bz2"


async def _do_download(page, demo_url: str, dest: Path) -> None:
    dest_part = dest.with_name(dest.name + ".part")
    dest_part.unlink(missing_ok=True)

    async with page.expect_download(timeout=60_000) as dl_info:
        try:
            await page.goto(demo_url, wait_until="commit", timeout=30_000)
        except Exception:
            pass  # "Download is starting" is the normal signal
    download = await dl_info.value

    start = time.monotonic()
    tmp = await download.path()
    if tmp is None:
        raise RuntimeError("download failed — path() returned None")

    shutil.copy2(tmp, dest_part)
    await download.delete()
    dest_part.rename(dest)
    elapsed = int(time.monotonic() - start)
    size_mb = dest.stat().st_size / 1_048_576
    print(f"[download] → {dest.name} ({size_mb:.1f} MB, {elapsed}s)")


async def _run(match: dict, dest_dir: Path, retries: int) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = archive_path(match, dest_dir)
    if dest.exists():
        print(f"[download] SKIP (exists): {dest.name}")
        return dest

    print(
        f"[download] starting {match['match_id']} -> {dest.name}",
        flush=True,
    )

    pw = await async_playwright().start()

    async def new_browser_page():
        browser = await pw.chromium.launch(**_browser_launch_kwargs())
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
    last_err: Exception | None = None
    try:
        for attempt in range(1, retries + 1):
            try:
                await _do_download(page, match["demo_url"], dest)
                return dest
            except Exception as e:
                last_err = e
                print(f"[download] ERROR (attempt {attempt}/{retries}): {e}")
                if attempt < retries:
                    try:
                        await browser.close()
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    browser, page = await new_browser_page()
    finally:
        try:
            await browser.close()
        except Exception:
            pass
        await pw.stop()

    raise RuntimeError(f"download failed after {retries} attempts: {last_err}")


async def download_archive(match: dict, dest_dir: Path, retries: int = 3) -> Path:
    """Download one match archive to dest_dir. Returns Path to the .dem.bz2 file."""
    if not match.get("demo_url"):
        raise ValueError(f"match {match.get('match_id')} has no demo_url")
    return await _run(match, dest_dir, retries)
