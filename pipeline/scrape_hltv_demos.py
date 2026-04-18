#!/usr/bin/env python3.13
"""
Extract top-100 most recent Mirage map demo download links from HLTV.

Usage:
    python scrape_hltv_demos.py

Output:
    mirage_demos.json — match metadata + demo download URLs
"""

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

RESULTS_URL = "https://www.hltv.org/results?map=de_mirage"
OUTPUT = Path("mirage_demos.json")
TARGET = 50
DELAY = 2.0  # seconds between match page requests

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_stealth = Stealth()


async def get_match_urls(page) -> list[str]:
    print(f"Loading: {RESULTS_URL}")
    await page.goto(RESULTS_URL, wait_until="domcontentloaded", timeout=30_000)
    await page.wait_for_timeout(2_000)

    all_hrefs: list[str] = await page.evaluate(
        "() => [...document.querySelectorAll('a[href]')].map(a => a.href)"
    )

    seen, urls = set(), []
    for href in all_hrefs:
        if href not in seen and re.fullmatch(
            r"https://www\.hltv\.org/matches/\d+/[^#?]+", href
        ):
            seen.add(href)
            urls.append(href)

    return urls[:TARGET]


async def get_demo_url(page, match_url: str) -> tuple[str | None, str | None]:
    """Return (demo_url, error). Prefers Mirage-labeled demo when multiple maps exist."""
    try:
        await page.goto(match_url, wait_until="domcontentloaded", timeout=20_000)
        await page.wait_for_timeout(2_000)

        links: list[dict] = await page.evaluate("""
            () => {
                const results = [];
                for (const a of document.querySelectorAll('a[href*="/download/demo/"]')) {
                    // Walk up the DOM tree to find the nearest map-name context
                    let ctx = '';
                    let el = a;
                    for (let i = 0; i < 8; i++) {
                        el = el.parentElement;
                        if (!el) break;
                        const t = el.innerText || '';
                        if (/mirage|dust2|nuke|inferno|ancient|vertigo|anubis|overpass/i.test(t)) {
                            ctx = t.trim().slice(0, 300);
                            break;
                        }
                    }
                    results.push({ href: a.href, ctx });
                }
                return results;
            }
        """)

        if not links:
            return None, "no demo links found"

        # Prefer the link whose context mentions Mirage; fall back to the first
        demo = next((l for l in links if "mirage" in l["ctx"].lower()), links[0])
        return demo["href"], None

    except PlaywrightTimeout:
        return None, "timeout"
    except Exception as e:
        return None, str(e)


async def main() -> None:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        ctx = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await ctx.new_page()
        await _stealth.apply_stealth_async(page)

        match_urls = await get_match_urls(page)
        print(f"Collected {len(match_urls)} Mirage match URLs\n")

        matches = []
        for i, url in enumerate(match_urls, 1):
            m = re.search(r"/matches/(\d+)/(.+)", url)
            match_id = m.group(1) if m else "?"
            slug = m.group(2) if m else url

            print(f"[{i:3}/{len(match_urls)}] {slug}", end=" ... ", flush=True)
            demo_url, error = await get_demo_url(page, url)
            print(demo_url or f"SKIP ({error})")

            matches.append({
                "rank": i,
                "match_id": match_id,
                "match_url": url,
                "slug": slug,
                "demo_url": demo_url,
                "error": error,
            })

            await asyncio.sleep(DELAY)

        await browser.close()

    demos_found = sum(1 for m in matches if m["demo_url"])
    out = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "target_map": "Mirage",
        "total_matches": len(matches),
        "demos_found": demos_found,
        "matches": matches,
    }
    OUTPUT.write_text(json.dumps(out, indent=2))
    print(f"\nDone: {demos_found}/{len(matches)} demos found → {OUTPUT}")


asyncio.run(main())
