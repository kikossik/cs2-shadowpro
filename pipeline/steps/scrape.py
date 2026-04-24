"""Scrape recent HLTV match pages for demo download URLs and match metadata.

Returns a list of match dicts:
    {match_id, match_url, slug, demo_url, team1, team2, event_name, match_date, error}

match_date is an ISO date string (YYYY-MM-DD) or None if extraction fails.
All metadata fields are best-effort; pipeline continues with None values if HLTV
HTML changes.
"""
from __future__ import annotations

import asyncio
import os
import re

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import Stealth

from backend.log import get_logger

log = get_logger("SCRAPE")

RESULTS_URL = "https://www.hltv.org/results"

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


async def _wait_past_cloudflare(page, timeout_ms: int = 30_000) -> None:
    """Poll until page title is no longer Cloudflare's challenge page."""
    try:
        await page.wait_for_function(
            "() => !document.title.toLowerCase().includes('just a moment')",
            timeout=timeout_ms,
        )
    except PlaywrightTimeout:
        pass  # caller will see the failure via missing content


async def _collect_match_urls(page, limit: int, results_url: str) -> list[str]:
    await page.goto(results_url, wait_until="domcontentloaded", timeout=30_000)
    await _wait_past_cloudflare(page)
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

    return urls[:limit]


async def _get_match_info(page, match_url: str) -> dict:
    """Visit a match page and extract demo URL + metadata. All fields best-effort."""
    result: dict = {
        "demo_url":   None,
        "team1":      None,
        "team2":      None,
        "event_name": None,
        "match_date": None,
        "error":      None,
    }
    try:
        await page.goto(match_url, wait_until="domcontentloaded", timeout=20_000)
        await _wait_past_cloudflare(page)
        await page.wait_for_timeout(2_000)

        info = await page.evaluate("""() => {
            const demoLink = document.querySelector('a[href*="/download/demo/"]');
            const teams = [...document.querySelectorAll('.teamName')];
            const event = document.querySelector('.event a, .matchInfoEmpty .event');
            const dateEl = document.querySelector('[data-unix]');
            // Debug: collect all hrefs containing 'demo' to diagnose selector misses
            const demoHrefs = [...document.querySelectorAll('a[href]')]
                .map(a => a.href)
                .filter(h => h.toLowerCase().includes('demo'));
            return {
                demo_url:    demoLink ? demoLink.href : null,
                team1:       teams[0] ? teams[0].textContent.trim() : null,
                team2:       teams[1] ? teams[1].textContent.trim() : null,
                event_name:  event    ? event.textContent.trim()    : null,
                unix_ms:     dateEl   ? dateEl.getAttribute('data-unix') : null,
                debug_hrefs: demoHrefs.slice(0, 10),
            };
        }""")

        result["demo_url"]   = info.get("demo_url")
        result["team1"]      = info.get("team1")
        result["team2"]      = info.get("team2")
        result["event_name"] = info.get("event_name")

        unix_ms = info.get("unix_ms")
        if unix_ms:
            from datetime import datetime, timezone
            ts = int(unix_ms) / 1000
            result["match_date"] = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()

        if not result["demo_url"]:
            result["error"] = "no demo links found"
            debug = info.get("debug_hrefs", [])
            title = await page.title()
            log.debug("page title: %r", title)
            log.debug("demo-related hrefs: %s", debug)

    except PlaywrightTimeout:
        result["error"] = "timeout"
    except Exception as e:
        result["error"] = str(e)

    return result


async def _run(limit: int, delay: float, results_url: str) -> list[dict]:
    async with async_playwright() as pw:
        log.info("launching chromium headless=%s limit=%s url=%s", _headless(), limit, results_url)
        browser = await pw.chromium.launch(**_browser_launch_kwargs())
        ctx = await browser.new_context(
            user_agent=UA,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await ctx.new_page()
        await _stealth.apply_stealth_async(page)

        match_urls = await _collect_match_urls(page, limit, results_url)
        log.info("collected %d match URLs", len(match_urls))

        matches = []
        for i, url in enumerate(match_urls, 1):
            m = re.search(r"/matches/(\d+)/(.+)", url)
            match_id = m.group(1) if m else "?"
            slug     = m.group(2) if m else url

            info = await _get_match_info(page, url)
            status = info["demo_url"] or f"SKIP ({info['error']})"
            log.info("[%3d/%d] %s -> %s", i, len(match_urls), slug, status)

            matches.append({
                "match_id":   match_id,
                "match_url":  url,
                "slug":       slug,
                "demo_url":   info["demo_url"],
                "team1":      info["team1"],
                "team2":      info["team2"],
                "event_name": info["event_name"],
                "match_date": info["match_date"],
                "error":      info["error"],
            })
            await asyncio.sleep(delay)

        await browser.close()
        return matches


async def scrape_pro_matches(
    limit: int = 50,
    delay: float = 2.0,
    results_url: str = RESULTS_URL,
) -> list[dict]:
    """Scrape top-N recent HLTV match pages. Returns list of match metadata dicts."""
    return await _run(limit, delay, results_url)
