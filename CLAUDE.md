# CS2 ShadowPro — Project Context

```
CS2 Pro Situation Mirror — B2C SaaS, ~$5/month

Automatically finds professional CS2 player demos containing situations similar to your own gameplay
and shows a side-by-side comparison of what the pro did vs. what you did. No invented AI coaching —
real pro decision-making in comparable scenarios.

A "situation" is defined by: map area, side (CT/T), economy tier, player count (NvN), round phase
(pre/post-plant), utility, available time remaining (not limited to this, these are first feature
ideas). The system matches these against a pre-built index of parsed pro demos from HLTV/tier-1
matches (nothing is built yet).

User flow: Connect FACEIT (faceit developer api) → last 10 matches auto-imported → situations
extracted from each round → matched against pro database → select a good threshold and show the
after results with good accuracy → report shows: "In round 7, you held B apps in a 2v2 full-buy.
Here's what NiKo did in the same scenario." ...

Supporting features - these are some additional ideas i have, not sure how to fit it all together
and whether it makes sense to fit it all: session stats (ADR, KAST, K/D), pattern detection across
matches (death location clusters, side imbalance, utility drought), Claude-narrated findings.

Stack: Python3.13, the rest i'm open for ideas, as this will likely be computationally expensive

MVP scope: Mirage only, 200 latest HLTV/tier-1 matches indexed, built the app, clean modern UI
and good video rendering side by side (drop supporting features from MVP scope)
```

## Environment

- **Python 3.13** always
- **Venv**: `/home/tomyan/Code/VENV/cs2_shadowpro`
- Run scripts: `/home/tomyan/Code/VENV/cs2_shadowpro/bin/python <script>`
- Global coding rules: `Code/CLAUDE_RULES/CLAUDE.md`

## What Works (as of April 2026)

### `scrape_hltv_demos.py`
- Playwright + playwright-stealth (headless=False), loads `hltv.org/results?map=de_mirage`
- Collects 50 match URLs (TARGET=50), visits each match page, finds `a[href*="/download/demo/"]`
- For BO3/BO5: walks up DOM tree to prefer Mirage-associated demo link
- Outputs `mirage_demos.json` — match_id, match_url, slug, demo_url per match
- Uses `domcontentloaded` (not `networkidle` — HLTV ads keep connections open forever)

### `download_demos.py`
- Reads `mirage_demos.json`, downloads each `.dem.bz2` to `demos/`
- `page.goto` on a download URL throws `"Download is starting"` — caught, `expect_download` still fires
- `download.path()` (not `save_as()`) truly blocks until complete — calls `wait_for_finished()` internally
- `shutil.copy2` from Playwright temp dir → `.part` → atomic rename to final; then `download.delete()` immediately frees the temp copy (otherwise Playwright holds all temp files until browser close, exhausting RAM across 50 downloads)
- Ticker prints elapsed seconds every 5s so terminal never looks frozen
- SIGINT handler: Ctrl+C finishes current download then stops
- Resume-safe: skips already-downloaded files
- Browser crash recovery: canceled downloads can kill the entire browser process; script uses `async_playwright().start()` (not `async with`) so it can fully tear down and restart a fresh browser + context + page mid-loop, retrying up to 3 times per match

## Hard-Won Lessons

- HLTV uses Cloudflare — httpx/requests gets 403. Must stay in Playwright browser session.
- `wait_until="networkidle"` times out on HLTV — use `domcontentloaded`
- `download.save_as()` does NOT block until done — use `download.path()`
- `page.goto` on a download URL throws `"Download is starting"` — catch it, `expect_download` still works
- A canceled download can crash the entire browser process (not just the page/context) — must recreate from scratch
- Demo files are `.dem.bz2` (bzip2). Must decompress before parsing.
