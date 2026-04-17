# Plan: End-to-end roadmap for CS2 ShadowPro MVP

## Context

Ingest + exploratory parse are done: `scrape_hltv_demos.py` (50 Mirage matches), `download_demos.py` (47 `.dem.bz2`), `decompress_demos.py` (extracts Mirage map from the RAR), `parse_one_demo.py` (one demo → 12 Parquet tables via `awpy`), and `explore_demo_data.ipynb` (cell-by-cell tour of every DataFrame).

That notebook answered the question "what can I build situations from?" — 6 of the 7 situation dimensions (map area, side, NvN, phase, utility, time remaining) are either present or directly derivable from the default awpy output. Only **economy tier** and **active equipment** are missing from `ticks`; both require reparsing with explicit `player_props`.

The remaining path to MVP is: extend the parse → extract and index situations → match user situations against pro situations → render side-by-side 2D radar. This plan supersedes the prior tactical "parse demos" plan, which is now complete.

## Locked-in decisions (from discussion)

- **Playback:** 2D radar side-by-side. Video deferred to v2/premium.
- **Parser:** `awpy` v2 (polars DataFrames).
- **Pro demo corpus:** build the pipeline on current ~47 demos; re-run scrape/download/decompress/index to reach 200 just before launch.
- **Store:** SQLite for MVP (single file, zero-ops). Migrate to Postgres only if scale demands it.
- **Backend:** FastAPI.
- **Frontend:** React + TypeScript. Radar as `<canvas>`.
- **Maps:** Mirage only for MVP (unchanged).
- **Auth:** Steam OpenID (not FACEIT — FACEIT dropped due to integration complexity). User demo ingestion pulls demos linked to the authenticated Steam ID.

## Pipeline at a glance

```
[scrape] → [download] → [decompress] → [parse (reparse w/ more props)]
  → [extract situations] → situations.db
                                 ↑
Steam login → pull user matches → download → decompress → parse → extract ┘
                                                                        ↓
                                        [match user situation → top-K pro situations]
                                                                        ↓
                                                  [render side-by-side 2D radar clips]
                                                                        ↓
                                                 [matches landing → per-round report]
```

## Situation schema (concrete)

One row per (player, sampled tick). Stored in `situations.db` (SQLite).

| column                | type    | source                                                      |
|-----------------------|---------|-------------------------------------------------------------|
| `id`                  | INTEGER | autoincrement                                               |
| `source`              | TEXT    | `'pro'` or `'user'`                                         |
| `demo_id`             | TEXT    | basename of `.dem`                                          |
| `round_num`           | INT     | `rounds.round_num`                                          |
| `tick`                | INT     | sampled tick                                                |
| `player_steamid`      | INT     | `ticks.steamid`                                             |
| `player_side`         | TEXT    | `ticks.side` (ct/t)                                         |
| `player_place`        | TEXT    | `ticks.place` — **map-area bucket**                         |
| `player_x/y/z`        | REAL    | `ticks.X/Y/Z`                                               |
| `economy_bucket`      | TEXT    | `eco` / `semi` / `full` — from `ticks.balance` + held gun   |
| `active_weapon`       | TEXT    | `ticks.active_weapon` (after reparse)                       |
| `alive_ct`            | INT     | derived: `count(ticks.health>0 AND side='ct') at tick`      |
| `alive_t`             | INT     | derived: same, T side                                       |
| `phase`               | TEXT    | `freeze` / `pre_plant` / `post_plant`                       |
| `time_remaining_s`    | REAL    | derived from `rounds.freeze_end`, `rounds.end`, `bomb_plant`|
| `smokes_active`       | INT     | count of `smokes` rows where `start_tick ≤ t ≤ end_tick`    |
| `mollies_active`      | INT     | same for `infernos`                                         |
| `clip_start_tick`     | INT     | tick − 3s (for radar playback window)                       |
| `clip_end_tick`       | INT     | tick + 12s                                                  |

Index on `(source, player_place, player_side, alive_ct, alive_t, phase, economy_bucket)` for the matching query.

## Sampling strategy (decision moments)

Don't snapshot every tick. Sample per-player at:

1. **`freeze_end` tick** — opening positions
2. **Every 5 seconds** after freeze_end
3. **Every kill event in this round** (at the kill tick)
4. **Bomb plant tick, bomb defuse tick**

De-duplicate situations within ≤1s of each other per player. Expected yield: ~15–25 situations per player per round → ~3–5k per demo → ~150k–1M across the pro corpus. Well within SQLite's comfort zone.

## Matching algorithm

Per user situation `u`, run:

```sql
SELECT * FROM situations
WHERE source = 'pro'
  AND player_place = :u_place
  AND player_side  = :u_side
  AND alive_ct     = :u_alive_ct
  AND alive_t      = :u_alive_t
  AND phase        = :u_phase
  AND economy_bucket = :u_economy
  AND abs(time_remaining_s - :u_time) < 20
ORDER BY
  ((player_x - :u_x) * (player_x - :u_x)
 + (player_y - :u_y) * (player_y - :u_y)) ASC
LIMIT 5;
```

Then score each candidate: `0.4 * strict_feature_match + 0.3 * spatial_proximity + 0.2 * utility_overlap + 0.1 * time_closeness`. Threshold for "good enough to show" is tuned empirically on ~50 eyeballed cases.

Present top 3 per user situation with a **why-it-matched breakdown** ("Same map area, same 2v2, same full-buy, NiKo was 320 units away").

## Side-by-side 2D radar viewer

- React + `<canvas>`. Mirage radar PNG as background (public Valve asset via `awpy` or community).
- Left pane: user's situation. Right pane: matched pro situation. Synced tick scrubber across both.
- Draw per tick in the `[clip_start_tick, clip_end_tick]` window:
  - Focal player as a filled circle with side color + name label
  - Other alive players as smaller circles
  - Active smokes as gray disks, molotovs as orange patches
  - Kill events as X markers at the position, fade in over the following ticks
  - Situation feature strip at the bottom: `2v2 | Full-buy | Post-plant | 0:42 remaining`
- `awpy.plot` has a radar overlay helper that can be used as a reference / fallback for server-side pre-rendered thumbnails.

## Stack summary

| Layer          | Choice                                   | Why                                           |
|----------------|------------------------------------------|-----------------------------------------------|
| Language       | Python 3.13                              | locked; venv at `/home/tomyan/Code/VENV/cs2_shadowpro` |
| Parser         | `awpy>=2.0.0` (already installed)        | polars DataFrames, CS2-native                 |
| DB             | SQLite (`situations.db`)                 | zero-ops, plenty fast at this scale           |
| Backend        | FastAPI                                  | async, small, native to Python stack          |
| Frontend       | React + TS + HTML canvas                 | meets "clean modern UI" MVP goal              |
| Auth           | Steam OpenID (already implemented)       | simpler than FACEIT; user owns their Steam ID |
| Deploy target  | TBD (Fly.io / Railway / Hetzner)         | picked later; compute is mostly one-off index |

## Milestones

**M1 — reparse with richer player_props** *(small)*
- Update `parse_one_demo.py` → `Demo(path, player_props=["balance","active_weapon","armor","has_defuser","flash_duration"])`.
- Verify `ticks` now has those columns on a single demo.
- Inspect in the notebook to confirm economy_bucket can be classified.

**M2 — situation extractor (one demo)** *(core)*
- New `extract_situations.py`: given a parsed demo, produce a polars DataFrame matching the schema above.
- Implement the sampling strategy + all derived features (`alive_ct`, `phase`, `time_remaining_s`, `utility_active_*`).
- Dump to `situations_sample.parquet`. Spot-check a round by hand against the notebook.
- **Gotcha from M1:** `ticks.active_weapon` is a UInt32 weapon-definition ID (e.g. `1851612`), not a string name. For economy bucketing, use `ticks.inventory` instead — it's a `list[str]` of human-readable weapon names (`["AK-47", "High Explosive Grenade"]`). Classify by set membership: rifles/AWP → `full`; SMGs/shotguns → `semi`; pistols-only → `eco`. A weapon-index → category map for `active_weapon` can wait until we need "what is the player *actively holding* right now".

**M3 — batch extractor + SQLite index** *(scale the M2 work)*
- New `batch_extract.py`: iterate over `demos_decompressed/*.dem`, parse, extract, bulk-insert into `situations.db`.
- Add indexes. Confirm query latency on a typical categorical filter.

**M4 — matching CLI + quality eyeball** *(the core value-prop test)*
- New `match_situation.py`: takes a situation JSON (or a `(demo, round, tick, steamid)` selector), runs the SQL, prints top-5 pros with scores + trajectories on radar as PNG.
- Eyeball ~20 queries. Tune the scoring weights + thresholds.
- **This is the point where we know whether the product actually works.** Do not skip.

**M4.5 — Situation Viewer + Matches landing (design implementation)** *(inserted after M4)* ✓ DONE
- Vite + React + TS app under `web/`. Viewer (`web/src/Viewer.tsx`) and Matches landing (`web/src/matches/`) both implemented.
- Steam OpenID login (`web/src/LoginPage.tsx`) integrated; routing: Login → Matches → Viewer.
- Static mock data; backend wiring is M6's job. `DESIGN.md` covers the data contract.

**M5 — Steam demo ingestion** *(new code, external integration)*
- Resolve demo download URLs from a user's Steam ID (Steam Web API or CS2 match history endpoint).
- Pull last 10 matches; queue demo downloads.
- Run user demos through `decompress → parse → extract` into the same SQLite under `source='user'`.
- Note: Steam may not expose demo URLs directly — confirm access method before starting; fallback is manual upload.

**M6 — backend + viewer wiring** *(UI integration)*
- FastAPI backend exposing `GET /matches/{steam_id}`, `GET /report/{match_id}`, and `GET /situation/{id}` returning the JSON shapes the frontend consumes (see `DESIGN.md` § Data contract).
- Replace `web/src/mockData.ts` and `web/src/matches/mockMatches.ts` with API fetches.
- Wire Steam ID from session to the matches endpoint.

**M7 — scale to 200 + launch polish**
- Re-run scrape to 200 matches; batch-download + re-index.
- Landing page, billing (Stripe $5/mo), deploy.

## Critical files to create / created

- `reparse_demo.py` *(or extend `parse_one_demo.py`)* — M1
- `extract_situations.py` — M2 ✓
- `situations_db.py` (schema + connection helpers) — M2/M3 ✓
- `batch_extract.py` — M3 ✓
- `match_situation.py` — M4 ✓
- `steam_client.py` — M5 (replaces `faceit_client.py`)
- `server/` (FastAPI app) — M6
- `web/` (React app) — M4.5 ✓

## Existing utilities to reuse

- `scrape_hltv_demos.py`, `download_demos.py`, `decompress_demos.py` — unchanged; re-run with `TARGET=200` at M7.
- `parse_one_demo.py` — the DataFrame-discovery loop will be reused in `batch_extract.py`.
- `awpy.plot` — candidate for radar background/pre-rendered thumbnails.

## Verification (per milestone)

| Milestone | How we know it's done                                                                    |
|-----------|------------------------------------------------------------------------------------------|
| M1        | One reparsed demo's `ticks` has `balance`, `active_weapon`. Spot-check in notebook.      |
| M2        | `situations_sample.parquet` — manually trace 3 rows back to the notebook; features match.|
| M3        | `situations.db` row count ≈ expected; indexed categorical query returns in < 50 ms.      |
| M4        | 15+ out of 20 eyeball queries produce matches a human would accept as "same situation".  |
| M5        | End-to-end on your own Steam demo: user rows appear in DB with `source='user'`.          |
| M6        | Load a report in the browser; matches list populated from API; scrub the radar.           |
| M7        | Live URL, 200 pro demos indexed, Steam login works, one paid test transaction.            |

## Risks / open questions (to revisit, not blockers)

- **`place` granularity** — Mirage's `place` values like `"Mid"` may be too coarse for good matches. Might need to sub-bucket large places by XY quantiles. Defer until M4 eyeballing.
- **Economy bucket boundaries** — common split: full-buy (≥$4000 + rifle), semi-buy ($2000–$4000), eco (<$2000 or pistol/smg only). Confirm after M1 when `balance` is parsed.
- **Steam demo access** — CS2 match history may not expose demo download URLs via a public API. Investigate: Steam Web API `GetMatchHistory`, CSGO's `GetRecentMatchStats`, or scraping matchroom URLs. Manual upload is the fallback. **Check this before M5.**
- **Steam OpenID verification** — `openid.check_authentication` server-side call is currently deferred (noted in `main.tsx`). Must be implemented before M7 launch to prevent spoofed Steam IDs.
- **Radar asset licensing** — the Mirage radar PNG ships with CS2 assets. Using it in a paid product may or may not be allowed under Valve's content policy. Review before launch.
- **Compute at M3 scale** — parsing 200 demos sequentially is ~1–2 hrs. Fine as a one-off, but if we iterate on schema, consider caching the parsed Parquets per demo.
