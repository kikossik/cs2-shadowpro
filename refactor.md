 Port groundup mapping into cs2-shadowpro-clean (deliverable: refactor.md)                                                                       
                                                        
 Context

 The simplification phase is done (prior version of this plan). The placeholder
 mapper in backend/round_mapper.py returns the top-1 ANN candidate, which is
 not what you want. You've decided the real mapping algorithm should follow the
 exact implementation in cs2-shadowpro-groundup/cs2_awpy_lab.py — an
 in-memory team-window + nav-mesh matcher with no event_windows / pgvector / ANN
 corpus.

 Goal of this plan: produce refactor.md (a single file at the repo root)
 that a Codex agent can pick up and execute end-to-end. We do NOT write code
 ourselves in this plan; we write a precise instruction document.

 Decisions confirmed by user:
 - Single logic mode: always run "both" (original + nav). No ?logic= parameter.
 - Frontend is not exposed to a logic toggle — it renders the output of both.
 - Deliverable = refactor.md, not direct code edits.

 ---
 What groundup does (the target behavior)

 cs2-shadowpro-groundup/cs2_awpy_lab.py implements two independent matchers:

 1. select_best_pro_round_match ("original")
   - Per-demo team-window catalog: 6s windows, 2s stride, 8 sampled ticks
   - Filter user windows: round contains user player, freeze_end+20s cutoff, ≥2 alive each side
   - Per user window: retrieve top-20 pros by team-context distance (z-scored geometry + place/weapon/plant penalties)
   - Score every player assignment on retrieved pairs; aggregate by (pro_demo, pro_round, pro_player)
   - Score: mean_similarity × (1 + 0.15·(streak−1)) × (0.7 + 0.3·coverage)
 2. select_best_pro_route_match ("nav")
   - awpy.nav mesh per map
   - 1 Hz player route profiles → nav area IDs + support/enemy/bomb flags → route steps
   - Per pro player same-side: gate on start area sim (≥0.55), start context (≥0.34), bomb-state match
   - Compute shared route prefix; score 0.75·prefix_score + 0.25·coach_value

 Both run in memory at request time against parsed parquet caches. No ANN, no
 event_windows, no feature blobs, no clean-v2 artifact.

 What groundup needs from each demo:
 - ticks parquet with: tick, round_num, steamid, name, side, X, Y, Z, velocity_X, velocity_Y, yaw, is_alive, is_bomb_planted, place,
 active_weapon_name, current_equip_value, has_defuser
 - rounds parquet with: round_num, start, freeze_end, end, winner, reason, bomb_plant, bomb_site
 - header.json with map_name
 - Tickrate: 128 (groundup hardcodes; -clean parses 64 — must reconcile)

 ---
 What is dead weight in -clean/pipeline/ for groundup-style matching

 Confirmed by reading both repos:

 ┌──────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────┐
 │                                   Path                                   │                  Used by groundup matching?                   │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/features/extract_windows.py                                     │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/features/featurize_windows.py                                   │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/features/vectorize.py                                           │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/steps/build_artifact.py                                         │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/jobs/build_corpus.py                                            │ NO (rebuilds artifacts/event_windows)                         │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ backend/retrieval.py                                                     │ NO (ANN layer)                                                │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ backend/round_mapper.py (placeholder)                                    │ REPLACE                                                       │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ db.event_windows table + HNSW index                                      │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ db.ann_search_event_windows etc.                                         │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ derived_pro/, derived_user/ directories                                  │ NO                                                            │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/steps/scrape.py                                                 │ KEEP unchanged                                                │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/steps/download.py                                               │ KEEP unchanged                                                │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/steps/decompress.py                                             │ KEEP unchanged                                                │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/jobs/seed_corpus.py, refresh_pro_corpus.py,                     │ KEEP (orchestration only — but their inner ingest call        │
 │ ingest_local_demos.py                                                    │ changes)                                                      │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ backend/processing.py (parquet writer)                                   │ CHANGE — different parquet set + player_props                 │
 ├──────────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
 │ pipeline/steps/ingest.py                                                 │ CHANGE — drop artifact + event_windows steps                  │
 └──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────┘

 ---
 Combined "both" payload shape

 The new API result returns BOTH algorithm outputs side-by-side. Frontend
 chooses what to render (Viewer can show two pro replays, or one with both
 scores annotated — that's a UI decision, not a backend one).

 {
   "query": { "demo_id": "...", "round_num": 5 },
   "best_match": {        // higher-score of the two, for backward-compat
     "logic": "original" | "nav",
     ... full match fields ...
   },
   "original": { ... } | null,   // select_best_pro_round_match output
   "nav":      { ... } | null    // select_best_pro_route_match output
 }

 best_match.logic lets the UI know which one was picked as primary.

 ---
 Deliverable: refactor.md

 Single Markdown file at the repo root of cs2-shadowpro-clean. Codex
 reads it and executes the changes. It must be self-contained: assume Codex
 hasn't seen this conversation.

 refactor.md outline (exactly what will be in it)

 # Refactor: replace ANN-based round mapping with groundup in-memory matcher

 ## Background and motivation
 - Two-paragraph framing: current state (placeholder ANN top-1), target state
   (port of groundup mapping), why (matching quality + simpler data layer).
 - Pointers: cs2-shadowpro-groundup/cs2_awpy_lab.py is the reference;
   run_shadowpro.py shows the entry-point shape.

 ## Reference implementation map
 Table mapping each groundup function to the new home in -clean:

 | Groundup symbol | New location |
 |---|---|
 | DEFAULT_EVENTS, FOCUSED_PLAYER_PROPS, FOCUSED_WORLD_PROPS | backend/round_mapper.py constants |
 | DemoArtifacts dataclass | backend/round_mapper.py |
 | parse_or_load_demo + cache helpers | backend/round_mapper.py (adapted to canonical paths) |
 | build_team_window_catalog and helpers | backend/round_mapper.py |
 | retrieve_similar_team_windows | backend/round_mapper.py |
 | build_window_player_tracks, _best_track_assignment, _player_alignment_cost, _player_difference_components, _angular_difference_degrees,
 _score_all_player_assignments | backend/round_mapper.py |
 | select_best_pro_round_match | backend/round_mapper.py |
 | _longest_consecutive_streaks | backend/round_mapper.py |
 | _load_nav_mesh, _nav_area_records, _point_in_polygon_xy, _lookup_nav_area_id | backend/round_mapper.py |
 | _sample_round_ticks, _teammate_support_count, _enemy_pressure_count, _nearby_teammate_deaths, _infer_bomb_planted | backend/round_mapper.py |
 | _collapse_route_steps, _build_round_route_profiles | backend/round_mapper.py |
 | _nav_area_similarity, _shared_route_prefix, _count_route_steps_through, _post_break_local_conversion, _classify_route_break,
 _build_route_window_scores | backend/round_mapper.py |
 | select_best_pro_route_match | backend/round_mapper.py |
 | compute_round_divergence_timeline, compute_route_divergence_timeline | backend/round_mapper.py (used by API to populate timeline fields) |
 | weapon-family / place / jaccard / dict-overlap helpers | backend/round_mapper.py |

 ## Step-by-step changes

 ### 1. Delete files (whole-folder)
 - pipeline/features/                           (3 files)
 - pipeline/steps/build_artifact.py
 - pipeline/jobs/build_corpus.py
 - backend/retrieval.py

 ### 2. Rewrite backend/round_mapper.py
 The single new module that owns the matching. Public API:

 ```python
 async def map_user_round_to_pro_round(
     demo_id: str,
     round_num: int,
 ) -> dict | None:
     """Return both 'original' and 'nav' matches plus a 'best_match' pointer."""

 Implementation must:
 - Load user demo via DemoArtifacts.from_parquet_dir(parquet_dir, demo_id, role='user')
 - Discover candidate pro demos for the same map by querying games where source_type='pro' AND map_name=user.map_name. Convert each to
 DemoArtifacts the same way.
 - Run select_best_pro_round_match (original) and select_best_pro_route_match (nav) sequentially.
 - Build a unified payload with:
   {
 "query": {...},
 "best_match": <higher-score of the two, with .logic field>,
 "original": ,
 "nav":      ,
   }
 - Resolve match_demo_id back to a games row to enrich the payload with team1_name/team2_name/event_name/match_date for the UI.

 DemoArtifacts in -clean must read from the existing parquet_pro/{demo_id}/ and parquet_user/{steam_id}/{demo_id}/ layout (see
 backend/config.py: derived_match_dir, resolve_managed_path). Tickrate field comes from games.tick_rate (currently 64); groundup hardcodes 128 —
  read from the row, do not hardcode.

 3. Rewrite backend/processing.py and pipeline/steps/ingest.py parquet output

 User and pro ingestion both must:
 - Use FOCUSED_PLAYER_PROPS + FOCUSED_WORLD_PROPS from groundup (verbatim).
 - Call awpy.parsers.utils.fix_common_names(ticks).
 - Filter ticks to demo.in_play_ticks via semi-join.
 - Apply awpy.parsers.rounds.apply_round_num to ticks.
 - Write 7 parquets: ticks, rounds, kills, damages, shots, bomb, grenades.
 - Write {demo_id}_header.json next to the parquets with the demo header (for tickrate + map_name; tickrate is parsed from header, no hardcode).

 Drop entirely:
 - _write_parquets paths for smokes/infernos/flashes/grenade_paths (not used).
 - All event_windows extraction calls (extract_match_event_windows).
 - All build_match_artifact calls.
 - Imports from pipeline/features/* and pipeline/steps/build_artifact.

 4. Simplify backend/db.py

 Drop:
 - ann_search_event_windows
 - list_event_window_candidates
 - count_event_windows
 - upsert_event_windows_batch
 - upsert_event_window
 - get_event_window
 - _format_embedding
 - set_match_artifact_path
 - VECTOR_DIM import from pipeline.features.vectorize

 Keep:
 - All games/users/events/teams/demo_jobs/job_runs/round_analysis_cache helpers.

 5. db/schema.sql + new migration

 Migration db/migrations/20260502_drop_event_windows.sql:
 - DROP TABLE shadowpro.event_windows CASCADE;
 - ALTER TABLE shadowpro.games DROP COLUMN feature_version;
 - ALTER TABLE shadowpro.games DROP COLUMN artifact_version;
 - ALTER TABLE shadowpro.games DROP COLUMN artifact_path;

 Update db/schema.sql to match (drop the table + columns + indexes).

 6. Update backend/round_analysis_service.py

 - Remove the simple "wrap mapper" version; logic stays the same but the result payload now contains best_match/original/nav.
 normalize_round_analysis_result must inject map display on best_match, original, and nav.

 7. Update backend/main.py

 - The route stays at GET /api/round-analysis/{demo_id}/{round_num} (no ?logic).
 - Remove dependence on retrieval module.

 8. Update backend/worker.py precompute

 - Stays the same; signature already takes (demo_id, round_num).

 9. Update web/src/replay/types.ts

 Replace BestMatch with:
 export interface MatchSide {
   logic: "original" | "nav";
   source_match_id: string;
   round_num: number;
   score: number;
   matched_pro_steamid?: number;
   matched_pro_player?: string;
   map_name: string | null;
   map: { key: string; name: string; display: string };
   event_name: string | null;
   team1_name: string | null;
   team2_name: string | null;
   team_ct: string | null;
   team_t: string | null;
   match_date: string | null;
   // Algorithm-specific extras (route fields are nav-only)
   shared_route_steps?: number;
   matched_prefix_duration_sec?: number;
   break_event_label?: string;
   break_time_sec?: number;
   longest_streak?: number;
   coverage?: number;
 }
 export interface RoundAnalysisResult {
   query: SimilarityQuery;
   best_match: MatchSide | null;
   original: MatchSide | null;
   nav: MatchSide | null;
 }

 10. Update web/src/Viewer.tsx

 - Read result.original and result.nav.
 - Render two pro replays side-by-side, each labeled with its logic and score.
 - best_match used for backward-compat highlighting only.

 11. Update architecture/ARCHITECTURE_EXACT.md

 - Round Analysis section: replace ANN diagram with in-memory groundup flow.
 - Module map: backend/round_mapper.py owns matching; retrieval.py gone; pipeline/features gone.

 Performance and caching notes

 - Groundup matching is CPU-heavy at request time (1–5s per pro demo for catalog build, scaled by N pro demos).
 - Keep the round_analysis_cache table and the worker precompute path — these are essential.
 - DemoArtifacts loading should cache parsed parquet frames in-process to avoid re-reading on every API call within the worker process. Use
 functools.lru_cache(maxsize=8) on a "load DemoArtifacts by parquet_dir + demo_id" helper.

 File-level acceptance checklist

 - git grep -nE 'event_windows|extract_windows|featurize_windows|vectorize|build_artifact|retrieval' returns nothing in code paths (only in
 migrations / archive).
 - backend/round_mapper.py exposes map_user_round_to_pro_round(demo_id, round_num) and the call returns {best_match, original, nav, query}.
 - No imports of asyncpg or pgvector related to event_windows in any backend module.
 - DB schema has no event_windows table, no feature_version/artifact_version/artifact_path columns on games.
 - Worker import precompute populates round_analysis_cache for every round.

 End-to-end verification

 1. DB reset: drop schema, re-run db/schema.sql.
 2. docker compose up db web worker frontend — boots cleanly.
 3. python -m pipeline.jobs.refresh_pro_corpus --results-url "https://www.hltv.org/results?map=de_mirage" --limit 10 → ingests 10 Mirage demos.
 parquet_pro/{id}/ has the 7 new parquets. derived_pro/ is not created.
 4. POST /api/import a Mirage user demo. Worker processes; round_analysis_cache populated.
 5. GET /api/round-analysis/{demo_id}/{round} → 200 with {best_match, original, nav, query}.
 6. UI Viewer shows the round + at least one pro pane based on best_match.

 What is OUT of scope

 - Frontend redesign for two side-by-side pro panes — leave for follow-up; current single-pane Viewer reads best_match and is fine for now.
 - Performance tuning beyond LRU caching DemoArtifacts.
 - Removing demos_pro / demos_user folders.

 That's the deliverable. The Codex agent runs through these steps in order.

 ---

 ## Why no exploration agents this round

 The prior conversation already mapped both repos in depth (architecture, data
 flow, dead code, schema). This planning round is purely about packaging that
 analysis into refactor.md instructions. No new code exploration is required;
 the only judgment call left is the payload shape (already decided above with
 the user) and whether to merge nav+original scores or expose both (user
 decided: expose both, frontend renders the output of both).

 ---

 ## Critical files to be modified by Codex (when refactor.md is executed)

 add:     refactor.md                                 (this plan's deliverable)
 delete:  pipeline/features/extract_windows.py
 delete:  pipeline/features/featurize_windows.py
 delete:  pipeline/features/vectorize.py
 delete:  pipeline/features/init.py
 delete:  pipeline/steps/build_artifact.py
 delete:  pipeline/jobs/build_corpus.py
 delete:  backend/retrieval.py

 rewrite: backend/round_mapper.py                     (~1500 LOC port from groundup)
 rewrite: backend/processing.py                        (groundup parquet set)
 rewrite: pipeline/steps/ingest.py                    (groundup parquet set)
 modify:  backend/db.py                               (drop event_windows / artifact helpers)
 modify:  backend/round_analysis_service.py           (normalize new payload)
 modify:  backend/main.py                             (drop retrieval imports)
 modify:  db/schema.sql                               (drop event_windows + 3 columns)
 add:     db/migrations/20260502_drop_event_windows.sql
 modify:  web/src/replay/types.ts                     (new MatchSide shape)
 modify:  web/src/Viewer.tsx                          (read original/nav alongside best_match)
 modify:  architecture/ARCHITECTURE_EXACT.md          (update Round Analysis section)

 Reference files Codex must read carefully (read-only):
 - /home/tomyan/Code/cs2-shadowpro-groundup/cs2_awpy_lab.py (entire file)
 - /home/tomyan/Code/cs2-shadowpro-groundup/run_shadowpro.py
 - /home/tomyan/Code/cs2-shadowpro-clean/backend/config.py (path normalization)
 - /home/tomyan/Code/cs2-shadowpro-clean/db/schema.sql (current shape)

 ---

 ## Verification (after Codex finishes)

 1. `python3 -c "import ast; [ast.parse(open(f).read()) for f in
 ['backend/round_mapper.py','backend/processing.py','backend/db.py','backend/main.py','pipeline/steps/ingest.py']]; print('OK')"` — syntax
 compile.
 2. `git grep -nE 'event_windows|extract_windows|featurize_windows|vectorize|build_artifact|retrieval'` returns nothing in code (only in
 migrations).
 3. DB reset → docker compose up → boot clean.
 4. `python -m pipeline.jobs.refresh_pro_corpus --results-url "https://www.hltv.org/results?map=de_mirage" --limit 10` → 10 Mirage demos
 ingested, 7 groundup parquets each.
 5. Import one Mirage user demo → worker precomputes → `select * from round_analysis_cache where game_id = '<demo_id>'` shows N rows with
 `result_json.best_match`, `result_json.original`, `result_json.nav`.
 6. `curl /api/round-analysis/<demo_id>/1` returns the combined payload.
 7. UI Viewer renders side-by-side replay (existing single-pane behavior is acceptable for this milestone).
