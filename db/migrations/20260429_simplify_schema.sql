-- Data-preserving migration for the flat 9-table schema.
--
-- This avoids deleting the Docker volume. Existing dimensional tables are
-- renamed to *_legacy_20260429, new tables are created under the canonical
-- names, and known data is copied forward. Keep the legacy tables until the
-- app has been smoke-tested.

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

-- Keep users/maps in place; move replaced tables aside so their data survives.
ALTER TABLE IF EXISTS round_analysis_results RENAME TO round_analysis_results_legacy_20260429;
ALTER TABLE IF EXISTS round_analysis_cache RENAME TO round_analysis_cache_legacy_20260429;
ALTER TABLE IF EXISTS demo_jobs RENAME TO demo_jobs_legacy_20260429;
ALTER TABLE IF EXISTS event_windows RENAME TO event_windows_legacy_20260429;
ALTER TABLE IF EXISTS game_player_stats RENAME TO game_player_stats_legacy_20260429;
ALTER TABLE IF EXISTS game_artifacts RENAME TO game_artifacts_legacy_20260429;
ALTER TABLE IF EXISTS rounds RENAME TO rounds_legacy_20260429;
ALTER TABLE IF EXISTS game_teams RENAME TO game_teams_legacy_20260429;
ALTER TABLE IF EXISTS games RENAME TO games_legacy_20260429;
ALTER TABLE IF EXISTS match_teams RENAME TO match_teams_legacy_20260429;
ALTER TABLE IF EXISTS matches RENAME TO matches_legacy_20260429;
ALTER TABLE IF EXISTS teams RENAME TO teams_legacy_20260429;
ALTER TABLE IF EXISTS events RENAME TO events_legacy_20260429;
ALTER TABLE IF EXISTS match_types RENAME TO match_types_legacy_20260429;
ALTER TABLE IF EXISTS job_runs RENAME TO job_runs_legacy_20260429;
ALTER SEQUENCE IF EXISTS job_runs_id_seq RENAME TO job_runs_legacy_20260429_id_seq;

-- Table renames do not free the old primary-key index names.
ALTER INDEX IF EXISTS round_analysis_cache_pkey RENAME TO round_analysis_cache_legacy_20260429_pkey;
ALTER INDEX IF EXISTS demo_jobs_pkey RENAME TO demo_jobs_legacy_20260429_pkey;
ALTER INDEX IF EXISTS event_windows_pkey RENAME TO event_windows_legacy_20260429_pkey;
ALTER INDEX IF EXISTS games_pkey RENAME TO games_legacy_20260429_pkey;
ALTER INDEX IF EXISTS teams_pkey RENAME TO teams_legacy_20260429_pkey;
ALTER INDEX IF EXISTS events_pkey RENAME TO events_legacy_20260429_pkey;
ALTER INDEX IF EXISTS job_runs_pkey RENAME TO job_runs_legacy_20260429_pkey;

CREATE TABLE IF NOT EXISTS maps (
    map_name          TEXT PRIMARY KEY,
    display_name      TEXT    NOT NULL,
    pos_x             FLOAT   NOT NULL,
    pos_y             FLOAT   NOT NULL,
    map_scale         FLOAT   NOT NULL,
    has_lower_level   BOOLEAN NOT NULL DEFAULT FALSE,
    lower_level_max_z FLOAT   NOT NULL DEFAULT -1000000.0
);

CREATE TABLE IF NOT EXISTS users (
    steam_id        TEXT PRIMARY KEY,
    match_auth_code TEXT,
    last_share_code TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE events (
    event_id        TEXT PRIMARY KEY,
    event_name      TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE
);

CREATE TABLE teams (
    team_id         TEXT PRIMARY KEY,
    team_name       TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE
);

CREATE TABLE games (
    game_id          TEXT PRIMARY KEY,
    source_type      TEXT NOT NULL CHECK (source_type IN ('user', 'pro')),
    match_type       TEXT NOT NULL DEFAULT 'unknown'
                     CHECK (match_type IN ('unknown', 'premier', 'competitive', 'faceit', 'hltv')),
    map_name         TEXT NOT NULL REFERENCES maps (map_name),
    played_at        TIMESTAMPTZ,

    demo_stem        TEXT NOT NULL,
    parquet_dir      TEXT,
    artifact_path    TEXT,
    artifact_version TEXT,
    parser_version   TEXT,
    feature_version  TEXT,
    tick_rate        SMALLINT CHECK (tick_rate IS NULL OR tick_rate > 0),

    round_count      SMALLINT CHECK (round_count IS NULL OR round_count >= 0),
    ct_round_wins    SMALLINT CHECK (ct_round_wins IS NULL OR ct_round_wins >= 0),
    t_round_wins     SMALLINT CHECK (t_round_wins IS NULL OR t_round_wins >= 0),

    ingest_status    TEXT NOT NULL DEFAULT 'pending'
                     CHECK (ingest_status IN ('pending', 'processing', 'ready', 'error')),
    ingest_error     TEXT,
    ingested_at      TIMESTAMPTZ,

    steam_id         TEXT REFERENCES users (steam_id) ON DELETE CASCADE,
    share_code       TEXT,
    user_side_first  TEXT CHECK (user_side_first IN ('ct', 't')),
    user_result      TEXT CHECK (user_result IN ('win', 'draw', 'loss')),
    user_rounds_won  SMALLINT CHECK (user_rounds_won IS NULL OR user_rounds_won >= 0),
    user_rounds_lost SMALLINT CHECK (user_rounds_lost IS NULL OR user_rounds_lost >= 0),
    user_kills       SMALLINT CHECK (user_kills IS NULL OR user_kills >= 0),
    user_deaths      SMALLINT CHECK (user_deaths IS NULL OR user_deaths >= 0),
    user_assists     SMALLINT CHECK (user_assists IS NULL OR user_assists >= 0),
    user_hs_pct      FLOAT CHECK (user_hs_pct IS NULL OR (user_hs_pct >= 0 AND user_hs_pct <= 100)),

    external_match_id TEXT,
    source_url        TEXT,
    source_slug       TEXT,
    event_id          TEXT REFERENCES events (event_id),
    team1_id          TEXT REFERENCES teams (team_id),
    team2_id          TEXT REFERENCES teams (team_id),
    map_number        SMALLINT CHECK (map_number IS NULL OR map_number > 0),

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT games_source_mutex CHECK (
        (
            source_type = 'user'
            AND steam_id IS NOT NULL
            AND external_match_id IS NULL
            AND source_url IS NULL
            AND source_slug IS NULL
            AND event_id IS NULL
            AND team1_id IS NULL
            AND team2_id IS NULL
            AND map_number IS NULL
        )
        OR
        (
            source_type = 'pro'
            AND steam_id IS NULL
            AND share_code IS NULL
            AND user_side_first IS NULL
            AND user_result IS NULL
            AND user_rounds_won IS NULL
            AND user_rounds_lost IS NULL
            AND user_kills IS NULL
            AND user_deaths IS NULL
            AND user_assists IS NULL
            AND user_hs_pct IS NULL
        )
    ),
    CONSTRAINT games_pro_external_unique UNIQUE (external_match_id, map_number)
);

CREATE INDEX games_steam_idx
    ON games (steam_id, played_at DESC)
    WHERE source_type = 'user';
CREATE INDEX games_event_idx
    ON games (event_id, played_at DESC)
    WHERE source_type = 'pro';
CREATE INDEX games_status_idx
    ON games (ingest_status, updated_at DESC);
CREATE INDEX games_external_idx
    ON games (external_match_id)
    WHERE source_type = 'pro';

CREATE TABLE event_windows (
    window_id       TEXT PRIMARY KEY,
    game_id         TEXT        NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    map_name        TEXT        NOT NULL REFERENCES maps (map_name),
    round_num       SMALLINT    NOT NULL CHECK (round_num > 0),
    start_tick      INTEGER     NOT NULL CHECK (start_tick >= 0),
    anchor_tick     INTEGER     NOT NULL CHECK (anchor_tick >= 0),
    end_tick        INTEGER     NOT NULL CHECK (end_tick >= 0),
    side_to_query   TEXT        CHECK (side_to_query IN ('ct', 't')),
    phase           TEXT,
    site            TEXT,
    anchor_kind     TEXT,
    alive_ct        SMALLINT    CHECK (alive_ct IS NULL OR alive_ct >= 0),
    alive_t         SMALLINT    CHECK (alive_t IS NULL OR alive_t >= 0),
    feature_version TEXT        NOT NULL,
    feature_path    TEXT        NOT NULL,
    embedding       vector(54),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT event_windows_tick_order CHECK (
        start_tick <= anchor_tick AND anchor_tick <= end_tick
    )
);

CREATE INDEX event_windows_game_idx
    ON event_windows (game_id);
CREATE INDEX event_windows_filter_idx
    ON event_windows (map_name, feature_version);
CREATE INDEX event_windows_anchor_idx
    ON event_windows (map_name, round_num, anchor_tick);
CREATE INDEX event_windows_embed_idx
    ON event_windows USING hnsw (embedding vector_cosine_ops);

CREATE TABLE round_analysis_cache (
    game_id            TEXT     NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    round_num          SMALLINT NOT NULL CHECK (round_num > 0),
    logic              TEXT     NOT NULL CHECK (logic IN ('nav', 'original', 'both')),
    matcher_version    TEXT     NOT NULL,
    pro_corpus_version TEXT     NOT NULL,
    status             TEXT     NOT NULL CHECK (status IN ('pending', 'done', 'error')),
    result_json        JSONB,
    error_message      TEXT,
    invalidated_at     TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_id, round_num, logic, matcher_version, pro_corpus_version)
);

CREATE INDEX round_analysis_lookup_idx
    ON round_analysis_cache (game_id, round_num, logic, updated_at DESC);

CREATE TABLE demo_jobs (
    job_id      TEXT PRIMARY KEY,
    steam_id    TEXT        NOT NULL REFERENCES users (steam_id) ON DELETE CASCADE,
    demo_path   TEXT        NOT NULL,
    game_id     TEXT        NOT NULL,
    match_type  TEXT        NOT NULL DEFAULT 'unknown'
                CHECK (match_type IN ('unknown', 'premier', 'competitive', 'faceit')),
    status      TEXT        NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'processing', 'done', 'error')),
    result_json JSONB,
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX demo_jobs_pending_idx
    ON demo_jobs (status, created_at ASC)
    WHERE status = 'pending';
CREATE INDEX demo_jobs_steam_idx
    ON demo_jobs (steam_id, created_at DESC);

CREATE TABLE job_runs (
    id              BIGSERIAL PRIMARY KEY,
    job_name        TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'done', 'error')),
    items_processed INTEGER,
    error_message   TEXT,
    stats_json      JSONB
);

CREATE INDEX job_runs_name_start_idx
    ON job_runs (job_name, started_at DESC);

INSERT INTO maps (map_name, display_name, pos_x, pos_y, map_scale, has_lower_level, lower_level_max_z) VALUES
    ('de_ancient',  'Ancient',  -2953, 2164, 5.0,  FALSE, -1000000.0),
    ('de_anubis',   'Anubis',   -2796, 3328, 5.22, FALSE, -1000000.0),
    ('de_dust2',    'Dust 2',   -2476, 3239, 4.4,  FALSE, -1000000.0),
    ('de_inferno',  'Inferno',  -2087, 3870, 4.9,  FALSE, -1000000.0),
    ('de_mirage',   'Mirage',   -3230, 1713, 5.0,  FALSE, -1000000.0),
    ('de_nuke',     'Nuke',     -3453, 2887, 7.0,  TRUE,  -495.0),
    ('de_overpass', 'Overpass', -4831, 1781, 5.2,  FALSE, -1000000.0)
ON CONFLICT (map_name) DO UPDATE
    SET display_name = EXCLUDED.display_name,
        pos_x = EXCLUDED.pos_x,
        pos_y = EXCLUDED.pos_y,
        map_scale = EXCLUDED.map_scale,
        has_lower_level = EXCLUDED.has_lower_level,
        lower_level_max_z = EXCLUDED.lower_level_max_z;

-- Dimensions: dedupe by normalized names while keeping one stable old id.
INSERT INTO events (event_id, event_name, normalized_name)
SELECT DISTINCT ON (normalized_name)
       event_id, event_name, normalized_name
FROM (
    SELECT event_id,
           btrim(event_name) AS event_name,
           lower(regexp_replace(btrim(event_name), '\s+', ' ', 'g')) AS normalized_name,
           updated_at,
           created_at
    FROM events_legacy_20260429
    WHERE event_name IS NOT NULL AND btrim(event_name) <> ''
) src
ORDER BY normalized_name, updated_at DESC NULLS LAST, created_at DESC NULLS LAST, event_id
ON CONFLICT (normalized_name) DO UPDATE
    SET event_name = EXCLUDED.event_name;

INSERT INTO teams (team_id, team_name, normalized_name)
SELECT DISTINCT ON (normalized_name)
       team_id, team_name, normalized_name
FROM (
    SELECT team_id,
           btrim(team_name) AS team_name,
           lower(regexp_replace(btrim(team_name), '\s+', ' ', 'g')) AS normalized_name,
           updated_at,
           created_at
    FROM teams_legacy_20260429
    WHERE team_name IS NOT NULL AND btrim(team_name) <> ''
) src
ORDER BY normalized_name, updated_at DESC NULLS LAST, created_at DESC NULLS LAST, team_id
ON CONFLICT (normalized_name) DO UPDATE
    SET team_name = EXCLUDED.team_name;

-- Games: flatten matches, match_teams, game_artifacts, and game_player_stats.
WITH latest_artifacts AS (
    SELECT DISTINCT ON (game_id)
           game_id, path, version
    FROM game_artifacts_legacy_20260429
    WHERE kind = 'match_artifact'
    ORDER BY game_id, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
),
game_rows AS (
    SELECT
        g.game_id,
        g.source_type,
        CASE
            WHEN g.source_type = 'pro' THEN 'hltv'
            WHEN m.match_type IN ('unknown', 'premier', 'competitive', 'faceit') THEN m.match_type
            ELSE 'unknown'
        END AS match_type,
        g.map_name,
        m.played_at,
        g.demo_stem,
        g.parquet_dir,
        la.path AS artifact_path,
        COALESCE(la.version, g.artifact_version) AS artifact_version,
        g.parser_version,
        g.window_feature_version AS feature_version,
        g.tick_rate,
        g.round_count,
        g.ct_round_wins,
        g.t_round_wins,
        g.ingest_status,
        g.ingest_error,
        g.ingested_at,
        CASE WHEN g.source_type = 'user' THEN m.steam_id END AS steam_id,
        CASE WHEN g.source_type = 'user' THEN m.share_code END AS share_code,
        CASE WHEN g.source_type = 'user' THEN gps.side_first END AS user_side_first,
        CASE WHEN g.source_type = 'user' THEN gps.result END AS user_result,
        CASE WHEN g.source_type = 'user' THEN gps.rounds_won END AS user_rounds_won,
        CASE WHEN g.source_type = 'user' THEN gps.rounds_lost END AS user_rounds_lost,
        CASE WHEN g.source_type = 'user' THEN gps.kills END AS user_kills,
        CASE WHEN g.source_type = 'user' THEN gps.deaths END AS user_deaths,
        CASE WHEN g.source_type = 'user' THEN gps.assists END AS user_assists,
        CASE WHEN g.source_type = 'user' THEN gps.hs_pct END AS user_hs_pct,
        CASE WHEN g.source_type = 'pro' THEN m.external_match_id END AS external_match_id,
        CASE WHEN g.source_type = 'pro' THEN m.source_url END AS source_url,
        CASE WHEN g.source_type = 'pro' THEN m.source_slug END AS source_slug,
        CASE WHEN g.source_type = 'pro' THEN en.event_id END AS event_id,
        CASE WHEN g.source_type = 'pro' THEN tn1.team_id END AS team1_id,
        CASE WHEN g.source_type = 'pro' THEN tn2.team_id END AS team2_id,
        CASE WHEN g.source_type = 'pro' THEN g.map_number END AS map_number,
        g.created_at,
        g.updated_at
    FROM games_legacy_20260429 g
    JOIN matches_legacy_20260429 m ON m.match_id = g.match_id
    LEFT JOIN latest_artifacts la ON la.game_id = g.game_id
    LEFT JOIN game_player_stats_legacy_20260429 gps
        ON gps.game_id = g.game_id AND gps.steam_id = m.steam_id
    LEFT JOIN events_legacy_20260429 el ON el.event_id = m.event_id
    LEFT JOIN events en
        ON en.normalized_name = lower(regexp_replace(btrim(el.event_name), '\s+', ' ', 'g'))
    LEFT JOIN match_teams_legacy_20260429 mt1
        ON mt1.match_id = m.match_id AND mt1.team_slot = 1
    LEFT JOIN teams_legacy_20260429 tl1 ON tl1.team_id = mt1.team_id
    LEFT JOIN teams tn1
        ON tn1.normalized_name = lower(regexp_replace(btrim(tl1.team_name), '\s+', ' ', 'g'))
    LEFT JOIN match_teams_legacy_20260429 mt2
        ON mt2.match_id = m.match_id AND mt2.team_slot = 2
    LEFT JOIN teams_legacy_20260429 tl2 ON tl2.team_id = mt2.team_id
    LEFT JOIN teams tn2
        ON tn2.normalized_name = lower(regexp_replace(btrim(tl2.team_name), '\s+', ' ', 'g'))
    WHERE g.map_name IN (SELECT map_name FROM maps)
)
INSERT INTO games (
    game_id, source_type, match_type, map_name, played_at,
    demo_stem, parquet_dir, artifact_path, artifact_version,
    parser_version, feature_version, tick_rate,
    round_count, ct_round_wins, t_round_wins,
    ingest_status, ingest_error, ingested_at,
    steam_id, share_code, user_side_first, user_result,
    user_rounds_won, user_rounds_lost,
    user_kills, user_deaths, user_assists, user_hs_pct,
    external_match_id, source_url, source_slug, event_id, team1_id, team2_id, map_number,
    created_at, updated_at
)
SELECT
    game_id, source_type, match_type, map_name, played_at,
    demo_stem, parquet_dir, artifact_path, artifact_version,
    parser_version, feature_version, tick_rate,
    round_count, ct_round_wins, t_round_wins,
    ingest_status, ingest_error, ingested_at,
    steam_id, share_code, user_side_first, user_result,
    user_rounds_won, user_rounds_lost,
    user_kills, user_deaths, user_assists, user_hs_pct,
    external_match_id, source_url, source_slug, event_id, team1_id, team2_id, map_number,
    created_at, updated_at
FROM game_rows
WHERE (source_type = 'pro' OR steam_id IS NOT NULL)
ON CONFLICT (game_id) DO UPDATE
    SET source_type = EXCLUDED.source_type,
        match_type = EXCLUDED.match_type,
        map_name = EXCLUDED.map_name,
        played_at = EXCLUDED.played_at,
        demo_stem = EXCLUDED.demo_stem,
        parquet_dir = EXCLUDED.parquet_dir,
        artifact_path = EXCLUDED.artifact_path,
        artifact_version = EXCLUDED.artifact_version,
        parser_version = EXCLUDED.parser_version,
        feature_version = EXCLUDED.feature_version,
        tick_rate = EXCLUDED.tick_rate,
        round_count = EXCLUDED.round_count,
        ct_round_wins = EXCLUDED.ct_round_wins,
        t_round_wins = EXCLUDED.t_round_wins,
        ingest_status = EXCLUDED.ingest_status,
        ingest_error = EXCLUDED.ingest_error,
        ingested_at = EXCLUDED.ingested_at,
        steam_id = EXCLUDED.steam_id,
        share_code = EXCLUDED.share_code,
        user_side_first = EXCLUDED.user_side_first,
        user_result = EXCLUDED.user_result,
        user_rounds_won = EXCLUDED.user_rounds_won,
        user_rounds_lost = EXCLUDED.user_rounds_lost,
        user_kills = EXCLUDED.user_kills,
        user_deaths = EXCLUDED.user_deaths,
        user_assists = EXCLUDED.user_assists,
        user_hs_pct = EXCLUDED.user_hs_pct,
        external_match_id = EXCLUDED.external_match_id,
        source_url = EXCLUDED.source_url,
        source_slug = EXCLUDED.source_slug,
        event_id = EXCLUDED.event_id,
        team1_id = EXCLUDED.team1_id,
        team2_id = EXCLUDED.team2_id,
        map_number = EXCLUDED.map_number,
        updated_at = EXCLUDED.updated_at;

INSERT INTO event_windows (
    window_id, game_id, map_name, round_num,
    start_tick, anchor_tick, end_tick, side_to_query,
    phase, site, anchor_kind, alive_ct, alive_t,
    feature_version, feature_path, embedding, created_at
)
SELECT
    ew.window_id, ew.game_id, ew.map_name, ew.round_num,
    ew.start_tick, ew.anchor_tick, ew.end_tick, ew.side_to_query,
    ew.phase, ew.site, ew.anchor_kind, ew.alive_ct, ew.alive_t,
    ew.feature_version, ew.feature_path, ew.embedding, ew.created_at
FROM event_windows_legacy_20260429 ew
JOIN games g ON g.game_id = ew.game_id
WHERE ew.game_id IS NOT NULL
  AND ew.round_num > 0
  AND ew.start_tick >= 0
  AND ew.anchor_tick >= 0
  AND ew.end_tick >= 0
  AND ew.start_tick <= ew.anchor_tick
  AND ew.anchor_tick <= ew.end_tick
  AND ew.feature_version IS NOT NULL
  AND ew.feature_path IS NOT NULL
ON CONFLICT (window_id) DO UPDATE
    SET game_id = EXCLUDED.game_id,
        map_name = EXCLUDED.map_name,
        round_num = EXCLUDED.round_num,
        start_tick = EXCLUDED.start_tick,
        anchor_tick = EXCLUDED.anchor_tick,
        end_tick = EXCLUDED.end_tick,
        side_to_query = EXCLUDED.side_to_query,
        phase = EXCLUDED.phase,
        site = EXCLUDED.site,
        anchor_kind = EXCLUDED.anchor_kind,
        alive_ct = EXCLUDED.alive_ct,
        alive_t = EXCLUDED.alive_t,
        feature_version = EXCLUDED.feature_version,
        feature_path = EXCLUDED.feature_path,
        embedding = EXCLUDED.embedding;

INSERT INTO round_analysis_cache (
    game_id, round_num, logic, matcher_version, pro_corpus_version,
    status, result_json, error_message, invalidated_at, created_at, updated_at
)
SELECT
    r.demo_id, r.round_num, r.logic, r.matcher_version, r.pro_corpus_version,
    r.status, r.result_json, r.error_message, r.invalidated_at, r.created_at, r.updated_at
FROM round_analysis_results_legacy_20260429 r
JOIN games g ON g.game_id = r.demo_id
WHERE r.round_num > 0
ON CONFLICT (game_id, round_num, logic, matcher_version, pro_corpus_version) DO UPDATE
    SET status = EXCLUDED.status,
        result_json = EXCLUDED.result_json,
        error_message = EXCLUDED.error_message,
        invalidated_at = EXCLUDED.invalidated_at,
        updated_at = EXCLUDED.updated_at;

INSERT INTO demo_jobs (
    job_id, steam_id, demo_path, game_id, match_type,
    status, result_json, error, created_at, updated_at
)
SELECT
    job_id,
    steam_id,
    demo_path,
    demo_id,
    CASE WHEN match_type IN ('unknown', 'premier', 'competitive', 'faceit')
         THEN match_type ELSE 'unknown' END,
    status,
    result_json,
    error,
    created_at,
    updated_at
FROM demo_jobs_legacy_20260429
WHERE steam_id IN (SELECT steam_id FROM users)
ON CONFLICT (job_id) DO UPDATE
    SET steam_id = EXCLUDED.steam_id,
        demo_path = EXCLUDED.demo_path,
        game_id = EXCLUDED.game_id,
        match_type = EXCLUDED.match_type,
        status = EXCLUDED.status,
        result_json = EXCLUDED.result_json,
        error = EXCLUDED.error,
        updated_at = EXCLUDED.updated_at;

INSERT INTO job_runs (
    id, job_name, started_at, finished_at, status,
    items_processed, error_message, stats_json
)
SELECT
    id, job_name, started_at, finished_at, status,
    items_processed, error_message, stats_json
FROM job_runs_legacy_20260429
ON CONFLICT (id) DO UPDATE
    SET job_name = EXCLUDED.job_name,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        status = EXCLUDED.status,
        items_processed = EXCLUDED.items_processed,
        error_message = EXCLUDED.error_message,
        stats_json = EXCLUDED.stats_json;

SELECT setval(
    pg_get_serial_sequence('job_runs', 'id'),
    GREATEST(COALESCE((SELECT MAX(id) FROM job_runs), 1), 1),
    (SELECT COUNT(*) > 0 FROM job_runs)
);

COMMIT;
