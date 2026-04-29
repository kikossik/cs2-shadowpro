-- CS2 ShadowPro Clean - PostgreSQL schema
-- Run: psql -d <db_name> -f schema.sql

CREATE SCHEMA IF NOT EXISTS shadowpro;
SET search_path TO shadowpro, public;

-- Maps (reference + coord config)
CREATE TABLE IF NOT EXISTS maps (
    map_name          TEXT PRIMARY KEY,
    display_name      TEXT    NOT NULL,
    pos_x             FLOAT   NOT NULL,
    pos_y             FLOAT   NOT NULL,
    map_scale         FLOAT   NOT NULL,
    has_lower_level   BOOLEAN NOT NULL DEFAULT FALSE,
    lower_level_max_z FLOAT   NOT NULL DEFAULT -1000000.0
);

-- Users
CREATE TABLE IF NOT EXISTS users (
    steam_id        TEXT PRIMARY KEY,
    match_auth_code TEXT,
    last_share_code TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pro tournament/event dimension
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    event_name      TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE
);

-- Pro team dimension
CREATE TABLE IF NOT EXISTS teams (
    team_id         TEXT PRIMARY KEY,
    team_name       TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE
);

-- Games: one parsed map/demo, the atomic analysis unit
CREATE TABLE IF NOT EXISTS games (
    game_id          TEXT PRIMARY KEY,
    source_type      TEXT NOT NULL CHECK (source_type IN ('user', 'pro')),
    match_type       TEXT NOT NULL DEFAULT 'unknown'
                     CHECK (match_type IN ('unknown', 'premier', 'competitive', 'faceit', 'hltv')),
    map_name         TEXT NOT NULL REFERENCES maps (map_name),
    played_at        TIMESTAMPTZ,

    demo_stem        TEXT NOT NULL,
    parquet_dir      TEXT,
    parser_version   TEXT,
    tick_rate        SMALLINT CHECK (tick_rate IS NULL OR tick_rate > 0),

    round_count      SMALLINT CHECK (round_count IS NULL OR round_count >= 0),
    ct_round_wins    SMALLINT CHECK (ct_round_wins IS NULL OR ct_round_wins >= 0),
    t_round_wins     SMALLINT CHECK (t_round_wins IS NULL OR t_round_wins >= 0),

    ingest_status    TEXT NOT NULL DEFAULT 'pending'
                     CHECK (ingest_status IN ('pending', 'processing', 'ready', 'error')),
    ingest_error     TEXT,
    ingested_at      TIMESTAMPTZ,

    -- User-only fields
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

    -- Pro-only fields
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

CREATE INDEX IF NOT EXISTS games_steam_idx
    ON games (steam_id, played_at DESC)
    WHERE source_type = 'user';
CREATE INDEX IF NOT EXISTS games_event_idx
    ON games (event_id, played_at DESC)
    WHERE source_type = 'pro';
CREATE INDEX IF NOT EXISTS games_status_idx
    ON games (ingest_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS games_external_idx
    ON games (external_match_id)
    WHERE source_type = 'pro';

-- Round analysis cache
CREATE TABLE IF NOT EXISTS round_analysis_cache (
    game_id        TEXT     NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    round_num      SMALLINT NOT NULL CHECK (round_num > 0),
    status         TEXT     NOT NULL CHECK (status IN ('pending', 'done', 'error')),
    result_json    JSONB,
    error_message  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_id, round_num)
);

-- Demo import job queue
CREATE TABLE IF NOT EXISTS demo_jobs (
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

CREATE INDEX IF NOT EXISTS demo_jobs_pending_idx
    ON demo_jobs (status, created_at ASC)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS demo_jobs_steam_idx
    ON demo_jobs (steam_id, created_at DESC);

-- Job runs
CREATE TABLE IF NOT EXISTS job_runs (
    id              BIGSERIAL PRIMARY KEY,
    job_name        TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL CHECK (status IN ('running', 'done', 'error')),
    items_processed INTEGER,
    error_message   TEXT,
    stats_json      JSONB
);

CREATE INDEX IF NOT EXISTS job_runs_name_start_idx
    ON job_runs (job_name, started_at DESC);

-- Seed: all competitive maps used by the app
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
