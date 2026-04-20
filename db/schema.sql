-- CS2 ShadowPro — PostgreSQL schema
-- Run: psql -d <db_name> -f schema.sql

-- ── Maps (reference + coord config) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS maps (
    map_name          TEXT PRIMARY KEY,
    display_name      TEXT    NOT NULL,
    pos_x             FLOAT   NOT NULL,
    pos_y             FLOAT   NOT NULL,
    map_scale         FLOAT   NOT NULL,
    has_lower_level   BOOLEAN NOT NULL DEFAULT FALSE,
    lower_level_max_z FLOAT   NOT NULL DEFAULT -1000000.0
);

-- ── Users ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    steam_id        TEXT PRIMARY KEY,
    match_auth_code TEXT,
    last_share_code TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Pro matches (HLTV-sourced) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pro_matches (
    match_id     TEXT PRIMARY KEY,
    hltv_url     TEXT,
    map_name     TEXT NOT NULL REFERENCES maps (map_name),
    event_name   TEXT,
    team_ct      TEXT,
    team_t       TEXT,
    score_ct     SMALLINT,
    score_t      SMALLINT,
    match_date   DATE,
    -- Abs path to directory holding {match_id}_{field}.parquet files
    parquet_dir  TEXT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pro_matches_map ON pro_matches (map_name);

-- ── User matches ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_matches (
    demo_id         TEXT PRIMARY KEY,
    steam_id        TEXT        NOT NULL REFERENCES users (steam_id) ON DELETE CASCADE,
    map_name        TEXT        NOT NULL REFERENCES maps (map_name),
    share_code      TEXT,
    match_date      TIMESTAMPTZ,
    score_ct        SMALLINT,
    score_t         SMALLINT,
    user_side_first TEXT CHECK (user_side_first IN ('ct', 't')),
    user_result     TEXT CHECK (user_result IN ('win', 'draw', 'loss')),
    kills           SMALLINT,
    deaths          SMALLINT,
    assists         SMALLINT,
    hs_pct          FLOAT,
    round_count     SMALLINT,
    -- Abs path to directory holding {demo_id}_{field}.parquet files
    parquet_dir     TEXT,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_matches_steam ON user_matches (steam_id);
CREATE INDEX IF NOT EXISTS idx_user_matches_map   ON user_matches (map_name);

-- ── Event windows (retrieval corpus for user/pro situation matching) ─────────
CREATE TABLE IF NOT EXISTS event_windows (
    window_id       TEXT PRIMARY KEY,
    source_type     TEXT        NOT NULL CHECK (source_type IN ('user', 'pro')),
    source_match_id TEXT        NOT NULL,
    steam_id        TEXT,
    map_name        TEXT        NOT NULL REFERENCES maps (map_name),
    round_num       SMALLINT    NOT NULL,
    start_tick      INTEGER     NOT NULL,
    anchor_tick     INTEGER     NOT NULL,
    end_tick        INTEGER     NOT NULL,
    side_to_query   TEXT        CHECK (side_to_query IN ('ct', 't')),
    phase           TEXT,
    site            TEXT,
    anchor_kind     TEXT,
    alive_ct        SMALLINT,
    alive_t         SMALLINT,
    feature_version TEXT        NOT NULL,
    feature_path    TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_windows_source
    ON event_windows (source_type, source_match_id);
CREATE INDEX IF NOT EXISTS idx_event_windows_map_phase
    ON event_windows (map_name, phase, source_type);
CREATE INDEX IF NOT EXISTS idx_event_windows_anchor
    ON event_windows (map_name, round_num, anchor_tick);

-- ── Job runs (audit trail for scheduled workers) ──────────────────────────────
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

CREATE INDEX IF NOT EXISTS idx_job_runs_name_start ON job_runs (job_name, started_at DESC);

-- ── Seed: all 7 competitive maps ──────────────────────────────────────────────
INSERT INTO maps (map_name, display_name, pos_x, pos_y, map_scale, has_lower_level, lower_level_max_z) VALUES
    ('de_ancient',  'Ancient',  -2953, 2164, 5.0,  FALSE, -1000000.0),
    ('de_anubis',   'Anubis',   -2796, 3328, 5.22, FALSE, -1000000.0),
    ('de_dust2',    'Dust 2',   -2476, 3239, 4.4,  FALSE, -1000000.0),
    ('de_inferno',  'Inferno',  -2087, 3870, 4.9,  FALSE, -1000000.0),
    ('de_mirage',   'Mirage',   -3230, 1713, 5.0,  FALSE, -1000000.0),
    ('de_nuke',     'Nuke',     -3453, 2887, 7.0,  TRUE,  -495.0),
    ('de_overpass', 'Overpass', -4831, 1781, 5.2,  FALSE, -1000000.0)
ON CONFLICT DO NOTHING;
