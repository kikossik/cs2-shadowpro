-- CS2 ShadowPro Clean — PostgreSQL schema
-- Run: psql -d <db_name> -f schema.sql

CREATE EXTENSION IF NOT EXISTS vector;

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

-- ── Match dimensions ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_types (
    match_type  TEXT PRIMARY KEY,
    description TEXT
);

INSERT INTO match_types (match_type, description) VALUES
    ('unknown',     'Unknown or not yet classified'),
    ('premier',     'Valve Premier match'),
    ('competitive', 'Valve Competitive match'),
    ('faceit',      'FACEIT match'),
    ('hltv',        'HLTV-sourced pro match')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS events (
    event_id         TEXT PRIMARY KEY,
    source_type      TEXT NOT NULL,
    source_event_id  TEXT,
    event_name       TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_type, source_event_id),
    UNIQUE (source_type, event_name)
);

CREATE TABLE IF NOT EXISTS teams (
    team_id          TEXT PRIMARY KEY,
    source_type      TEXT NOT NULL,
    source_team_id   TEXT,
    team_name        TEXT NOT NULL,
    normalized_name  TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_type, source_team_id),
    UNIQUE (source_type, normalized_name)
);

CREATE TABLE IF NOT EXISTS matches (
    match_id          TEXT PRIMARY KEY,
    source_type       TEXT NOT NULL CHECK (source_type IN ('user', 'pro')),
    match_type        TEXT NOT NULL DEFAULT 'unknown' REFERENCES match_types (match_type),
    external_match_id TEXT,
    source_url        TEXT,
    source_slug       TEXT,
    share_code        TEXT,
    steam_id          TEXT REFERENCES users (steam_id) ON DELETE CASCADE,
    event_id          TEXT REFERENCES events (event_id),
    played_at         TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_type, external_match_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_source_type ON matches (source_type, match_type);
CREATE INDEX IF NOT EXISTS idx_matches_steam ON matches (steam_id, played_at DESC);
CREATE INDEX IF NOT EXISTS idx_matches_event ON matches (event_id, played_at DESC);

CREATE TABLE IF NOT EXISTS match_teams (
    match_id          TEXT NOT NULL REFERENCES matches (match_id) ON DELETE CASCADE,
    team_slot         SMALLINT NOT NULL CHECK (team_slot IN (1, 2)),
    team_id           TEXT NOT NULL REFERENCES teams (team_id),
    source_team_name  TEXT,
    PRIMARY KEY (match_id, team_slot)
);

-- A game is one parsed map/demo. This is the grain used by replay, retrieval,
-- artifacts, and round facts.
CREATE TABLE IF NOT EXISTS games (
    game_id                TEXT PRIMARY KEY,
    match_id               TEXT NOT NULL REFERENCES matches (match_id) ON DELETE CASCADE,
    source_type            TEXT NOT NULL CHECK (source_type IN ('user', 'pro')),
    map_name               TEXT NOT NULL REFERENCES maps (map_name),
    map_number             SMALLINT,
    demo_stem              TEXT NOT NULL,
    demo_path              TEXT,
    parquet_dir            TEXT,
    artifact_path          TEXT,
    ct_round_wins          SMALLINT,
    t_round_wins           SMALLINT,
    team1_score            SMALLINT,
    team2_score            SMALLINT,
    round_count            SMALLINT,
    tick_rate              SMALLINT,
    parser_version         TEXT,
    artifact_version       TEXT,
    window_feature_version TEXT,
    ingest_status          TEXT NOT NULL DEFAULT 'pending'
        CHECK (ingest_status IN ('pending', 'processing', 'ready', 'error')),
    ingest_error           TEXT,
    ingested_at            TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_games_match ON games (match_id);
CREATE INDEX IF NOT EXISTS idx_games_source_map ON games (source_type, map_name);
CREATE INDEX IF NOT EXISTS idx_games_status ON games (ingest_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS game_teams (
    game_id          TEXT NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    team_id          TEXT NOT NULL REFERENCES teams (team_id),
    source_team_slot SMALLINT CHECK (source_team_slot IN (1, 2)),
    side_first       TEXT CHECK (side_first IN ('ct', 't')),
    score            SMALLINT,
    won              BOOLEAN,
    PRIMARY KEY (game_id, team_id)
);

CREATE TABLE IF NOT EXISTS rounds (
    game_id           TEXT     NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    round_num         SMALLINT NOT NULL,
    start_tick        INTEGER,
    freeze_end_tick   INTEGER,
    end_tick          INTEGER,
    official_end_tick INTEGER,
    winner_side       TEXT CHECK (winner_side IN ('ct', 't')),
    reason            TEXT,
    bomb_plant_tick   INTEGER,
    bomb_site         TEXT,
    duration_ticks    INTEGER,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_id, round_num)
);

CREATE INDEX IF NOT EXISTS idx_rounds_winner ON rounds (winner_side);
CREATE INDEX IF NOT EXISTS idx_rounds_bomb_site ON rounds (bomb_site);

CREATE TABLE IF NOT EXISTS game_artifacts (
    artifact_id BIGSERIAL PRIMARY KEY,
    game_id     TEXT NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    kind        TEXT NOT NULL,
    version     TEXT NOT NULL,
    path        TEXT NOT NULL,
    content_hash TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (game_id, kind, version)
);

CREATE INDEX IF NOT EXISTS idx_game_artifacts_game ON game_artifacts (game_id, kind);

-- ── Legacy pro map demos (kept for compatibility during migration) ───────────
CREATE TABLE IF NOT EXISTS pro_matches (
    match_id     TEXT PRIMARY KEY,
    hltv_match_id TEXT,
    hltv_url     TEXT,
    map_name     TEXT NOT NULL REFERENCES maps (map_name),
    match_type   TEXT NOT NULL DEFAULT 'hltv' REFERENCES match_types (match_type),
    event_name   TEXT,
    team1_name   TEXT,
    team2_name   TEXT,
    team_ct      TEXT,
    team_t       TEXT,
    ct_round_wins SMALLINT,
    t_round_wins  SMALLINT,
    score_ct     SMALLINT,
    score_t      SMALLINT,
    round_count  SMALLINT,
    match_date   DATE,
    parquet_dir  TEXT,
    -- Path to single JSON artifact containing all rounds for this match.
    -- Populated by pipeline/steps/build_artifact.py after ingest.
    artifact_path TEXT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pro_matches_map ON pro_matches (map_name);

-- ── User matches ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_matches (
    demo_id         TEXT PRIMARY KEY,
    steam_id        TEXT        NOT NULL REFERENCES users (steam_id) ON DELETE CASCADE,
    map_name        TEXT        NOT NULL REFERENCES maps (map_name),
    share_code      TEXT,
    match_type      TEXT        NOT NULL DEFAULT 'unknown' REFERENCES match_types (match_type),
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
    parquet_dir     TEXT,
    -- Path to single JSON artifact containing all rounds for this match.
    artifact_path   TEXT,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_matches_steam ON user_matches (steam_id);
CREATE INDEX IF NOT EXISTS idx_user_matches_map   ON user_matches (map_name);

-- ── Event windows (retrieval corpus for user/pro situation matching) ─────────
CREATE TABLE IF NOT EXISTS event_windows (
    window_id       TEXT PRIMARY KEY,
    game_id         TEXT        REFERENCES games (game_id) ON DELETE CASCADE,
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
    embedding       vector(54),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_windows_source
    ON event_windows (source_type, source_match_id);
CREATE INDEX IF NOT EXISTS idx_event_windows_game
    ON event_windows (game_id);
CREATE INDEX IF NOT EXISTS idx_event_windows_map_phase
    ON event_windows (map_name, phase, source_type);
CREATE INDEX IF NOT EXISTS idx_event_windows_anchor
    ON event_windows (map_name, round_num, anchor_tick);

CREATE INDEX IF NOT EXISTS idx_event_windows_embedding
    ON event_windows USING hnsw (embedding vector_cosine_ops);

-- ── Round analysis cache (result state keyed by logic + corpus versions) ─────
CREATE TABLE IF NOT EXISTS round_analysis_results (
    cache_key           TEXT PRIMARY KEY,
    demo_id             TEXT        NOT NULL,
    round_num           SMALLINT    NOT NULL,
    logic               TEXT        NOT NULL CHECK (logic IN ('nav', 'original', 'both')),
    matcher_version     TEXT        NOT NULL,
    pro_corpus_version  TEXT        NOT NULL,
    status              TEXT        NOT NULL CHECK (status IN ('pending', 'done', 'error')),
    result_json         JSONB,
    error_message       TEXT,
    invalidated_at      TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_round_analysis_lookup
    ON round_analysis_results (demo_id, round_num, logic, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_round_analysis_versions
    ON round_analysis_results (logic, matcher_version, pro_corpus_version, updated_at DESC);

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
