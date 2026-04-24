-- Add canonical dimensional match/game/round schema.
-- Safe to run repeatedly against an existing clean database.

CREATE EXTENSION IF NOT EXISTS vector;

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
    artifact_id  BIGSERIAL PRIMARY KEY,
    game_id      TEXT NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    kind         TEXT NOT NULL,
    version      TEXT NOT NULL,
    path         TEXT NOT NULL,
    content_hash TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (game_id, kind, version)
);

CREATE INDEX IF NOT EXISTS idx_game_artifacts_game ON game_artifacts (game_id, kind);

ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS hltv_match_id TEXT;
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS match_type TEXT NOT NULL DEFAULT 'hltv' REFERENCES match_types (match_type);
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS team1_name TEXT;
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS team2_name TEXT;
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS ct_round_wins SMALLINT;
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS t_round_wins SMALLINT;
ALTER TABLE pro_matches ADD COLUMN IF NOT EXISTS round_count SMALLINT;

ALTER TABLE user_matches ADD COLUMN IF NOT EXISTS match_type TEXT NOT NULL DEFAULT 'unknown' REFERENCES match_types (match_type);
ALTER TABLE event_windows ADD COLUMN IF NOT EXISTS game_id TEXT REFERENCES games (game_id) ON DELETE CASCADE;
CREATE INDEX IF NOT EXISTS idx_event_windows_game ON event_windows (game_id);

UPDATE pro_matches
SET hltv_match_id = COALESCE(hltv_match_id, NULLIF(split_part(match_id, '_', 1), '')),
    team1_name = COALESCE(team1_name, team_ct),
    team2_name = COALESCE(team2_name, team_t),
    ct_round_wins = COALESCE(ct_round_wins, score_ct),
    t_round_wins = COALESCE(t_round_wins, score_t),
    round_count = COALESCE(round_count, score_ct + score_t)
WHERE hltv_match_id IS NULL
   OR team1_name IS NULL
   OR team2_name IS NULL
   OR ct_round_wins IS NULL
   OR t_round_wins IS NULL
   OR round_count IS NULL;

INSERT INTO events (event_id, source_type, event_name, updated_at)
SELECT 'hltv_event_' || substr(md5(lower(trim(event_name))), 1, 16),
       'hltv',
       event_name,
       NOW()
FROM (
    SELECT DISTINCT event_name
    FROM pro_matches
    WHERE event_name IS NOT NULL AND trim(event_name) <> ''
) event_names
WHERE event_name IS NOT NULL AND trim(event_name) <> ''
ON CONFLICT (source_type, event_name) DO UPDATE
    SET event_name = EXCLUDED.event_name,
        updated_at = NOW();

INSERT INTO matches (
    match_id, source_type, match_type, external_match_id, source_url,
    source_slug, event_id, played_at, updated_at
)
SELECT match_id_key,
       'pro',
       'hltv',
       external_match_id,
       hltv_url,
       source_slug,
       event_id,
       match_date::timestamptz,
       NOW()
FROM (
    SELECT DISTINCT ON (COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)))
        'hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)) AS match_id_key,
        COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)) AS external_match_id,
        hltv_url,
        CASE WHEN position('_' IN match_id) > 0 THEN substring(match_id from position('_' IN match_id) + 1) ELSE match_id END AS source_slug,
        CASE
            WHEN event_name IS NULL OR trim(event_name) = '' THEN NULL
            ELSE 'hltv_event_' || substr(md5(lower(trim(event_name))), 1, 16)
        END AS event_id,
        match_date
    FROM pro_matches
    ORDER BY COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)), match_date DESC NULLS LAST
) source_matches
ON CONFLICT (match_id) DO UPDATE
    SET source_url = COALESCE(EXCLUDED.source_url, matches.source_url),
        source_slug = COALESCE(EXCLUDED.source_slug, matches.source_slug),
        event_id = COALESCE(EXCLUDED.event_id, matches.event_id),
        played_at = COALESCE(EXCLUDED.played_at, matches.played_at),
        updated_at = NOW();

INSERT INTO teams (team_id, source_type, team_name, normalized_name, updated_at)
SELECT 'team_' || substr(md5(lower(trim(team_name))), 1, 16),
       'hltv',
       team_name,
       lower(trim(team_name)),
       NOW()
FROM (
    SELECT team1_name AS team_name FROM pro_matches WHERE team1_name IS NOT NULL AND trim(team1_name) <> ''
    UNION
    SELECT team2_name AS team_name FROM pro_matches WHERE team2_name IS NOT NULL AND trim(team2_name) <> ''
) names
ON CONFLICT (source_type, normalized_name) DO UPDATE
    SET team_name = EXCLUDED.team_name,
        updated_at = NOW();

INSERT INTO match_teams (match_id, team_slot, team_id, source_team_name)
SELECT DISTINCT ON ('hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)))
       'hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)),
       1,
       'team_' || substr(md5(lower(trim(team1_name))), 1, 16),
       team1_name
FROM pro_matches
WHERE team1_name IS NOT NULL AND trim(team1_name) <> ''
ON CONFLICT (match_id, team_slot) DO UPDATE
    SET team_id = EXCLUDED.team_id,
        source_team_name = EXCLUDED.source_team_name;

INSERT INTO match_teams (match_id, team_slot, team_id, source_team_name)
SELECT DISTINCT ON ('hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)))
       'hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)),
       2,
       'team_' || substr(md5(lower(trim(team2_name))), 1, 16),
       team2_name
FROM pro_matches
WHERE team2_name IS NOT NULL AND trim(team2_name) <> ''
ON CONFLICT (match_id, team_slot) DO UPDATE
    SET team_id = EXCLUDED.team_id,
        source_team_name = EXCLUDED.source_team_name;

INSERT INTO games (
    game_id, match_id, source_type, map_name, demo_stem, parquet_dir, artifact_path,
    ct_round_wins, t_round_wins, round_count, tick_rate, artifact_version,
    window_feature_version, ingest_status, ingested_at, updated_at
)
SELECT match_id,
       'hltv_' || COALESCE(NULLIF(hltv_match_id, ''), split_part(match_id, '_', 1)),
       'pro',
       map_name,
       match_id,
       parquet_dir,
       artifact_path,
       ct_round_wins,
       t_round_wins,
       round_count,
       64,
       CASE WHEN artifact_path IS NOT NULL THEN 'clean-v2' ELSE NULL END,
       'v2',
       CASE WHEN parquet_dir IS NOT NULL AND artifact_path IS NOT NULL THEN 'ready' ELSE 'pending' END,
       ingested_at,
       NOW()
FROM pro_matches
ON CONFLICT (game_id) DO UPDATE
    SET match_id = EXCLUDED.match_id,
        map_name = EXCLUDED.map_name,
        parquet_dir = COALESCE(EXCLUDED.parquet_dir, games.parquet_dir),
        artifact_path = COALESCE(EXCLUDED.artifact_path, games.artifact_path),
        ct_round_wins = COALESCE(EXCLUDED.ct_round_wins, games.ct_round_wins),
        t_round_wins = COALESCE(EXCLUDED.t_round_wins, games.t_round_wins),
        round_count = COALESCE(EXCLUDED.round_count, games.round_count),
        artifact_version = COALESCE(EXCLUDED.artifact_version, games.artifact_version),
        window_feature_version = COALESCE(EXCLUDED.window_feature_version, games.window_feature_version),
        ingest_status = EXCLUDED.ingest_status,
        ingested_at = COALESCE(EXCLUDED.ingested_at, games.ingested_at),
        updated_at = NOW();

INSERT INTO game_artifacts (game_id, kind, version, path, updated_at)
SELECT match_id, 'match_artifact', 'clean-v2', artifact_path, NOW()
FROM pro_matches
WHERE artifact_path IS NOT NULL
ON CONFLICT (game_id, kind, version) DO UPDATE
    SET path = EXCLUDED.path,
        updated_at = NOW();

INSERT INTO matches (
    match_id, source_type, match_type, external_match_id, share_code,
    steam_id, played_at, updated_at
)
SELECT 'user_' || demo_id,
       'user',
       match_type,
       demo_id,
       share_code,
       steam_id,
       match_date,
       NOW()
FROM user_matches
ON CONFLICT (match_id) DO UPDATE
    SET match_type = EXCLUDED.match_type,
        share_code = COALESCE(EXCLUDED.share_code, matches.share_code),
        steam_id = EXCLUDED.steam_id,
        played_at = COALESCE(EXCLUDED.played_at, matches.played_at),
        updated_at = NOW();

INSERT INTO games (
    game_id, match_id, source_type, map_name, demo_stem, parquet_dir, artifact_path,
    ct_round_wins, t_round_wins, round_count, tick_rate, artifact_version,
    window_feature_version, ingest_status, ingested_at, updated_at
)
SELECT demo_id,
       'user_' || demo_id,
       'user',
       map_name,
       demo_id,
       parquet_dir,
       artifact_path,
       NULL,
       NULL,
       round_count,
       64,
       CASE WHEN artifact_path IS NOT NULL THEN 'clean-v2' ELSE NULL END,
       'v2',
       CASE WHEN parquet_dir IS NOT NULL THEN 'ready' ELSE 'pending' END,
       processed_at,
       NOW()
FROM user_matches
ON CONFLICT (game_id) DO UPDATE
    SET match_id = EXCLUDED.match_id,
        map_name = EXCLUDED.map_name,
        parquet_dir = COALESCE(EXCLUDED.parquet_dir, games.parquet_dir),
        artifact_path = COALESCE(EXCLUDED.artifact_path, games.artifact_path),
        round_count = COALESCE(EXCLUDED.round_count, games.round_count),
        artifact_version = COALESCE(EXCLUDED.artifact_version, games.artifact_version),
        window_feature_version = COALESCE(EXCLUDED.window_feature_version, games.window_feature_version),
        ingest_status = EXCLUDED.ingest_status,
        ingested_at = COALESCE(EXCLUDED.ingested_at, games.ingested_at),
        updated_at = NOW();

INSERT INTO game_artifacts (game_id, kind, version, path, updated_at)
SELECT demo_id, 'match_artifact', 'clean-v2', artifact_path, NOW()
FROM user_matches
WHERE artifact_path IS NOT NULL
ON CONFLICT (game_id, kind, version) DO UPDATE
    SET path = EXCLUDED.path,
        updated_at = NOW();

UPDATE event_windows
SET game_id = source_match_id
WHERE game_id IS NULL
  AND EXISTS (SELECT 1 FROM games WHERE games.game_id = event_windows.source_match_id);
