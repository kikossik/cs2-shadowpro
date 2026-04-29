-- Drop and recreate round_analysis_cache without the unused versioning axes.
-- Existing rows are stale (algorithm is being replaced), so a destructive reset
-- is fine and cheaper than a data-preserving migration.

SET search_path TO shadowpro, public;

DROP TABLE IF EXISTS round_analysis_cache;

CREATE TABLE round_analysis_cache (
    game_id        TEXT     NOT NULL REFERENCES games (game_id) ON DELETE CASCADE,
    round_num      SMALLINT NOT NULL CHECK (round_num > 0),
    status         TEXT     NOT NULL CHECK (status IN ('pending', 'done', 'error')),
    result_json    JSONB,
    error_message  TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_id, round_num)
);
