SET search_path TO shadowpro, public;

DROP TABLE IF EXISTS shadowpro.event_windows CASCADE;

ALTER TABLE IF EXISTS shadowpro.games
    DROP COLUMN IF EXISTS feature_version,
    DROP COLUMN IF EXISTS artifact_version,
    DROP COLUMN IF EXISTS artifact_path;
