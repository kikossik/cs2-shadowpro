-- Move the simplified app tables out of public into the shadowpro schema.
--
-- Run after 20260429_simplify_schema.sql. Legacy *_legacy_20260429 tables stay
-- in public, while the app's current tables live in shadowpro.

BEGIN;

CREATE SCHEMA IF NOT EXISTS shadowpro;
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

ALTER TABLE IF EXISTS public.maps SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.users SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.events SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.teams SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.games SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.event_windows SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.round_analysis_cache SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.demo_jobs SET SCHEMA shadowpro;
ALTER TABLE IF EXISTS public.job_runs SET SCHEMA shadowpro;
ALTER SEQUENCE IF EXISTS public.job_runs_id_seq SET SCHEMA shadowpro;

-- Make psql sessions land on the app schema by default too. The application
-- also sets search_path on every asyncpg connection.
DO $$
BEGIN
    EXECUTE format(
        'ALTER DATABASE %I SET search_path = shadowpro, public',
        current_database()
    );
END $$;

COMMIT;
