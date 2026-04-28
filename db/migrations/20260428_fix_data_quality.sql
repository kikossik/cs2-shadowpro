-- Fix data-quality issues in the dimensional model.
-- Safe to run repeatedly.

-- Issue 3: source_event_id was stored as '' instead of NULL for events that
-- have no HLTV event ID.  NULL is the correct sentinel; '' breaks the
-- UNIQUE (source_type, source_event_id) constraint semantics (PostgreSQL
-- treats '' as a real value, so multiple hltv events with source_event_id=''
-- would conflict, while multiple NULLs coexist fine).
UPDATE events
SET source_event_id = NULL
WHERE source_event_id = '';

-- Issue 4: Round analysis results cached under the old matcher version
-- (clean-v2) are never reused — the cache key includes the version, so they
-- only surface as stale fallbacks that trigger needless recomputation.
-- Delete them so the cache table only contains current-version results.
DELETE FROM round_analysis_results
WHERE matcher_version <> 'clean-v3';
