-- Migration 035: backfill coverage rows for tradable instruments that
-- joined the universe after the initial seed and therefore have no row.
--
-- seed_coverage is a first-run-only bootstrap: once the coverage table
-- is non-empty, subsequent universe syncs never add rows for newly-
-- added instruments. Over time this leaves a growing set of tradable
-- instruments without any coverage row — invisible to every
-- UPDATE-based coverage audit / gate because those no-op on missing
-- rows.
--
-- Fix is twofold:
--   1. nightly_universe_sync now also calls bootstrap_missing_coverage_rows
--      to close the gap going forward (see PR for #292).
--   2. This migration closes the backlog for any instrument that is
--      currently tradable but has no coverage row.
--
-- Idempotent: NOT EXISTS + ON CONFLICT DO NOTHING. Safe to re-run.

INSERT INTO coverage (instrument_id, coverage_tier)
SELECT i.instrument_id, 3
FROM instruments i
WHERE i.is_tradable = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM coverage c WHERE c.instrument_id = i.instrument_id
  )
ON CONFLICT DO NOTHING;
