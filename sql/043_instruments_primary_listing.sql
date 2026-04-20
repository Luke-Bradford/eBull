-- 043_instruments_primary_listing.sql
--
-- Slice 0 of per-stock research page spec (docs/superpowers/specs/2026-04-20-per-stock-research-page.md).
--
-- Problem: `instruments.symbol` is indexed but not UNIQUE. Symbol
-- collisions across exchanges exist (e.g. `VOD` on NMS vs `VOD.L` on
-- LSE — distinct instruments that share a ticker prefix), and the
-- current `UPPER(symbol) = %s LIMIT 1` lookups in app/api/instruments.py
-- pick winners nondeterministically.
--
-- Fix: tag one listing per symbol as the primary. The research page
-- resolves symbol → instrument_id as:
--   1. ORDER BY is_primary_listing DESC, instrument_id ASC LIMIT 1
--   2. Alternate listings surfaced via a `?id=` override on the
--      research URL (see spec §2).
--
-- Backfill strategy (non-destructive):
--   - Default value TRUE; every existing row becomes is_primary_listing=TRUE.
--   - Then demote collisions: for any symbol with >1 row, keep the
--     lowest instrument_id as TRUE and mark the rest FALSE.
--   - Future ingests set TRUE by default; the universe sync can
--     explicitly flip when a new listing becomes primary.

ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS is_primary_listing BOOLEAN NOT NULL DEFAULT TRUE;

-- Demote collisions: within each (UPPER(symbol)) group, keep the row
-- with the lowest instrument_id as primary, mark the rest secondary.
-- Guarded with `AND i.is_primary_listing = TRUE` so a re-run of this
-- migration does not undo an operator's manual promotion of a
-- non-minimum instrument_id.
UPDATE instruments AS i
SET is_primary_listing = FALSE
WHERE i.is_primary_listing = TRUE
  AND EXISTS (
    SELECT 1
    FROM instruments AS j
    WHERE UPPER(j.symbol) = UPPER(i.symbol)
      AND j.instrument_id < i.instrument_id
      AND j.is_primary_listing = TRUE
);

-- NOT adding a partial-unique index on `UPPER(symbol) WHERE
-- is_primary_listing`: `sync_universe()` upserts on `instrument_id`
-- and has no collision-aware logic for setting `is_primary_listing`
-- on a new row whose UPPER(symbol) already exists elsewhere. A unique
-- constraint would abort the whole universe sync transaction on the
-- first collision. The application-level tiebreaker in the symbol→id
-- lookups (`ORDER BY is_primary_listing DESC, instrument_id ASC`) is
-- sufficient for deterministic resolution; enforcing at-most-one-primary
-- at the DB level is deferred until the ingest path learns to demote
-- correctly. Filed as a separate tech-debt follow-up.
-- Non-unique expression index to support the ORDER BY tiebreaker.
-- The lookups filter on `UPPER(symbol)` so a plain `(symbol, ...)`
-- btree would not seek; the expression leads with `UPPER(symbol)` to
-- match the predicate.
CREATE INDEX IF NOT EXISTS idx_instruments_symbol_primary
    ON instruments ((UPPER(symbol)), is_primary_listing DESC, instrument_id);
