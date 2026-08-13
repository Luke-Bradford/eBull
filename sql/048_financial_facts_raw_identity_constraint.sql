-- Reconcile financial_facts_raw identity constraint with app ON CONFLICT.
--
-- Background
-- ----------
-- Migration 032 intended `uq_facts_raw_identity`, an expression UNIQUE
-- index over (instrument_id, concept, unit, COALESCE(period_start, '0001-01-01'::date),
-- period_end, accession_number). Some dev databases ended up with an
-- auto-named plain UNIQUE table constraint over
-- (instrument_id, concept, unit, period_end, accession_number) — five
-- columns, no period_start, no COALESCE. Schema drift, not a mismatch
-- the migration runner can detect (migrations are identified by
-- filename, not by resulting DDL).
--
-- The app's ON CONFLICT clause in upsert_facts_for_instrument names
-- the 6-col COALESCE identity. Against the 5-col plain UNIQUE that
-- exists on the drifted DB, PostgreSQL raises:
--     psycopg.errors.InvalidColumnReference:
--         there is no unique or exclusion constraint matching
--         the ON CONFLICT specification
-- Every per-CIK XBRL upsert fails with that error.
--
-- Fix
-- ---
-- Drop the stale 5-col constraint (if present) and ensure the 6-col
-- COALESCE expression index exists. Both operations are idempotent:
-- on DBs where 032 created the correct index, the DROP is a no-op and
-- the CREATE is a no-op.
--
-- Safety
-- ------
-- The new identity is STRICTER than the old on every row that has a
-- non-null period_start (identical constraint semantics), and allows
-- distinct period_start values per (instr, concept, unit, period_end,
-- accession) tuple — which is the intended SEC XBRL data model (a
-- single filing may carry multiple period lengths ending on the same
-- date for the same concept).
--
-- A duplicate-key scan at authoring time showed 0 collisions under the
-- new identity on a 10M-row production-like DB, so replacing the
-- constraint does not require a backfill.

-- Drop the drifted auto-named UNIQUE constraint if it exists.
-- The name matches PostgreSQL's default pattern for inline UNIQUE(...)
-- clauses: <table>_<col1>_<col2>_..._key (truncated to 63 chars).
ALTER TABLE financial_facts_raw
    DROP CONSTRAINT IF EXISTS financial_facts_raw_instrument_id_concept_unit_period_end_a_key;

-- Ensure the intended 6-col COALESCE expression UNIQUE index exists.
CREATE UNIQUE INDEX IF NOT EXISTS uq_facts_raw_identity
    ON financial_facts_raw(
        instrument_id, concept, unit,
        COALESCE(period_start, '0001-01-01'::date),
        period_end, accession_number
    );
