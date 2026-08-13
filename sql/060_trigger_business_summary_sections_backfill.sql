-- 060_trigger_business_summary_sections_backfill.sql
--
-- #449 — trigger a sections backfill on instruments that already have
-- a blob-only ``instrument_business_summary`` row from migration 055.
--
-- Migration 059 shipped the sections table empty. The ingester's 7-day
-- TTL skips recently-parsed instruments, so rows whose ``last_parsed_at``
-- was stamped inside the last week would *not* re-enter the candidate
-- list on the next scheduled tick — those instruments would render an
-- empty sections panel despite already having a blob summary.
--
-- Fix: back-date ``last_parsed_at`` on every row that has a non-empty
-- body (real extraction, not a tombstone) so the ingester picks it up
-- on its next pass and populates the matching sections rows. The blob
-- itself is preserved — the re-parse writes both blob (unchanged
-- content on identical HTML) and sections.
--
-- Rows with an empty body (tombstones) are left alone: they failed
-- first time and shouldn't be hammered again outside the normal TTL.

UPDATE instrument_business_summary
SET last_parsed_at = TIMESTAMP '1970-01-01 00:00:00+00'
WHERE body <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM instrument_business_summary_sections s
      WHERE s.instrument_id = instrument_business_summary.instrument_id
        AND s.source_accession = instrument_business_summary.source_accession
  );
