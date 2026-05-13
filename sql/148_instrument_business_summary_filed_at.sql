-- 148_instrument_business_summary_filed_at.sql
--
-- Adds ``filed_at`` to ``instrument_business_summary`` so the
-- manifest-driven 10-K parser (#1151) can gate ``ON CONFLICT DO
-- UPDATE`` on filed_at-ASC drain order. Without this gate, the
-- manifest worker would render OLDEST → NEWEST 10-K body per
-- instrument during first-install drain: final state correct but
-- operator briefly sees the 2018 Item 1 narrative before the 2024
-- update fires.
--
-- Backfill matches by ``source_accession`` against
-- ``filing_events.provider_filing_id`` for SEC rows. Pre-#1151 rows
-- that pre-date their filing_events ancestor or were written from
-- a filing we no longer have stay NULL — the new conditional ON
-- CONFLICT treats NULL incumbents as "no incumbent" so the first
-- dated write re-baselines them cleanly. Tombstone rows from
-- ``record_parse_attempt`` carry no source filing and stay NULL
-- forever; they are out of the manifest parser's path entirely.
--
-- Population: ``instrument_business_summary`` holds at most one row
-- per instrument (~4031 in the current universe), so the backfill
-- UPDATE scans a bounded set.

ALTER TABLE instrument_business_summary
    ADD COLUMN IF NOT EXISTS filed_at TIMESTAMPTZ;

-- Coerce DATE → TIMESTAMPTZ explicitly under UTC so the result
-- doesn't depend on session timezone. ``::timestamptz`` would use
-- the session TZ (Codex pre-push round 2 finding) — the conditional
-- ON CONFLICT gate compares wall-clock instants, so a session-TZ
-- shift would silently move the gate's boundary.
UPDATE instrument_business_summary ibs
   SET filed_at = fe.filing_date::timestamp AT TIME ZONE 'UTC'
  FROM filing_events fe
 WHERE fe.provider = 'sec'
   AND fe.provider_filing_id = ibs.source_accession
   AND ibs.filed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_instrument_business_summary_filed_at
    ON instrument_business_summary (filed_at);

COMMENT ON COLUMN instrument_business_summary.filed_at IS
    'TIMESTAMPTZ of the 10-K filing this row was extracted from. '
    'Gates the conditional ON CONFLICT in upsert_business_summary '
    'so a filed_at-ASC manifest drain does not render stale-then-'
    'fresh. NULL means the row pre-dates the column or originated '
    'as a service-level tombstone (record_parse_attempt).';
