-- Migration 073 — purge FMP-sourced data from read paths (#532).
--
-- Stage 1 of FMP removal stopped future writes. This migration
-- purges already-ingested FMP-backed rows so user-visible
-- behaviour matches the new free-regulated-source-only posture.
--
-- Tables affected:
--
-- 1. ``financial_periods`` — DELETE WHERE source = 'fmp'.
--    Non-US instruments lose their FMP-derived rows. Honest
--    state under #532: those issuers have no regulated-source
--    fundamentals until per-region PRs (#516-#523) land their
--    own free providers.
--
-- 2. ``financial_periods_raw`` — same (the raw layer that feeds
--    ``financial_periods`` via the source-priority CASE).
--
-- 3. ``analyst_estimates`` — TRUNCATE. Table is FMP-only.
--    No replacement source in v1 (free analyst consensus is rare;
--    deferred to a follow-up spec).
--
-- 4. ``earnings_events`` — TRUNCATE. Table is FMP-only.
--    Replacement comes via SEC 8-K Item 2.02 / 8-K item events
--    (already wired) and per-region calendars in future PRs.
--
-- 5. ``fundamentals_snapshot`` — DELETE rows for instruments
--    without a primary SEC CIK. The table has no ``source``
--    column so SEC- and FMP-written rows are indistinguishable,
--    but rows for instruments without a SEC CIK can only have
--    been FMP-written. SEC-CIK rows stay (they get refreshed by
--    the next SEC fundamentals run; even if FMP previously
--    overlaid them, SEC overwrites on next ingest).
--
--    Stage 3 (#540) retrofits consumers to a regulated-source-
--    only model; stage 4 (#541) drops the table outright. Until
--    then this surgical purge keeps user-visible behaviour
--    honest under the FMP-free posture.
--
-- ``instrument_profile`` is FMP-only but its writers are still in
-- the codebase (gated by FMP_API_KEY which is now empty). Drop
-- the table in stage 2 (#539) along with the writer code.

BEGIN;

DELETE FROM financial_periods WHERE source = 'fmp';
DELETE FROM financial_periods_raw WHERE source = 'fmp';
TRUNCATE TABLE analyst_estimates;
TRUNCATE TABLE earnings_events;

DELETE FROM fundamentals_snapshot fs
 WHERE NOT EXISTS (
        SELECT 1 FROM external_identifiers e
         WHERE e.instrument_id  = fs.instrument_id
           AND e.provider       = 'sec'
           AND e.identifier_type = 'cik'
           AND e.is_primary     = TRUE
       );

COMMIT;
