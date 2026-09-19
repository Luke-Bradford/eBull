-- #2329 residual — repair the 27 `blockholder_filings` rows left with a NULL
-- `instrument_id` that a later, successful re-ingest was forbidden to fill.
--
-- Data repair only. No schema change, no DDL, one column, one bounded cohort.
--
-- ## What went wrong
--
-- `blockholders._upsert_filing_row` was `ON CONFLICT DO NOTHING`, enforcing the
-- documented per-reporter immutability contract (one accession x one reporter ==
-- one row). That is correct for ownership figures and wrong for the issuer LINK:
-- a NULL `instrument_id` is an absence, not a value, and `DO NOTHING` made the
-- absence permanent.
--
-- These 8 accessions were first ingested on 2026-06-14 12:44Z by the pre-#1628
-- CUSIP-only resolver, which correctly returned NULL. `c4f1d2e9` (#1628) added
-- the CIK single-sibling fallback at 2026-06-14 14:43Z — two hours later. The
-- manifest pipeline (`app/services/manifest_parsers/sec_13dg.py`) then re-ingested
-- all 8 on 2026-06-20..06-25, resolved the issuer, wrote
-- `ownership_blockholders_observations` and refreshed
-- `ownership_blockholders_current` — and could not touch the link here.
--
-- Operator-visible: both blockholder drill-through readers filter
-- `WHERE bf.instrument_id = %(iid)s` (`app/api/instruments.py:4575` and `:4657`),
-- so the ownership card listed the holder (sourced from the observation) while
-- the drill-through omitted the filing behind it.
--
-- The recurrence is fixed in code, in the same PR: the conflict action is now a
-- conditional `DO UPDATE` restricted to the NULL -> non-NULL transition on
-- `instrument_id`. This migration repairs only what is already stranded, since
-- the code fix reaches an accession only if something re-ingests it again.
--
-- ## Why the cohort is FROZEN and not derived at execution time
--
-- The obvious rule — "take the instrument from the accession's own live
-- observation" — selects exactly these 8 accessions on dev today (27 rows,
-- 8 instruments, 0 ambiguous, 0 rows carrying a conflicting non-NULL link).
-- But a migration that re-derives it would repair whatever qualifies whenever
-- and wherever it runs, which is not what was reviewed (Codex checkpoint 1
-- finding 8). `known_to IS NULL` also means *unretired*, not *authoritative*
-- (finding 9), and the observations DDL does not constrain `source_accession`
-- provenance (finding 10).
--
-- So the reviewed `(accession_number, instrument_id)` pairs are pinned below and
-- the live observation must STILL agree with the pinned value for a row to be
-- touched. Two independent authorities, both required:
--
--   * the pinned pair, reviewed against `_resolve_issuer_to_instrument_id` run
--     on each accession's stored `issuer_cusip` / `issuer_cik` (all 8 agree —
--     the repair reconciles, it never chooses); and
--   * the accession's current, unambiguous, live observation.
--
-- If either has moved since review, the affected rows are simply not updated and
-- stay NULL for a human to look at. Failing to repair is the safe direction.
--
-- Idempotent by construction: `instrument_id IS NULL` stops matching once
-- repaired. Expected effect on dev: 27 rows.

-- #2363 — never queue for a lock indefinitely behind an abandoned reader.
-- Ordinary DML here (no DDL, no ACCESS EXCLUSIVE), but a live ingest holding a
-- row lock on the same accession must not stall the migration silently.
SET lock_timeout = '15s';

WITH reviewed (accession_number, instrument_id) AS (
    VALUES
        ('0000919574-26-003322', 9092::BIGINT),
        ('0001104659-26-060004', 1050446),
        ('0001104659-26-063028', 1750),
        ('0001178913-26-003068', 1049659),
        ('0001274173-26-000157', 1049521),
        ('0001446580-26-000063', 2488),
        ('0001470831-26-000452', 8858),
        ('0001484529-26-000003', 12208)
),
-- Second authority: the accession's own live observation, still unambiguous.
live_observation AS (
    SELECT source_accession,
           min(instrument_id) AS instrument_id
    FROM   ownership_blockholders_observations
    WHERE  known_to IS NULL
      AND  source_accession IN (SELECT accession_number FROM reviewed)
    GROUP  BY source_accession
    HAVING count(DISTINCT instrument_id) = 1
),
confirmed AS (
    SELECT r.accession_number, r.instrument_id
    FROM   reviewed r
    JOIN   live_observation o
      ON   o.source_accession = r.accession_number
     AND   o.instrument_id    = r.instrument_id
)
UPDATE blockholder_filings b
SET    instrument_id = c.instrument_id
FROM   confirmed c
WHERE  b.accession_number = c.accession_number
  AND  b.instrument_id IS NULL;

RESET lock_timeout;
