-- 191_backfill_insider_observations_filed_at.sql
--
-- #899 — insider observations `filed_at` carried event-date semantics
-- (Form 4: txn_date @ midnight; Form 3: as_of @ midnight) on the
-- write-through + legacy-sync paths, while the bulk insider dataset
-- path wrote the TRUE SEC filing date — one column, mixed meanings.
-- The trade/as-of date is not lost by this rewrite: it is the natural
-- key's `period_end`. `filed_at` is NOT part of the observation
-- conflict identity (sql/113), so this is a pure value UPDATE with no
-- identity drift.
--
-- Backfill: stamp the SEC filing timestamp from the canonical
-- `sec_filing_manifest` (#1233), falling back to
-- `filing_events.filing_date` for legacy-cohort accessions with no
-- manifest row. Rows whose accession resolves in NEITHER source are
-- left untouched (the writer paths keep the event-date fallback for
-- the same cohort, logged). Scoped to source IN ('form4','form3') —
-- Form 5 observations are written under source='form4' by design
-- (manifest_parsers/insider_345.py module docstring) and resolve via
-- their own accessions in the same join.
--
-- `ownership_insiders_current` is NOT touched here: backfilled
-- filed_at participates in the refresh MERGE's winner tie-break, so
-- mirrors must be RECOMPUTED (refresh_insiders_current_batch over
-- affected instruments — operator step recorded in the PR), not
-- value-copied. A plain UPDATE also does not advance observations
-- `ingested_at`, deliberately: the drift sweep must not treat a
-- semantic relabel as fresh ingest data.
--
-- No explicit BEGIN/COMMIT: the migration runner wraps the body + the
-- schema_migrations INSERT in one transaction
-- (app/db/migrations.run_migrations).

UPDATE ownership_insiders_observations o
SET filed_at = src.sec_filed_at
FROM (
    SELECT m.accession_number, m.filed_at AS sec_filed_at
    FROM sec_filing_manifest m
    UNION ALL
    -- DATE -> timestamptz pinned to UTC (sql/148 precedent): a bare
    -- ::timestamptz cast is session-timezone dependent and can shift
    -- the UTC calendar date.
    SELECT fe.provider_filing_id, MAX(fe.filing_date)::timestamp AT TIME ZONE 'UTC'
    FROM filing_events fe
    WHERE fe.provider = 'sec'
      AND NOT EXISTS (
          SELECT 1 FROM sec_filing_manifest m2
          WHERE m2.accession_number = fe.provider_filing_id
      )
    GROUP BY fe.provider_filing_id
) AS src
WHERE o.source IN ('form4', 'form3')
  AND o.source_accession = src.accession_number
  AND src.sec_filed_at IS NOT NULL
  AND o.filed_at IS DISTINCT FROM src.sec_filed_at;
