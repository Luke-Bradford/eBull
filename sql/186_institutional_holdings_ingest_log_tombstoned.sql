-- 186_institutional_holdings_ingest_log_tombstoned.sql
--
-- #1532 — pre-2013 13F "archive index missing files" filings are
-- deterministically unparseable (the infotable-XML mandate started 2013).
-- The manifest worker (app/services/manifest_parsers/sec_13f_hr.py) already
-- TOMBSTONES them in sec_filing_manifest, but it ALSO stamped a
-- `status='failed'` row in institutional_holdings_ingest_log. The
-- ingest_sweep_adapter reds `sec_13f_sweep` on those 55k+ log `failed`
-- rows, contradicting #1530's "red = actionable" goal (a tombstone is a
-- deliberate permanent skip, not an operator action item).
--
-- This migration:
--   1. Widens the log `status` CHECK to admit 'tombstoned' (mirrors
--      sec_filing_manifest.ingest_status, which already has it).
--   2. Backfills existing archive-missing `failed` log rows -> 'tombstoned'
--      so the dev/prod page clears immediately.
--
-- Code change lands in the same PR so NEW archive-missing accessions are
-- written 'tombstoned' from both 13F ingest paths.
--
-- Scope of the backfill is matched on the deterministic error string, NOT
-- on filing year: a transient fetch timeout on a pre-2013 accession (e.g.
-- the lone manifest `failed` row, 0001085146-09-001941 = "fetch error:
-- The read operation timed out") is a RETRYABLE failure and must stay
-- `failed` until it drains to a real archive-missing tombstone.
--
-- No explicit BEGIN/COMMIT: the migration runner wraps the body + the
-- schema_migrations INSERT in one transaction (app/db/migrations.run_migrations);
-- the wider CHECK must commit-with the UPDATE so the reclassify doesn't
-- violate the old constraint mid-transaction (DDL is applied before the
-- DML below, in declaration order, within the same tx).
ALTER TABLE institutional_holdings_ingest_log
    DROP CONSTRAINT IF EXISTS institutional_holdings_ingest_log_status_check;
ALTER TABLE institutional_holdings_ingest_log
    ADD CONSTRAINT institutional_holdings_ingest_log_status_check
    CHECK (status IN ('success', 'partial', 'failed', 'tombstoned'));

UPDATE institutional_holdings_ingest_log
   SET status = 'tombstoned'
 WHERE status = 'failed'
   AND error LIKE 'archive index missing files%';
