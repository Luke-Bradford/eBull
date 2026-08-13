-- runner: autocommit
-- #1685 — partial index for the recent-first manifest slice. Serves the
-- per-source recent query (`WHERE ingest_status='pending' AND source=? AND
-- filed_at>=? ORDER BY filed_at DESC, accession_number DESC`) so a worker tick
-- does not scan the ~1.46M-row pending backlog. Built CONCURRENTLY (no worker
-- lock) — hence the autocommit directive (CONCURRENTLY cannot run in a tx
-- block). The leading DROP CONCURRENTLY IF EXISTS clears any INVALID index a
-- prior interrupted concurrent build may have left under this name (a bare
-- CREATE ... IF NOT EXISTS would skip it). Both statements are idempotent.
DROP INDEX CONCURRENTLY IF EXISTS idx_manifest_recent;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_manifest_recent
  ON sec_filing_manifest (source, filed_at DESC, accession_number DESC)
  WHERE ingest_status = 'pending';
