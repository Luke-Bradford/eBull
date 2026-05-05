-- 125_n_port_ingest_log.sql
--
-- Issue #917 — N-PORT mutual-fund holdings ingest (Phase 3 PR1).
--
-- Per-accession tombstone log mirroring
-- ``institutional_holdings_ingest_log``. The ingester writes one row
-- per attempted accession (success / partial / failed) so re-runs
-- skip already-attempted work without re-fetching SEC.
--
-- ``fund_series_id`` is nullable because parse failures may abort
-- before the series identifier is extracted (e.g. malformed XML
-- header).
--
-- The accession is globally unique within SEC EDGAR; PRIMARY KEY on
-- ``accession_number`` alone is sufficient. Re-recording the same
-- accession overwrites the prior attempt — this lets a follow-up
-- run that succeeds promote a 'partial' or 'failed' accession to
-- 'success'.

BEGIN;

CREATE TABLE IF NOT EXISTS n_port_ingest_log (
    accession_number        TEXT PRIMARY KEY,
    filer_cik               TEXT NOT NULL,
    -- Codex pre-push review (2026-05-05) finding #5: same regex CHECK
    -- as ``ownership_funds_observations`` / ``ownership_funds_current`` /
    -- ``sec_fund_series`` so a synthetic series id can never land in
    -- the tombstone log either. Nullable because the field is unset
    -- on parse-failure tombstones (e.g. NPortMissingSeriesError fires
    -- before series_id is extracted).
    fund_series_id          TEXT
        CHECK (fund_series_id IS NULL OR fund_series_id ~ '^S[0-9]{9}$'),
    period_of_report        DATE,
    status                  TEXT NOT NULL
        CHECK (status IN ('success', 'partial', 'failed')),
    holdings_inserted       INTEGER NOT NULL DEFAULT 0,
    holdings_skipped        INTEGER NOT NULL DEFAULT 0,
    error                   TEXT,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_n_port_ingest_log_filer
    ON n_port_ingest_log (filer_cik);

CREATE INDEX IF NOT EXISTS idx_n_port_ingest_log_status
    ON n_port_ingest_log (status, fetched_at DESC);

COMMENT ON TABLE n_port_ingest_log IS
    'Per-accession tombstone for N-PORT ingest attempts (#917). Re-runs read this table via _existing_accessions_for_fund_filer to skip already-attempted accessions. Re-record overwrites the prior attempt.';

COMMIT;
