-- Migration 034: external_data_watermarks (issue #269)
--
-- A single per-source watermark store used by every incremental-fetch
-- adapter to remember "what was the newest thing we saw last time from
-- this provider." Replaces the pattern of each job rolling its own
-- bespoke storage (per-row last_verified_at on domain tables, implicit
-- "latest job_runs row" lookups, etc.).
--
-- The `source` column is a stable string identifier for the data source
-- under a specific fetch contract — e.g. `sec.tickers` for the CIK
-- mapping, `sec.submissions` for per-company filings listings,
-- `frankfurter.latest` for the ECB rate feed. Keep this identifier
-- documented in docs/superpowers/plans/2026-04-17-lightweight-etl-audit.md
-- so future adapters pick the same name.
--
-- The `key` column is the per-entity key: CIK for per-company SEC
-- watermarks, `global` for singleton sources (no per-entity dimension).
--
-- `watermark` is the opaque provider-native token — ETag string,
-- accession_number, ISO date, etc. The caller knows how to interpret it;
-- this table is pure key-value storage.

CREATE TABLE IF NOT EXISTS external_data_watermarks (
    source          TEXT NOT NULL,
    key             TEXT NOT NULL,
    watermark       TEXT NOT NULL,
    watermark_at    TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    response_hash   TEXT,
    PRIMARY KEY (source, key)
);

-- Index helps `SELECT key FROM external_data_watermarks WHERE source = ?`
-- scans which the incremental-fetch jobs run to enumerate "CIKs we have
-- a prior watermark for" vs "CIKs still needing initial backfill."
CREATE INDEX IF NOT EXISTS idx_external_data_watermarks_source
    ON external_data_watermarks(source);
