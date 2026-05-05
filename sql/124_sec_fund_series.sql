-- 124_sec_fund_series.sql
--
-- Issue #917 — N-PORT mutual-fund holdings ingest (Phase 3 PR1).
--
-- Reference table mapping ``fund_series_id`` → canonical name +
-- filer CIK. Populated incrementally by the N-PORT ingester (UPSERT
-- on every accession). Decoupled from ``ownership_funds_observations``
-- so the rollup endpoint (in #919) can JOIN to a small dedicated
-- reference table to render fund names without scanning the
-- partitioned observations parent.
--
-- ``fund_filer_cik`` is intentionally NOT a foreign key to
-- ``institutional_filers`` — many RICs are not 13F-HR filers and
-- therefore do not have an ``institutional_filers`` row. The N-PORT
-- ingester observes the filer CIK from the filing header; the
-- universe walk for fund-filer discovery is a follow-up
-- (out-of-scope for #917).
--
-- ``last_seen_period_end`` lets the ingester skip series whose latest
-- public N-PORT is older than the configured staleness window
-- without re-fetching the submissions index.

BEGIN;

CREATE TABLE IF NOT EXISTS sec_fund_series (
    fund_series_id          TEXT PRIMARY KEY CHECK (fund_series_id ~ '^S[0-9]{9}$'),
    fund_series_name        TEXT NOT NULL,
    fund_filer_cik          TEXT NOT NULL,
    last_seen_period_end    DATE,
    first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sec_fund_series_filer
    ON sec_fund_series (fund_filer_cik);

COMMENT ON TABLE sec_fund_series IS
    'Reference table for SEC fund series (N-PORT seriesId), populated by the N-PORT ingester. Decoupled from institutional_filers because not every RIC is a 13F-HR filer.';

COMMIT;
