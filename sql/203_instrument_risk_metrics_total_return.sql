-- 203_instrument_risk_metrics_total_return.sql
--
-- #1635 + #1633-vnext — SEC-derived total-return series in the risk-metrics
-- evidence layer, feeding the scoring v1.3 Calmar reward.
-- Spec: docs/specs/ranking/2026-06-19-sec-total-return-calmar-v1.3.md
--
-- Adds a total-return CAGR (price return + reinvested per-share dividends sourced
-- from financial_facts_raw us-gaap dividend/distribution concepts) + its Calmar
-- (tr_cagr / |max_drawdown|) + a per-window coverage status, ALONGSIDE the
-- existing price-return cagr/calmar. The status gates the scoring reward off any
-- TR series we cannot reconstruct confidently.
--
-- metric_version is NOT bumped (stays 'risk_v1'): these are ADDITIVE, NULLABLE
-- evidence columns under a stable version (docs/settled-decisions.md: additive-
-- nullable evidence under a stable metric_version is blessed — do NOT bump the
-- version to add evidence columns; existing metrics are byte-identical). Pre-#1635
-- rows keep NULL; the nullable tr_status disambiguates the two NULL meanings:
--   tr_status IS NULL          -> not computed then (a pre-#1635 row)
--   tr_status = 'no_dividends' -> computed, instrument is a non-payer (TR == price)
--
-- tr_status has its OWN domain (a distinct coverage axis, not the RiskStatus
-- enum): {ok, tr_incomplete, no_dividends}.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS on both tables (prevention-log #644 — a
-- new column on an already-applied "new table" must be paired with IF NOT EXISTS).
-- ADD COLUMN on the partitioned observations parent propagates to all partitions.
--
-- NEVER edit this file after it is applied — fixes go in sql/204
-- (prevention-log #1333: schema_migrations content_sha256 drift guard).

BEGIN;

-- ---------------------------------------------------------------------------
-- Table 1: instrument_risk_metrics_observations (append-only audit log)
-- ---------------------------------------------------------------------------
ALTER TABLE instrument_risk_metrics_observations
    ADD COLUMN IF NOT EXISTS tr_cagr      NUMERIC,
    ADD COLUMN IF NOT EXISTS tr_calmar    NUMERIC,
    ADD COLUMN IF NOT EXISTS tr_n_periods INT,
    ADD COLUMN IF NOT EXISTS tr_status    TEXT
        CHECK (tr_status IS NULL OR tr_status IN (
            'ok', 'tr_incomplete', 'no_dividends'));

-- ---------------------------------------------------------------------------
-- Table 2: instrument_risk_metrics_current (latest write-through)
-- ---------------------------------------------------------------------------
ALTER TABLE instrument_risk_metrics_current
    ADD COLUMN IF NOT EXISTS tr_cagr      NUMERIC,
    ADD COLUMN IF NOT EXISTS tr_calmar    NUMERIC,
    ADD COLUMN IF NOT EXISTS tr_n_periods INT,
    ADD COLUMN IF NOT EXISTS tr_status    TEXT
        CHECK (tr_status IS NULL OR tr_status IN (
            'ok', 'tr_incomplete', 'no_dividends'));

COMMIT;
