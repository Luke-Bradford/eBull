-- 202_instrument_risk_metrics_sector_beta.sql
--
-- #1674 — sector-relative beta/excess in the risk-metrics evidence layer.
-- Spec: docs/specs/metrics/2026-06-19-sector-relative-beta.md
--
-- Adds a SECOND OLS beta + excess-CAGR computed against each instrument's sector
-- SPDR ETF (resolved from instrument_sec_profile.sic via #1634's
-- resolve_sector_spdr), ALONGSIDE the existing SPY beta. Evidence/display only —
-- NOT a scoring input (settled-decisions: market-beta-vs-SPY is excluded from the
-- scoring penalty; sector beta as a scoring factor would need its own
-- full-population justification + operator sign-off).
--
-- metric_version is NOT bumped (stays 'risk_v1'): these are ADDITIVE, NULLABLE
-- evidence columns. Pre-#1674 rows + instruments with no resolvable sector keep
-- NULL. The nullable status column disambiguates the two NULL meanings:
--   sector_beta_status IS NULL              -> not computed then (a pre-#1674 row)
--   sector_beta_status = 'benchmark_missing' -> computed, no sector benchmark
-- (See docs/settled-decisions.md: additive-nullable evidence under a stable
-- metric_version is blessed — do not "fix" the mixed-schema-under-one-version by
-- bumping the version.)
--
-- Status values reuse the existing *_status domain verbatim (no new enum):
-- sector_beta_status comes from risk_metrics.beta_status (>=60 aligned -> ok),
-- sector_excess_cagr_status from risk_metrics.excess_cagr's own returned status.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS on both tables (prevention-log #644 — a
-- new column on an already-applied "new table" must be paired with IF NOT EXISTS).
-- ADD COLUMN on the partitioned observations parent propagates to all partitions.
--
-- NEVER edit this file after it is applied — fixes go in sql/203
-- (prevention-log #1333: schema_migrations content_sha256 drift guard).

BEGIN;

-- ---------------------------------------------------------------------------
-- Table 1: instrument_risk_metrics_observations (append-only audit log)
-- ---------------------------------------------------------------------------
ALTER TABLE instrument_risk_metrics_observations
    ADD COLUMN IF NOT EXISTS sector_benchmark_instrument_id BIGINT,
    ADD COLUMN IF NOT EXISTS sector_beta                    NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_beta_r2                 NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_beta_n_obs              INT,
    ADD COLUMN IF NOT EXISTS sector_beta_status             TEXT
        CHECK (sector_beta_status IS NULL OR sector_beta_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    ADD COLUMN IF NOT EXISTS sector_excess_cagr             NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_excess_cagr_status      TEXT
        CHECK (sector_excess_cagr_status IS NULL OR sector_excess_cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale'));

-- ---------------------------------------------------------------------------
-- Table 2: instrument_risk_metrics_current (latest write-through)
-- ---------------------------------------------------------------------------
ALTER TABLE instrument_risk_metrics_current
    ADD COLUMN IF NOT EXISTS sector_benchmark_instrument_id BIGINT,
    ADD COLUMN IF NOT EXISTS sector_beta                    NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_beta_r2                 NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_beta_n_obs              INT,
    ADD COLUMN IF NOT EXISTS sector_beta_status             TEXT
        CHECK (sector_beta_status IS NULL OR sector_beta_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    ADD COLUMN IF NOT EXISTS sector_excess_cagr             NUMERIC,
    ADD COLUMN IF NOT EXISTS sector_excess_cagr_status      TEXT
        CHECK (sector_excess_cagr_status IS NULL OR sector_excess_cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale'));

COMMIT;
