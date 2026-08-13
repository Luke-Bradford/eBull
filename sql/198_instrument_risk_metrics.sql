-- 198_instrument_risk_metrics.sql
--
-- #591 PR-B, Task B2 — two-layer risk-metrics persistence for the
-- instrument risk drill. Spec:
--   docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md
--
-- TWO-LAYER RATIONALE (mirrors the ownership observations/_current model,
-- sql/114-116):
--   - instrument_risk_metrics_observations is APPEND-ONLY. price_daily is a
--     MUTABLE table (ingest writes ON CONFLICT DO UPDATE) — a corrected or
--     re-fetched bar overwrites the prior close in place. That means a past
--     metric computation is NOT reconstructable from current price_daily: the
--     observation row IS the audit record of "what we computed, from what data,
--     when". Never UPDATE/DELETE an observation; a recompute appends a new row.
--   - instrument_risk_metrics_current is the latest write-through snapshot, one
--     row per (instrument, metric_version, window_key), rebuilt deterministically
--     by B3's upsert from the winning observation.
--
-- WHY computed_at IS IN THE OBSERVATIONS PK:
--   A vendor correction to a historical bar that does NOT advance the latest
--   close date produces DIFFERENT metrics for the same
--   (instrument_id, as_of_date, metric_version, window_key). Without computed_at
--   in the key the corrected recompute would collide with — and silently
--   overwrite — the prior row, defeating the audit purpose. computed_at
--   disambiguates the two computations. as_of_date (the partition key) is in the
--   PK as Postgres requires the partition column to be part of any PK.
--
-- UNITS: all returns/ratios are stored as FRACTIONS (0.10 == 10%). No _pct
-- suffix. Skew / excess_kurtosis / beta / r2 are dimensionless.
--
-- COLUMN ALIGNMENT (B3 dataclass -> column), app/services/risk_metrics.py:
--   WindowMetrics.annualized_vol  -> vol_annualized
--   WindowMetrics.r2 / BetaResult.r2 -> beta_r2
--   WindowMetrics.excess_cagr     -> excess_cagr_vs_spy
--   WindowMetrics.excess_cagr_status -> excess_cagr_status
--   DrawdownResult.peak_date      -> max_dd_peak_date
--   DrawdownResult.trough_date    -> max_dd_trough_date
--   DistributionResult.{skew,excess_kurtosis,var_5,worst_day,best_day} -> same
--   DistributionResult.n_obs      -> (folded into distribution evidence; n_returns is the window count)
-- The trailing_* / excess_trailing_* columns are populated by B3 from
-- risk_metrics.trailing_return / excess_trailing_return per-window.
--
-- RETENTION = NONE (no sweep). At weekly cadence over the tradable universe
-- this is ~800k rows/yr, quarterly-partitioned and bounded. A retention sweep
-- would DELETE exactly the historical audit trail this table exists to keep.
-- If size ever bites, DROP whole old PARTITIONs (cheap, metadata-only) rather
-- than row-level retention.
--
-- Idempotent: every CREATE uses IF NOT EXISTS; re-applying is a no-op.
--
-- NEVER edit this file after it is applied — fixes go in sql/199
-- (prevention-log #1333: schema_migrations content_sha256 drift guard).

BEGIN;

-- ---------------------------------------------------------------------------
-- Table 1: instrument_risk_metrics_observations (append-only audit log)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_risk_metrics_observations (
    instrument_id             BIGINT NOT NULL,
    as_of_date                DATE NOT NULL,
    metric_version            TEXT NOT NULL,
    window_key                TEXT NOT NULL
        CHECK (window_key IN ('1y', '3y', 'full')),
    computed_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Scalar metrics (FRACTIONS; nullable — thin history -> NULL + status flag).
    cagr                      NUMERIC,
    excess_cagr_vs_spy        NUMERIC,
    max_drawdown              NUMERIC,
    max_dd_peak_date          DATE,
    max_dd_trough_date        DATE,
    current_drawdown          NUMERIC,
    vol_annualized            NUMERIC,
    beta                      NUMERIC,
    beta_r2                   NUMERIC,
    skew                      NUMERIC,
    excess_kurtosis           NUMERIC,
    var_5                     NUMERIC,
    worst_day                 NUMERIC,
    best_day                  NUMERIC,
    calmar                    NUMERIC,
    trailing_1m               NUMERIC,
    trailing_3m               NUMERIC,
    trailing_6m               NUMERIC,
    trailing_1y               NUMERIC,
    excess_trailing_1m        NUMERIC,
    excess_trailing_3m        NUMERIC,
    excess_trailing_6m        NUMERIC,
    excess_trailing_1y        NUMERIC,

    -- Evidence. NO foreign key on instrument_id / benchmark_instrument_id: an
    -- append-only audit log must survive instrument churn (delisting / merge /
    -- re-id), matching the ownership observations which also do NOT FK
    -- instrument_id.
    n_returns                 INT,
    beta_n_obs                INT,
    benchmark_instrument_id   BIGINT,
    window_days               INT,

    -- Per-metric discrete status (NOT a JSONB — queryable, CHECK-guarded).
    -- Values mirror risk_metrics.RiskStatus.
    cagr_status               TEXT
        CHECK (cagr_status IS NULL OR cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    vol_status                TEXT
        CHECK (vol_status IS NULL OR vol_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    beta_status               TEXT
        CHECK (beta_status IS NULL OR beta_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    drawdown_status           TEXT
        CHECK (drawdown_status IS NULL OR drawdown_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    distribution_status       TEXT
        CHECK (distribution_status IS NULL OR distribution_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    calmar_status             TEXT
        CHECK (calmar_status IS NULL OR calmar_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    trailing_status           TEXT
        CHECK (trailing_status IS NULL OR trailing_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    excess_cagr_status        TEXT
        CHECK (excess_cagr_status IS NULL OR excess_cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),

    PRIMARY KEY (instrument_id, as_of_date, metric_version, window_key, computed_at)
) PARTITION BY RANGE (as_of_date);

COMMENT ON TABLE instrument_risk_metrics_observations IS
    'Append-only per-(instrument, as_of, version, window) risk-metric audit log. price_daily is mutable so past computations are not reconstructable — the observation IS the audit record. Rebuild source for instrument_risk_metrics_current. Spec: docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md (#591 PR-B).';

-- Quarterly partitions 2010-Q1 .. 2040-Q4 (real lower bound 2010-01-01,
-- NEVER MINVALUE — a stray pre-2010 as_of_date routes to _default, visible).
DO $$
DECLARE
    yr     INT;
    qtr    INT;
    qstart DATE;
    qend   DATE;
    pname  TEXT;
BEGIN
    FOR yr IN 2010..2040 LOOP
        FOR qtr IN 1..4 LOOP
            qstart := MAKE_DATE(yr, (qtr - 1) * 3 + 1, 1);
            qend := qstart + INTERVAL '3 months';
            pname := FORMAT('instrument_risk_metrics_observations_%sq%s', yr, qtr);
            EXECUTE FORMAT(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF instrument_risk_metrics_observations FOR VALUES FROM (%L) TO (%L)',
                pname, qstart, qend
            );
        END LOOP;
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS instrument_risk_metrics_observations_default
    PARTITION OF instrument_risk_metrics_observations DEFAULT;

CREATE INDEX IF NOT EXISTS idx_risk_obs_instrument_asof
    ON instrument_risk_metrics_observations (instrument_id, as_of_date DESC);

-- ---------------------------------------------------------------------------
-- Table 2: instrument_risk_metrics_current (latest write-through)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instrument_risk_metrics_current (
    instrument_id             BIGINT NOT NULL,
    metric_version            TEXT NOT NULL,
    window_key                TEXT NOT NULL
        CHECK (window_key IN ('1y', '3y', 'full')),
    as_of_date                DATE NOT NULL,
    -- Carried from the winning observation (B3's upsert tiebreak: latest
    -- as_of_date, then latest computed_at).
    computed_at               TIMESTAMPTZ,
    -- This table's own write time (distinct from the observation's computed_at).
    refreshed_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Scalar metrics (FRACTIONS), same as observations.
    cagr                      NUMERIC,
    excess_cagr_vs_spy        NUMERIC,
    max_drawdown              NUMERIC,
    max_dd_peak_date          DATE,
    max_dd_trough_date        DATE,
    current_drawdown          NUMERIC,
    vol_annualized            NUMERIC,
    beta                      NUMERIC,
    beta_r2                   NUMERIC,
    skew                      NUMERIC,
    excess_kurtosis           NUMERIC,
    var_5                     NUMERIC,
    worst_day                 NUMERIC,
    best_day                  NUMERIC,
    calmar                    NUMERIC,
    trailing_1m               NUMERIC,
    trailing_3m               NUMERIC,
    trailing_6m               NUMERIC,
    trailing_1y               NUMERIC,
    excess_trailing_1m        NUMERIC,
    excess_trailing_3m        NUMERIC,
    excess_trailing_6m        NUMERIC,
    excess_trailing_1y        NUMERIC,

    -- Evidence (no FK — same churn rationale as observations).
    n_returns                 INT,
    beta_n_obs                INT,
    benchmark_instrument_id   BIGINT,
    window_days               INT,

    -- Per-metric discrete status, same domain as observations.
    cagr_status               TEXT
        CHECK (cagr_status IS NULL OR cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    vol_status                TEXT
        CHECK (vol_status IS NULL OR vol_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    beta_status               TEXT
        CHECK (beta_status IS NULL OR beta_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    drawdown_status           TEXT
        CHECK (drawdown_status IS NULL OR drawdown_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    distribution_status       TEXT
        CHECK (distribution_status IS NULL OR distribution_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    calmar_status             TEXT
        CHECK (calmar_status IS NULL OR calmar_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    trailing_status           TEXT
        CHECK (trailing_status IS NULL OR trailing_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),
    excess_cagr_status        TEXT
        CHECK (excess_cagr_status IS NULL OR excess_cagr_status IN (
            'ok', 'insufficient_history', 'partial_window', 'benchmark_missing',
            'benchmark_insufficient_history', 'invalid_price_chain', 'stale')),

    PRIMARY KEY (instrument_id, metric_version, window_key)
);

COMMENT ON TABLE instrument_risk_metrics_current IS
    'Materialised latest-per-(instrument, version, window) risk-metric snapshot. Rebuilt deterministically by B3 from the winning observation (latest as_of_date, then latest computed_at). Spec: docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md (#591 PR-B).';

-- For the future ranking "all instruments at window 3y / version risk_v1" scan.
CREATE INDEX IF NOT EXISTS idx_risk_current_window_version
    ON instrument_risk_metrics_current (window_key, metric_version);

COMMIT;
