-- 196_portfolio_value_v2_fx_eod.sql
--
-- #1594 PR-A (spec docs/proposals/etl/2026-06-13-portfolio-value-v2-fx-eod.md)
-- — per-day dated FX + end-of-day portfolio equity snapshots.
--
-- Three new tables, no ALTER of existing schema:
--
-- 1. fx_rates_daily — dated USD-base ECB reference rates (Frankfurter
--    time-series). Mirrors the live_fx_rates convention so the per-day
--    convert() path has parity (direct + inverse only; no USD cross-rate).
--    Distinct from the tax fx_rates table (sql/013) on purpose — see spec
--    §21 R1 (dropping ECB rows into fx_rates would silently change the
--    safety-critical USD tax-disposal path). ECB rates are immutable per
--    date → ON CONFLICT DO NOTHING on the writer.
--
-- 2. portfolio_eod_snapshots — one row per closed trading session holding
--    the operator's total equity (positions + cash) in display currency.
--    Reverses #393's informal "no NAV snapshot table" posture (operator-
--    approved 2026-06-12 roadmap; /api/v1/balances/history returns 403 on
--    the demo key so we persist our own). snapshot_date is data-anchored
--    (MAX price_daily.price_date), NOT wall-clock — idempotent re-runs.
--
-- 3. portfolio_eod_position_snapshots — per-broker-position audit evidence
--    for each snapshot. Keyed (snapshot_date, position_id): broker_positions
--    carries multiple position_id per instrument, so keying on instrument_id
--    would collide. instrument_id kept as a column for chart aggregation.
--
-- Not partitioned: single account, O(hundreds) rows/year (same rationale as
-- trade_events sql/194).

BEGIN;

-- A. Per-day dated FX (USD-base ECB reference rates).
CREATE TABLE IF NOT EXISTS fx_rates_daily (
    rate_date       DATE NOT NULL,
    base_currency   TEXT NOT NULL,
    quote_currency  TEXT NOT NULL,
    rate            NUMERIC(18, 10) NOT NULL CHECK (rate > 0),  -- 1 base = rate quote
    source          TEXT NOT NULL DEFAULT 'frankfurter.timeseries',
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (rate_date, base_currency, quote_currency)
);

-- Carry-forward lookup: most-recent rate on/before a target date for a pair.
CREATE INDEX IF NOT EXISTS idx_fx_rates_daily_pair_date
    ON fx_rates_daily (base_currency, quote_currency, rate_date DESC);

-- B. Daily portfolio equity snapshot (own-table; reverses #393 no-NAV posture).
CREATE TABLE IF NOT EXISTS portfolio_eod_snapshots (
    snapshot_date     DATE PRIMARY KEY,
    display_currency  TEXT NOT NULL,
    total_value       NUMERIC(20, 4) NOT NULL,   -- positions_value + cash_value
    positions_value   NUMERIC(20, 4) NOT NULL,
    cash_value        NUMERIC(20, 4) NOT NULL,
    -- which fx_rates_daily.rate_date actually priced this snapshot
    -- (carry-forward on weekends/holidays — the real rate date used).
    fx_rate_date      DATE,
    -- Closed-set skip counters: positions_total = positions_priced
    --                            + positions_no_price + positions_no_fx
    positions_total       INTEGER NOT NULL DEFAULT 0,  -- real positions seen
    positions_priced      INTEGER NOT NULL DEFAULT 0,  -- contributed to positions_value
    positions_no_price    INTEGER NOT NULL DEFAULT 0,  -- no price_daily close on/before date
    positions_no_fx       INTEGER NOT NULL DEFAULT 0,  -- priced but native->display FX missing
    cash_no_fx_currencies INTEGER NOT NULL DEFAULT 0,  -- cash currencies dropped for missing FX
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- C. Per-position breakdown — audit evidence (CLAUDE.md auditability).
CREATE TABLE IF NOT EXISTS portfolio_eod_position_snapshots (
    snapshot_date     DATE NOT NULL
                      REFERENCES portfolio_eod_snapshots(snapshot_date) ON DELETE CASCADE,
    position_id       BIGINT NOT NULL,            -- broker positionID (real, >= 0)
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id),
    units             NUMERIC(20, 8) NOT NULL,
    close_price       NUMERIC(20, 8),             -- native-ccy close used (NULL if none on/before date)
    native_currency   TEXT,
    value_display     NUMERIC(20, 4),             -- close*units in display ccy (NULL if skipped)
    price_status      TEXT NOT NULL DEFAULT 'priced'
                      CHECK (price_status IN ('priced', 'no_price', 'no_fx')),
    PRIMARY KEY (snapshot_date, position_id)
);

CREATE INDEX IF NOT EXISTS idx_eod_position_snap_instrument
    ON portfolio_eod_position_snapshots (instrument_id, snapshot_date);

COMMIT;
