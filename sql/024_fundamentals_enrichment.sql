-- Migration 024: fundamentals enrichment schema
--
-- Adds three tables that extend the tradable-universe data model with
-- provider-sourced fundamental data points needed by the ranking and
-- thesis engines:
--
--   instrument_profile   — static/slowly-changing profile fields (beta, float,
--                          market-cap, employees, IPO date).  One row per
--                          instrument, refreshed daily from FMP /v3/profile.
--
--   earnings_events      — historical and upcoming earnings reports with
--                          estimate vs. actual EPS/revenue and surprise %.
--                          Idempotent upsert key: (instrument_id, fiscal_date_ending).
--
--   analyst_estimates    — weekly snapshot of consensus EPS/revenue forecasts
--                          and rating distribution.
--                          Idempotent upsert key: (instrument_id, as_of_date).
--
-- Also adds:
--
--   instrument_valuation — view that joins the latest fundamentals_snapshot
--                          with the live quotes row to produce derived
--                          multiples (P/E, P/B, P/FCF, FCF yield,
--                          debt/equity, live market cap).
--
--   CURRENCY ASSUMPTION (v1): fundamentals are stored in the instrument's
--   reporting currency (usually USD); quotes are in the instrument's trading
--   currency on eToro.  For most US equities these are the same.  A future
--   migration must add explicit currency normalisation before this view is
--   used for cross-currency comparisons.

-- ---------------------------------------------------------------------------
-- 1. instrument_profile
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS instrument_profile (
    instrument_id       BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    beta                NUMERIC(10,4),
    public_float        BIGINT,           -- shares available for public trading
    avg_volume_30d      BIGINT,           -- 30-day average daily volume
    market_cap          NUMERIC(20,2),    -- latest market cap from provider
    employees           INTEGER,
    ipo_date            DATE,
    is_actively_trading BOOLEAN,
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- 2. earnings_events
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS earnings_events (
    earnings_event_id  BIGSERIAL PRIMARY KEY,
    instrument_id      BIGINT NOT NULL REFERENCES instruments(instrument_id),
    fiscal_date_ending DATE NOT NULL,
    reporting_date     DATE,
    eps_estimate       NUMERIC(12,4),
    eps_actual         NUMERIC(12,4),
    revenue_estimate   NUMERIC(20,2),
    revenue_actual     NUMERIC(20,2),
    surprise_pct       NUMERIC(10,4),    -- (actual - estimate) / |estimate| * 100
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, fiscal_date_ending)
);

CREATE INDEX IF NOT EXISTS idx_earnings_events_instrument
    ON earnings_events(instrument_id, fiscal_date_ending DESC);

-- ---------------------------------------------------------------------------
-- 3. analyst_estimates
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analyst_estimates (
    estimate_id        BIGSERIAL PRIMARY KEY,
    instrument_id      BIGINT NOT NULL REFERENCES instruments(instrument_id),
    as_of_date         DATE NOT NULL,
    consensus_eps_fq   NUMERIC(12,4),    -- next fiscal quarter
    consensus_eps_fy   NUMERIC(12,4),    -- next fiscal year
    consensus_rev_fq   NUMERIC(20,2),
    consensus_rev_fy   NUMERIC(20,2),
    analyst_count      INTEGER,
    buy_count          INTEGER,
    hold_count         INTEGER,
    sell_count         INTEGER,
    price_target_mean  NUMERIC(18,6),
    price_target_high  NUMERIC(18,6),
    price_target_low   NUMERIC(18,6),
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_analyst_estimates_instrument
    ON analyst_estimates(instrument_id, as_of_date DESC);

-- ---------------------------------------------------------------------------
-- 4. instrument_valuation view
--
-- Uses DISTINCT ON to select the latest fundamentals_snapshot per instrument
-- (more readable and typically faster than a correlated MAX subquery).
-- Guards are applied so that division-by-zero and nonsense ratios are
-- suppressed: NULL is returned whenever a denominator is zero or NULL.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW instrument_valuation AS
WITH priced AS (
    -- Best available price: last if positive, else bid/ask midpoint.
    -- Matches the scorer's fallback logic in _load_instrument_data.
    SELECT instrument_id,
           COALESCE(
               NULLIF(GREATEST(last, 0), 0),
               CASE WHEN bid > 0 AND ask > 0 THEN (bid + ask) / 2 END
           )                       AS price,
           quoted_at
    FROM quotes
)
SELECT
    fs.instrument_id,
    p.price                                                         AS current_price,
    p.quoted_at                                                     AS price_as_of,
    fs.as_of_date                                                   AS fundamentals_as_of,

    -- Live market cap: price × shares (both must be positive)
    CASE
        WHEN p.price > 0 AND fs.shares_outstanding > 0
        THEN p.price * fs.shares_outstanding
    END                                                             AS market_cap_live,

    -- Price / Earnings
    CASE
        WHEN p.price > 0 AND fs.eps > 0
        THEN p.price / fs.eps
    END                                                             AS pe_ratio,

    -- Price / Book
    CASE
        WHEN p.price > 0 AND fs.book_value > 0
        THEN p.price / fs.book_value
    END                                                             AS pb_ratio,

    -- Price / Free Cash Flow  (market cap / total FCF)
    CASE
        WHEN p.price > 0 AND fs.shares_outstanding > 0 AND fs.fcf > 0
        THEN (p.price * fs.shares_outstanding) / fs.fcf
    END                                                             AS p_fcf_ratio,

    -- FCF Yield  (total FCF / market cap)
    CASE
        WHEN p.price > 0 AND fs.shares_outstanding > 0
        THEN fs.fcf / (p.price * fs.shares_outstanding)
    END                                                             AS fcf_yield,

    -- Debt / Equity  (total debt / (book value per share × shares))
    CASE
        WHEN fs.book_value > 0 AND fs.shares_outstanding > 0
        THEN fs.debt / (fs.book_value * fs.shares_outstanding)
    END                                                             AS debt_equity_ratio

FROM (
    -- Latest fundamentals_snapshot per instrument
    SELECT DISTINCT ON (instrument_id)
        instrument_id,
        as_of_date,
        eps,
        book_value,
        fcf,
        shares_outstanding,
        debt
    FROM fundamentals_snapshot
    ORDER BY instrument_id, as_of_date DESC
) fs
JOIN priced p USING (instrument_id);
