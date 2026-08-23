-- #2833 step 2 -- prospective spread evidence for CORE SLEEVE CANDIDATES.
--
-- Why a table and not a query over `quotes`:  `quotes` is intentionally ONE
-- MUTABLE CURRENT ROW per instrument (`quotes_pkey` is UNIQUE on
-- `instrument_id` alone), exactly as sql/306 already records.  #2833's pass
-- bar is "a spread percentile over ~5 trading days of stored quote
-- snapshots", and `quotes_refresh` arm 5 (#2864) correctly widened the
-- REFRESH scope to the proved candidates -- but the store it feeds keeps one
-- row per instrument, so five trading days of hourly refreshes still leave
-- n=1.  Measured on dev 2026-08-23: every candidate (3417, 3418, 3434, 3075,
-- 15445, 15446, 14465) had exactly ONE `quotes` row.  Waiting longer cannot
-- change that; it is a property of the primary key, not of elapsed time.
--
-- Why not `strategy_quote_observations` (sql/306): that table is FK-bound to
-- `strategy_intraday_universe_versions` and its members are the bounded
-- intraday research panel -- 8 symbols whose "SPY" is instrument 3000, NOT
-- the proved candidate SPY.RTH 3417.  Its bucket CHECK is also five-minute,
-- while this lane samples on `quotes_refresh`'s hourly tick.  Relaxing a
-- CHECK on an immutable evidence table to admit a different cohort is worse
-- than a sibling with its own, honest constraints.
--
-- Membership is the arm-5 predicate itself (latest eligibility proof per
-- (instrument, environment) passing), so a candidate leaves this lane the
-- same way it leaves quoting: by being re-proved.  No extra provider calls --
-- `refresh_quotes` already holds these Quote objects.

CREATE TABLE IF NOT EXISTS strategy_core_quote_observations (
    instrument_id      BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    sample_bucket      TIMESTAMPTZ NOT NULL,
    observed_at        TIMESTAMPTZ NOT NULL,
    quote_at           TIMESTAMPTZ,
    bid                NUMERIC(18,6),
    ask                NUMERIC(18,6),
    last               NUMERIC(18,6),
    spread_bps         NUMERIC(18,8),
    -- eToro `conversionRateAsk`/`conversionRateBid` mid: the documented
    -- "conversion rate from the INSTRUMENT'S currency to USD" (live portal
    -- `market-data/get-instrument-market-rates`, verified 2026-08-23).  This
    -- is the only per-instrument denomination signal the API exposes --
    -- `get-instrument-display-data` carries no currency field at all, and
    -- `instruments.currency` is a VENUE lookup off `exchanges.currency`
    -- (universe.py: "currency is a property of where a listing trades"), so
    -- every `.L` line reads GBP regardless of what it is actually
    -- denominated in.  Measured 2026-08-23: CSPX.L / IUMO.L / IUQA.L /
    -- R1VL.L all return EXACTLY 1.0 (USD-denominated) while stored as GBP;
    -- IUSA.L returns 0.0136315 (GBX) and QDVA/QDVB/QDVI.DE 1.1677 (EUR).
    -- ⚠ NULL means the provider OMITTED the rate -- it does NOT mean USD.
    -- Readers must test `= 1` explicitly, never `IS NOT DISTINCT FROM NULL`
    -- (same posture as data-engineer I23: NULL is "not yet checked").
    conversion_rate    NUMERIC(24,12) CHECK (conversion_rate IS NULL OR conversion_rate > 0),
    observation_status TEXT NOT NULL
        CHECK (observation_status IN ('observed', 'missing', 'invalid')),
    refusal_reason     TEXT CHECK (refusal_reason IS NULL OR (
        btrim(refusal_reason) <> '' AND length(refusal_reason) <= 100
    )),
    source             TEXT NOT NULL CHECK (source ~ '^etoro/core_candidate/best_bid_ask$'),
    PRIMARY KEY (instrument_id, sample_bucket),
    CONSTRAINT strategy_core_quote_observation_bucket CHECK (
        date_trunc('hour', sample_bucket) = sample_bucket
        AND observed_at >= sample_bucket
        AND observed_at < sample_bucket + interval '1 hour'
    ),
    CONSTRAINT strategy_core_quote_observation_shape CHECK (
        (
            observation_status = 'observed'
            AND refusal_reason IS NULL
            AND quote_at IS NOT NULL
            AND bid > 0 AND ask > 0 AND ask >= bid
            AND spread_bps IS NOT NULL AND spread_bps >= 0
            AND (last IS NULL OR last > 0)
        ) OR (
            observation_status IN ('missing', 'invalid')
            AND refusal_reason IS NOT NULL
            AND quote_at IS NULL AND bid IS NULL AND ask IS NULL
            AND last IS NULL AND spread_bps IS NULL
            -- a refused observation carries no denomination either: the rate
            -- rides on the same response the bid/ask came from.
            AND conversion_rate IS NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_core_quote_observations_retention
    ON strategy_core_quote_observations (sample_bucket);

CREATE INDEX IF NOT EXISTS idx_strategy_core_quote_observations_lookup
    ON strategy_core_quote_observations (instrument_id, sample_bucket DESC)
    WHERE observation_status = 'observed';

-- The first observation in an hourly bucket is the evidence.  A later manual
-- dispatch must not replace it with a quote that knows more of the bucket --
-- same reasoning as sql/307, and the reason the writer uses DO NOTHING.
CREATE OR REPLACE FUNCTION reject_strategy_core_quote_observation_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'core quote observations are immutable; insert the next hourly bucket';
END;
$$;

DROP TRIGGER IF EXISTS trg_strategy_core_quote_observation_immutable
    ON strategy_core_quote_observations;

CREATE TRIGGER trg_strategy_core_quote_observation_immutable
BEFORE UPDATE ON strategy_core_quote_observations
FOR EACH ROW EXECUTE FUNCTION reject_strategy_core_quote_observation_update();

COMMENT ON TABLE strategy_core_quote_observations IS
    'Hourly prospective best-bid/ask samples for core sleeve candidates '
    '(#2833 step 2), carrying the per-instrument USD conversion rate. '
    'First observation in a bucket is immutable; missing/invalid coverage is '
    'explicit. `quotes` cannot serve this: it is one mutable row per instrument.';
