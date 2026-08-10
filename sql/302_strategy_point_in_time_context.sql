-- #2508 — point-in-time market classification and compact decision context.
--
-- Classification history is transition-only: a daily universe refresh confirms
-- the current row and writes a new row only when type/listing changes. Decision
-- context is one narrow row per fired/refused candidate, never one row per bar
-- or per not-fired scan. This keeps the evidence needed to explain cohort
-- performance without duplicating the intraday price store.

CREATE TABLE IF NOT EXISTS instrument_market_classification_history (
    instrument_id BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    effective_from DATE NOT NULL,
    effective_to DATE,
    last_confirmed_on DATE NOT NULL,
    provider_exchange_id TEXT,
    primary_listing_market TEXT NOT NULL
        CHECK (primary_listing_market IN ('nyse', 'nasdaq', 'other', 'unknown')),
    instrument_type_id INTEGER,
    security_type TEXT NOT NULL
        CHECK (security_type IN ('common_stock', 'etf', 'other', 'unknown')),
    source_event TEXT NOT NULL
        CHECK (source_event IN ('imported', 'classification_change')),
    PRIMARY KEY (instrument_id, effective_from),
    CONSTRAINT instrument_market_classification_dates_ordered
        CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT instrument_market_classification_confirmed_after_start
        CHECK (last_confirmed_on >= effective_from),
    CONSTRAINT instrument_market_classification_closed_at_confirmation
        CHECK (effective_to IS NULL OR effective_to = last_confirmed_on)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_instrument_market_classification_current
    ON instrument_market_classification_history (instrument_id)
    WHERE effective_to IS NULL;

ALTER TABLE instrument_market_classification_history
    ADD CONSTRAINT instrument_market_classification_no_overlap
    EXCLUDE USING GIST (
        instrument_id WITH =,
        daterange(effective_from, effective_to, '[]') WITH &&
    );

-- Start the prospective record honestly. CURRENT_DATE is the first date on
-- which this database can evidence the current provider classification; it is
-- deliberately not backdated to instruments.first_seen_at.
INSERT INTO instrument_market_classification_history (
    instrument_id, effective_from, effective_to, last_confirmed_on,
    provider_exchange_id, primary_listing_market, instrument_type_id,
    security_type, source_event
)
SELECT i.instrument_id, CURRENT_DATE, NULL, CURRENT_DATE,
       i.exchange,
       CASE lower(coalesce(e.description, ''))
           WHEN 'nyse' THEN 'nyse'
           WHEN 'nasdaq' THEN 'nasdaq'
           ELSE CASE WHEN i.exchange IS NULL THEN 'unknown' ELSE 'other' END
       END,
       i.instrument_type_id,
       CASE lower(coalesce(t.description, ''))
           WHEN 'stocks' THEN 'common_stock'
           WHEN 'etf' THEN 'etf'
           ELSE CASE WHEN i.instrument_type_id IS NULL THEN 'unknown' ELSE 'other' END
       END,
       'imported'
FROM instruments i
LEFT JOIN exchanges e ON e.exchange_id = i.exchange
LEFT JOIN etoro_instrument_types t ON t.instrument_type_id = i.instrument_type_id
WHERE i.is_tradable
ON CONFLICT (instrument_id, effective_from) DO NOTHING;

CREATE TABLE IF NOT EXISTS strategy_decision_contexts (
    context_id BIGSERIAL PRIMARY KEY,
    strategy_id TEXT NOT NULL CHECK (strategy_id <> ''),
    strategy_version TEXT NOT NULL CHECK (strategy_version <> ''),
    instrument_id BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    decision_at TIMESTAMPTZ NOT NULL,
    signal_id BIGINT UNIQUE REFERENCES strategy_signals(signal_id) ON DELETE CASCADE,
    candidate_verdict TEXT NOT NULL
        CHECK (candidate_verdict IN ('eligible', 'refused')),
    refusal_reason TEXT,
    context_version TEXT NOT NULL CHECK (context_version <> ''),
    classification_effective_from DATE,
    security_type TEXT CHECK (security_type IN ('common_stock', 'etf', 'other', 'unknown')),
    primary_listing_market TEXT CHECK (primary_listing_market IN ('nyse', 'nasdaq', 'other', 'unknown')),
    provider_exchange_id TEXT,
    instrument_type_id INTEGER,
    as_traded_price NUMERIC,
    price_band TEXT CHECK (price_band IN ('under_5', '5_to_20', '20_to_50', '50_to_150', '150_plus')),
    trailing_median_share_volume NUMERIC,
    trailing_median_dollar_volume NUMERIC,
    dollar_volume_band TEXT CHECK (dollar_volume_band IN ('under_1m', '1m_to_10m', '10m_to_25m', '25m_to_100m', '100m_plus')),
    relative_volume NUMERIC,
    spread_bps NUMERIC,
    realised_volatility NUMERIC,
    gap_pct NUMERIC,
    market_sector_residual_z NUMERIC,
    vix NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_decision_context_unique
        UNIQUE (strategy_id, strategy_version, instrument_id, decision_at),
    CONSTRAINT strategy_decision_context_verdict_reason
        CHECK ((candidate_verdict = 'refused') = (refusal_reason IS NOT NULL)),
    CONSTRAINT strategy_decision_context_positive_inputs
        CHECK (
            (as_traded_price IS NULL OR as_traded_price > 0)
            AND (trailing_median_share_volume IS NULL OR trailing_median_share_volume >= 0)
            AND (trailing_median_dollar_volume IS NULL OR trailing_median_dollar_volume >= 0)
            AND (relative_volume IS NULL OR relative_volume >= 0)
            AND (spread_bps IS NULL OR spread_bps >= 0)
            AND (realised_volatility IS NULL OR realised_volatility >= 0)
            AND (vix IS NULL OR vix >= 0)
        ),
    -- An eligible row is a fully attributable decision. A refused row retains
    -- the partial evidence and named reason, allowing coverage gaps to be
    -- counted without pretending they form a tradable cohort.
    CONSTRAINT strategy_decision_context_eligible_complete
        CHECK (
            candidate_verdict = 'refused' OR (
                classification_effective_from IS NOT NULL
                AND security_type IS NOT NULL AND security_type <> 'unknown'
                AND primary_listing_market IS NOT NULL AND primary_listing_market <> 'unknown'
                AND as_traded_price IS NOT NULL AND price_band IS NOT NULL
                AND trailing_median_share_volume IS NOT NULL
                AND trailing_median_dollar_volume IS NOT NULL AND dollar_volume_band IS NOT NULL
                AND relative_volume IS NOT NULL AND spread_bps IS NOT NULL
                AND realised_volatility IS NOT NULL AND gap_pct IS NOT NULL
                AND market_sector_residual_z IS NOT NULL AND vix IS NOT NULL
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_strategy_decision_context_candidate
    ON strategy_decision_contexts (strategy_id, strategy_version, decision_at DESC);

CREATE INDEX IF NOT EXISTS idx_strategy_decision_context_cohort
    ON strategy_decision_contexts (
        strategy_id, strategy_version, security_type,
        primary_listing_market, price_band, dollar_volume_band
    )
    WHERE candidate_verdict = 'eligible';

COMMENT ON TABLE instrument_market_classification_history IS
    'Prospective, transition-only eToro security-type and primary-listing '
    'classification. Imported rows begin when first observed and must never be '
    'used to infer older history. Primary listing is not execution venue.';

COMMENT ON TABLE strategy_decision_contexts IS
    'One compact point-in-time context per fired/refused candidate; never per '
    'bar or routine not-fired scan. Supports type/listing/price/liquidity cohort '
    'analysis while keeping rolling inputs in their bounded source stores.';
