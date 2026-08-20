-- #2485 / #2484 -- bounded prospective eToro best-bid/ask evidence.
--
-- `quotes` is intentionally one mutable current row per instrument.  It cannot
-- establish the cost that was observable at a historical decision or fill.
-- This table samples only the predeclared active intraday research panel, at
-- most once per instrument per five-minute bucket, and expires after 24
-- months.  Missing/invalid responses are rows too: absence is not a zero
-- spread and collection coverage must remain measurable.

CREATE TABLE IF NOT EXISTS strategy_quote_observations (
    universe_version   TEXT NOT NULL
        REFERENCES strategy_intraday_universe_versions(universe_version) ON DELETE RESTRICT,
    instrument_id      BIGINT NOT NULL
        REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    sample_bucket      TIMESTAMPTZ NOT NULL,
    observed_at        TIMESTAMPTZ NOT NULL,
    quote_at           TIMESTAMPTZ,
    bid                NUMERIC(18,6),
    ask                NUMERIC(18,6),
    last               NUMERIC(18,6),
    spread_bps         NUMERIC(18,8),
    observation_status TEXT NOT NULL
        CHECK (observation_status IN ('observed', 'missing', 'invalid')),
    refusal_reason     TEXT CHECK (refusal_reason IS NULL OR (
        btrim(refusal_reason) <> '' AND length(refusal_reason) <= 100
    )),
    source             TEXT NOT NULL CHECK (source ~ '^etoro/[^/]+/best_bid_ask$'),
    PRIMARY KEY (universe_version, instrument_id, sample_bucket),
    CONSTRAINT strategy_quote_observation_bucket CHECK (
        date_trunc('minute', sample_bucket) = sample_bucket
        AND mod(extract(minute FROM sample_bucket)::integer, 5) = 0
        AND observed_at >= sample_bucket
        AND observed_at < sample_bucket + interval '5 minutes'
    ),
    CONSTRAINT strategy_quote_observation_shape CHECK (
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
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_quote_observations_retention
    ON strategy_quote_observations (sample_bucket);

CREATE INDEX IF NOT EXISTS idx_strategy_quote_observations_lookup
    ON strategy_quote_observations (instrument_id, sample_bucket DESC)
    WHERE observation_status = 'observed';

COMMENT ON TABLE strategy_quote_observations IS
    'Five-minute prospective best-bid/ask samples for the bounded active '
    'strategy research panel. Latest observation within a forming bucket wins; '
    'missing/invalid coverage is explicit; rows expire after 24 months.';
