-- 276_strategy_observation_storage.sql
--
-- #2448 — bounded signal and intraday observation storage.
--
-- The durable signal ledger keeps FIRED decisions because outcomes and future
-- strategy trades refer to signal_id. Routine not-fired/not-evaluable detail
-- moves to a 90-day range-partitioned observation relation, while this compact
-- daily census remains durable. This prevents a daily scan from growing the
-- heavily indexed strategy_signals relation by the measured 8.42 GB/year.

CREATE TABLE IF NOT EXISTS strategy_signal_daily_counts (
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    signal_bar_date  DATE NOT NULL,
    signal_kind      TEXT NOT NULL CHECK (signal_kind IN ('entry', 'exit')),
    verdict          TEXT NOT NULL CHECK (verdict IN ('fired', 'not_fired', 'not_evaluable')),
    -- Empty string is the closed, non-null representation of "no reason" so
    -- it can participate in the primary key. It is legal exactly when the
    -- verdict is not `not_evaluable`.
    reason_code      TEXT NOT NULL CHECK (reason_code IN (
        '', 'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price'
    )),
    row_count        BIGINT NOT NULL CHECK (row_count > 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        strategy_id, strategy_version, signal_bar_date,
        signal_kind, verdict, reason_code
    ),
    CONSTRAINT strategy_signal_daily_counts_reason_matches_verdict CHECK (
        (verdict = 'not_evaluable') = (reason_code <> '')
    )
);

-- Seed the durable census before the writer changes. Idempotence is required
-- because migrations are replayed against test databases; a conflict with a
-- different count is deliberately not overwritten.
INSERT INTO strategy_signal_daily_counts (
    strategy_id, strategy_version, signal_bar_date,
    signal_kind, verdict, reason_code, row_count
)
SELECT strategy_id, strategy_version, signal_bar_date,
       signal_kind, verdict, COALESCE(not_evaluable_reason, ''), COUNT(*)
FROM strategy_signals
GROUP BY strategy_id, strategy_version, signal_bar_date,
         signal_kind, verdict, COALESCE(not_evaluable_reason, '')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS strategy_signal_observations (
    strategy_id      TEXT NOT NULL,
    strategy_version TEXT NOT NULL,
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    signal_bar_date  DATE NOT NULL,
    signal_kind      TEXT NOT NULL CHECK (signal_kind IN ('entry', 'exit')),
    verdict          TEXT NOT NULL CHECK (verdict IN ('not_fired', 'not_evaluable')),
    reason_code      TEXT NOT NULL CHECK (reason_code IN (
        '', 'missing_volume', 'missing_spread', 'insufficient_warmup',
        'quarantined_bar', 'series_break', 'not_listed',
        'ambiguous_intrabar', 'no_fill_bar', 'thin_cross_section',
        'unusable_fill_price'
    )),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (
        strategy_id, strategy_version, instrument_id,
        signal_bar_date, signal_kind
    ),
    CONSTRAINT strategy_signal_observations_reason_matches_verdict CHECK (
        (verdict = 'not_evaluable') = (reason_code <> '')
    )
) PARTITION BY RANGE (signal_bar_date);

CREATE INDEX IF NOT EXISTS idx_strategy_signal_observations_instrument_date
    ON strategy_signal_observations (instrument_id, signal_bar_date DESC);

-- Intraday bars are observations, not derived indicator series and not ticks.
-- The first partition level keeps the three independently capped/retained tiers
-- physically separable; leaf range partitions are created by the bounded writer.
CREATE TABLE IF NOT EXISTS strategy_intraday_bars (
    timeframe       TEXT NOT NULL CHECK (timeframe IN ('30m', '5m', '1m')),
    bar_time        TIMESTAMPTZ NOT NULL,
    instrument_id   BIGINT NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    open             NUMERIC NOT NULL CHECK (open > 0),
    high             NUMERIC NOT NULL CHECK (high > 0),
    low              NUMERIC NOT NULL CHECK (low > 0),
    close            NUMERIC NOT NULL CHECK (close > 0),
    volume           NUMERIC CHECK (volume IS NULL OR volume >= 0),
    source           TEXT NOT NULL,
    captured_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (timeframe, bar_time, instrument_id),
    CONSTRAINT strategy_intraday_bars_ohlc_shape CHECK (
        high >= GREATEST(open, close, low)
        AND low <= LEAST(open, close, high)
    )
) PARTITION BY LIST (timeframe);

CREATE TABLE IF NOT EXISTS strategy_intraday_bars_30m
    PARTITION OF strategy_intraday_bars FOR VALUES IN ('30m')
    PARTITION BY RANGE (bar_time);

CREATE TABLE IF NOT EXISTS strategy_intraday_bars_5m
    PARTITION OF strategy_intraday_bars FOR VALUES IN ('5m')
    PARTITION BY RANGE (bar_time);

CREATE TABLE IF NOT EXISTS strategy_intraday_bars_1m
    PARTITION OF strategy_intraday_bars FOR VALUES IN ('1m')
    PARTITION BY RANGE (bar_time);

CREATE INDEX IF NOT EXISTS idx_strategy_intraday_bars_instrument_time
    ON strategy_intraday_bars (instrument_id, timeframe, bar_time DESC);

COMMENT ON TABLE strategy_signal_daily_counts IS
    'Durable daily census for every evaluated strategy signal. #2448.';
COMMENT ON TABLE strategy_signal_observations IS
    'Non-fired/not-evaluable signal detail retained for 90 days in monthly partitions. Fired rows remain durable in strategy_signals. #2448.';
COMMENT ON TABLE strategy_intraday_bars IS
    'Bounded completed OHLCV bars for strategy context/setup/execution tiers; never forming bars, indicators, or ticks. #2448.';
