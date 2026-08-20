-- 315_strategy_forecast_outcomes.sql
--
-- #2553: one compact terminal path result per immutable opportunity forecast.
-- Immature windows have no row and are retried; polling and source bars are not
-- copied into this ledger.

CREATE TABLE IF NOT EXISTS strategy_opportunity_forecast_outcomes (
    forecast_outcome_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    forecast_id                BIGINT NOT NULL
        REFERENCES strategy_opportunity_forecasts(forecast_id) ON DELETE RESTRICT,
    resolver_version           TEXT NOT NULL CHECK (resolver_version <> ''),
    input_rule_set_version     TEXT NOT NULL CHECK (input_rule_set_version <> ''),
    outcome                    TEXT NOT NULL CHECK (
        outcome IN ('target_first','stop_first','timeout','ambiguous','unresolved')
    ),
    reason                     TEXT CHECK (
        reason IS NULL OR reason IN (
            'series_break','quarantined_bar','missing_bar_data','unorderable_exit_levels'
        )
    ),
    exit_bar_date              DATE,
    exit_price                 NUMERIC,
    market_bars_held           INTEGER CHECK (market_bars_held IS NULL OR market_bars_held >= 0),
    gross_return_pct           NUMERIC,
    resolved_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (forecast_id,resolver_version,input_rule_set_version),
    CHECK ((outcome='unresolved') = (reason IS NOT NULL)),
    CHECK (
        (exit_bar_date IS NULL) = (outcome='unresolved')
        AND (market_bars_held IS NULL) = (outcome='unresolved')
    ),
    CHECK (
        (exit_price IS NULL) = (outcome IN ('ambiguous','unresolved'))
        AND (gross_return_pct IS NULL) = (outcome IN ('ambiguous','unresolved'))
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_forecast_outcomes_forecast
    ON strategy_opportunity_forecast_outcomes (forecast_id,resolver_version,input_rule_set_version);
CREATE INDEX IF NOT EXISTS idx_strategy_forecast_outcomes_calibration
    ON strategy_opportunity_forecast_outcomes (resolver_version,input_rule_set_version,outcome,forecast_id);

CREATE TABLE IF NOT EXISTS strategy_forecast_outcome_cursor (
    id                         BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    last_forecast_id           BIGINT NOT NULL CHECK (last_forecast_id >= 0),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE strategy_opportunity_forecast_outcomes IS
    'One immutable terminal target/stop/timeout path result per exact forecast and resolver/input version; no immature or polling rows.';
COMMENT ON COLUMN strategy_opportunity_forecast_outcomes.gross_return_pct IS
    'Observed gross price return only. It is not represented as net performance and cannot substitute for reconciled costs.';
COMMENT ON TABLE strategy_forecast_outcome_cursor IS
    'Single mutable round-robin cursor preventing immature forecasts from starving later mature forecasts.';
