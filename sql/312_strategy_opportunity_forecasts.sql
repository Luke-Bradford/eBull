-- 312_strategy_opportunity_forecasts.sql
--
-- #2545: one compact, immutable forecast per fired entry decision.  These
-- tables store decision-bearing evidence only; feature vectors and non-firing
-- heartbeats remain outside the database.

CREATE TABLE IF NOT EXISTS strategy_forecast_calibrations (
    calibration_id             TEXT PRIMARY KEY CHECK (calibration_id <> ''),
    model_version              TEXT NOT NULL CHECK (model_version <> ''),
    holdout_start              DATE NOT NULL,
    holdout_end                DATE NOT NULL CHECK (holdout_end >= holdout_start),
    sample_size                INTEGER NOT NULL CHECK (sample_size >= 100),
    brier_score                NUMERIC(12,8) NOT NULL CHECK (brier_score >= 0 AND brier_score <= 1),
    calibration_error          NUMERIC(12,8) NOT NULL CHECK (calibration_error >= 0 AND calibration_error <= 1),
    passed                     BOOLEAN NOT NULL,
    evidence_ref               TEXT NOT NULL CHECK (evidence_ref <> ''),
    recorded_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_opportunity_forecasts (
    forecast_id                       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    signal_id                         BIGINT NOT NULL UNIQUE
        REFERENCES strategy_signals(signal_id) ON DELETE RESTRICT,
    forecast_policy_version           TEXT NOT NULL CHECK (forecast_policy_version <> ''),
    decided_at                        TIMESTAMPTZ NOT NULL,
    valid_through                     TIMESTAMPTZ NOT NULL CHECK (valid_through >= decided_at),
    side                              TEXT NOT NULL CHECK (side = 'long'),
    horizon_market_days               INTEGER NOT NULL CHECK (horizon_market_days > 0 AND horizon_market_days <= 60),
    setup_version                     TEXT NOT NULL CHECK (setup_version <> ''),
    exit_policy_version               TEXT NOT NULL CHECK (exit_policy_version <> ''),
    calibration_id                    TEXT NOT NULL
        REFERENCES strategy_forecast_calibrations(calibration_id) ON DELETE RESTRICT,
    target_probability                NUMERIC(12,8) NOT NULL CHECK (target_probability >= 0 AND target_probability <= 1),
    stop_probability                  NUMERIC(12,8) NOT NULL CHECK (stop_probability >= 0 AND stop_probability <= 1),
    timeout_probability               NUMERIC(12,8) NOT NULL CHECK (timeout_probability >= 0 AND timeout_probability <= 1),
    target_net_return_pct             NUMERIC(12,8) NOT NULL CHECK (target_net_return_pct > 0),
    stop_net_return_pct               NUMERIC(12,8) NOT NULL CHECK (stop_net_return_pct < 0),
    timeout_net_return_pct            NUMERIC(12,8) NOT NULL,
    expected_duration_hours           NUMERIC(12,4) NOT NULL CHECK (expected_duration_hours > 0),
    uncertainty_penalty_pct           NUMERIC(12,8) NOT NULL CHECK (uncertainty_penalty_pct >= 0),
    tail_penalty_pct                  NUMERIC(12,8) NOT NULL CHECK (tail_penalty_pct >= 0),
    correlation_penalty_pct           NUMERIC(12,8) NOT NULL CHECK (correlation_penalty_pct >= 0),
    cost_stress_penalty_pct           NUMERIC(12,8) NOT NULL CHECK (cost_stress_penalty_pct >= 0),
    conservative_net_expectancy_pct   NUMERIC(12,8) NOT NULL,
    cost_model_id                     TEXT NOT NULL CHECK (cost_model_id <> ''),
    created_at                        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_opportunity_forecast_probability_sum CHECK (
        abs(target_probability + stop_probability + timeout_probability - 1) <= 0.000001
    ),
    CONSTRAINT strategy_opportunity_forecast_expectancy_reconciles CHECK (
        abs(
            conservative_net_expectancy_pct - (
                target_probability * target_net_return_pct
                + stop_probability * stop_net_return_pct
                + timeout_probability * timeout_net_return_pct
                - uncertainty_penalty_pct
                - tail_penalty_pct
                - correlation_penalty_pct
                - cost_stress_penalty_pct
            )
        ) <= 0.000001
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_opportunity_forecasts_validity
    ON strategy_opportunity_forecasts (valid_through, forecast_id);

ALTER TABLE strategy_entry_preflights
    ADD COLUMN IF NOT EXISTS forecast_id BIGINT
        REFERENCES strategy_opportunity_forecasts(forecast_id) ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_strategy_entry_preflights_forecast
    ON strategy_entry_preflights (forecast_id) WHERE forecast_id IS NOT NULL;

COMMENT ON TABLE strategy_opportunity_forecasts IS
    'One immutable decision-bearing forecast per fired entry signal; never a heartbeat or feature store.';
COMMENT ON COLUMN strategy_opportunity_forecasts.conservative_net_expectancy_pct IS
    'After-cost probability-weighted return less frozen uncertainty, tail, correlation and cost-stress penalties.';
