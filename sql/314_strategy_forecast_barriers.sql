-- 314_strategy_forecast_barriers.sql
--
-- #2551: bind the triple-barrier experiment to the order it authorises.
-- Nullable preserves historical forecasts; the writer requires both for new
-- rows and the executor rejects legacy NULL geometry before broker access.

ALTER TABLE strategy_opportunity_forecasts
    ADD COLUMN IF NOT EXISTS target_barrier_pct NUMERIC(12,8),
    ADD COLUMN IF NOT EXISTS stop_barrier_pct NUMERIC(12,8);

ALTER TABLE strategy_opportunity_forecasts
    DROP CONSTRAINT IF EXISTS strategy_opportunity_forecast_barrier_shape;
ALTER TABLE strategy_opportunity_forecasts
    ADD CONSTRAINT strategy_opportunity_forecast_barrier_shape CHECK (
        (target_barrier_pct IS NULL AND stop_barrier_pct IS NULL)
        OR (
            target_barrier_pct > 0 AND target_barrier_pct <= 1000
            AND stop_barrier_pct > 0 AND stop_barrier_pct < 100
        )
    );

COMMENT ON COLUMN strategy_opportunity_forecasts.target_barrier_pct IS
    'Gross price distance above entry used by the target-first path label; distinct from the conditional after-cost payoff.';
COMMENT ON COLUMN strategy_opportunity_forecasts.stop_barrier_pct IS
    'Gross price distance below entry used by loss-at-stop sizing and the stop-first path label; distinct from the conditional after-cost payoff.';
