-- 453_ranking_pot_exec_level_observations.sql
--
-- #2842 slice 5c-ii-c — the broker-held protection levels of each ranking-pot position (spec
-- docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md §7.4, "The broker-held levels"; r3-108).
--
--   ranking_pot_exec_level_observations — per (pot trade, broker position), the four protection fields of
--                                         the exact-position `get_portfolio` row the position manager read,
--                                         CHANGE-ONLY: written by `app/services/ranking_pot_held_levels.py`
--                                         only when they differ from the pair's latest row (latest = highest
--                                         id). The first row precedes every repair this system sends.
--
-- The SENT levels are `ranking_pot_exec_submissions`; repairs are `strategy_position_operations`.
--
-- ⚠ APPEND-ONLY, BY TRIGGER (sql/264: a trigger binds the superuser this app connects as).

CREATE TABLE IF NOT EXISTS ranking_pot_exec_level_observations (
    observation_id     BIGSERIAL PRIMARY KEY,
    strategy_trade_id  BIGINT NOT NULL
        REFERENCES ranking_pot_exec_submissions (strategy_trade_id) ON DELETE RESTRICT,
    broker_position_id BIGINT NOT NULL CHECK (broker_position_id > 0),
    -- The manager cycle's instant (taken before the read); ordering uses observation_id.
    observed_at        TIMESTAMPTZ NOT NULL,
    -- As the broker reported them, unrounded; NULL = the field was absent.
    stop_loss_rate     NUMERIC,
    take_profit_rate   NUMERIC,
    -- As parsed: an absent flag reads as "no level" (the manager's fail-safe reading).
    is_no_stop_loss    BOOLEAN NOT NULL,
    is_no_take_profit  BOOLEAN NOT NULL,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ranking_pot_exec_level_observations_latest
    ON ranking_pot_exec_level_observations (strategy_trade_id, broker_position_id, observation_id DESC);

-- The pair is one the engine owns: an ownership row binds this broker position to this pot trade.
CREATE OR REPLACE FUNCTION ranking_pot_exec_level_observations_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM strategy_position_ownership
        WHERE strategy_trade_id = NEW.strategy_trade_id AND broker_position_id = NEW.broker_position_id
    ) THEN
        RAISE EXCEPTION 'broker position % is not owned by pot trade %', NEW.broker_position_id,
            NEW.strategy_trade_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_level_observations_guard ON ranking_pot_exec_level_observations;
CREATE TRIGGER trg_ranking_pot_exec_level_observations_guard
BEFORE INSERT ON ranking_pot_exec_level_observations
FOR EACH ROW EXECUTE FUNCTION ranking_pot_exec_level_observations_guard();

DROP TRIGGER IF EXISTS trg_ranking_pot_exec_level_observations_append_only ON ranking_pot_exec_level_observations;
CREATE TRIGGER trg_ranking_pot_exec_level_observations_append_only
BEFORE UPDATE OR DELETE ON ranking_pot_exec_level_observations
FOR EACH ROW EXECUTE FUNCTION prevent_ranking_pot_mutation();
