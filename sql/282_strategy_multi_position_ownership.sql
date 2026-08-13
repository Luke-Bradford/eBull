-- 282_strategy_multi_position_ownership.sql
--
-- #2454 review correction: one broker order may return several
-- positionExecutions[].positionId values. Exact ownership retains all of them.

DROP INDEX IF EXISTS idx_strategy_position_one_active_trade;

CREATE INDEX IF NOT EXISTS idx_strategy_position_active_trade
    ON strategy_position_ownership (strategy_trade_id, broker_position_id)
    WHERE status = 'active';

COMMENT ON INDEX idx_strategy_position_active_trade IS
    'All active exact broker positions for one strategy trade. An entry order '
    'may produce multiple positionExecutions; every id is owned independently.';
