-- 283_strategy_order_position_executions.sql
--
-- #2454 review correction: same-instrument identity is not provenance. The
-- reconciler records every exact positionExecutions[].positionId returned by
-- detailed lookup for a strategy-origin order. Ownership may be claimed only
-- through this mapping.

CREATE TABLE IF NOT EXISTS strategy_order_position_executions (
    order_id           BIGINT NOT NULL
        REFERENCES orders(order_id) ON DELETE RESTRICT,
    broker_position_id BIGINT NOT NULL UNIQUE CHECK (broker_position_id > 0),
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, broker_position_id)
);

CREATE INDEX IF NOT EXISTS idx_strategy_order_position_executions_order
    ON strategy_order_position_executions (order_id, broker_position_id);

COMMENT ON TABLE strategy_order_position_executions IS
    'Exact positionExecutions[].positionId values from detailed lookup of a '
    'strategy-origin order. Same-instrument broker positions absent here are '
    'not claimable and remain manual/unowned.';
