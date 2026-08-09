-- 284_strategy_ownership_constraint_corrections.sql
--
-- #2454 PR review corrections. PostgreSQL CHECK accepts NULL, so released
-- ownership must test release_reason IS NOT NULL explicitly. A trade may have
-- several exit/ratchet/reconcile orders over its life, but exactly one entry
-- order establishes its position-execution provenance.

ALTER TABLE strategy_position_ownership
    DROP CONSTRAINT IF EXISTS strategy_position_ownership_release_shape;

ALTER TABLE strategy_position_ownership
    ADD CONSTRAINT strategy_position_ownership_release_shape CHECK (
        (status = 'active' AND released_at IS NULL AND release_reason IS NULL)
        OR (
            status = 'released'
            AND released_at IS NOT NULL
            AND release_reason IS NOT NULL
            AND release_reason <> ''
        )
    );

ALTER TABLE strategy_trade_orders
    DROP CONSTRAINT IF EXISTS strategy_trade_orders_one_purpose;

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_trade_orders_one_entry
    ON strategy_trade_orders (strategy_trade_id)
    WHERE purpose = 'entry';

COMMENT ON INDEX idx_strategy_trade_orders_one_entry IS
    'One entry order establishes one strategy trade; later purposes may have '
    'multiple attempts/events and remain distinguished by order_id.';
