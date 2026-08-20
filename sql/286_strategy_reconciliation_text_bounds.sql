-- 286_strategy_reconciliation_text_bounds.sql
--
-- #2451 hardening kept separate because sql/285 was exercised against the dev
-- database before review completed. Bound broker-owned text before persistence
-- and give the trigger function a strategy-specific name.

CREATE OR REPLACE FUNCTION strategy_prevent_request_id_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.strategy_request_id IS NOT NULL
       AND NEW.strategy_request_id IS DISTINCT FROM OLD.strategy_request_id THEN
        RAISE EXCEPTION 'strategy_request_id is immutable once assigned';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_orders_strategy_request_id_immutable ON orders;
CREATE TRIGGER trg_orders_strategy_request_id_immutable
BEFORE UPDATE OF strategy_request_id ON orders
FOR EACH ROW EXECUTE FUNCTION strategy_prevent_request_id_change();

DROP FUNCTION IF EXISTS prevent_strategy_request_id_change();

ALTER TABLE strategy_order_position_executions
    ADD CONSTRAINT strategy_order_position_execution_text_bounds CHECK (
        position_state IS NULL OR char_length(position_state) BETWEEN 1 AND 64
    );

ALTER TABLE strategy_order_reconciliation_state
    ADD CONSTRAINT strategy_order_reconciliation_text_bounds CHECK (
        (broker_status IS NULL OR char_length(broker_status) BETWEEN 1 AND 64)
        AND (last_error_code IS NULL OR char_length(last_error_code) BETWEEN 1 AND 64)
    );

ALTER TABLE strategy_execution_blocks
    DROP CONSTRAINT strategy_execution_blocks_reason_check;

ALTER TABLE strategy_execution_blocks
    ADD CONSTRAINT strategy_execution_blocks_reason_check CHECK (
        char_length(reason) BETWEEN 1 AND 500
    ),
    ADD CONSTRAINT strategy_execution_blocks_source_bound CHECK (
        char_length(source) BETWEEN 1 AND 64
    );
