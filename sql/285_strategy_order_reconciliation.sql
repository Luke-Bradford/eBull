-- 285_strategy_order_reconciliation.sql
--
-- #2451 / #2437 slice 6. Persist the broker submission UUID before I/O, keep
-- only one compact reconciliation state per strategy order, and retain exact
-- position execution facts without a polling-event heap.

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS strategy_request_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_strategy_request_id
    ON orders (strategy_request_id)
    WHERE strategy_request_id IS NOT NULL;

ALTER TABLE orders
    DROP CONSTRAINT IF EXISTS orders_strategy_request_origin_check;

ALTER TABLE orders
    ADD CONSTRAINT orders_strategy_request_origin_check CHECK (
        strategy_request_id IS NULL OR execution_origin = 'strategy'
    );

CREATE OR REPLACE FUNCTION prevent_strategy_request_id_change()
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
FOR EACH ROW EXECUTE FUNCTION prevent_strategy_request_id_change();

-- A position may occur in its opening order and later exact-position close or
-- patch orders. Provenance is the (order, position) pair; ownership remains
-- globally unique in strategy_position_ownership.
ALTER TABLE strategy_order_position_executions
    DROP CONSTRAINT IF EXISTS strategy_order_position_executions_broker_position_id_key;

ALTER TABLE strategy_order_position_executions
    ADD COLUMN IF NOT EXISTS position_state TEXT,
    ADD COLUMN IF NOT EXISTS remaining_units NUMERIC(28,10),
    ADD COLUMN IF NOT EXISTS opening_units NUMERIC(28,10),
    ADD COLUMN IF NOT EXISTS average_price NUMERIC(28,10),
    ADD COLUMN IF NOT EXISTS execution_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS fees NUMERIC(28,10),
    ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_strategy_order_position_execution_position
    ON strategy_order_position_executions (broker_position_id, order_id);

CREATE TABLE IF NOT EXISTS strategy_order_reconciliation_state (
    order_id              BIGINT PRIMARY KEY
        REFERENCES orders(order_id) ON DELETE RESTRICT,
    state                 TEXT NOT NULL DEFAULT 'unresolved' CHECK (state IN (
        'unresolved', 'pending', 'resolved', 'rejected', 'not_found',
        'ambiguous', 'error'
    )),
    first_unresolved_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at       TIMESTAMPTZ,
    reconciled_at         TIMESTAMPTZ,
    attempt_count         INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    broker_status         TEXT,
    position_count        INTEGER NOT NULL DEFAULT 0 CHECK (position_count >= 0),
    last_error_code       TEXT,
    last_payload_sha256   TEXT,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_order_reconciliation_hash_check CHECK (
        last_payload_sha256 IS NULL OR last_payload_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT strategy_order_reconciliation_resolved_shape CHECK (
        (state IN ('resolved', 'rejected') AND reconciled_at IS NOT NULL)
        OR (state NOT IN ('resolved', 'rejected') AND reconciled_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_strategy_order_reconciliation_backlog
    ON strategy_order_reconciliation_state (first_unresolved_at, order_id)
    WHERE state NOT IN ('resolved', 'rejected');

-- One current kill condition per source. Poll success updates this row; it does
-- not append heartbeats. Later health controls share this bounded table.
CREATE TABLE IF NOT EXISTS strategy_execution_blocks (
    source       TEXT PRIMARY KEY,
    active       BOOLEAN NOT NULL,
    reason       TEXT NOT NULL CHECK (reason <> ''),
    blocked_at   TIMESTAMPTZ,
    cleared_at   TIMESTAMPTZ,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT strategy_execution_blocks_time_shape CHECK (
        (active AND blocked_at IS NOT NULL AND cleared_at IS NULL)
        OR (NOT active AND cleared_at IS NOT NULL)
    )
);

COMMENT ON COLUMN orders.strategy_request_id IS
    'Immutable UUID committed before strategy broker I/O and reused as the '
    'eToro v2 X-Request-Id idempotency key and orders:lookup referenceId.';

COMMENT ON TABLE strategy_order_reconciliation_state IS
    'One bounded current-state row per strategy order; repeated polling updates '
    'this row rather than retaining unbounded response/event payloads.';
