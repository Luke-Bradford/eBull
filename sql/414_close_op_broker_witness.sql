-- #2979 half a — record that an uncertain engine close was settled by the broker's
-- whole-close witness.
--
-- Spec: docs/proposals/execution/2026-09-23-uncertain-close-witness-release.md.
--
-- A close whose outcome we lost (`crash_before_submission_identity`, or
-- `broker_close_uncertain`) is terminal at `reconcile_required`. It used to block the
-- #2965 release and the core preflight forever. The stamp records that the position
-- was then witnessed closed whole at the broker, so the close has nothing left to act
-- on. It never claims the close was OURS: `status` and `last_error_code` are untouched.
--
-- Written only in the transaction that releases the ownership
-- (`strategy_position_manager._release_whole_broker_close` / `_finish_close`). The
-- CHECK cannot require the released ownership, because that is a cross-table fact.
--
-- Measured on dev before this migration:
-- `SELECT count(*) FROM strategy_position_operations WHERE operation_type = 'close'` = 0.

ALTER TABLE strategy_position_operations
    ADD COLUMN IF NOT EXISTS broker_close_witnessed_at TIMESTAMPTZ;

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_broker_witness_shape;
ALTER TABLE strategy_position_operations
    ADD CONSTRAINT strategy_position_operations_broker_witness_shape CHECK (
        broker_close_witnessed_at IS NULL
        -- `last_error_code IS NOT NULL` is load-bearing. `NULL IN (...)` is UNKNOWN,
        -- and an UNKNOWN arm passes a CHECK.
        OR (
            operation_type = 'close'
            AND status = 'reconcile_required'
            AND last_error_code IS NOT NULL
            AND last_error_code IN ('crash_before_submission_identity', 'broker_close_uncertain')
        )
    );

COMMENT ON COLUMN strategy_position_operations.broker_close_witnessed_at IS
    'When the position this uncertain close addressed was witnessed closed whole at the '
    'broker (#2979). The close has nothing left to act on. Does NOT mean the close was ours.';
