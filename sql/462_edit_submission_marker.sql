-- #3546 gap G: position EDITS get #2979's pre-call marker.
--
-- `_submit_edit` now commits `submitting` (sql/377's in-flight status) before the
-- PATCH, so an edit still at `intent_persisted` provably never reached the broker
-- and is released (`rejected`, `edit_never_submitted`) instead of being written
-- `reconcile_required`, which blocked the same stable repair on every later visit.
--
-- That proof holds only for rows written by the marked path. An edit row written
-- before this migration may sit at `intent_persisted` AFTER its PATCH was sent, so
-- it keeps the old lost-identity treatment. This column is the provenance: new
-- edit intents set it TRUE; every existing row, and every close (whose marker
-- predates this, #2979), stays FALSE.
ALTER TABLE strategy_position_operations
    ADD COLUMN IF NOT EXISTS edit_marker_enforced BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN strategy_position_operations.edit_marker_enforced IS
    'TRUE = an edit intent written by a path that commits status=submitting before the '
    'PATCH (#3546 gap G), so intent_persisted proves the PATCH was never sent. FALSE = '
    'a close, or an edit predating the marker: intent_persisted proves nothing.';

-- The released row must not forbid the fresh repair at the same levels one layer
-- down either. `sql/406` keeps every non-`applied` repair/ratchet in the material
-- identity index so a verb the broker REFUSED cannot be re-entered. An
-- `edit_never_submitted` row was refused by nobody -- the PATCH never left -- so it
-- is excluded exactly as `applied` is. `rejected` (broker) and `reconcile_required`
-- rows stay indexed; `NULLS NOT DISTINCT` is preserved for the same reason as 406.
DROP INDEX IF EXISTS idx_strategy_position_operation_material_identity;

CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_position_operation_material_identity
    ON strategy_position_operations (
        ownership_id, operation_type, desired_stop_rate,
        desired_take_profit_rate, completed_bar_at
    ) NULLS NOT DISTINCT
    WHERE operation_type IN ('fixed_exit_repair', 'stop_ratchet')
      AND status <> 'applied'
      AND NOT (status = 'rejected' AND last_error_code IS NOT DISTINCT FROM 'edit_never_submitted');
