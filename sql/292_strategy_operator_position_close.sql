-- 292_strategy_operator_position_close.sql
--
-- #2467 — operator-requested closes use the existing exact-owned position
-- manager.  This adds one bounded audit reason to the material-operation
-- ledger; it creates no quote, valuation, or position-history storage.

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT IF EXISTS strategy_position_operations_trigger_code_check;

ALTER TABLE strategy_position_operations
    ADD CONSTRAINT strategy_position_operations_trigger_code_check CHECK (
        trigger_code IN (
            'entry_exit_gap',
            'causal_resistance_break',
            'timeout',
            'strategy_exit',
            'emergency_risk',
            'operator_close'
        )
    );

COMMENT ON COLUMN strategy_position_operations.trigger_code IS
    'Why a material exact-position mutation occurred. operator_close is a '
    'session-authenticated full close from the automated strategy workspace.';
