-- 436_trial_protection_failed.sql
--
-- #3471 slice 2c-iii-a — spec §15 O10: a demo-trial leg whose SL/TP repair fails is closed,
-- and the close is recorded under its own trigger code, `protection_failed`, so the audit trail
-- tells a protection close from a deadline, age-out or operator close.
-- Writer: app/services/strategy_position_manager.py::_trial_close.
--
-- The drift check compares the SET of quoted codes in the live constraint with sql/435's list,
-- as sql/435 does, so a server that renders the definition differently does not trip it.
--
-- One transaction (the runner applies each file in one), so nothing is half-applied.

DO $$
DECLARE
    live  TEXT;
    codes TEXT[];
BEGIN
    SELECT pg_get_constraintdef(oid) INTO live
    FROM pg_constraint
    WHERE conrelid = 'strategy_position_operations'::regclass
      AND conname = 'strategy_position_operations_trigger_code_check';
    SELECT array_agg(m[1] ORDER BY m[1]) INTO codes FROM regexp_matches(live, '''([a-z_]+)''', 'g') AS m;
    IF codes IS DISTINCT FROM ARRAY['causal_resistance_break', 'core_rebalance', 'emergency_risk',
                                    'entry_exit_gap', 'exit_deadline', 'operator_close', 'strategy_exit',
                                    'timeout'] THEN
        RAISE EXCEPTION 'strategy_position_operations_trigger_code_check drifted: %', live;
    END IF;
END $$;

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT strategy_position_operations_trigger_code_check,
    ADD CONSTRAINT strategy_position_operations_trigger_code_check CHECK (trigger_code IN (
        'entry_exit_gap', 'causal_resistance_break', 'timeout', 'strategy_exit',
        'emergency_risk', 'operator_close', 'core_rebalance', 'exit_deadline', 'protection_failed'
    ));
