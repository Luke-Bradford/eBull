-- 451_ranking_pot_exits.sql
--
-- #2842 slice 5c-i — ranking-pot-v1's exits (spec docs/proposals/execution/2026-10-01-2842-ranking-pot-v1.md
-- §7.4, "The exits, wind-down stamps and `completed` writer").
--
--   1. `rerank_exit`: the trigger code a pot re-rank / ineligibility / wind-down close is recorded under.
--      Writer: app/services/strategy_position_manager.py::_pot_close. The drift check compares the SET of
--      quoted codes in the live constraint with sql/436's list, as sql/436 does.
--   2. The executed-flat fence (r3-64): `→ completed` is refused while any trade funding one of the
--      declaration's lifecycles is not `closed` / `failed`, or holds an active position ownership. The
--      writer (`ranking_pot_exits.complete_if_flat`) checks the same first; this trigger makes it binding on
--      every writer. A second BEFORE INSERT trigger, so sql/448's transition function is not restated.
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
                                    'entry_exit_gap', 'exit_deadline', 'operator_close', 'protection_failed',
                                    'strategy_exit', 'timeout'] THEN
        RAISE EXCEPTION 'strategy_position_operations_trigger_code_check drifted: %', live;
    END IF;
END $$;

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT strategy_position_operations_trigger_code_check,
    ADD CONSTRAINT strategy_position_operations_trigger_code_check CHECK (trigger_code IN (
        'entry_exit_gap', 'causal_resistance_break', 'timeout', 'strategy_exit',
        'emergency_risk', 'operator_close', 'core_rebalance', 'exit_deadline', 'protection_failed',
        'rerank_exit'
    ));

CREATE OR REPLACE FUNCTION ranking_pot_completed_executed_flat()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.to_state <> 'completed' THEN
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM ranking_pot_exec_lifecycles l
          JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id
          JOIN strategy_trades t ON t.funding_decision_id = fd.funding_decision_id
         WHERE l.declaration_id = NEW.declaration_id
           AND (t.status NOT IN ('closed', 'failed')
                OR EXISTS (SELECT 1 FROM strategy_position_ownership own
                            WHERE own.strategy_trade_id = t.strategy_trade_id AND own.status = 'active'))
    ) THEN
        RAISE EXCEPTION 'executed_not_flat: ranking pot % still holds or is entering an executed position',
            NEW.declaration_id;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM ranking_pot_exec_lifecycles l
          JOIN strategy_funding_decisions fd ON fd.signal_id = l.signal_id AND fd.verdict = 'allocated'
         WHERE l.declaration_id = NEW.declaration_id
           AND NOT EXISTS (SELECT 1 FROM strategy_trades t WHERE t.funding_decision_id = fd.funding_decision_id)
    ) THEN
        RAISE EXCEPTION 'executed_not_flat: ranking pot % has an allocated entry with no trade', NEW.declaration_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ranking_pot_completed_executed_flat ON ranking_pot_state_events;
CREATE TRIGGER trg_ranking_pot_completed_executed_flat
BEFORE INSERT ON ranking_pot_state_events
FOR EACH ROW EXECUTE FUNCTION ranking_pot_completed_executed_flat();
