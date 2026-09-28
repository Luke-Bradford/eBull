-- 435_ai_trial_exit_deadline.sql
--
-- #3471 slice 2c-ii — spec §8 "Per-trade deadline": `strategy_trades.exit_deadline_session`,
-- the NYSE session on which a demo-trial leg exits (session `horizon_days`, counting the leg's
-- own fill session as 0), and the position-operation trigger code the manager closes it under.
-- Spec: docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md §8.
-- Writer: app/services/strategy_order_reconciliation.py::_apply_detail (the update that opens
-- the trade), via app/services/ai_trial_deadline.py. Reader: strategy_position_manager.
--
-- ⚠ Deviation from §8's "NOT NULL for trades of a demo_trial strategy", stated: the deadline
-- counts from the ACTUAL fill session, which does not exist while the trade is `planned` or
-- `submitted`. So the rule is "NOT NULL once a trial trade is open, closing or closed", and
-- the trade is trial-owned iff `ai_trial_trade_links` names it (the executor writes that link in
-- the allocation transaction, while the trade is still `planned` — enforced below).
--
-- No trial trade exists in any environment: `ai_trial_executor.execute_trial_signal` is the only
-- writer of `ai_trial_trade_links`, and nothing calls it at origin/main
-- (`git grep -n execute_trial_signal origin/main -- app scripts` names only its own module).
-- So every existing `strategy_trades` row is non-trial with a NULL deadline, which the trigger
-- accepts; nothing needs a backfill.
--
-- One transaction (the runner applies each file in one), so nothing is half-applied.

ALTER TABLE strategy_trades ADD COLUMN IF NOT EXISTS exit_deadline_session DATE;

COMMENT ON COLUMN strategy_trades.exit_deadline_session IS
    '#3471 §8: the NYSE session a demo-trial leg exits on (from 15:00 UTC). Set with the fill, '
    'immutable, present iff the trade is an open/closing/closed ai_trial_trade_links leg.';

-- Present iff trial-owned and past `submitted`; never changed once set.
CREATE OR REPLACE FUNCTION strategy_trades_exit_deadline_verify()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    is_trial BOOLEAN;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.exit_deadline_session IS NOT NULL
       AND NEW.exit_deadline_session IS DISTINCT FROM OLD.exit_deadline_session THEN
        RAISE EXCEPTION 'strategy trade % exit deadline is immutable (% -> %)',
            NEW.strategy_trade_id, OLD.exit_deadline_session, NEW.exit_deadline_session;
    END IF;
    is_trial := EXISTS (SELECT 1 FROM ai_trial_trade_links WHERE strategy_trade_id = NEW.strategy_trade_id);
    IF NEW.exit_deadline_session IS NOT NULL AND NOT is_trial THEN
        RAISE EXCEPTION 'strategy trade % is not a trial leg and cannot carry an exit deadline',
            NEW.strategy_trade_id;
    END IF;
    IF is_trial AND NEW.status IN ('open', 'closing', 'closed') AND NEW.exit_deadline_session IS NULL THEN
        RAISE EXCEPTION 'trial strategy trade % cannot be % without an exit deadline',
            NEW.strategy_trade_id, NEW.status;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_strategy_trades_exit_deadline_verify ON strategy_trades;
CREATE TRIGGER trg_strategy_trades_exit_deadline_verify
BEFORE INSERT OR UPDATE ON strategy_trades
FOR EACH ROW EXECUTE FUNCTION strategy_trades_exit_deadline_verify();

-- A trade is linked to a trial leg only while it is still `planned`; otherwise a link written
-- after the open would name a trade the deadline rule above never saw.
-- ⚠ The row lock is what makes the pair of triggers an invariant (Codex ckpt-2): without it a
-- link insert reading `planned` and a concurrent open not yet seeing the link both commit. It
-- conflicts with the UPDATE's own row lock, so whichever runs second sees the other's commit
-- (a PL/pgSQL query takes a fresh snapshot per statement under READ COMMITTED).
CREATE OR REPLACE FUNCTION ai_trial_trade_links_planned()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    trade_status TEXT;
BEGIN
    SELECT status INTO trade_status FROM strategy_trades WHERE strategy_trade_id = NEW.strategy_trade_id
    FOR NO KEY UPDATE;
    IF trade_status IS DISTINCT FROM 'planned' THEN
        RAISE EXCEPTION 'trade % is %, not planned; a trial link is written at allocation',
            NEW.strategy_trade_id, trade_status;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_ai_trial_trade_links_planned ON ai_trial_trade_links;
CREATE TRIGGER trg_ai_trial_trade_links_planned
BEFORE INSERT ON ai_trial_trade_links
FOR EACH ROW EXECUTE FUNCTION ai_trial_trade_links_planned();

-- The manager's deadline close. Built from the LIVE constraint, read from the dev cluster
-- 2026-09-28 with `pg_get_constraintdef` (= sql/411's). A mismatch refuses (fail-closed).
DO $$
DECLARE
    live TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid) INTO live
    FROM pg_constraint
    WHERE conrelid = 'strategy_position_operations'::regclass
      AND conname = 'strategy_position_operations_trigger_code_check';
    IF live IS DISTINCT FROM
        'CHECK ((trigger_code = ANY (ARRAY[''entry_exit_gap''::text, ''causal_resistance_break''::text, '
        '''timeout''::text, ''strategy_exit''::text, ''emergency_risk''::text, ''operator_close''::text, '
        '''core_rebalance''::text])))'
    THEN
        RAISE EXCEPTION 'strategy_position_operations_trigger_code_check drifted: %', live;
    END IF;
END $$;

ALTER TABLE strategy_position_operations
    DROP CONSTRAINT strategy_position_operations_trigger_code_check,
    ADD CONSTRAINT strategy_position_operations_trigger_code_check CHECK (trigger_code IN (
        'entry_exit_gap', 'causal_resistance_break', 'timeout', 'strategy_exit',
        'emergency_risk', 'operator_close', 'core_rebalance', 'exit_deadline'
    ));
