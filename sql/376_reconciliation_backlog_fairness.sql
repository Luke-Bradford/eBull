-- 376_reconciliation_backlog_fairness.sql
--
-- #2948. reconcile_backlog selected ORDER BY first_unresolved_at, order_id
-- LIMIT n. Both keys are immutable for a non-terminal row -- first_unresolved_at
-- is DEFAULT now() at insert and is written by no statement in
-- app/services/strategy_order_reconciliation.py -- so while the oldest n orders
-- stayed unresolved, order n+1 was never visited again. Selection now rotates on
-- last_attempt_at, which every attempt path writes.
--
-- This index matches the new ORDER BY. NULLS FIRST must be declared: Postgres
-- defaults an ascending index to NULLS LAST, which would not serve
-- "ORDER BY last_attempt_at ASC NULLS FIRST".
--
-- idx_strategy_order_reconciliation_backlog (sql/285) is KEPT: it still serves
-- enforce_reconciliation_slo's min(first_unresolved_at) + age filter, which is
-- the entry-block safety control and is unchanged by #2948.

CREATE INDEX IF NOT EXISTS idx_strategy_order_reconciliation_fairness
    ON strategy_order_reconciliation_state (
        last_attempt_at NULLS FIRST, first_unresolved_at, order_id
    )
    WHERE state NOT IN ('resolved', 'rejected');

COMMENT ON INDEX idx_strategy_order_reconciliation_fairness IS
    'Least-recently-attempted rotation for reconcile_backlog (#2948). A fixed '
    'ORDER BY on immutable keys starves every order past the batch limit.';
