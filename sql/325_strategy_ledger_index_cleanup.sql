-- runner: autocommit
-- #2486 — the UNIQUE constraint index already supplies the exact
-- (signal_id, rule_set_version, input_rule_set_version) lookup used by all
-- four strategy_outcomes readers. The duplicate non-unique index had 111,204
-- scans but was structurally interchangeable and occupied 10 MB for four live
-- rows after historical churn.
--
-- CONCURRENTLY keeps reads and writes available. The migration is deliberately
-- autocommit and idempotent: if a timeout interrupts it after the drop, the next
-- run can safely replay the drop and both reindexes. The two compact unique
-- indexes are rebuilt to reclaim pre-bounded-ledger pages (14 MB and 10 MB on
-- dev before this migration). Five-second lock acquisition refuses rather than
-- disrupting an active workload; the five-minute statement limit bounds the
-- maintenance operation.
SET lock_timeout = '5s';
SET statement_timeout = '5min';

DROP INDEX CONCURRENTLY IF EXISTS idx_strategy_outcomes_signal_versions;
REINDEX INDEX CONCURRENTLY strategy_outcomes_unique;
REINDEX INDEX CONCURRENTLY strategy_signals_unique;

RESET statement_timeout;
RESET lock_timeout;
