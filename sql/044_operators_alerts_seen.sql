-- Migration 044: operator-scoped alert acknowledgement + guard-rejection read index
--
-- 1. operators.alerts_last_seen_decision_id — NULL = never acknowledged (all in-window rows unseen).
--    Integer cursor keyed off decision_audit.decision_id (BIGSERIAL, unique).
--    Timestamp-based cursor was rejected: decision_time is TIMESTAMPTZ (microsecond resolution,
--    NOT unique under load), which leaves a tie-break race where a row inserted between GET
--    and POST at the same decision_time as rejections[0] would be silently acked. decision_id
--    is monotonic for the guard stage (single-threaded scheduler invocations; no concurrent
--    writers), so a strict > comparison fully closes the race.
-- 2. Partial index on decision_audit supports the dashboard GET scan.
--    Narrowed to stage='execution_guard' + pass_fail='FAIL' because
--    (a) the /alerts endpoint filters on both, (b) other stages write to
--    decision_audit (e.g. order_execution, deferred_retry) and must not
--    be indexed as guard rejections.

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_decision_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_decision_audit_guard_failed_recent
    ON decision_audit (decision_time DESC)
    WHERE pass_fail = 'FAIL' AND stage = 'execution_guard';
