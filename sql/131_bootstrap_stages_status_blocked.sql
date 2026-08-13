-- 131_bootstrap_stages_status_blocked.sql
--
-- Extend the bootstrap_stages.status CHECK to allow `blocked`
-- (#1020 ETL orchestration design).
--
-- New semantics:
--   pending  - not yet attempted
--   running  - in progress
--   success  - completed cleanly
--   error    - invoker raised (runtime or precondition)
--   skipped  - operator-policy skip (e.g. legacy fallback bypassed
--              because bulk path succeeded). Already in the existing
--              CHECK; preserved.
--   blocked  - NEW: orchestrator never invoked the stage because a
--              dependency stage finished error/blocked. Distinct
--              from `error` because no run-attempt was made.
--
-- The migration must NOT drop `skipped` from the existing CHECK.

BEGIN;

ALTER TABLE bootstrap_stages
    DROP CONSTRAINT IF EXISTS bootstrap_stages_status_check;

ALTER TABLE bootstrap_stages
    ADD CONSTRAINT bootstrap_stages_status_check
    CHECK (status IN ('pending', 'running', 'success', 'error', 'skipped', 'blocked'));

COMMIT;
