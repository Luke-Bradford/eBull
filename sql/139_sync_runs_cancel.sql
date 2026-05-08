-- 139_sync_runs_cancel.sql
--
-- Issue #1065 (umbrella #1064) — admin control hub rewrite, PR1 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Schema migrations / sql/139.
--
-- ## Why
--
-- The orchestrator full-sync (`orchestrator_full_sync`) writes
-- `sync_runs`, not `job_runs`. Cooperative cancel for that process
-- needs:
--
--   * `cancel_requested_at` TIMESTAMPTZ — fast-path observation
--     column; mirrors `bootstrap_runs.cancel_requested_at` (sql/136)
--     and `job_runs.cancel_requested_at` (sql/137).
--   * widened status CHECK to include `cancelled` so the cancel
--     observation point can transition the row.
--
-- Existing set verified at sql/033:21:
--   ('running', 'complete', 'partial', 'failed').
--
-- Codex round 4 R4-B2 caught this — without the widen, the worker's
-- UPDATE to mark the active sync_run cancelled would fail.
--
-- ## Finalizer-preserves-cancelled invariant
--
-- The existing sync orchestrator finalisation path computes terminal
-- `sync_runs.status` from per-layer outcomes (complete / partial /
-- failed). PR6 amends the finaliser's UPDATE to:
--
--   UPDATE sync_runs
--      SET status = ?, finished_at = now(), ...
--    WHERE sync_run_id = ?
--      AND status = 'running'                 -- preserve 'cancelled'
--
-- so the cancel checkpoint's transition is never overwritten
-- (Codex round 5 R5-W4).

BEGIN;

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;

ALTER TABLE sync_runs DROP CONSTRAINT IF EXISTS sync_runs_status_check;
ALTER TABLE sync_runs ADD CONSTRAINT sync_runs_status_check
    CHECK (status IN ('running', 'complete', 'partial', 'failed', 'cancelled'));

COMMIT;
