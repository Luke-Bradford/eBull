-- 136_bootstrap_runs_cancel.sql
--
-- Issue #1065 (umbrella #1064) — admin control hub rewrite, PR1 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Schema migrations / sql/136.
--
-- ## Why
--
-- Adds cooperative-cancel support for bootstrap runs. A new
-- `cancel_requested_at` column lets the orchestrator check at stage
-- boundaries and exit cleanly, and a new terminal `cancelled` status
-- distinguishes operator-cancel from `partial_error` (errors during a
-- run) and from `complete` (no errors).
--
-- ## Boot recovery
--
-- On jobs-process startup, any `bootstrap_runs` row with
-- `cancel_requested_at IS NOT NULL AND status='running'` is swept to
-- `cancelled` and a note appended to `bootstrap_runs.notes`:
--   "terminated by operator before jobs restart".
--
-- (`bootstrap_runs` has no `last_error` column; the existing `notes`
-- column is the audit field — Codex round 2 R2-B3.)
--
-- ## Scheduler gate
--
-- `_bootstrap_complete` returns (False, ...) for `cancelled`, same as
-- `partial_error`. Operator must Iterate or Re-run to advance the gate.

BEGIN;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;

ALTER TABLE bootstrap_runs DROP CONSTRAINT IF EXISTS bootstrap_runs_status_check;
ALTER TABLE bootstrap_runs ADD CONSTRAINT bootstrap_runs_status_check
    CHECK (status IN ('running', 'complete', 'partial_error', 'cancelled'));

ALTER TABLE bootstrap_state DROP CONSTRAINT IF EXISTS bootstrap_state_status_check;
ALTER TABLE bootstrap_state ADD CONSTRAINT bootstrap_state_status_check
    CHECK (status IN ('pending', 'running', 'complete', 'partial_error', 'cancelled'));

COMMIT;
