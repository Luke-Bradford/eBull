-- 141_job_runs_params_snapshot.sql
--
-- Issue: PR1 of #1064 admin-control-hub follow-up sequence.
-- Plan: docs/internal/plans/pr1-job-registry-refactor.md (uncommitted scratchpad).
-- Audit (PR0, merged c413623): docs/wiki/job-registry-audit.md.
--
-- ## Why
--
-- Operator-locked decision: every job_runs row must record the params
-- dict the run was invoked with. Without this column the operator
-- cannot tell which param-set produced which row — the audit trail
-- regresses the moment ParamMetadata + per-job operator-tunable params
-- land. Three populate paths (PR1 step 8 tests pin all three):
--
--   1. Manual operator trigger via POST /jobs/<name>/run — payload.params
--      stored verbatim after validate_job_params(allow_internal_keys=False).
--   2. Scheduled cron fire — materialise_scheduled_params(job_name)
--      reads ParamMetadata.default values; column reflects effective
--      params, not raw {}.
--   3. Bootstrap dispatcher — StageSpec.params dict passed through
--      validate_job_params(allow_internal_keys=True); column reflects
--      bootstrap-supplied dict including audit-only internal keys
--      (e.g. source_label).
--
-- Control envelope keys (e.g. _override_bootstrap_gate) are NEVER
-- persisted here — they live in pending_job_requests.payload.control
-- and are stripped before reaching this column.
--
-- ## Lock impact
--
-- PG 14+ optimises ADD COLUMN ... DEFAULT <constant> to a metadata-
-- only update — no full-table rewrite. Safe online; sub-second.
--
-- ## ETL clauses
--
-- CLAUDE.md ETL clauses 8-11 NOT applicable: job_runs is operator-
-- process audit metadata, not ownership/fundamentals/observations
-- source data. Clause 12 (PR description records verification)
-- recorded in the PR body.

BEGIN;

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS params_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMENT ON COLUMN job_runs.params_snapshot IS
    'Effective params dict the run was invoked with. Manual triggers '
    'write the validated operator payload; scheduled fires write '
    'registry defaults from ParamMetadata; bootstrap dispatcher writes '
    'StageSpec.params (including internal audit-only keys). Control '
    'envelope flags (override_bootstrap_gate, etc.) are stripped before '
    'persistence — only operator-visible job parameters live here.';

COMMIT;
