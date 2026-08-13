-- 130_bootstrap_archive_results.sql
--
-- Per-bootstrap-run audit trail for B-stage and C-stage archive
-- writes (#1020 ETL orchestration design).
--
-- Each Phase C ingester writes one row per processed archive
-- (e.g. ('sec_submissions_ingest', 'submissions.zip')). Each B-stage
-- writes one row per scheduler-job invocation
-- (archive_name = '__job__').
--
-- The row's existence proves "the stage ran in the current bootstrap
-- run" — the freshness invariant downstream stages query before
-- writing. ``rows_written`` is operator telemetry only; an idempotent
-- re-run against a populated reference table legitimately reports 0
-- upserts and that does NOT defeat the freshness proof.

BEGIN;

CREATE TABLE IF NOT EXISTS bootstrap_archive_results (
    bootstrap_run_id  BIGINT NOT NULL REFERENCES bootstrap_runs(id) ON DELETE CASCADE,
    stage_key         TEXT NOT NULL,
    archive_name      TEXT NOT NULL,
    rows_written      BIGINT NOT NULL DEFAULT 0,
    rows_skipped      JSONB NOT NULL DEFAULT '{}'::jsonb,
    completed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bootstrap_run_id, stage_key, archive_name)
);

COMMENT ON TABLE bootstrap_archive_results IS
    'Per-bootstrap-run per-archive write audit. Existence of a row proves the stage ran in this run; rows_written is telemetry only. Spec: docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md.';

CREATE INDEX IF NOT EXISTS idx_bootstrap_archive_results_run
    ON bootstrap_archive_results (bootstrap_run_id);

COMMIT;
