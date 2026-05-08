-- 137_job_runs_per_item_telemetry.sql
--
-- Issue #1065 (umbrella #1064) — admin control hub rewrite, PR1 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Schema migrations / sql/137.
--
-- ## Why
--
-- The Processes table renders per-run telemetry beyond a single
-- scalar `row_count`. This migration adds:
--
--   * rows_skipped_by_reason (JSONB) — `{"unresolved_cusip": 42, ...}`.
--     Adapters without per-reason granularity emit `{"unknown": <n>}`.
--   * rows_errored (INT) — count of items that errored mid-run.
--   * error_classes (JSONB) — grouped error summary:
--     `{"ConnectionTimeout": {"count": 12, "sample_message": "...",
--                              "last_subject": "CIK 320193",
--                              "last_seen_at": "..."}}`
--   * cancel_requested_at (TIMESTAMPTZ) — operator-click moment;
--     fast-path observation column for cooperative cancel.
--   * cancelled_at (TIMESTAMPTZ) — terminal cancel timestamp.
--
-- Plus widens job_runs_status_check to allow 'cancelled' (mirrors
-- the sql/020 widening pattern; Codex round 1 B1).
--
-- Plus a per-process history accelerator index for the Runs tab.
-- NON-concurrent because the migration runner is transactional;
-- table is small at our scale (tens of thousands of rows), expected
-- build time sub-second (Codex round 2 R2-B4 + R2-W7/W8).
--
-- ## Lock impact
--
-- PG 14+ optimises ADD COLUMN ... DEFAULT <constant> to a
-- metadata-only update — no full-table rewrite. The CHECK widen
-- takes a brief AccessExclusive on job_runs to drop+add the
-- constraint; existing rows are already a subset of the new set so
-- validation is fast. Index build at our table size completes in
-- sub-second locally. Safe online.
--
-- ## Naming
--
-- `rows_skipped_by_reason` (not plain `rows_skipped`) disambiguates
-- from existing scalar `rows_skipped` in `bootstrap_archive_results`
-- and various ingest-log columns (Codex round 1 W9).

BEGIN;

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS rows_skipped_by_reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS rows_errored           INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS error_classes          JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS cancel_requested_at    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancelled_at           TIMESTAMPTZ;

-- Widen the existing CHECK to permit the new 'cancelled' status.
-- Mirrors the sql/020 widening pattern.
ALTER TABLE job_runs DROP CONSTRAINT IF EXISTS job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check
    CHECK (status IN ('running', 'success', 'failure', 'skipped', 'cancelled'));

-- Per-process history accelerator. Used by the Runs tab in the
-- Processes drill-in route to fetch the last 50 runs for a job_name
-- in start-time-descending order.
CREATE INDEX IF NOT EXISTS job_runs_status_started_idx
    ON job_runs (job_name, started_at DESC)
    WHERE status IN ('failure', 'success', 'cancelled');

COMMIT;
