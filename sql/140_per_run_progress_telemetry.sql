-- 140_per_run_progress_telemetry.sql
--
-- Issue #1069 (umbrella #1064) — admin control hub rewrite, PR2 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Operator-amendment round 1 / A3 — per-process progress reporting.
--
-- ## Why
--
-- Operator A3 amendment: per-running-row "Processed: X" ticker, with
-- optional "Rows: N · Processed: X (Y%)" when target is known, plus
-- warning chips. The columns added here are schema-only; producer
-- (`JobTelemetryAggregator.record_processed` / `record_warning` /
-- `maybe_flush`) and consumer (Processes envelope adapters) wiring
-- arrive in PR3.
--
-- Mirror columns onto `job_runs`, `bootstrap_stages`, `sync_runs` so
-- the Processes table envelope can render parity progress UX across
-- mechanisms.
--
-- ## Bounded vs unbounded
--
-- `target_count` is nullable — NULL means unbounded (e.g. SEC drain
-- "anything since T?"). FE renders `Processed: 312` only. When set
-- (e.g. bootstrap_filings_history_seed over a CIK list), FE renders
-- `Rows: 1547 · Processed: 312 (20%)`.
--
-- ## Mid-flight stuck (4th stale case)
--
-- `last_progress_at` is the heartbeat: producer's `record_processed`
-- bumps it. Stale-detection (PR8) flags any `status='running'` row
-- whose `last_progress_at < now() - STALE_PROGRESS_THRESHOLD` (default
-- 5 min, per-job override).
--
-- ## warning_classes shape
--
-- Mirrors `job_runs.error_classes` (sql/137):
--   `{"RateLimited": {"count": 12, "sample_message": "...",
--                      "last_subject": "CIK 320193",
--                      "last_seen_at": "..."}}`
--
-- Producer-side: `record_warning(error_class, message, subject)`
-- (PR3) aggregates onto the JSONB at flush time.
--
-- ## Lock impact
--
-- PG 14+ `ADD COLUMN ... DEFAULT <constant>` is metadata-only, no
-- table rewrite. NULL-defaulted columns (`target_count`,
-- `last_progress_at`) are also metadata-only. All ALTERs run in one
-- transaction with the migration runner.

BEGIN;

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_count      INTEGER,
    ADD COLUMN IF NOT EXISTS last_progress_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE bootstrap_stages
    ADD COLUMN IF NOT EXISTS processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_count      INTEGER,
    ADD COLUMN IF NOT EXISTS last_progress_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS target_count      INTEGER,
    ADD COLUMN IF NOT EXISTS last_progress_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;

COMMIT;
