-- 135_process_stop_requests.sql
--
-- Issue #1065 (umbrella #1064) — admin control hub rewrite, PR1 of 10.
--
-- Spec: docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md
--       §Schema migrations / sql/135.
--
-- ## Why
--
-- The admin control hub adds a cooperative-cancel signal so an
-- operator can stop a long-running bootstrap, scheduled job, or
-- orchestrator full-sync mid-flight. Watermarks (data_freshness_index,
-- sec_filing_manifest, etc.) plus ON CONFLICT idempotency mean Iterate
-- after Cancel resumes from the last committed state — no re-fetch,
-- no double-write.
--
-- This migration is the persistence layer for that signal. The cancel
-- handler runs ONE transaction:
--   1. SELECT ... FOR UPDATE on the active run row in the
--      mechanism-specific table (bootstrap_runs / job_runs / sync_runs).
--   2. INSERT a process_stop_requests row pinning target_run_kind +
--      target_run_id from the locked row. Partial unique index
--      catches concurrent inserts as UniqueViolation -> 409.
--   3. UPDATE the active run's cancel_requested_at fast-path column
--      (added in sql/136 + sql/137 + sql/139) for low-latency
--      observation by the worker.
--
-- ## Worker poll
--
--   SELECT id, mode FROM process_stop_requests
--    WHERE target_run_kind = ? AND target_run_id = ?
--      AND completed_at IS NULL
--    ORDER BY requested_at DESC
--    LIMIT 1;
--
-- The worker pins on the EXACT run id it owns, so a stop row for a
-- later run cannot wrongly cancel the current one.
--
-- ## Boot recovery
--
-- On jobs-process startup (app/services/process_stop.py), abandoned
-- stop rows older than 6 hours are swept:
--   UPDATE process_stop_requests
--      SET completed_at = now()              -- frees partial-unique slot
--          -- observed_at left NULL: sentinel "abandoned, never observed"
--    WHERE completed_at IS NULL
--      AND requested_at < now() - INTERVAL '6 hours';
--
-- ## Codex review record (post-rounds 1-6)
--
--   target_run_kind + target_run_id are NOT NULL and pinned at insert
--   (round 1 B2/B3 + round 3 B4 widened to include 'sync_run').
--   Partial unique on (target_run_kind, target_run_id) WHERE
--   completed_at IS NULL prevents duplicate active stop rows
--   (round 1 W6).

BEGIN;

CREATE TABLE IF NOT EXISTS process_stop_requests (
    id                       BIGSERIAL PRIMARY KEY,
    process_id               TEXT        NOT NULL,
    mechanism                TEXT        NOT NULL
        CHECK (mechanism IN ('bootstrap', 'scheduled_job', 'ingest_sweep')),
    target_run_kind          TEXT        NOT NULL
        CHECK (target_run_kind IN ('bootstrap_run', 'job_run', 'sync_run')),
    target_run_id            BIGINT      NOT NULL,
    mode                     TEXT        NOT NULL
        CHECK (mode IN ('cooperative', 'terminate')),
    requested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    requested_by_operator_id UUID REFERENCES operators(operator_id),
    observed_at              TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ
);

-- Partial unique: at most one ACTIVE stop request per (run_kind, run_id).
-- A second cancel against the same in-flight run hits UniqueViolation
-- atomically rather than racing.
CREATE UNIQUE INDEX IF NOT EXISTS process_stop_requests_active_unq
    ON process_stop_requests (target_run_kind, target_run_id)
    WHERE completed_at IS NULL;

-- Forensic lookup: list all stop requests for a process across history.
CREATE INDEX IF NOT EXISTS process_stop_requests_process_idx
    ON process_stop_requests (process_id, requested_at DESC);

COMMIT;
