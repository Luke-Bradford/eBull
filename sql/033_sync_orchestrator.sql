-- 033_sync_orchestrator.sql
--
-- Phase 1 of Data Orchestrator
-- (spec: docs/superpowers/specs/2026-04-16-data-orchestrator-and-observability-design.md)
-- (plan: docs/superpowers/plans/2026-04-16-data-orchestrator-p1.md)
--
-- Creates:
--   sync_runs                       -- one row per orchestrator invocation
--   sync_layer_progress             -- one row per emitted layer per sync run
--   idx_sync_runs_single_running    -- authoritative concurrency gate (partial unique)
--   idx_sync_runs_started           -- lookup index for recent runs

CREATE TABLE IF NOT EXISTS sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency', 'job')),
    scope_detail   TEXT,
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0,
    error_category TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_started
    ON sync_runs(started_at DESC);

-- Authoritative concurrency gate: at most one 'running' sync_runs row
-- across all scopes. Duplicate INSERT -> UniqueViolation -> SyncAlreadyRunning.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_runs_single_running
    ON sync_runs((TRUE))
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped', 'partial')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,
    items_done     INTEGER,
    row_count      INTEGER,
    error_category TEXT,
    skip_reason    TEXT,
    PRIMARY KEY (sync_run_id, layer_name)
);
