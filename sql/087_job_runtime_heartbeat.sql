-- 087_job_runtime_heartbeat.sql
--
-- Issue #719 — multi-subsystem heartbeat for the out-of-process jobs runtime.
--
-- Each supervised subsystem (scheduler, manual_listener, queue_drainer,
-- main) writes its own row every 10s. The API /system/jobs endpoint
-- reads ALL rows; jobs_process_state is "healthy" only if every expected
-- subsystem has now() - last_beat_at < 60s.
--
-- This addresses the listener-died-but-heartbeat-thread-still-up failure
-- mode the round-1 Codex review identified — a stalled subsystem fails
-- the per-row health check even when other threads in the same process
-- keep beating.

CREATE TABLE IF NOT EXISTS job_runtime_heartbeat (
    subsystem            TEXT PRIMARY KEY,
    last_beat_at         TIMESTAMPTZ NOT NULL,
    pid                  INTEGER NOT NULL,
    process_started_at   TIMESTAMPTZ NOT NULL,
    notes                JSONB                       -- restart counts, last claim ts, etc.
);

COMMENT ON TABLE job_runtime_heartbeat IS
    'Per-subsystem liveness for the jobs process (#719). Allowed '
    'subsystem keys: scheduler, manual_listener, queue_drainer, main. '
    'Each subsystem upserts every 10s; the API surfaces stale '
    'subsystems (>60s since last beat) so a partial process failure '
    'is visible without log grepping.';
