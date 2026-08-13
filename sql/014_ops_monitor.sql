-- Migration 014: ops monitor — job run tracking
--
-- job_runs records every scheduled job execution: start time, finish time,
-- outcome, row count, and optional error detail.  The ops monitor queries
-- this table to detect missing or failed runs.

CREATE TABLE IF NOT EXISTS job_runs (
    run_id      BIGSERIAL PRIMARY KEY,
    job_name    TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running',   -- running | success | failure
    row_count   INTEGER,
    error_msg   TEXT,
    CONSTRAINT job_runs_status_check CHECK (status IN ('running', 'success', 'failure'))
);

CREATE INDEX IF NOT EXISTS idx_job_runs_name_started
    ON job_runs(job_name, started_at DESC);
