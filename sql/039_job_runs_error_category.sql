-- 039_job_runs_error_category.sql
-- Adds error_category column to job_runs so the legacy job runner
-- can persist the same taxonomy as sync_layer_progress.

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS error_category TEXT;

CREATE INDEX IF NOT EXISTS idx_job_runs_error_category
    ON job_runs(error_category)
    WHERE error_category IS NOT NULL;
