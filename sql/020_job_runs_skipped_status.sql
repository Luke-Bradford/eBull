-- Migration 020: add 'skipped' status to job_runs
--
-- Jobs that are skipped due to unmet prerequisites (e.g. no coverage rows,
-- missing API keys) are now recorded with status='skipped' instead of
-- status='success' with row_count=0.  This lets the Admin UI distinguish
-- "ran and did nothing" from "deliberately skipped because upstream data
-- was not ready".
--
-- Issue: #146

ALTER TABLE job_runs
    DROP CONSTRAINT job_runs_status_check,
    ADD CONSTRAINT job_runs_status_check CHECK (status IN ('running', 'success', 'failure', 'skipped'));
