-- 086_sync_runs_linked_request.sql
--
-- Issue #719 — link sync_runs back to the queue request that triggered them.
-- Mirror of 085 for the sync orchestrator; lets boot-recovery use a parallel
-- NOT EXISTS clause against terminal sync_runs status values
-- (complete / failed / partial / cancelled) to skip replaying completed
-- sync requests.

ALTER TABLE sync_runs
    ADD COLUMN IF NOT EXISTS linked_request_id BIGINT
        REFERENCES pending_job_requests(request_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_sync_runs_linked_request_id
    ON sync_runs (linked_request_id)
    WHERE linked_request_id IS NOT NULL;

COMMENT ON COLUMN sync_runs.linked_request_id IS
    'Queue request that triggered this sync (#719). NULL for scheduled '
    'fires (boot_sweep, scheduled, catch_up). Populated by '
    '_start_sync_run when the dispatcher claim path passes the '
    'request_id through.';
