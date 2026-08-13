-- 085_job_runs_linked_request.sql
--
-- Issue #719 — link manual_job runs back to the queue request that triggered
-- them. The dispatch wrapper inside the jobs process executor sets this on
-- the same conn that opens the job_runs row, so boot-recovery can use a
-- NOT EXISTS clause against terminal job_runs to skip replaying completed
-- requests.
--
-- ON DELETE SET NULL: a queue retention sweep deleting old completed
-- pending_job_requests rows must not cascade to job_runs (the run itself
-- is the canonical history; the queue row is just the trigger receipt).

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS linked_request_id BIGINT
        REFERENCES pending_job_requests(request_id) ON DELETE SET NULL;

-- Index supports the boot-drain NOT EXISTS subquery and the operator
-- /jobs/requests pivot view. Sparse — most legacy job_runs rows pre-#719
-- are NULL, so a partial index on non-null is small and hot.
CREATE INDEX IF NOT EXISTS idx_job_runs_linked_request_id
    ON job_runs (linked_request_id)
    WHERE linked_request_id IS NOT NULL;

COMMENT ON COLUMN job_runs.linked_request_id IS
    'Queue request that triggered this run (#719). NULL for scheduled '
    'fires and for any run created before the queue existed. Populated '
    'by the jobs-process dispatch wrapper at the same time the row is '
    'inserted, so boot-recovery can skip replaying requests whose run '
    'already reached a terminal status.';
