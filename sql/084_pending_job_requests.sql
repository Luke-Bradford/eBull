-- 084_pending_job_requests.sql
--
-- Issue #719 — durable trigger queue for the out-of-process job runtime.
--
-- API publishers INSERT a row + pg_notify('ebull_job_request', request_id::text);
-- the jobs-process listener (and its 5s poll fallback) claims rows atomically
-- via UPDATE ... RETURNING and dispatches.
--
-- LISTEN/NOTIFY alone is lossy — events sent while the jobs process is down
-- or reconnecting are dropped silently. Storing the request DURABLY first and
-- using NOTIFY only as a wakeup hint preserves trigger durability across
-- jobs-process restarts. The 24h TTL on boot-drain prevents indefinite
-- replay after a long-disabled jobs process.

CREATE TABLE IF NOT EXISTS pending_job_requests (
    request_id    BIGSERIAL PRIMARY KEY,
    request_kind  TEXT NOT NULL CHECK (request_kind IN ('manual_job', 'sync')),
    job_name      TEXT,                   -- populated for request_kind='manual_job'
    payload       JSONB,                  -- populated for request_kind='sync' (scope JSON)
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    requested_by  TEXT,                   -- operator id / 'service-token' / 'scheduler-catchup'
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'claimed', 'dispatched', 'completed', 'rejected')),
    claimed_at    TIMESTAMPTZ,
    claimed_by    TEXT,                   -- jobs-process boot id (pid + boot timestamp)
    error_msg     TEXT
);

-- Partial index on the hot path: the listener's poll fallback and boot-drain
-- both scan unclaimed rows ordered by requested_at. Bounded by status=pending
-- so the planner never traverses completed rows.
CREATE INDEX IF NOT EXISTS idx_pending_job_requests_unclaimed
    ON pending_job_requests (requested_at)
    WHERE status = 'pending';

-- Operator-facing /jobs/requests filter index. Most queries scope by status
-- + requested_at DESC so a composite index on those two columns covers the
-- common reads without forcing a scan of the JSONB payload column.
CREATE INDEX IF NOT EXISTS idx_pending_job_requests_status_requested_at
    ON pending_job_requests (status, requested_at DESC);

COMMENT ON TABLE pending_job_requests IS
    'Durable trigger queue for the jobs process (#719). Every API publisher '
    '(POST /jobs/{name}/run, POST /sync) writes a row here before pg_notify, '
    'so a trigger sent while the jobs process is restarting is replayed on '
    'boot rather than lost. Status flow: pending → claimed → dispatched → '
    'completed; or pending → rejected on dispatch error. The jobs-process '
    'boot-drainer resets stale claimed/dispatched rows to pending and '
    'replays them under the singleton-fence advisory-lock invariant.';

COMMENT ON COLUMN pending_job_requests.claimed_by IS
    'Jobs-process boot identifier (pid + monotonic boot timestamp). Stale '
    'boot ids are detected and reset by the boot-drainer; safe because the '
    'JOBS_PROCESS_LOCK_KEY advisory lock guarantees only one live process.';
