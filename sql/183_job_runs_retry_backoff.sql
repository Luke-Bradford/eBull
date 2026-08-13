-- 183_job_runs_retry_backoff.sql
--
-- Issue: #1509 (T3 of epic #1508). Spec:
-- docs/specs/ops/2026-06-07-job-retry-backoff.md
--
-- ## Why
--
-- A failed scheduled job has no near-term retry — it waits a full cadence
-- (daily ≤24h, weekly ≤7d, yearly ≤12mo) for its next natural fire. These
-- two columns give record_job_finish (app/services/ops_monitor.py) a place
-- to schedule a transient-failure retry on capped exponential backoff,
-- which the new jobs_retry_sweeper re-fires through the audited manual
-- queue. Permanent failures (auth/schema-drift/db-constraint/missing-key,
-- classified via REMEDIES[category].self_heal) leave next_retry_at NULL and
-- surface as Needs-attention immediately — so e.g. #1516's NUMERIC overflow
-- (DB_CONSTRAINT) never retry-storms.
--
--   * next_retry_at — when the failed run should be retried. Set only on a
--     status='failure' row; NULL = no retry (permanent / exhausted / success).
--   * attempt — this run's attempt number within the current failure streak
--     (1 = first natural fire). Persisted for backoff math + observability.
--
-- ## Lock impact
--
-- PG 14+ optimises ADD COLUMN ... DEFAULT <constant> to a metadata-only
-- update — no full-table rewrite. Safe online; sub-second.
--
-- ## ETL clauses
--
-- CLAUDE.md ETL clauses 8-11 NOT applicable: job_runs is operator-process
-- audit metadata, not ownership/fundamentals/observations source data.
-- Clause 12 (PR records verification) recorded in the PR body.
--
-- No explicit BEGIN/COMMIT: the migration runner wraps the body + the
-- schema_migrations INSERT in one transaction (app/db/migrations.run_migrations);
-- an inline COMMIT would split them (prevention-log: tx-bound migrations).

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempt       SMALLINT NOT NULL DEFAULT 1;

-- Sweeper hot path: due retries are a tiny subset, so a partial index keeps
-- the scan off the full table.
CREATE INDEX IF NOT EXISTS job_runs_due_retry_idx
    ON job_runs (next_retry_at)
    WHERE next_retry_at IS NOT NULL;

COMMENT ON COLUMN job_runs.next_retry_at IS
    'Set on a status=''failure'' row when a transient-failure retry is '
    'scheduled (now + capped exponential backoff). NULL = no retry pending '
    '(permanent failure, exhausted attempts, or a non-failure terminal). '
    'Driven by app/services/ops_monitor.record_job_finish; consumed by the '
    'jobs_retry_sweeper scheduled job (#1509).';

COMMENT ON COLUMN job_runs.attempt IS
    'Attempt number of this run within the current consecutive-failure '
    'streak (1 = first natural fire). Used for backoff math and audit (#1509).';
