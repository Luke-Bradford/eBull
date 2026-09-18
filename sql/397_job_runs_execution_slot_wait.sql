-- #3159 clause 2: record how long a fire waited for an execution slot, durably.
--
-- `_job_execution_slot` is the outermost boundary of every runtime entry path --
-- "parameter-error audit writes, gate/prerequisite checks, advisory locks, the
-- job body, and terminal writes all occur only after a slot is held"
-- (`app/jobs/runtime.py`, `wrapped()`).  The prelude stamps `started_at` INSIDE
-- that boundary, so a fire that parked on the semaphore for 19 minutes and a
-- fire admitted instantly produce byte-identical rows, and
-- `finished_at - started_at` measures the body only.  The wait existed solely in
-- stderr plus a process-local in-memory snapshot a restart discards.
--
-- Measured 7 days to 2026-09-18 on dev: 567 of 13,054 rows (4.3%) are
-- `skipped / max_instances_active`, across 11 jobs.  For three of them the
-- job's own body cannot explain the collision at all -- `jobs_retry_sweeper`
-- (300 s cadence, max body 0.20 s, 74 skips), `jobs_liveness_watchdog` (900 s,
-- 0.56 s, 9) and `expected_filings_poller` (900 s, 1.93 s, 6).  Reproduce with:
--
--     select job_name, count(*) from job_runs
--      where started_at > now() - interval '7 days'
--        and status = 'skipped' and error_msg like '%max_instances_active%'
--      group by 1 order by 2 desc;
--
-- ⚠ NULLABLE WITH NO DEFAULT, deliberately.  `DEFAULT 0` would make every row
-- that never passed through a slot read as an admitted-instantly row -- the
-- prevention-log trap where a default value stands in for a measurement.  The
-- fast path writes an explicit 0.000 instead, so the three states stay distinct:
-- NULL = not written from inside a slot, 0.000 = slot held, admitted
-- immediately, > 0 = admission blocked for that many seconds before started_at.
--
-- ⚠ `numeric`, not `double precision`: three decimals is what the runtime's own
-- admission log line has always printed ("acquired %s execution capacity after
-- %.3fs"), and `job_runs` carries no float column today.  Verified 2026-09-18
-- that psycopg3 adapts a Python `float` into this column without an explicit
-- cast (1140.4567 -> 1140.457, 0.0 -> 0.000, None -> NULL).
--
-- ⚠ PRECISION 12, not 9, and the reason is a failure mode rather than a range
-- estimate.  `numeric(9,3)` caps at 999999.999 (11.6 days); a wait past the cap
-- raises `numeric field overflow` INSIDE the prelude's INSERT, which would make
-- an instrumentation column able to FAIL the job it instruments -- and it would
-- do so precisely in the pathological-wait case the column exists to observe.
-- 12 digits cannot be reached by any wall-clock this process can survive.
--
-- No index.  Every consumer is a bounded-window scan already filtered on
-- `started_at`.
ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS execution_slot_wait_seconds numeric(12,3);

COMMENT ON COLUMN job_runs.execution_slot_wait_seconds IS
    'Seconds this fire waited on its lane''s execution semaphore BEFORE started_at '
    '(#3159 clause 2). 0.000 = a slot was held and admission was immediate. NULL = '
    'the row was not written from inside an execution slot: the three '
    '_PRELUDE_OPT_OUT_JOBS, record_job_skip rows written outside the prelude '
    '(param-validation abort, lane_busy), and _tracked_job''s record_job_start '
    'fallback. Not backfillable -- the wait was never persisted before this column.';
