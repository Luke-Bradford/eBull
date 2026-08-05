-- 254_job_runs_degraded_progress.sql
--
-- #2218 — a job that makes ZERO progress must stop reporting 'success'.
--
--
-- THE DEFECT THIS CLOSES
-- ---------------------------------------------------------------------------
-- Job health is derived from COMPLETION, not from PROGRESS. Two instances
-- found within an hour of each other on 2026-08-03, neither flagged by any
-- automated check:
--
--   * `cusip_resolver_post_bulk_sweep` — the resolver binds a whole-batch
--     OpenFIGI failure to an `api_errors` counter instead of raising, and the
--     invoker records `row_count = report.promoted`. A pass where EVERY CUSIP
--     errored logs `row_count = 0, status = 'success'`. OpenFIGI resolution
--     stopped 2026-06-18 and the job reported healthy through 2026-08-02.
--     Measured on this DB 2026-08-05: the last four runs are
--     `success / row_count 0`, unbroken.
--   * `ncen_classifier_yearly` — one run, 2026-06-04, `success`, 0 rows, and
--     `ncen_filer_classifications` is still empty. On a YEARLY cadence it
--     would not fire again until 2027.
--
-- The health surface is self-consistent — `/system/status`, `job_runs` success
-- rates and the admin verdict all agree these jobs are fine, because all three
-- derive from the same "did it finish" signal. There was no detector for the
-- no-op-success class at all; both instances were found by ad-hoc query.
--
--
-- WHY A COUNTER COLUMN AND NOT "ZERO ROWS = FAILURE"
-- ---------------------------------------------------------------------------
-- ⚠ Plenty of jobs legitimately have nothing to do on a given run, so a blanket
-- "row_count = 0 is a failure" would be noise that trains the operator to
-- ignore the signal. The distinction is `candidates_seen`: a job that saw no
-- work and did none is healthy; a job that saw work and produced no terminal
-- outcome at all is stalled. That distinction cannot be inferred after the
-- fact from `row_count`, which is why the counters have to be PERSISTED rather
-- than left in log lines — #2213 was diagnosed by log archaeology, and the
-- point of this migration is that the next one is a query.
--
-- `progress_json` is deliberately free-shaped: every job counts different
-- things (`api_errors` / `unresolved_by_openfigi` / `parse_failures` /
-- `fetch_failures`), and freezing that into columns would mean a migration per
-- job. The VERDICT is what is standardised, in
-- `app/services/job_progress.py`; this column is its evidence.

ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS progress_json JSONB;

-- 'degraded' — the run completed and made no progress. Distinct from
-- 'failure' (raised) and from 'success' (completed AND progressed), because
-- collapsing it into either loses the thing the operator needs: a degraded run
-- is not an incident to page on, and it is not fine.
ALTER TABLE job_runs
    DROP CONSTRAINT IF EXISTS job_runs_status_check;
-- ⚠ 'cancelled' is in this list because sql/137 added it. The obvious way to
-- write this migration is to copy the most recent list you happen to find —
-- sql/020's, which predates sql/137 — and that DROPS 'cancelled'. It fails
-- silently in the worst direction: a database with no cancelled rows accepts
-- the constraint and then rejects the next cancelled run, and this dev DB has
-- exactly zero (`select status, count(*) from job_runs group by 1` →
-- success/skipped/failure/running only), so it applied clean. Caught by Codex
-- at checkpoint 2, not by any gate.
--
-- The full vocabulary, derived from the constraint by
-- tests/test_job_terminal_status_contract.py so a sixth member cannot be
-- added here and missed in the five SQL terminal-status filters that read it.
ALTER TABLE job_runs
    ADD CONSTRAINT job_runs_status_check
        CHECK (status IN ('running', 'success', 'failure', 'skipped', 'cancelled', 'degraded'));

COMMENT ON COLUMN job_runs.progress_json IS
    'Per-job progress counters, as the job itself counts them: '
    '{"candidates_seen": N, "outcomes": {...}, "errors": {...}}. The verdict '
    'derived from it is app/services/job_progress.py::degradation_reason, and '
    'status = ''degraded'' is that verdict. Persisted rather than logged '
    'because #2213 took log archaeology to diagnose — the successor should be '
    'a query. NULL means the job does not report progress, which is NOT the '
    'same as reporting zero.';
