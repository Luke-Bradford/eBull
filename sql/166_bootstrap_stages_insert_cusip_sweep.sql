-- 166_bootstrap_stages_insert_cusip_sweep.sql
--
-- #1233 PR-1b (spec §5) — stage renumber + new-S13 row insert for
-- ``cusip_resolver_post_bulk_sweep``.
--
-- ## What this migration does
--
-- 1. Shifts every existing ``bootstrap_stages.stage_order`` row at or
--    above 13 up by one (13 -> 14, 14 -> 15, …, 26 -> 27) so the
--    orchestrator can slot the new S13 ``cusip_resolver_post_bulk_sweep``
--    (lane=``openfigi``) between Phase C bulk ingest (S8-S12) and the
--    per-CIK secondary-pages walk (formerly S13, now S14).
--
-- 2. INSERTs the new S13 row into every existing run that was shifted,
--    so an in-flight run picks the new stage up via the orchestrator's
--    normal dispatch loop (which iterates persisted ``bootstrap_stages``
--    rows, NOT the in-code catalogue). Without this step, in-flight
--    runs at deploy time would skip the OpenFIGI sweep entirely AND
--    never write ``bootstrap_runs.coverage_floor_met``. Codex 2
--    pre-push review #1233 PR-1b BLOCKING.
--
-- ## Why renumber + insert in one migration
--
-- An in-flight bootstrap run at deploy time has rows in
-- ``bootstrap_stages`` whose ``(run_id, stage_order)`` UNIQUE matches
-- the orchestrator's ``_BOOTSTRAP_STAGE_SPECS`` catalogue. If we
-- silently changed the catalogue without renumbering + slotting the
-- new row, the orchestrator would either:
--   (a) treat the existing S13 (sec_submissions_files_walk) row as
--       the new ``cusip_resolver_post_bulk_sweep`` and dispatch the
--       wrong invoker against the wrong stage_key, or
--   (b) finish the run with the new stage absent — never advertising
--       ``coverage_floor_met``.
-- Renumbering at the DB level keeps every existing run consistent
-- with the new catalogue shape; inserting the new S13 row keeps the
-- per-run stage count parity (post-shift = 27 stages per run).
--
-- ## Why this is safe to run repeatedly
--
-- Idempotent re-application would otherwise double-shift. We GATE
-- the shift on whether the post-renumber state is already in place:
--
--   * Pre-state: catalogue has ``sec_submissions_files_walk`` at S13;
--     no row exists for stage_key ``cusip_resolver_post_bulk_sweep``.
--   * Post-state: ``sec_submissions_files_walk`` at S14;
--     ``cusip_resolver_post_bulk_sweep`` exists at S13 for every run.
--
-- The gate: only shift if there exists ANY row with stage_key =
-- 'sec_submissions_files_walk' AND stage_order = 13. The INSERT is
-- gated by ``NOT EXISTS`` per-run so a run that already has the new
-- row is untouched.
--
-- Concurrent-run safety: the orchestrator holds JobLock('bootstrap')
-- when scaffolding new runs, so a deploy that lands this migration
-- between an in-flight bootstrap's stage transitions cannot race
-- with the catalogue-scaffold step. Live stage transitions touch
-- ``status`` / ``started_at`` / ``finished_at`` only — never
-- ``stage_order``. (#1184 ContextVar locking pattern.)

BEGIN;

-- Step 1: detect pre-shift state via a single boolean. The CTE
-- captures the state ONCE; both subsequent statements key off it
-- so a partially-applied state (shift happened, insert didn't) can
-- still be detected and the insert completed.

WITH pre_shift_state AS (
    SELECT EXISTS (
        SELECT 1 FROM bootstrap_stages
         WHERE stage_key = 'sec_submissions_files_walk'
           AND stage_order = 13
    ) AS needs_shift
)
UPDATE bootstrap_stages
   SET stage_order = stage_order + 1
 WHERE stage_order >= 13
   AND (SELECT needs_shift FROM pre_shift_state);

-- Step 2: insert the new S13 row for every run whose stages have
-- been shifted (post-shift sentinel: stage_key='sec_submissions_files_walk'
-- AT stage_order=14). The ``NOT EXISTS`` guard per-run makes the
-- INSERT idempotent — a run that already has the new row is untouched.
--
-- Status='pending' for runs that haven't reached S13's position yet
-- (most common case — Phase C blocks Phase D); for finished runs
-- (status='complete' / 'partial_error' / 'cancelled') the new row
-- stays 'pending' too, which is the correct audit-history outcome
-- (the orchestrator's run-complete check uses bootstrap_state, not
-- per-run stage counts, so a lingering 'pending' row on a finished
-- run is harmless).

INSERT INTO bootstrap_stages (
    bootstrap_run_id,
    stage_key,
    stage_order,
    lane,
    job_name,
    status,
    attempt_count
)
SELECT DISTINCT
    bs.bootstrap_run_id,
    'cusip_resolver_post_bulk_sweep',
    13,
    'openfigi',
    'cusip_resolver_post_bulk_sweep',
    'pending',
    0
  FROM bootstrap_stages bs
 WHERE bs.stage_key = 'sec_submissions_files_walk'
   AND bs.stage_order = 14
   AND NOT EXISTS (
       SELECT 1 FROM bootstrap_stages e
        WHERE e.bootstrap_run_id = bs.bootstrap_run_id
          AND e.stage_key = 'cusip_resolver_post_bulk_sweep'
   );

COMMIT;
