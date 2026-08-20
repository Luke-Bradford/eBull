-- 225_bootstrap_stages_lane_fair_value_band.sql
--
-- #2024 (#2009 A8) — register the ``fair_value_band`` lane for the terminal
-- fair-value-band first-load stage (S28).
--
-- The lane CHECK constraint on ``bootstrap_stages.lane`` was last widened by
-- sql/165 (``openfigi``, #1233 PR-1b). This migration adds
-- ``'fair_value_band'`` so the new S28 ``fair_value_band`` stage
-- (``_BOOTSTRAP_STAGE_SPECS`` insertion in
-- ``app/services/bootstrap_orchestrator.py``) can be persisted.
--
-- Why a dedicated lane (not ``db``): the ``fair_value_band_refresh`` job is
-- already source-locked to the ``fair_value_band`` JobLock lane
-- (``MANUAL_TRIGGER_JOB_SOURCES``, app/jobs/sources.py — it is the sole writer
-- of ``fair_value_band_observations`` / ``fair_value_band_current``, so it runs
-- write-disjoint from every other lane). The registry invariant
-- (``test_job_registry::test_real_registry_has_no_conflicting_lane_duplicates``)
-- requires every source path for a job_name to resolve to the SAME lane, so the
-- bootstrap StageSpec lane MUST also be ``fair_value_band``.
--
-- The legacy ``'sec'`` lane stays valid (carried since migration 132) for
-- pre-#1020 run history rows — sql/147 + sql/165 preserve it the same way.
--
-- Idempotent: DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT under BEGIN/COMMIT.
-- Re-running this migration is a no-op once applied.

BEGIN;

ALTER TABLE bootstrap_stages
    DROP CONSTRAINT IF EXISTS bootstrap_stages_lane_check;

ALTER TABLE bootstrap_stages
    ADD CONSTRAINT bootstrap_stages_lane_check
    CHECK (lane IN (
        'init',
        'etoro',
        'sec',
        'sec_rate',
        'sec_bulk_download',
        'db',
        'db_filings',
        'db_fundamentals_raw',
        'db_ownership_inst',
        'db_ownership_insider',
        'db_ownership_funds',
        'openfigi',
        'fair_value_band'
    ));

COMMIT;
