-- 157: institutional_filers.last_13f_hr_at — bound bootstrap 13F sweep cohort (#1010)
--
-- Adds an HR-only filing-recency signal to ``institutional_filers``.
--
--   - ``last_filing_at`` (existing) — most recent 13F-* (HR + HR/A + NT + NT/A).
--   - ``last_13f_hr_at`` (NEW)      — most recent 13F-HR or 13F-HR/A only.
--
-- Backstory: ``bootstrap_sec_13f_recent_sweep`` (stage 21) walks the
-- full 11,205-row ``institutional_filers`` cohort, taking ~8h to
-- complete first-install bootstrap. Most filers below the top-N
-- AUM cohort file 13F-NT (notice-only — no holdings) and contribute
-- no observations. ``last_filing_at`` cannot distinguish them
-- because it includes both HR and NT.
--
-- This migration adds the HR-only column. ``sec_13f_filer_directory_sync``
-- populates it from form.idx (HR-only frozenset). The bootstrap stage
-- 21 dispatch filters cohort to filers with ``last_13f_hr_at`` ≥
-- today() - 380d. Standalone manual / sweep-adapter / Admin "Run now"
-- paths keep the full cohort (safety-net for previously-inactive
-- filers re-emerging).
--
-- Spec: docs/superpowers/specs/2026-05-19-1010-13f-cohort-bound.md.
-- Migration is tx-wrapped by the runner — no explicit BEGIN/COMMIT.

-- 1. Add column. Nullable: NT-only filers + filers with no HR in the
--    observations table stay NULL and are excluded by the cohort
--    filter (which uses ``IS NOT NULL AND >= cutoff``).
ALTER TABLE institutional_filers
    ADD COLUMN IF NOT EXISTS last_13f_hr_at TIMESTAMPTZ;

-- 2. Backfill from the canonical 13F-HR sink. This is an
--    approximation, not ground truth — see spec §3.1 backfill
--    caveat. The next scheduled ``sec_13f_filer_directory_sync`` run
--    re-populates from form.idx (the canonical source) and corrects
--    any miss.
UPDATE institutional_filers f
SET last_13f_hr_at = sub.max_filed_at
FROM (
    SELECT filer_cik, MAX(filed_at) AS max_filed_at
    FROM ownership_institutions_observations
    WHERE source = '13f'
    GROUP BY filer_cik
) sub
WHERE f.cik = sub.filer_cik
  AND f.last_13f_hr_at IS NULL;  -- idempotent on re-run

-- 3. Index for the cohort filter + ORDER BY. DESC NULLS LAST matches
--    the ``ORDER BY last_13f_hr_at DESC NULLS LAST, cik`` shape used
--    by ``list_directory_filer_ciks`` when the cohort filter is
--    active.
CREATE INDEX IF NOT EXISTS idx_institutional_filers_last_13f_hr_at
    ON institutional_filers (last_13f_hr_at DESC NULLS LAST);

COMMENT ON COLUMN institutional_filers.last_13f_hr_at IS
    'Most recent 13F-HR / 13F-HR/A filing date observed for this CIK '
    '(HR-only; excludes 13F-NT). Set by sec_13f_filer_directory_sync '
    'from form.idx + by the legacy per-filing ingest. Bootstrap '
    'stage 21 filters cohort on this column; standalone paths use '
    'the full cohort. #1010.';
