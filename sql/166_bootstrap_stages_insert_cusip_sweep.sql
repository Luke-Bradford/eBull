-- 166_bootstrap_stages_insert_cusip_sweep.sql
--
-- #1233 PR-1b (spec §5) — stage renumber for ``cusip_resolver_post_bulk_sweep``.
--
-- ## What this migration does
--
-- Shifts every existing ``bootstrap_stages.stage_order`` row at or
-- above 13 up by one (13 -> 14, 14 -> 15, …, 26 -> 27) so the orchestrator
-- can slot the new S13 ``cusip_resolver_post_bulk_sweep`` (lane=``openfigi``)
-- between Phase C bulk ingest (S8-S12) and the per-CIK secondary-pages
-- walk (formerly S13, now S14).
--
-- ## Why renumber at the DB level
--
-- An in-flight bootstrap run at deploy time has rows in
-- ``bootstrap_stages`` whose ``(run_id, stage_order)`` UNIQUE matches
-- the orchestrator's ``_BOOTSTRAP_STAGE_SPECS`` catalogue. If we
-- silently changed the catalogue without renumbering existing rows
-- the orchestrator would either:
--   (a) re-scaffold the missing S13 slot mid-run and corrupt the
--       ordering, or
--   (b) treat the existing S13 (sec_submissions_files_walk) row as
--       the new ``cusip_resolver_post_bulk_sweep`` and dispatch the
--       wrong invoker against the wrong stage_key.
-- Neither is acceptable. Renumbering at the DB level keeps every
-- existing run consistent with the new catalogue shape — the
-- now-S14 ``sec_submissions_files_walk`` row is still recognisable
-- by its ``stage_key`` and the orchestrator picks up where it left off.
--
-- ## Why this is safe to run repeatedly
--
-- Idempotent re-application would otherwise double-shift (13 -> 14
-- on first run, 14 -> 15 on second run). To prevent that we GATE the
-- shift on whether the post-renumber state is already in place:
--
--   * Pre-state: catalogue has ``sec_submissions_files_walk`` at S13;
--     no row exists for stage_key ``cusip_resolver_post_bulk_sweep``.
--   * Post-state: ``sec_submissions_files_walk`` at S14; the orchestrator
--     will create the S13 row for ``cusip_resolver_post_bulk_sweep``
--     on the next ``run_bootstrap_orchestrator`` invocation that
--     scaffolds a fresh run (existing in-flight runs see the empty
--     S13 slot and the orchestrator handles the missing-row case by
--     re-scaffolding only stages NOT already present for the run_id).
--
-- The gate: only shift if there exists ANY row with stage_key =
-- 'sec_submissions_files_walk' AND stage_order = 13. If none exist
-- (either never-ran-this-migration's-target or post-shift), this
-- migration is a no-op.
--
-- Concurrent-run safety: the orchestrator holds JobLock('bootstrap')
-- when scaffolding new runs, so a deploy that lands this migration
-- between an in-flight bootstrap's stage transitions cannot race
-- with the catalogue-scaffold step. Live stage transitions touch
-- ``status`` / ``started_at`` / ``finished_at`` only — never
-- ``stage_order``. (#1184 ContextVar locking pattern.)

BEGIN;

-- Take a single update path: shift all rows with stage_order >= 13
-- UPward by 1, but ONLY when the catalogue's old S13 stage_key is
-- still in slot 13 somewhere in the table. This makes the migration
-- idempotent without resorting to a non-SQL guard.
--
-- Idempotency guard: the WHERE clause checks for the pre-shift sentinel.
-- After a successful first run the sentinel row sits at stage_order=14,
-- so the subselect returns false and the UPDATE is a no-op.
UPDATE bootstrap_stages
   SET stage_order = stage_order + 1
 WHERE stage_order >= 13
   AND EXISTS (
       SELECT 1 FROM bootstrap_stages
        WHERE stage_key = 'sec_submissions_files_walk'
          AND stage_order = 13
   );

COMMIT;
