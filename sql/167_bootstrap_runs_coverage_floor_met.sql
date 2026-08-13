-- 167_bootstrap_runs_coverage_floor_met.sql
--
-- #1233 PR-1b (spec §5) — record-only floor flag for bootstrap runs.
--
-- ## What this adds
--
-- One nullable BOOLEAN column on ``bootstrap_runs``:
--   * ``coverage_floor_met`` — set by the S13
--     ``cusip_resolver_post_bulk_sweep`` invoker after the sweep
--     completes, based on the post-sweep ``compute_cusip_coverage``
--     ratio against the 80% floor.
--
-- NULL = never computed (legacy runs pre-PR-1b, or the stage
-- skipped / cascade-skipped before reaching the cov-compute step).
-- TRUE = post-sweep CUSIP coverage ratio >= 0.80.
-- FALSE = ratio < 0.80 (between 50% and 80% — the hard 50% floor
--         is still enforced by ``assert_cusip_coverage`` at the
--         per-stage precondition layer, so a 0-50% coverage state
--         tripped the existing BootstrapPreconditionError already
--         and would not have reached S13).
--
-- ## Why a column (not a status)
--
-- v2 of the spec proposed extending the ``status`` enum with a
-- ``partial_complete`` value. The v3 review (Codex Critical) rejected
-- that because it requires CHECK-constraint updates across every
-- run-history audit query AND breaks the simple "complete = success"
-- semantic that operator tooling relies on. The column is
-- informational only — the admin panel renders an amber chip when
-- ``coverage_floor_met = FALSE`` but the run still transitions to
-- ``status = 'complete'``. No code path blocks on this column.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS guards repeat application.

BEGIN;

ALTER TABLE bootstrap_runs
    ADD COLUMN IF NOT EXISTS coverage_floor_met BOOLEAN DEFAULT NULL;

COMMIT;
