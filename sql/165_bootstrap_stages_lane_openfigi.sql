-- 165_bootstrap_stages_lane_openfigi.sql
--
-- #1233 PR-1b (spec §5) — register the new ``openfigi`` lane for the
-- CUSIP resolver post-bulk sweep stage.
--
-- The lane CHECK constraint on ``bootstrap_stages.lane`` was last
-- widened by sql/147 (db-lane family split, #1141). This migration
-- adds ``'openfigi'`` to the set so the new S13
-- ``cusip_resolver_post_bulk_sweep`` stage (sql/166 renumber + the
-- ``_BOOTSTRAP_STAGE_SPECS`` insertion in
-- ``app/services/bootstrap_orchestrator.py``) can be persisted.
--
-- Why a dedicated lane (not ``sec_rate``):
--
--   * OpenFIGI lives on its own host (api.openfigi.com), so it does
--     NOT share the SEC 10 req/s per-IP bucket. Putting it on the
--     ``sec_rate`` lane would silently steal SEC budget and serialise
--     OpenFIGI behind every per-CIK SEC fetch — exactly the conflation
--     SD-1 ("OpenFIGI on its own Lane") forbids.
--   * OpenFIGI rate-limit is independent and tier-dependent (unkeyed
--     25/min × 10 jobs = 250 mappings/min; keyed 25/6s × 100 jobs =
--     25,000 mappings/min). The ``openfigi`` lane serialises within
--     itself (cap=1) so the per-process rate limiter holds.
--
-- The legacy ``'sec'`` lane stays valid (carried since migration 132)
-- for pre-#1020 run history rows — sql/147 preserves it the same way.
--
-- Idempotent: DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT under
-- BEGIN/COMMIT. Re-running this migration is a no-op once applied.

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
        'openfigi'
    ));

COMMIT;
