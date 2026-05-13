-- 147_bootstrap_stages_lane_family_split.sql
--
-- Issue #1141 (Task E of audit umbrella #1136) — DB-lane source
-- split by table family.
--
-- Spec: docs/superpowers/specs/2026-05-13-db-lane-family-split.md
--
-- ## Why
--
-- Bootstrap Phase C bulk ingesters all carried lane='db' (single
-- source lock). PR1c #1064 had locked everything onto one db source
-- as a step toward source-keyed JobLocks; the side effect was
-- serialising five disjoint-table-family writes that #1020 designed
-- to run in parallel. Measured wall-clock cost on bootstrap_run_id=3:
-- 283 min serial vs 110 min cross-source-parallel (~3-4 h saving).
--
-- This migration extends the bootstrap_stages.lane CHECK to accept
-- the five new family lane names. The corresponding source registry
-- entries are added in app/jobs/sources.py::Lane in the same PR.
--
-- ## Family lanes
--
--   db_filings           - sec_submissions_ingest -> filing_events
--   db_fundamentals_raw  - sec_companyfacts_ingest -> company_facts
--   db_ownership_inst    - sec_13f_ingest_from_dataset
--                          -> ownership_institutions_observations
--   db_ownership_insider - sec_insider_ingest_from_dataset
--                          -> insider_transactions + form3_holdings_initial
--   db_ownership_funds   - sec_nport_ingest_from_dataset
--                          -> n_port_* + sec_fund_series
--
-- The existing 'db' lane stays valid (Phase E derivations
-- fundamentals_sync + ownership_observations_backfill remain on it;
-- so do every scheduler db-source job).
-- The legacy 'sec' lane stays valid for pre-#1020 run history rows
-- (same pattern as migration 132).

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
        'db_ownership_funds'
    ));

COMMIT;
