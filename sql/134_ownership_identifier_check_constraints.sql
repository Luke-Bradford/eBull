-- 134_ownership_identifier_check_constraints.sql
--
-- Defensive regex CHECK constraints on the ownership_*_observations
-- + _current tables (#1043). Catches malformed CIK / accession at
-- the storage boundary instead of letting bad data ride through to
-- operator-visible reports.
--
-- Format guarantees:
--   CIK         — 10-digit zero-padded (^[0-9]{10}$)
--   Accession   — ^[0-9]{10}-[0-9]{2}-[0-9]{6}$
--
-- Existing format CHECKs:
--   sql/123 / 124 / 125 already CHECK fund_series_id ~ '^S[0-9]{9}$'.
--   sql/109 already CHECKs cik_raw_documents.cik ~ '^[0-9]{10}$'.
--
-- All constraints are added with plain ADD CONSTRAINT ... CHECK in
-- a single transaction. The greenfield assumption holds — every
-- ownership_*_observations table is empty on dev/CI by the time this
-- migration runs (preceded by the C-stages that populate them in
-- production), so NOT VALID + VALIDATE is unnecessary.
--
-- These are CHECK constraints, not FK constraints — the CIK is a
-- TEXT identifier across institutional_filers / blockholder_filers /
-- (none) for fund managers, so a single FK target doesn't exist.

BEGIN;

-- ---------------------------------------------------------------------
-- Insiders (sql/113) — holder_cik nullable
-- ---------------------------------------------------------------------

ALTER TABLE ownership_insiders_observations
    ADD CONSTRAINT chk_insiders_obs_holder_cik
    CHECK (holder_cik IS NULL OR holder_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_insiders_observations
    ADD CONSTRAINT chk_insiders_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

ALTER TABLE ownership_insiders_current
    ADD CONSTRAINT chk_insiders_cur_holder_cik
    CHECK (holder_cik IS NULL OR holder_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_insiders_current
    ADD CONSTRAINT chk_insiders_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

-- ---------------------------------------------------------------------
-- Institutions (sql/114) — filer_cik NOT NULL
-- ---------------------------------------------------------------------

ALTER TABLE ownership_institutions_observations
    ADD CONSTRAINT chk_institutions_obs_filer_cik
    CHECK (filer_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_institutions_observations
    ADD CONSTRAINT chk_institutions_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

ALTER TABLE ownership_institutions_current
    ADD CONSTRAINT chk_institutions_cur_filer_cik
    CHECK (filer_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_institutions_current
    ADD CONSTRAINT chk_institutions_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

-- ---------------------------------------------------------------------
-- Blockholders (sql/115) — reporter_cik NOT NULL
-- ---------------------------------------------------------------------

ALTER TABLE ownership_blockholders_observations
    ADD CONSTRAINT chk_blockholders_obs_reporter_cik
    CHECK (reporter_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_blockholders_observations
    ADD CONSTRAINT chk_blockholders_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

ALTER TABLE ownership_blockholders_current
    ADD CONSTRAINT chk_blockholders_cur_reporter_cik
    CHECK (reporter_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_blockholders_current
    ADD CONSTRAINT chk_blockholders_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

-- ---------------------------------------------------------------------
-- Treasury + DEF 14A (sql/116) — accession-only (no CIK column)
-- ---------------------------------------------------------------------

ALTER TABLE ownership_treasury_observations
    ADD CONSTRAINT chk_treasury_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');
ALTER TABLE ownership_treasury_current
    ADD CONSTRAINT chk_treasury_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

ALTER TABLE ownership_def14a_observations
    ADD CONSTRAINT chk_def14a_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');
ALTER TABLE ownership_def14a_current
    ADD CONSTRAINT chk_def14a_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

-- ---------------------------------------------------------------------
-- Funds (sql/123) — fund_filer_cik NOT NULL
-- ---------------------------------------------------------------------

ALTER TABLE ownership_funds_observations
    ADD CONSTRAINT chk_funds_obs_fund_filer_cik
    CHECK (fund_filer_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_funds_observations
    ADD CONSTRAINT chk_funds_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

ALTER TABLE ownership_funds_current
    ADD CONSTRAINT chk_funds_cur_fund_filer_cik
    CHECK (fund_filer_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_funds_current
    ADD CONSTRAINT chk_funds_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');

-- ---------------------------------------------------------------------
-- ESOP (sql/127) — accession + plan_trustee_cik (nullable)
-- ---------------------------------------------------------------------

ALTER TABLE ownership_esop_observations
    ADD CONSTRAINT chk_esop_obs_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');
ALTER TABLE ownership_esop_observations
    ADD CONSTRAINT chk_esop_obs_plan_trustee_cik
    CHECK (plan_trustee_cik IS NULL OR plan_trustee_cik ~ '^[0-9]{10}$');
ALTER TABLE ownership_esop_current
    ADD CONSTRAINT chk_esop_cur_source_accession
    CHECK (source_accession IS NULL OR source_accession ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');
ALTER TABLE ownership_esop_current
    ADD CONSTRAINT chk_esop_cur_plan_trustee_cik
    CHECK (plan_trustee_cik IS NULL OR plan_trustee_cik ~ '^[0-9]{10}$');

COMMIT;
