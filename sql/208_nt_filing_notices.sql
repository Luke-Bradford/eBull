-- 208_nt_filing_notices.sql
--
-- Issue #1015 item 1 — NT 10-K / NT 10-Q late-filing parser (Form 12b-25).
--
-- Upgrades NT 10-K / NT 10-Q from metadata-only to PARSE+RAW via a new
-- manifest source ``sec_nt``. This migration:
--
--   1. Creates ``nt_filing_notices`` — one row per parsed Form 12b-25, keyed
--      on accession_number (mirrors eight_k_filings shape). Tombstones live in
--      the manifest (``sec_filing_manifest.ingest_status='tombstoned'``), not
--      in this table — NT is manifest-native from day one, so no in-table
--      tombstone column is needed.
--   2. Widens three CHECK constraints for the new source/kind:
--      - filing_raw_documents.document_kind  += 'nt_body'
--      - sec_filing_manifest.source          += 'sec_nt'
--      - data_freshness_index.source         += 'sec_nt'
--
-- Source rule: SEC Form 12b-25 / Rule 12b-25 (17 CFR 240.12b-25). Cover form
-- NT 10-K (annual) / NT 10-Q (quarterly); grace period is 15 calendar days for
-- annual reports, 5 for quarterly. Part III = reason narrative; Part IV(3) =
-- anticipated significant change in results of operations vs the prior-year
-- period (NOT a restatement field).

BEGIN;

CREATE TABLE IF NOT EXISTS nt_filing_notices (
    accession_number            TEXT        PRIMARY KEY,
    instrument_id               BIGINT      NOT NULL
                                  REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Subject report the notice covers. Derived from the manifest form
    -- (NT 10-K -> '10-K', NT 10-Q -> '10-Q'), which is authoritative —
    -- NOT from the cover checkbox (brittle box-association).
    late_form                   TEXT        NOT NULL
                                  CHECK (late_form IN ('10-K', '10-Q')),
    -- "For Period Ended:" (or "For the Transition Period Ended:") date.
    -- NULL when the body line is absent / unparseable.
    period_of_report            DATE,
    -- TRUE when a Form 12b-25 "Transition Report on Form ..." cover box is
    -- checked; period then comes from the transition line.
    is_transition_report        BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Rule 12b-25(b) grace period: 15 days for annual (10-K), 5 for
    -- quarterly (10-Q). Deterministic from late_form.
    grace_period_days           SMALLINT    NOT NULL
                                  CHECK (grace_period_days IN (5, 15)),
    -- Part III narrative — why the report could not be filed on time.
    reason_text                 TEXT,
    -- Part IV(3): is a significant change in results of operations vs the
    -- corresponding prior-year period anticipated? NULL when the checkbox
    -- state can't be determined unambiguously (encoding varies 2016->2026).
    results_change_anticipated  BOOLEAN,
    -- Attached explanation text when results_change_anticipated is TRUE.
    results_change_explanation  TEXT,
    parser_version              INT         NOT NULL DEFAULT 1,
    parsed_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nt_filing_notices_instrument
    ON nt_filing_notices (instrument_id, period_of_report DESC);

COMMENT ON TABLE nt_filing_notices IS
    'Parsed SEC Form 12b-25 late-filing notices (NT 10-K / NT 10-Q). One row '
    'per accession; tombstones live in sec_filing_manifest. Source #1015.';

-- ---------------------------------------------------------------------------
-- Widen the three source/kind CHECK constraints for the new ``sec_nt`` source.
-- ---------------------------------------------------------------------------

ALTER TABLE filing_raw_documents
    DROP CONSTRAINT IF EXISTS filing_raw_documents_document_kind_check;
ALTER TABLE filing_raw_documents
    ADD CONSTRAINT filing_raw_documents_document_kind_check
    CHECK (document_kind IN (
        'primary_doc',
        'infotable_13f',
        'primary_doc_13dg',
        'form4_xml',
        'form3_xml',
        'form5_xml',
        'def14a_body',
        'nport_xml',
        'finra_short_interest_csv',
        'finra_regsho_daily_txt',
        'nt_body'
    ));

ALTER TABLE sec_filing_manifest
    DROP CONSTRAINT IF EXISTS sec_filing_manifest_source_check;
ALTER TABLE sec_filing_manifest
    ADD CONSTRAINT sec_filing_manifest_source_check
    CHECK (source IN (
        'sec_form3',
        'sec_form4',
        'sec_form5',
        'sec_13d',
        'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port',
        'sec_n_csr',
        'sec_10k',
        'sec_10q',
        'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily',
        'sec_nt'
    ));

ALTER TABLE data_freshness_index
    DROP CONSTRAINT IF EXISTS data_freshness_index_source_check;
ALTER TABLE data_freshness_index
    ADD CONSTRAINT data_freshness_index_source_check
    CHECK (source IN (
        'sec_form3',
        'sec_form4',
        'sec_form5',
        'sec_13d',
        'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port',
        'sec_n_csr',
        'sec_10k',
        'sec_10q',
        'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily',
        'sec_nt'
    ));

COMMIT;
