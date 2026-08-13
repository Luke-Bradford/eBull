-- 153_finra_regsho_daily_enum.sql
--
-- Issue #916 (Phase 6 PR 12) — FINRA RegSHO daily short volume ingest.
-- Spec: docs/superpowers/specs/2026-05-18-finra-regsho-daily.md.
--
-- Widens three CHECK constraint enums in lock-step so the new
-- ``finra_regsho_daily`` source + ``finra_regsho_daily_txt``
-- document_kind become legal values:
--
--   1. filing_raw_documents.document_kind — adds 'finra_regsho_daily_txt'
--      so the daily ingester can persist the pipe-delim file body
--      before parse (raw-payload-before-parse contract, #1168).
--   2. sec_filing_manifest.source — adds 'finra_regsho_daily' so the
--      synth FINRA manifest row can be UPSERTed by the ScheduledJob.
--   3. data_freshness_index.source — same; the freshness panel sees
--      the new daily slot.
--
-- The matching Python-side widenings land in the same PR:
--   - DocumentKind Literal at app/services/raw_filings.py:58.
--   - ManifestSource Literal at app/services/sec_manifest.py:106.
--   - _CADENCE map at app/services/data_freshness.py:69 (gets
--     ``finra_regsho_daily: timedelta(days=2)``).
--   - _UNMAPPED_MANIFEST_SOURCES allow-list entry at
--     app/services/capability_manifest_mapping.py:85.

BEGIN;

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
        'finra_regsho_daily_txt'
    ));

ALTER TABLE sec_filing_manifest
    DROP CONSTRAINT IF EXISTS sec_filing_manifest_source_check;

ALTER TABLE sec_filing_manifest
    ADD CONSTRAINT sec_filing_manifest_source_check
    CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily'
    ));

ALTER TABLE data_freshness_index
    DROP CONSTRAINT IF EXISTS data_freshness_index_source_check;

ALTER TABLE data_freshness_index
    ADD CONSTRAINT data_freshness_index_source_check
    CHECK (source IN (
        'sec_form3', 'sec_form4', 'sec_form5',
        'sec_13d', 'sec_13g',
        'sec_13f_hr',
        'sec_def14a',
        'sec_n_port', 'sec_n_csr',
        'sec_10k', 'sec_10q', 'sec_8k',
        'sec_xbrl_facts',
        'finra_short_interest',
        'finra_regsho_daily'
    ));

COMMIT;
