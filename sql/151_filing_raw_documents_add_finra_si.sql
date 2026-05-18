-- 151_filing_raw_documents_add_finra_si.sql
--
-- Issue #915 (Phase 6 PR 11) — FINRA bimonthly short interest ingest.
--
-- Adds the ``finra_short_interest_csv`` document_kind to
-- ``filing_raw_documents`` so the FINRA bimonthly ingester can persist
-- the pipe-delim file body before parse (raw-payload-before-parse
-- contract, #1168). Distinct kind keeps the re-wash targeting query
-- simple (mirror sql/122 N-PORT + sql/146 Form 5 precedent).
--
-- FINRA Equity Short Interest data publishes as a single pipe-delimited
-- file per settlement date (`shrt{YYYYMMDD}.csv`); we key the raw row
-- by the synthetic accession ``FINRA_SI_{YYYYMMDD}`` (per the documented
-- ``finra_universe`` subject_type + ``'FINRA_SI'`` subject_id singleton
-- in sql/118).

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
        'finra_short_interest_csv'
    ));

COMMIT;
