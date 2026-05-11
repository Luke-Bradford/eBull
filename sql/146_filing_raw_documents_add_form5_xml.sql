-- 146_filing_raw_documents_add_form5_xml.sql
--
-- Issue #873 — manifest-worker Form 5 parser adapter.
--
-- Adds the ``form5_xml`` document_kind to ``filing_raw_documents`` so the
-- Form 5 manifest parser can persist the raw XML body distinctly from the
-- Form 4 / Form 3 bodies. Mirrors the precedent set by sql/122 for N-PORT:
-- a distinct kind keeps the re-wash targeting query simple — a future
-- parser-version bump on Form 5 walks only ``form5_xml`` rows instead of
-- filtering across the shared Form 4 set.
--
-- Form 5 (annual statement of changes in beneficial ownership) uses the
-- same EDGAR ownership XML schema as Form 4. Persistence reuses
-- ``insider_filings`` + ``insider_transactions`` with ``document_type='5'``;
-- only the raw body kind needs its own enum slot.

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
        'nport_xml'
    ));

COMMIT;
