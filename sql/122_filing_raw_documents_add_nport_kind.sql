-- 122_filing_raw_documents_add_nport_kind.sql
--
-- Issue #917 — N-PORT mutual-fund holdings ingest (Phase 3 PR1).
--
-- Adds the ``nport_xml`` document_kind to ``filing_raw_documents``
-- so the N-PORT ingester can persist the raw filing body before
-- parse, mirroring the contract enforced for 13F's ``infotable_13f``.
--
-- Codex pre-impl review (2026-05-05) finding #10: the existing
-- generic ``primary_doc`` kind would work mechanically, but tagging
-- N-PORT bodies distinctly weakens re-wash targeting because a
-- parser-version bump on N-PORT would have to filter through every
-- mixed-source primary_doc row. Distinct ``nport_xml`` keeps the
-- re-wash query simple.

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
        'def14a_body',
        'nport_xml'
    ));

COMMIT;
