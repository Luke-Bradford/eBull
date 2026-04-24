-- 062_filing_documents.sql
--
-- SEC filing-index document capture (#452 Phase A). Follow-up to the
-- #448 operator directive: every structured upstream field lands in
-- SQL, no silent drops.
--
-- Current state: every 8-K / 10-K / 10-Q / Form 4 / etc. filing has
-- an ``{accession}-index.json`` at SEC EDGAR that lists every
-- document in the submission — primary document, exhibits, XBRL
-- instance, XBRL schema, graphics, cover-page XML, etc. The SEC
-- index is the authoritative manifest; ``filing_events`` currently
-- captures only the ``primary_document_url`` derived from the
-- submissions.json listing. Everything else is in the on-disk
-- ``data/raw/sec/sec_filing_*.json`` dump and unqueryable from SQL.
--
-- Storage model: one row per (filing_event_id, document_name).
-- ``is_primary`` flags the submission's primary document
-- (duplicated from ``filing_events.primary_document_url`` as a
-- ``is_primary = TRUE`` row for consistency — the reader can now
-- treat the primary document as one row of a structured list rather
-- than a special column).
--
-- What this unlocks:
--   - Query every EX-21 (subsidiary list) across the universe
--     without re-fetching.
--   - Find every cybersecurity-incident 8-K that attached a press
--     release exhibit (EX-99).
--   - Surface XBRL instance documents for the ingester without
--     re-walking the JSON.
--   - Retire ``data/raw/sec/sec_filing_*.json`` (~900 MB of the
--     1.1 GB ``data/raw/sec/`` footprint).

CREATE TABLE IF NOT EXISTS filing_documents (
    id                BIGSERIAL    PRIMARY KEY,
    filing_event_id   BIGINT       NOT NULL
                          REFERENCES filing_events(filing_event_id) ON DELETE CASCADE,
    -- Denormalised from the parent row for readable queries —
    -- a join-less ``SELECT * FROM filing_documents WHERE
    -- accession_number = '0000320193-24-000001'`` is common.
    accession_number  TEXT         NOT NULL,
    -- Filename inside the filing, e.g. "aapl-20240930.htm",
    -- "ex-21.htm", "Financial_Report.xlsx". Unique within a
    -- filing; used as the idempotency key.
    document_name     TEXT         NOT NULL,
    -- SEC-assigned document type classifier from the index JSON's
    -- ``type`` field. Examples: "10-K", "EX-21", "EX-99.1",
    -- "GRAPHIC", "XBRL INSTANCE DOCUMENT", "XBRL TAXONOMY EXTENSION
    -- SCHEMA DOCUMENT", "COVER". NULL when the index entry omits
    -- the ``type`` field (rare).
    document_type     TEXT,
    -- Human-readable description from the index JSON. Example for an
    -- EX-99.1: "Press Release dated March 15, 2026 announcing the
    -- credit facility". Often blank on exhibits that only carry the
    -- canonical description in the exhibit body itself.
    description       TEXT,
    -- File size in bytes. Useful for (a) a "skip the 50 MB graphic
    -- PDF" policy at fetch time, and (b) disk-retention forecasting
    -- when we decide to fetch exhibit bodies.
    size_bytes        BIGINT,
    -- True for the submission's primary document (the one SEC labels
    -- as the ``primaryDocument`` in the submissions.json listing).
    -- Duplicated to this row so the reader can walk a single list
    -- instead of special-casing the parent column.
    is_primary        BOOLEAN      NOT NULL DEFAULT FALSE,
    -- Fully-qualified URL reconstructed at ingest time from the
    -- accession + document_name. Callers don't have to build the
    -- URL themselves; ``document_url`` is the one thing they need
    -- to fetch the document body.
    document_url      TEXT         NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (filing_event_id, document_name)
);

CREATE INDEX IF NOT EXISTS idx_filing_documents_accession
    ON filing_documents (accession_number);

-- Type-scoped index for "find every EX-21 across the universe" and
-- other document-type sweeps. Partial because the long tail of types
-- is small and well-clustered.
CREATE INDEX IF NOT EXISTS idx_filing_documents_type
    ON filing_documents (document_type)
    WHERE document_type IS NOT NULL;

COMMENT ON TABLE filing_documents IS
    'Per-document manifest from each SEC filing''s ``{accession}-'
    'index.json``. One row per document in the filing (primary + '
    'exhibits + XBRL + graphics + cover). is_primary flags the '
    'submission''s primary document. Retires the raw ``sec_filing_*'
    '.json`` disk dump now that every field is captured here.';

COMMENT ON COLUMN filing_documents.document_type IS
    'SEC type classifier: "10-K", "EX-21", "EX-99.1", '
    '"GRAPHIC", "XBRL INSTANCE DOCUMENT", "COVER", etc. Cross-'
    'issuer queries rely on this to locate subsidiary lists, press-'
    'release exhibits, or XBRL artefacts without re-scanning the '
    'filing index JSON.';

COMMENT ON COLUMN filing_documents.is_primary IS
    'TRUE for the submission''s primary document — matches '
    'filing_events.primary_document_url for the same accession. '
    'Lets callers walk a single structured list without special-'
    'casing the parent row.';
