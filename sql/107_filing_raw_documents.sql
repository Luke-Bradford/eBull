-- 107_filing_raw_documents.sql
--
-- Single-source-of-truth store for the raw XML / HTML body of every
-- ownership filing the app ingests (operator audit 2026-05-03).
--
-- Pre-migration state: ownership-side filings (13F infotable, 13D/G
-- primary_doc, Form 4 / Form 3 XML, DEF 14A body) were fetched,
-- parsed into typed tables (``institutional_holdings``,
-- ``blockholder_filings``, ``insider_transactions``,
-- ``insider_initial_holdings``, ``def14a_beneficial_holdings``),
-- and the original document was dropped. Re-washing after a parser
-- bug discovery required re-fetching from SEC — slow, rate-limited,
-- and risky if the original document has been amended in the
-- interim.
--
-- ``filing_events.raw_payload_json`` (3.8M rows) was misleadingly
-- named — it stores only filing METADATA (URL, date, type), not the
-- document body. This migration closes that gap by adding the
-- canonical body store.
--
-- Schema decisions:
--
--   * Single table keyed on ``(accession_number, document_kind)``
--     rather than per-typed-table ``raw_payload`` columns. The 13F
--     case is decisive: one infotable.xml produces dozens to
--     thousands of ``institutional_holdings`` rows; storing the XML
--     on each row would 100x the storage. One row per accession
--     per document_kind keeps the body deduplicated.
--   * ``payload TEXT`` not ``BYTEA``. SEC documents are XML / HTML,
--     inherently text. Postgres TOAST compresses TEXT > 2KB
--     automatically (~5x compression ratio on XML), so storage
--     overhead vs BYTEA is negligible. SQL queries (grep for a
--     CIK across raw bodies) work directly on TEXT.
--   * ``document_kind`` is a CHECK-constrained set, NOT a free-text
--     column. The constrained set ensures every ingester registers
--     its kind explicitly; a typo gets caught at write time rather
--     than producing a quietly mis-classified row.
--   * ``parser_version`` records which parser the typed-table rows
--     were derived from. Re-wash decisions can compare against the
--     current parser version and skip rows that are already on the
--     latest parser.
--   * ``byte_count`` is a generated column on ``octet_length(payload)``
--     — useful for retention sweeps and operator-visible storage
--     accounting without re-computing on every read.
--
-- The table is the FOUNDATION. Per-ingester wiring lands in follow-
-- on PRs; this PR ships the table + helper + tests so the contract
-- is locked in before the rewrites.

CREATE TABLE IF NOT EXISTS filing_raw_documents (
    accession_number   TEXT NOT NULL,
    document_kind      TEXT NOT NULL
        CHECK (document_kind IN (
            'primary_doc',          -- generic SEC primary_doc.xml (any source)
            'infotable_13f',        -- 13F-HR infotable.xml (per accession)
            'primary_doc_13dg',     -- 13D/G primary_doc.xml
            'form4_xml',            -- Form 4 ownership XML
            'form3_xml',            -- Form 3 initial-holdings XML
            'def14a_body'           -- DEF 14A proxy statement body (HTML / text)
        )),
    -- Note: SEC submissions.json + companyfacts.json are keyed by
    -- CIK, not by accession number. They belong in their own
    -- per-CIK store, not this per-filing table. Claude PR 808 review
    -- (BLOCKING) caught the prior overload that smuggled CIKs into
    -- this column. A future PR adds a sibling ``cik_raw_documents``
    -- table.
    payload            TEXT NOT NULL,
    byte_count         INTEGER GENERATED ALWAYS AS (octet_length(payload)) STORED,
    parser_version     TEXT,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Free-form provenance hints. Useful when the same accession
    -- appears under multiple document_kinds (e.g. a 13F has both
    -- ``primary_doc`` and ``infotable_13f``) and the operator wants
    -- to know which CIK owned which body.
    source_url         TEXT,
    PRIMARY KEY (accession_number, document_kind)
);

-- Hot path for re-wash: walk every doc of a given kind in
-- fetched_at order. Partial index because most reads target one
-- kind at a time.
CREATE INDEX IF NOT EXISTS idx_filing_raw_documents_kind_fetched
    ON filing_raw_documents (document_kind, fetched_at DESC);

-- Hot path for ingester upserts and per-accession joins back to
-- the typed parse tables.
CREATE INDEX IF NOT EXISTS idx_filing_raw_documents_accession
    ON filing_raw_documents (accession_number);

-- Storage accounting: the operator-facing ingest-health page
-- aggregates ``byte_count`` per kind so a sweep can flag retention
-- bloat early. No index — the sweep is rare and a SeqScan is fine.

COMMENT ON TABLE filing_raw_documents IS
    'Canonical store for raw XML / HTML / JSON document bodies of '
    'every SEC filing ingested. One row per (accession_number, '
    'document_kind). Lets re-wash run against stored bodies instead '
    'of re-fetching from SEC. Closes the gap that ``filing_events.'
    'raw_payload_json`` left — that column stored only metadata '
    '(URL / date), not the document body.';

COMMENT ON COLUMN filing_raw_documents.parser_version IS
    'Parser version that wrote the typed-table rows derived from '
    'this body. Used by re-wash workflows to skip rows already on '
    'the latest parser. NULL is acceptable for retroactive seeds.';

COMMENT ON COLUMN filing_raw_documents.byte_count IS
    'Generated from octet_length(payload). Useful for the retention '
    'sweep and the operator-visible storage chip on the ingest-'
    'health page. STORED so the materialised value is indexed-friendly.';
