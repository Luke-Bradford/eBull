-- 109_cik_raw_documents.sql
--
-- Per-CIK raw-document store. Sibling of ``filing_raw_documents``
-- (migration 107) for SEC documents that are keyed by CIK rather
-- than by SEC accession number — submissions.json and
-- companyfacts.json are rolling per-issuer documents covering ALL
-- their filings, not per-filing artifacts.
--
-- Operator audit 2026-05-03 found that every reconciliation
-- spot-check re-fetches companyfacts.json from SEC at 10 req/s. A
-- write-through cache here turns those into hot reads when the
-- payload is recent enough — and gives the operator a "what did SEC
-- say last time?" audit trail for free.
--
-- Schema decisions:
--
--   * Separate table from ``filing_raw_documents`` rather than
--     overloading the accession_number column. PR 808 BLOCKING
--     review caught the prior overload that smuggled CIKs into
--     ``filing_raw_documents.accession_number``.
--   * ``cik`` column is the 10-digit zero-padded form so JOINs
--     against ``external_identifiers.identifier_value`` (also
--     padded) work without normalisation.
--   * ``document_kind`` CHECK-constrained to a closed set. Adding a
--     new kind requires a schema migration — intentional, because
--     it's the contract that the per-CIK store doesn't drift into a
--     dumping ground.
--   * ``payload TEXT`` not ``BYTEA``. SEC documents are JSON;
--     Postgres TOAST compresses TEXT > 2KB automatically (~5x ratio
--     on JSON). SQL queries (grep for a concept across raw bodies)
--     work directly on TEXT.
--   * ``byte_count`` generated column on ``octet_length(payload)``
--     for storage accounting without recomputing on every read.
--   * Composite PK ``(cik, document_kind)`` makes the UPSERT path
--     trivial and the write-through cache obvious.

CREATE TABLE IF NOT EXISTS cik_raw_documents (
    -- 10-digit zero-padded canonical form; CHECK enforces the
    -- invariant at the DB level so a direct-SQL writer can't bypass
    -- the application-layer ``store_cik_raw`` validation and
    -- silently split the cache (read path padds; an unpadded write
    -- would never be hit). Format: exactly 10 ASCII digits.
    cik              TEXT NOT NULL
        CHECK (cik ~ '^[0-9]{10}$'),
    document_kind    TEXT NOT NULL
        CHECK (document_kind IN (
            'submissions_json',     -- data.sec.gov/submissions/CIK{cik}.json
            'companyfacts_json'     -- data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
        )),
    payload          TEXT NOT NULL,
    byte_count       INTEGER GENERATED ALWAYS AS (octet_length(payload)) STORED,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_url       TEXT,
    PRIMARY KEY (cik, document_kind)
);

-- Hot path for cache freshness checks.
CREATE INDEX IF NOT EXISTS idx_cik_raw_documents_kind_fetched
    ON cik_raw_documents (document_kind, fetched_at DESC);

COMMENT ON TABLE cik_raw_documents IS
    'Per-CIK raw-document store for SEC documents keyed by CIK '
    '(submissions.json, companyfacts.json) rather than accession '
    'number. Sibling of filing_raw_documents (per-accession). '
    'Write-through cache for the reconciliation framework + '
    'one-shot SEC fetchers; also gives operators a "what did SEC '
    'say last time?" audit trail.';

COMMENT ON COLUMN cik_raw_documents.byte_count IS
    'Generated from octet_length(payload). Drives the storage chip '
    'on the ingest-health page.';
