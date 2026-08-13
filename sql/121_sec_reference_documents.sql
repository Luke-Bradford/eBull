-- 121_sec_reference_documents.sql
--
-- Per-quarterly-period raw-document store for SEC reference files
-- that are keyed by (document_kind, year, quarter) rather than by
-- accession_number or by CIK.
--
-- Triggered by #914 — the SEC Official List of Section 13(f)
-- Securities is published quarterly as
-- ``https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt``.
-- It's neither per-filing (so ``filing_raw_documents`` doesn't fit)
-- nor per-CIK (so ``cik_raw_documents`` doesn't fit) — it's a
-- bulk reference snapshot.
--
-- Operator audit: the eBull non-negotiable "raw API payloads
-- persisted before normalisation" applies to every external HTTP
-- fetch the app normalises. Without this table, the #914 backfill
-- would fetch the SEC TXT, parse it, and drop the raw bytes — a
-- parser bug discovered later would force a re-fetch from SEC
-- when the payload may already have been amended (the Official
-- List is mutable across quarters).
--
-- Schema decisions:
--
--   * Composite PK ``(document_kind, period_year, period_quarter)``
--     so the UPSERT path is trivial and the operator can grep
--     "what did SEC say in 2025Q4" without joining.
--   * ``document_kind`` CHECK-constrained to a closed set so a
--     typo at the application layer can't silently dump a row
--     into the wrong document_kind bucket.
--   * ``payload TEXT`` not ``BYTEA``. SEC reference docs are
--     ASCII / latin-1 text. Postgres TOAST compresses
--     TEXT > 2KB automatically.
--   * ``byte_count`` generated column on ``octet_length(payload)``
--     for storage accounting (mirrors ``filing_raw_documents``).
--   * ``period_quarter`` constrained to 1..4 — defends against an
--     application-layer typo passing 0 or 5.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry "When a migration adds
-- any table with a FK relationship, update _PLANNER_TABLES …".

CREATE TABLE IF NOT EXISTS sec_reference_documents (
    document_kind   TEXT NOT NULL
        CHECK (document_kind IN ('13f_securities_list')),
    period_year     INTEGER NOT NULL CHECK (period_year >= 1990),
    period_quarter  INTEGER NOT NULL CHECK (period_quarter BETWEEN 1 AND 4),
    payload         TEXT NOT NULL,
    byte_count      INTEGER GENERATED ALWAYS AS (octet_length(payload)) STORED,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_url      TEXT,
    PRIMARY KEY (document_kind, period_year, period_quarter)
);

CREATE INDEX IF NOT EXISTS idx_sec_reference_documents_kind_fetched
    ON sec_reference_documents (document_kind, fetched_at DESC);

COMMENT ON TABLE sec_reference_documents IS
    'Per-quarterly-period raw-document store for SEC reference files '
    'keyed by (document_kind, period_year, period_quarter). Sibling '
    'of filing_raw_documents (per-accession) and cik_raw_documents '
    '(per-CIK). First consumer: 13F-list CUSIP backfill (#914).';
