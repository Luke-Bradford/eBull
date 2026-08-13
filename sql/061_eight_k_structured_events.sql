-- 061_eight_k_structured_events.sql
--
-- 8-K structured-event normalisation (#450). The 054 migration added
-- ``dividend_events`` for Item 8.01 only; every other 8-K event
-- (Item 1.01 material agreement, 5.02 officer departure, 2.02
-- earnings release, 1.05 cybersecurity incident, etc.) was known
-- from ``filing_events.items`` as a bare code string but had no
-- structured body capture. Operators couldn't query "show every
-- executive departure in the universe last 90 days" without grep-
-- scanning raw HTML.
--
-- Storage model:
--
--   eight_k_filings             — one row per 8-K accession
--     └── eight_k_items         — one row per (accession, item_code)
--                                 with the body text of that item
--     └── eight_k_exhibits      — one row per (accession, exhibit_num)
--                                 from the Item 9.01 exhibits list
--     └── dividend_events       — pre-existing (054), keyed on the
--                                 same accession for joint queries
--
-- Dividend parsing (054/434) still runs on the Item 8.01 body but
-- now that body is captured in ``eight_k_items`` alongside every
-- other item. Tombstoning lives on ``eight_k_filings.is_tombstone``
-- so fetch failures and non-parseable filings never re-hit SEC
-- every tick.

CREATE TABLE IF NOT EXISTS eight_k_filings (
    accession_number       TEXT        PRIMARY KEY,
    instrument_id          BIGINT      NOT NULL
                              REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- Literal ``8-K`` or ``8-K/A``. Amendments adjust an earlier
    -- filing's content under a new accession; we store both
    -- original and amendment rows independently.
    document_type          TEXT        NOT NULL,
    is_amendment           BOOLEAN     NOT NULL DEFAULT FALSE,
    -- SEC "Date of Report" header field — the earliest event date
    -- covered by the filing. Distinct from ``filing_events.filing_date``
    -- (which is when the filing hit EDGAR). Often a few business days
    -- apart.
    date_of_report         DATE,
    -- Reporting party = the registrant as the filing header states
    -- it. Usually identical to ``instruments.company_name`` but can
    -- diverge for subsidiary filings or post-rename deltas, so we
    -- preserve the verbatim label.
    reporting_party        TEXT,
    -- Signature block at the foot of the filing. Tells the operator
    -- who certified the 8-K and when (not always the same as the
    -- filing_date).
    signature_name         TEXT,
    signature_title        TEXT,
    signature_date         DATE,
    -- Free-text remarks between Item 9.01 exhibits and the signature.
    -- Rare but sometimes carries forward-looking-statement
    -- disclaimers or related-party context we want visible.
    remarks                TEXT,
    primary_document_url   TEXT,
    fetched_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parser_version         INT         NOT NULL DEFAULT 1,
    -- TRUE when the fetch returned 404/410 or the HTML failed to
    -- produce any items. Reader queries exclude tombstones; ingester
    -- selector skips accessions with an existing row so tombstoned
    -- filings don't re-hit SEC every tick.
    is_tombstone           BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_eight_k_filings_instrument
    ON eight_k_filings (instrument_id, date_of_report DESC);

CREATE INDEX IF NOT EXISTS idx_eight_k_filings_report_date
    ON eight_k_filings (date_of_report DESC)
    WHERE is_tombstone = FALSE AND date_of_report IS NOT NULL;

COMMENT ON TABLE eight_k_filings IS
    'One row per 8-K filing accession. Filing-level header + signature '
    'block. Per-item bodies live in eight_k_items; exhibit references '
    'live in eight_k_exhibits. Dividend-specific parse output (#434) '
    'continues to live in dividend_events keyed on the same accession.';

COMMENT ON COLUMN eight_k_filings.date_of_report IS
    'SEC "Date of Report" — the event date the 8-K covers. Usually '
    '1-4 business days before the filing_date on filing_events.';

COMMENT ON COLUMN eight_k_filings.is_tombstone IS
    'Sentinel flag for filings that fetched to 404 / 410 or parsed '
    'to zero items. Tombstones are skipped by the ingester selector '
    'and filtered out by reader queries. Re-parse of the same '
    'accession under a better extractor flips this back to FALSE.';

-- ---------------------------------------------------------------------
-- eight_k_items — per-item body capture
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS eight_k_items (
    id                 BIGSERIAL   PRIMARY KEY,
    accession_number   TEXT        NOT NULL
                           REFERENCES eight_k_filings(accession_number) ON DELETE CASCADE,
    -- Item code matches ``sec_8k_item_codes.code`` (see migration 053).
    -- "1.01", "5.02", "8.01", etc.
    item_code          TEXT        NOT NULL,
    -- Denormalised label + severity from sec_8k_item_codes for
    -- read-path convenience. Bumped on upsert so a future lookup
    -- edit (label/severity wording change) propagates automatically.
    item_label         TEXT        NOT NULL,
    severity           TEXT,
    -- Source-order of this item within the filing. Items are listed
    -- in ascending SEC code order (1.01, 2.02, 5.02, 8.01, 9.01) but
    -- we store the actual source position so a renderer can walk the
    -- filing's own layout.
    item_order         INT         NOT NULL,
    -- Full body text of this item (HTML-stripped, whitespace-
    -- collapsed). Typical lengths: 200-5000 chars. Capped at 20 KB
    -- per item so an exhibit-heavy 9.01 body can't blow the row.
    body               TEXT        NOT NULL DEFAULT '',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, item_code)
);

CREATE INDEX IF NOT EXISTS idx_eight_k_items_code
    ON eight_k_items (item_code);

CREATE INDEX IF NOT EXISTS idx_eight_k_items_severity
    ON eight_k_items (severity)
    WHERE severity IN ('material', 'critical');

COMMENT ON TABLE eight_k_items IS
    'Per-item structured capture of an 8-K filing. One row per '
    '(accession, item_code). Body is the HTML-stripped item text; '
    'empty string means the item was listed in the filing header '
    'but had no narrative body (common for bare 9.01 exhibit '
    'pointers).';

COMMENT ON COLUMN eight_k_items.severity IS
    'Severity tier from sec_8k_item_codes (informational / material / '
    'critical). Denormalised here for read-path convenience.';

-- ---------------------------------------------------------------------
-- eight_k_exhibits — Item 9.01 exhibit pointers
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS eight_k_exhibits (
    id                 BIGSERIAL   PRIMARY KEY,
    accession_number   TEXT        NOT NULL
                           REFERENCES eight_k_filings(accession_number) ON DELETE CASCADE,
    -- SEC exhibit number: "99.1" (press release), "10.1" (material
    -- contract), "2.1" (acquisition agreement), etc. Preserved
    -- verbatim.
    exhibit_number     TEXT        NOT NULL,
    description        TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, exhibit_number)
);

COMMENT ON TABLE eight_k_exhibits IS
    'Exhibit pointers from Item 9.01 — SEC exhibit number + description '
    'text. The exhibit body itself (press release HTML, material '
    'contract PDF) lives at a different accession-relative URL and is '
    'not captured here; the pointer is enough for the thesis engine '
    'to link out or decide whether to fetch.';
