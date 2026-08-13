-- 057_insider_transactions.sql
--
-- Form 4 insider-transactions ingestion (#429). SEC Form 4 is the
-- per-insider per-transaction filing that directors, officers, and
-- 10% holders must file within two business days of any trade in
-- their company's stock. The submissions.json feed flags
-- ``insiderTransactionForIssuerExists: 1`` on issuers with ≥1 Form 4
-- filed; the actual transaction rows live in each Form 4 XML
-- document. Free, timely, and a strong sentiment signal.
--
-- Storage model: one row per (filing, transaction_row_num) — a
-- single Form 4 can report multiple transactions in one filing
-- (e.g. several sales across different prices on the same day). The
-- row index makes every physical transaction uniquely addressable
-- for audit and dedup.
--
-- No raw-file persistence: parsed rows are the durable artifact per
-- issue scope. If the regex/parser ever needs tuning, the source XML
-- is re-fetchable via the accession_number + SEC archive URL.

CREATE TABLE IF NOT EXISTS insider_transactions (
    id                  BIGSERIAL   PRIMARY KEY,
    instrument_id       BIGINT      NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    accession_number    TEXT        NOT NULL,
    -- Multiple transactions can appear under one accession. The
    -- row index disambiguates them without us having to invent a
    -- synthetic natural key.
    txn_row_num         INT         NOT NULL,
    filer_name          TEXT        NOT NULL,
    -- Pipe-joined roles from the Form 4 reportingOwnerRelationship
    -- block. Example values: "director", "officer:CFO",
    -- "director|officer:CEO", "ten_percent_owner". NULL when the
    -- filing omitted the relationship block entirely (rare).
    filer_role          TEXT,
    txn_date            DATE        NOT NULL,
    -- SEC transaction codes. Common: P (open-market buy), S (sale),
    -- A (grant), M (option exercise), F (tax withholding). Store
    -- raw so downstream consumers can filter / classify themselves
    -- without us baking editorial opinions into the schema.
    txn_code            TEXT        NOT NULL,
    -- Positive shares from the filing regardless of direction; the
    -- ``direct_indirect`` + ``txn_code`` combine to produce the
    -- signed net-buy/sell views at the service layer.
    shares              NUMERIC(20, 4),
    price               NUMERIC(18, 6),
    -- "D" (direct) or "I" (indirect) — who holds the shares post-
    -- transaction. Indirect usually means a trust or family member;
    -- direct is the filer's personal account.
    direct_indirect     CHAR(1),
    -- True iff the txn is on the derivative table (options /
    -- warrants / RSU grants) rather than non-derivative. Sentiment
    -- signals typically weight non-derivative trades more heavily —
    -- open-market buys are a stronger conviction marker than an
    -- option grant.
    is_derivative       BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, txn_row_num)
);

-- Composite index on (instrument_id, txn_date DESC) drives both the
-- "all transactions for this instrument" access pattern and the
-- recent-window scan the 90-day aggregation needs. A partial index
-- was considered for the recent-window case but would need a non-
-- IMMUTABLE ``CURRENT_DATE`` expression in the WHERE clause, which
-- Postgres rejects — so the composite covers it instead.
CREATE INDEX IF NOT EXISTS idx_insider_transactions_instrument_date
    ON insider_transactions (instrument_id, txn_date DESC);

COMMENT ON TABLE insider_transactions IS
    'Form 4 per-transaction rows. One row per transaction line in '
    'a Form 4 filing — a single accession can carry several. Parsed '
    'from the XML primary document; raw XML is not persisted.';

COMMENT ON COLUMN insider_transactions.txn_row_num IS
    'Zero-indexed position within the filing''s transaction list '
    '(non-derivative rows first, then derivative). Together with '
    'accession_number forms the UNIQUE dedup key.';

COMMENT ON COLUMN insider_transactions.txn_code IS
    'SEC transaction code. P=open-market purchase, S=sale, '
    'A=grant/award, M=option exercise, F=tax withholding, '
    'D=gift disposition, G=gift, plus a long tail. Stored raw.';

COMMENT ON COLUMN insider_transactions.shares IS
    'Unsigned share count from the filing. Combine with the '
    'transactionAcquiredDisposedCode at parse time to derive the '
    'signed net — A=acquired (positive), D=disposed (negative).';
