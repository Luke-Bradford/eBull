-- 057_insider_transactions_richness.sql
--
-- Form 4 insider-transactions normalisation expansion (#429 follow-up).
--
-- Scope change: the initial migration (056) captured a narrow slice of
-- Form 4 — filer name + role, txn date/code/shares/price/direct-or-
-- indirect, derivative flag. Everything else in the Form 4 XML (issuer
-- block, full reporting-owner identity + address, acquired/disposed
-- code, post-transaction holdings, derivative security details,
-- footnotes, remarks, signature, amendment linkage, equity-swap flag,
-- filing timeliness) was being dropped — with the assumption that the
-- raw XML would be re-fetchable from SEC if any of it was needed.
--
-- That assumption was rejected by the operator: every field on every
-- filing must land in SQL in a normalised, queryable shape, not on
-- disk and not discarded. This migration closes that gap for Form 4.
--
-- Design:
--
--   insider_filings                - one row per (accession)
--       └── insider_filers         - one row per (accession, filer_cik)
--       └── insider_transaction_footnotes
--                                  - one row per (accession, footnote_id)
--       └── insider_transactions   - one row per (accession, txn_row_num)
--           │                        (FKs to the filer on the filing)
--           └── footnote_refs JSONB inline
--                                  - [{"footnote_id": "F1", "field": "shares"}, ...]
--                                    preserves which Form-4 elements
--                                    pointed at which footnote; no
--                                    separate table because we only
--                                    ever render these next to the row,
--                                    never query across them.
--
-- Tombstoning (fetch 404 / parse failure) now lives on the filing row
-- (``insider_filings.is_tombstone``) instead of a synthetic
-- ``txn_row_num = -1 / filer_name = '__TOMBSTONE__'`` sentinel in
-- ``insider_transactions``. This (a) removes the reader filter on a
-- magic filer name, (b) lets the reader simply INNER JOIN filings and
-- exclude tombstones, (c) avoids inserting a misleading transaction
-- row for a filing that parsed to nothing.
--
-- Migration is forward-only. Existing ``insider_transactions`` rows
-- from migration 056 are preserved; a minimal ``insider_filings``
-- placeholder is backfilled for each existing accession so the NOT
-- NULL FK can be added. Fields unknown from the 056 snapshot
-- (issuer_cik, signature_name, etc.) stay NULL until the next
-- ingester pass re-parses the filing under the new schema — the
-- ingester upserts, so re-parsed filings overwrite the placeholder.

-- ---------------------------------------------------------------------
-- insider_filings — one row per filing accession
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS insider_filings (
    accession_number            TEXT        PRIMARY KEY,
    instrument_id               BIGINT      NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    -- ``4`` / ``4/A``. 4/A is an amendment; the amended original is
    -- linked through ``date_of_original_submission`` when the filer
    -- populated it (they're required to, but real filings occasionally
    -- omit it).
    document_type               TEXT        NOT NULL,
    -- Period-of-report: the statutory date the form covers (usually
    -- the transaction date or the latest one in a multi-row filing).
    -- Distinct from ``txn_date`` on the transaction row because a
    -- single filing's period-of-report differs across its rows only
    -- when rows span multiple dates.
    period_of_report            DATE,
    -- Only populated on 4/A amendments. Points back to the earlier
    -- accession being corrected — but SEC only provides the DATE, not
    -- the accession number, so downstream amendment-chain logic has
    -- to match on (issuer_cik, filer_cik, date).
    date_of_original_submission DATE,
    -- Rare edge case: Section 16 doesn't apply (e.g. former insider
    -- still in the reporting window). Preserve because the flag
    -- changes the interpretation of the filing.
    not_subject_to_section_16   BOOLEAN,
    -- Combined-filing flags: a Form 4 can also carry Form 3-style
    -- holdings or vice versa. True when the filer ticked the box.
    form3_holdings_reported     BOOLEAN,
    form4_transactions_reported BOOLEAN,
    -- Issuer block: CIK is the stable identity, name + symbol are
    -- denormalised for display convenience and survive ticker
    -- renames (unlike ``instruments.symbol``).
    issuer_cik                  TEXT,
    issuer_name                 TEXT,
    issuer_trading_symbol       TEXT,
    -- Free-text remarks block. Insiders use this to explain unusual
    -- filings — e.g. 10b5-1 plan adoption, gift recipient relationship,
    -- "shares held by spouse through LLC". Important context.
    remarks                     TEXT,
    -- Signature block: who certified the filing and when. Usually a
    -- lawyer or officer, sometimes the insider themselves.
    signature_name              TEXT,
    signature_date              DATE,
    -- URL the XML was fetched from, and when. Lets a later audit
    -- pass re-fetch exactly the same document without having to
    -- reconstruct the Archives URL shape.
    primary_document_url        TEXT,
    fetched_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Bumped whenever ``parse_form_4_xml`` changes shape. Lets us
    -- re-ingest only filings parsed under an older parser without
    -- re-fetching everything.
    parser_version              INT         NOT NULL DEFAULT 2,
    -- True when the fetch returned 404/410 or the XML failed to parse
    -- to any transactions. Replaces the old ``filer_name =
    -- '__TOMBSTONE__'`` sentinel on ``insider_transactions``. Reader
    -- excludes tombstoned filings from summaries via JOIN.
    is_tombstone                BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_insider_filings_instrument
    ON insider_filings (instrument_id, period_of_report DESC);

CREATE INDEX IF NOT EXISTS idx_insider_filings_issuer_cik
    ON insider_filings (issuer_cik)
    WHERE issuer_cik IS NOT NULL;

COMMENT ON TABLE insider_filings IS
    'One row per Form 4 filing accession. Header + issuer + signature '
    'fields; the per-transaction rows live in insider_transactions. '
    'is_tombstone=TRUE means the filing was unreachable or unparseable '
    'and has no transaction rows — the reader excludes tombstones.';

COMMENT ON COLUMN insider_filings.document_type IS
    'Literal Form 4 document type string: "4" or "4/A" (amendment). '
    'Amendments carry ``date_of_original_submission`` pointing back '
    'to the earlier filing being corrected.';

COMMENT ON COLUMN insider_filings.period_of_report IS
    'Statutory period covered by the filing. Usually the transaction '
    'date for single-row filings; the latest transaction date for '
    'multi-row filings.';

COMMENT ON COLUMN insider_filings.is_tombstone IS
    'Sentinel flag for filings that failed to fetch or parse. Such '
    'rows have no insider_transactions / insider_filers children. '
    'Set by the ingester so subsequent runs skip the accession; '
    'excluded from reader summaries via JOIN filter.';

-- ---------------------------------------------------------------------
-- insider_filers — one row per (accession, reporting owner)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS insider_filers (
    id                          BIGSERIAL   PRIMARY KEY,
    accession_number            TEXT        NOT NULL REFERENCES insider_filings(accession_number) ON DELETE CASCADE,
    -- SEC-assigned filer identifier. Stable across name spellings
    -- and address changes — the correct dedup key for
    -- ``unique_filers_90d`` and for cross-filing aggregation.
    filer_cik                   TEXT        NOT NULL,
    filer_name                  TEXT        NOT NULL,
    -- Address block from ``reportingOwnerAddress``. Useful for:
    -- detecting entity-vs-individual filers (P.O. Box / trust
    -- address vs personal), clustering related filers at the same
    -- address, and auditing out-of-state filings.
    street1                     TEXT,
    street2                     TEXT,
    city                        TEXT,
    state                       TEXT,
    zip_code                    TEXT,
    state_description           TEXT,
    -- Relationship flags from ``reportingOwnerRelationship``. All
    -- four are mutually non-exclusive — a director-officer is
    -- common. Storing each separately preserves every signal a
    -- downstream weighting might want (CEO buys vs director buys
    -- vs 10%-owner accumulation).
    is_director                 BOOLEAN,
    is_officer                  BOOLEAN,
    officer_title               TEXT,
    is_ten_percent_owner        BOOLEAN,
    is_other                    BOOLEAN,
    -- Free text describing the ``isOther`` relationship. Required
    -- by SEC when ``isOther`` is true; sometimes populated
    -- opportunistically.
    other_text                  TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, filer_cik)
);

CREATE INDEX IF NOT EXISTS idx_insider_filers_cik
    ON insider_filers (filer_cik);

COMMENT ON TABLE insider_filers IS
    'Reporting owners on a Form 4. One row per (accession, filer_cik) — '
    'a joint filing can have multiple insiders reporting on the same '
    'document. Transactions are linked back to the specific filer via '
    'insider_transactions.filer_cik.';

COMMENT ON COLUMN insider_filers.filer_cik IS
    'SEC-assigned CIK for the reporting owner. Preferred dedup key '
    'over filer_name because two insiders can share a name (e.g. '
    '"John Smith" at different issuers) but never a CIK.';

-- ---------------------------------------------------------------------
-- insider_transaction_footnotes — footnote text bodies
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS insider_transaction_footnotes (
    id                          BIGSERIAL   PRIMARY KEY,
    accession_number            TEXT        NOT NULL REFERENCES insider_filings(accession_number) ON DELETE CASCADE,
    -- SEC footnote identifier inside the filing: "F1", "F2", ...
    -- Unique per filing, not globally.
    footnote_id                 TEXT        NOT NULL,
    footnote_text               TEXT        NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, footnote_id)
);

COMMENT ON TABLE insider_transaction_footnotes IS
    'Footnote bodies from a Form 4. Insiders use footnotes to explain '
    'weighted-average prices, 10b5-1 plan adoption dates, gift '
    'recipient relationships, trust structures. Cross-referenced from '
    'insider_transactions.footnote_refs (JSONB list of '
    '{footnote_id, field}).';

-- ---------------------------------------------------------------------
-- insider_transactions — expansion of the existing table
-- ---------------------------------------------------------------------

-- Header comment fix: file name is 056 but the original header read 057.
-- (Cosmetic — no structural change required.)
COMMENT ON TABLE insider_transactions IS
    'Form 4 per-transaction rows. One row per transaction line in a '
    'Form 4 filing — a single accession can carry several across both '
    'the non-derivative and derivative tables. Parsed via '
    'parse_form_4_xml; every structured field from the XML lands on '
    'this table, its parent insider_filings, or insider_filers / '
    'insider_transaction_footnotes.';

-- Filer linkage: txn row is owned by a specific reporting owner on
-- the filing. NULLable for rows created by the older parser (pre-057)
-- until they are re-ingested.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS filer_cik TEXT;

-- Security-class disambiguation. A single Form 4 can touch several
-- share classes (common + preferred, Class A + Class B). Without
-- this column, multi-class trades collapse into one opaque figure.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS security_title TEXT;

-- Deemed-execution date is populated for 10b5-1 plan trades — the
-- trade physically executed on ``txn_date`` but was ``deemed'' to
-- have been arranged on the earlier plan adoption date. Critical
-- for filtering opportunistic vs plan-based trading.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS deemed_execution_date DATE;

-- A/D code from ``transactionAcquiredDisposedCode``. Redundant with
-- ``txn_code`` in most cases (P => A, S => D) but explicit in the
-- XML so we capture it — several codes (M, G, D, F) are
-- acquired-or-disposed-ambiguous until this flag is read.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS acquired_disposed_code CHAR(1);

-- Equity-swap flag from ``transactionCoding/equitySwapInvolved``.
-- True when the reported trade is the cash-settlement leg of a
-- total-return swap — economically different from a direct trade
-- and a weaker sentiment signal.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS equity_swap_involved BOOLEAN;

-- Timeliness flag from ``transactionTimeliness/value``. ``E`` = filed
-- early (before the event), ``L`` = filed late (after the 2-day
-- deadline). Late filings are a reporting-discipline signal.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS transaction_timeliness CHAR(1);

-- Post-transaction share count from ``postTransactionAmounts/
-- sharesOwnedFollowingTransaction/value``. The insider's running
-- balance after this trade — very strong signal (a CEO trimming
-- 1% of their holding is a different story than a CEO trimming
-- 50% of their holding).
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS post_transaction_shares NUMERIC(20, 4);

-- Free-text ownership-nature detail from ``ownershipNature/
-- natureOfOwnership/value``. Populated mostly when
-- ``direct_indirect = 'I'`` to explain the indirect holding — "By
-- spouse", "By Trust dated YYYY-MM-DD", "By GRAT", "By LLC",
-- "Held in 401(k)".
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS nature_of_ownership TEXT;

-- Derivative-specific fields. All NULL on non-derivative rows.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS conversion_exercise_price NUMERIC(18, 6);
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS exercise_date DATE;
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS expiration_date DATE;
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS underlying_security_title TEXT;
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS underlying_shares NUMERIC(20, 4);
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS underlying_value NUMERIC(20, 4);

-- Footnote references carried inline. Shape:
--   [{"footnote_id": "F1", "field": "transactionShares"}, ...]
-- ``field`` is the parent XML element name that carried the
-- ``footnoteId`` attribute — tells the reader which column in the
-- transaction row the footnote qualifies.
ALTER TABLE insider_transactions
    ADD COLUMN IF NOT EXISTS footnote_refs JSONB NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN insider_transactions.filer_cik IS
    'SEC CIK of the reporting owner this transaction belongs to. '
    'FKs logically into insider_filers(accession_number, filer_cik) '
    'to pick the specific insider on a joint filing. NULL for rows '
    'created by the pre-057 parser; refilled on the next ingest '
    'pass. unique_filers_90d dedup is computed over this column, '
    'not filer_name.';

COMMENT ON COLUMN insider_transactions.security_title IS
    'Security class the transaction touches, e.g. "Common Stock", '
    '"Class B Common Stock", "Series A Preferred". Multi-class '
    'issuers report separate rows per class; without this column '
    'they collapse into one opaque share count.';

COMMENT ON COLUMN insider_transactions.deemed_execution_date IS
    '10b5-1 plan adoption date. Populated when the trade was pre-'
    'arranged under Rule 10b5-1 — the insider did not choose to '
    'trade on txn_date. Distinguishes discretionary from scheduled '
    'trades.';

COMMENT ON COLUMN insider_transactions.acquired_disposed_code IS
    '"A" = acquired, "D" = disposed. Disambiguates txn_code values '
    'that can go either direction (M, G, D, F). Combine with shares '
    'to produce the signed net.';

COMMENT ON COLUMN insider_transactions.equity_swap_involved IS
    'TRUE when the reported trade is the cash-settlement leg of a '
    'total-return swap, not a direct share transaction. Economically '
    'different; weaker sentiment signal.';

COMMENT ON COLUMN insider_transactions.transaction_timeliness IS
    '"E" = early (filed before the deadline), "L" = late (filed '
    'after the 2-business-day window). Late filings are a reporting-'
    'discipline signal.';

COMMENT ON COLUMN insider_transactions.post_transaction_shares IS
    'Insider''s total share balance following this transaction. Much '
    'stronger signal than the trade size alone — a 1%-of-position '
    'trim reads differently from a 50%-of-position trim.';

COMMENT ON COLUMN insider_transactions.nature_of_ownership IS
    'Free-text indirect-ownership explanation: "By Trust dated X", '
    '"By Spouse", "By GRAT", "Held in 401(k)". Populated when '
    'direct_indirect = "I".';

COMMENT ON COLUMN insider_transactions.conversion_exercise_price IS
    'Strike price for derivative rows (options, warrants). NULL on '
    'non-derivative.';

COMMENT ON COLUMN insider_transactions.underlying_security_title IS
    'Name of the equity the derivative converts into. Usually the '
    'issuer''s common stock but can be a different class or an '
    'index reference.';

COMMENT ON COLUMN insider_transactions.footnote_refs IS
    'Inline list of footnote references for this row: '
    '[{"footnote_id": "F1", "field": "transactionShares"}, ...]. '
    'Footnote bodies live in insider_transaction_footnotes keyed '
    'on (accession_number, footnote_id).';

-- ---------------------------------------------------------------------
-- Backfill existing rows into the new shape
-- ---------------------------------------------------------------------

-- Step 1: create a placeholder insider_filings row for every existing
-- accession. document_type is populated from filing_events if we can
-- resolve the link; otherwise default to '4'. Tombstones carried by
-- the old sentinel (filer_name = '__TOMBSTONE__', txn_row_num = -1)
-- are migrated to insider_filings.is_tombstone = TRUE.
INSERT INTO insider_filings (
    accession_number,
    instrument_id,
    document_type,
    is_tombstone,
    fetched_at,
    parser_version
)
SELECT
    it.accession_number,
    MIN(it.instrument_id) AS instrument_id,
    COALESCE(MIN(fe.filing_type), '4') AS document_type,
    BOOL_OR(it.txn_row_num = -1 AND it.filer_name = '__TOMBSTONE__') AS is_tombstone,
    MIN(it.created_at) AS fetched_at,
    1 AS parser_version  -- 056-era rows parsed under the v1 parser
FROM insider_transactions it
LEFT JOIN filing_events fe
       ON fe.provider_filing_id = it.accession_number
      AND fe.provider = 'sec'
GROUP BY it.accession_number
ON CONFLICT (accession_number) DO NOTHING;

-- Step 2: delete legacy tombstone transaction rows. They're now
-- represented at the filing level via insider_filings.is_tombstone.
DELETE FROM insider_transactions
WHERE txn_row_num = -1
  AND filer_name = '__TOMBSTONE__';

-- Step 3: add the NOT NULL + FK constraint on accession_number now
-- that every existing row has a parent in insider_filings.
-- accession_number was already NOT NULL from 056; add the FK only.
-- If the constraint already exists (idempotent re-run), skip via
-- DO block.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'insider_transactions_accession_fk'
    ) THEN
        ALTER TABLE insider_transactions
            ADD CONSTRAINT insider_transactions_accession_fk
            FOREIGN KEY (accession_number)
            REFERENCES insider_filings(accession_number)
            ON DELETE CASCADE;
    END IF;
END$$;
