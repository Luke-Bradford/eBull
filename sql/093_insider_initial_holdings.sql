-- 093_insider_initial_holdings.sql
--
-- Form 3 initial-holdings seeding (#768). PR 1 of N: schema only —
-- the parser lands in this PR alongside; the ingester + service-
-- layer cumulative-update wiring follow in PR 2 + 3.
--
-- Form 3 is filed once when an insider becomes subject to Section 16
-- reporting (officer / director / 10% holder appointment). It records
-- the *snapshot* of their positions on that date. Form 4 then records
-- subsequent *changes*. Cumulative balance at any later point =
-- Form 3 baseline + signed sum of Form 4 deltas since.
--
-- Why this matters operationally:
--   * Insiders who never trade after appointment are invisible to the
--     ownership card today — no Form 4 events for them, so the per-
--     officer ring 3 wedges silently miss them.
--   * The L2 ownership-over-time chart (#756) starts the insider band
--     at zero before our coverage window because there's no baseline
--     to anchor the running total.
--
-- Storage model parallels insider_filings / insider_transactions
-- (migration 057) but for the snapshot semantics:
--   * One row per (accession, row_num) — non-derivative + derivative
--     holdings interleave inside a single accession; row_num
--     disambiguates them, mirroring insider_transactions.txn_row_num.
--   * shares is NUMERIC(20, 4) to match insider_transactions.shares so
--     downstream cumulative-sum queries stay arithmetic-clean.
--   * direct_indirect mirrors insider_transactions ('D' / 'I').
--   * is_derivative discriminates the two tables (a Form 3 can carry
--     both an underlying-equity holding and an option-grant holding
--     for the same officer).

CREATE TABLE IF NOT EXISTS insider_initial_holdings (
    id                          BIGSERIAL   PRIMARY KEY,
    instrument_id               BIGINT      NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    accession_number            TEXT        NOT NULL,
    -- Zero-indexed row within the filing's combined holdings list
    -- (non-derivative rows first, then derivative). Matches the
    -- txn_row_num convention in insider_transactions so the same
    -- (accession, row_num) pattern works across both tables.
    row_num                     INT         NOT NULL,
    filer_cik                   TEXT        NOT NULL,
    filer_name                  TEXT        NOT NULL,
    -- Pipe-joined roles from the Form 3 reportingOwnerRelationship
    -- block. Same convention as insider_transactions.filer_role.
    filer_role                  TEXT,
    -- periodOfReport from Form 3 — the snapshot date the filer
    -- declares their holdings as "as of". Distinct from filed_at —
    -- a Form 3 filed late can declare an as_of_date weeks earlier.
    -- Operator-facing copy and the cumulative-balance computation
    -- use this column, not the filing-time stamp.
    as_of_date                  DATE        NOT NULL,
    security_title              TEXT,
    -- Snapshot share count from
    -- postTransactionAmounts/sharesOwnedFollowingTransaction. NULL when
    -- the SEC value-branch is used instead (see value_owned below) or
    -- when the value parses as malformed.
    shares                      NUMERIC(20, 4),
    -- Snapshot value alternative from
    -- postTransactionAmounts/valueOwnedFollowingTransaction. SEC
    -- allows EITHER shares OR value (fractional-undivided-interest
    -- securities use the value branch). Both columns surface so the
    -- reader can pick the populated one without a silent drop —
    -- mirrors the Form 4 underlying_value precedent in migration 057.
    value_owned                 NUMERIC(18, 6),
    is_derivative               BOOLEAN     NOT NULL DEFAULT FALSE,
    -- "D" (direct) / "I" (indirect). Same encoding as
    -- insider_transactions.direct_indirect. Parser sanitises any other
    -- value to NULL so a malformed filing can't smuggle a third value
    -- past the read-side filter.
    direct_indirect             CHAR(1)     CHECK (direct_indirect IS NULL OR direct_indirect IN ('D', 'I')),
    nature_of_ownership         TEXT,
    -- Derivative-only fields. NULL on non-derivative rows.
    conversion_exercise_price   NUMERIC(18, 6),
    exercise_date               DATE,
    expiration_date             DATE,
    underlying_security_title   TEXT,
    underlying_shares           NUMERIC(20, 4),
    -- Value alternative for derivative underlyings (parallels Form 4
    -- post-057). Some derivative grants — performance / dollar-
    -- denominated awards — express the underlying as a value not a
    -- share count.
    underlying_value            NUMERIC(18, 6),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, row_num)
);

-- Footnote storage + filing-level metadata (issuer, signature,
-- noSecuritiesOwned flag, dateOfOriginalSubmission, joint-filing
-- owners list) lands in PR2 alongside the ingester that needs the
-- FK relationship to insider_filings. Pre-empting the table here
-- without an ingester to write it would create dead schema.

-- Hot path for the per-instrument cumulative-balance reader: walk
-- every Form 3 baseline row for one issuer ordered by snapshot date.
CREATE INDEX IF NOT EXISTS idx_insider_initial_holdings_instrument
    ON insider_initial_holdings (instrument_id, as_of_date DESC);

-- Hot path for the per-filer running total: pick the latest Form 3
-- snapshot per (filer, instrument) before folding Form 4 deltas.
CREATE INDEX IF NOT EXISTS idx_insider_initial_holdings_filer
    ON insider_initial_holdings (filer_cik, instrument_id, as_of_date DESC);

COMMENT ON TABLE insider_initial_holdings IS
    'Form 3 per-row holdings snapshot. One row per holding line in '
    'a Form 3 filing (non-derivative + derivative interleaved by row_num). '
    'Source: SEC EDGAR primary_doc.xml. Used as the cumulative-balance '
    'baseline in get_insider_summary; Form 4 deltas accrete on top.';

COMMENT ON COLUMN insider_initial_holdings.as_of_date IS
    'periodOfReport from Form 3 — snapshot date the filer declares. '
    'Distinct from the filing date; a late-filed Form 3 can declare '
    'an as_of_date earlier than its filing.';

COMMENT ON COLUMN insider_initial_holdings.row_num IS
    'Zero-indexed position within the filing''s combined holdings '
    'list (non-derivative rows first, then derivative). Matches the '
    'txn_row_num pattern in insider_transactions for symmetry.';
