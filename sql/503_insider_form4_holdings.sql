-- 503_insider_form4_holdings.sql
--
-- #3227 — a Form 4's Table I holding lines (``nonDerivativeHolding`` / ``derivativeHolding``).
-- Spec: docs/specs/ownership/2026-10-18-3227-form4-holding-rows.md, Change 2.
--
-- Form 4 General Instruction 4(c)(i): "Report acquisitions or dispositions and holdings of
-- non-derivative securities in Table I." ``parse_form_4_xml`` read the transaction rows only, so a
-- holding line never reached storage and a (holder, D/I) series reported only as a holding kept the
-- balance of the last filing that transacted in it.
--
-- A table of its own, not ``insider_initial_holdings``: every reader of that table treats its rows
-- as Form 3 snapshots (the ``form3`` derivation in ``sync_insiders``, de-collision clause 2, the
-- amendment filter, the drill-through counts, the rollup, the holder-name resolver, the instruments
-- API). Same columns as ``insider_initial_holdings`` minus ``as_of_date``, which a Form 4 does not
-- state per line (the observation's date is derived from the filing, spec Change 3), plus
-- ``footnote_refs`` as on ``insider_transactions``. Written by ``upsert_filing`` beside the
-- transactions, keyed like them on ``(accession_number, row_num)``; ``row_num`` is shared across
-- both holding tables, non-derivative first.

CREATE TABLE IF NOT EXISTS insider_form4_holdings (
    id                          BIGSERIAL   PRIMARY KEY,
    instrument_id               BIGINT      NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    accession_number            TEXT        NOT NULL
        REFERENCES insider_filings(accession_number) ON DELETE CASCADE,
    row_num                     INT         NOT NULL,
    filer_cik                   TEXT        NOT NULL,
    filer_name                  TEXT        NOT NULL,
    filer_role                  TEXT,
    security_title              TEXT,
    -- Column 5 as a share count; NULL on the SEC value branch or an unreadable amount.
    shares                      NUMERIC(20, 4),
    value_owned                 NUMERIC(18, 6),
    is_derivative               BOOLEAN     NOT NULL DEFAULT FALSE,
    direct_indirect             CHAR(1)     CHECK (direct_indirect IS NULL OR direct_indirect IN ('D', 'I')),
    nature_of_ownership         TEXT,
    conversion_exercise_price   NUMERIC(18, 6),
    exercise_date               DATE,
    expiration_date             DATE,
    underlying_security_title   TEXT,
    underlying_shares           NUMERIC(20, 4),
    underlying_value            NUMERIC(18, 6),
    footnote_refs               JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (accession_number, row_num)
);

CREATE INDEX IF NOT EXISTS idx_insider_form4_holdings_instrument_filer
    ON insider_form4_holdings (instrument_id, filer_cik);

COMMENT ON TABLE insider_form4_holdings IS
    'Form 4/4A Table I and II holding lines (no transaction, column 5 only), one row per line. '
    'Not Form 3 baselines: those are insider_initial_holdings. #3227.';
