-- 054_dividend_events.sql
--
-- 8-K Item 8.01 dividend calendar (#434). Follow-up to #426 dividend
-- history (XBRL-declared amounts) + #431 8-K items[] typing.
--
-- SEC XBRL gives per-period dps declared, but NOT the ex-date /
-- record-date / pay-date calendar that drives UI "next dividend"
-- banners and the optional pre-ex-date dividend-capture signal. That
-- calendar lives only in the free-form announcement text of 8-K
-- filings carrying Item 8.01 "Other Events". A regex parser is
-- acceptance-bar-limited to ≥80% of Dividend Aristocrats; the rest
-- fall back to "next dividend date unknown".
--
-- Storage model: one row per (instrument, source_accession) — the
-- UNIQUE constraint is idempotent under re-runs. We keep dates
-- nullable because real 8-Ks often announce only a subset
-- (e.g. ex-date + pay-date, no record-date separately named). A row
-- with all three nullable date fields NULL is still useful: it means
-- we saw an 8.01 that parsed as a dividend announcement but couldn't
-- pin any date — operator visibility matters more than silent skip.
--
-- Currency is stored explicitly (default 'USD' for SEC filers) so
-- later multi-currency issuers don't need a backfill.

CREATE TABLE IF NOT EXISTS dividend_events (
    id                  BIGSERIAL   PRIMARY KEY,
    instrument_id       BIGINT      NOT NULL REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    source_accession    TEXT        NOT NULL,
    declaration_date    DATE,
    ex_date             DATE,
    record_date         DATE,
    pay_date            DATE,
    dps_declared        NUMERIC(18, 6),
    currency            TEXT        NOT NULL DEFAULT 'USD',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_parsed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, source_accession)
);

CREATE INDEX IF NOT EXISTS idx_dividend_events_instrument_ex_date
    ON dividend_events (instrument_id, ex_date DESC NULLS LAST);

-- Ingester query path: "find upcoming dividends across the universe"
-- needs a global ex_date scan; this partial index keeps that fast
-- without bloating the per-instrument access pattern above.
CREATE INDEX IF NOT EXISTS idx_dividend_events_ex_date_future
    ON dividend_events (ex_date)
    WHERE ex_date IS NOT NULL;

COMMENT ON TABLE dividend_events IS
    'Per-filing dividend calendar extracted from 8-K Item 8.01 text. '
    'One row per (instrument, 8-K accession). Dates are nullable '
    'individually — a partially-parsed announcement still yields a '
    'row so operators can see the accession and follow the URL.';

COMMENT ON COLUMN dividend_events.source_accession IS
    'SEC accession number of the 8-K the dates were parsed from. '
    'Forms the idempotency key together with instrument_id.';

COMMENT ON COLUMN dividend_events.declaration_date IS
    'Board-declaration date as stated in the filing (not the filing '
    'date itself — 8-Ks are typically filed 1–4 business days after '
    'the board vote).';

COMMENT ON COLUMN dividend_events.dps_declared IS
    'Cash dividend per share stated in the announcement. NULL when '
    'the filing discloses a dividend calendar without an amount '
    '(e.g. "regular quarterly dividend" boilerplate).';

COMMENT ON COLUMN dividend_events.last_parsed_at IS
    'Bumped by the ingester''s ON CONFLICT path on every re-parse. The '
    'ingester''s candidate selector skips partial rows whose '
    'last_parsed_at is within the 7-day TTL so a stable partial row '
    'does not hammer SEC every daily run. Fresh rows set this to '
    'NOW() via the column default on insert.';
