-- 090_institutional_filers_holdings.sql
--
-- Issue #730 (Plan C.2) — schema for SEC 13F-HR institutional
-- holdings ingest. PR 1 of the four-part split: this PR ships the
-- tables + the XML parser; the ingester (PR 2), filer-type
-- classifier (PR 3), and reader API + frontend wiring (PR 4) follow.
--
-- 13F-HR is filed quarterly by every institutional manager with
-- discretionary AUM > $100M. Holdings live in the per-accession
-- ``infotable.xml``; filer metadata in ``primary_doc.xml``. The
-- ingest aggregates across filers to derive institutional ownership
-- per stock (Institutions slice on the ownership card #729) and
-- filters on filer_type to derive ETF ownership separately (ETFs
-- slice — every US-domiciled ETF files 13F-HR).
--
-- Schema decisions:
--   * institutional_filers keys on the filer's CIK (not its CRD —
--     CIK is what every other SEC ingest in this repo joins on).
--   * institutional_holdings keys on a synthetic BIGSERIAL plus a
--     uniqueness constraint on (accession_number, instrument_id) to
--     idempotently re-ingest the same 13F without duplicating rows.
--     accession_number is the 13F's accession (filer_cik-yy-seq) so
--     the same instrument across two filers' 13Fs lands as two
--     separate rows.
--   * voting_authority is one of three SEC-prescribed values:
--     SOLE / SHARED / NONE. Constrained via CHECK so a future
--     parser regression can't smuggle a fourth value into the
--     canonical store.
--   * is_put_call is NULL for the underlying-equity row and either
--     'PUT' or 'CALL' for option exposure rows. CHECK constrains
--     the non-NULL set.
--   * shares is NUMERIC (not INTEGER) so option-exposure values
--     (which can be reported as principal-amount equivalents on
--     bond holdings) and exotic share counts don't overflow.
--   * market_value_usd is reported by the filer in USD thousands
--     pre-2023 and USD whole dollars post-2022. The parser layer
--     does NOT normalise — it returns the raw value as Decimal, and
--     the service layer (PR 2) applies any unit conversion based on
--     ``period_of_report``. This split keeps the parser pure and
--     leaves the unit-policy decision in one place.
--   * filed_at is nullable: ``primary_doc.xml`` may be missing the
--     signature block on a malformed filing; the parser returns
--     ``None`` rather than raising so the rest of the holding rows
--     can still be ingested. The ingester (PR 2) decides whether to
--     persist filings with NULL filed_at or to tombstone them.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry "When a migration adds
-- any table with a FK relationship, update _PLANNER_TABLES …".

CREATE TABLE IF NOT EXISTS institutional_filers (
    filer_id           BIGSERIAL PRIMARY KEY,
    cik                TEXT NOT NULL UNIQUE,
    name               TEXT NOT NULL,
    -- Filer-type classification ships in PR 3 (#730). Nullable here
    -- because the parser can't determine type from the 13F payload
    -- alone — it requires a curated ETF-CIK list cross-reference.
    filer_type         TEXT
        CHECK (filer_type IS NULL OR filer_type IN ('ETF', 'INV', 'INS', 'BD', 'OTHER')),
    aum_usd            NUMERIC(20,2),
    last_filing_at     TIMESTAMPTZ,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS institutional_holdings (
    holding_id         BIGSERIAL PRIMARY KEY,
    filer_id           BIGINT NOT NULL REFERENCES institutional_filers(filer_id),
    instrument_id      BIGINT NOT NULL REFERENCES instruments(instrument_id),
    accession_number   TEXT NOT NULL,
    period_of_report   DATE NOT NULL,
    shares             NUMERIC(24,4) NOT NULL,
    market_value_usd   NUMERIC(20,2),
    voting_authority   TEXT
        CHECK (voting_authority IS NULL OR voting_authority IN ('SOLE', 'SHARED', 'NONE')),
    is_put_call        TEXT
        CHECK (is_put_call IS NULL OR is_put_call IN ('PUT', 'CALL')),
    filed_at           TIMESTAMPTZ
);

-- Idempotent re-ingest needs uniqueness on
-- ``(accession_number, instrument_id, is_put_call)`` so the same
-- accession's equity + PUT + CALL exposure on a single issuer (up to
-- three legal rows per issuer in the SEC schema) all coexist. A
-- plain UNIQUE constraint cannot include a nullable column safely
-- (Postgres treats two NULL is_put_call values as distinct, which
-- would let the same equity row insert twice on a re-ingest), so
-- this is a partial UNIQUE INDEX that COALESCEs the option-exposure
-- column to the sentinel ``'EQUITY'`` for the equity rows.
CREATE UNIQUE INDEX IF NOT EXISTS uq_holdings_accession_instrument_putcall
    ON institutional_holdings (
        accession_number,
        instrument_id,
        (COALESCE(is_put_call, 'EQUITY'))
    );

-- Hot path for the per-instrument ownership reader: walk holdings
-- for one instrument across the most recent quarters first.
CREATE INDEX IF NOT EXISTS idx_holdings_instrument_period
    ON institutional_holdings (instrument_id, period_of_report DESC);

-- Hot path for the per-filer aggregator (sum AUM, filer concentration):
-- walk every holding for one filer ordered by report date.
CREATE INDEX IF NOT EXISTS idx_holdings_filer_period
    ON institutional_holdings (filer_id, period_of_report DESC);
