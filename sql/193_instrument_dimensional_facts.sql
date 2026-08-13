-- 193_instrument_dimensional_facts.sql
--
-- #554 (spec docs/proposals/etl/2026-06-11-554-xbrl-dimensional-facts.md §D4)
-- — dimensional XBRL facts (business segments, product/service mix,
-- geographic revenue) extracted per 10-K accession by the sec_10k
-- manifest parser step 2 (parser version 10k-v2).
--
-- The companyfacts API carries NO dimensional facts (verified
-- 2026-06-11, spec §1) so these rows come from per-filing XBRL
-- instance parsing — they cannot live in ``financial_facts_raw``,
-- whose identity has no axis/member dimension.
--
-- Semantics mirror ``financial_facts_raw`` (sql/032): immutable
-- per-accession rows, reader dedupes by winning accession per
-- (instrument, axis, metric) ORDER BY filed_at DESC. Rewash of an
-- accession is delete-then-insert in one transaction (spec §D4).
--
-- ``member_qname`` is the honest grain for geography — members mix
-- ISO-backed qnames (``country:US``) with filer-custom members
-- (``aapl:OtherCountriesMember``); no ISO column by design (spec §1.3).
--
-- Not partitioned: ~25 rows/filing × ~25k 10-Ks ≈ 625k rows.

BEGIN;

CREATE TABLE IF NOT EXISTS instrument_dimensional_facts (
    fact_id          BIGSERIAL PRIMARY KEY,
    instrument_id    BIGINT NOT NULL REFERENCES instruments(instrument_id),
    axis             TEXT NOT NULL CHECK (axis IN
                       ('business_segment', 'product_service', 'geographic')),
    member_qname     TEXT NOT NULL,
    member_label     TEXT NOT NULL,
    metric           TEXT NOT NULL CHECK (metric IN
                       ('revenue', 'operating_income', 'assets')),
    unit             TEXT NOT NULL,
    -- TRUE when the member parents another member with a fact on the
    -- same axis in this filing (definition-linkbase domain-member
    -- arcs) — e.g. us-gaap:ProductMember is the subtotal of
    -- iPhone/Mac/iPad/Wearables on AAPL's product axis. Readers
    -- exclude subtotals so member rows sum to the consolidated total.
    is_subtotal      BOOLEAN NOT NULL DEFAULT FALSE,
    period_start     DATE,
    period_end       DATE NOT NULL,
    val              NUMERIC(30,6) NOT NULL,
    -- XBRL allows non-integer precision values like "INF" (same shape
    -- as financial_facts_raw.decimals); input to duplicate-fact
    -- arbitration at parse time.
    decimals         TEXT,
    source_accession TEXT NOT NULL,
    form_type        TEXT NOT NULL,
    filed_at         TIMESTAMPTZ NOT NULL,
    parser_version   TEXT NOT NULL,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Identity: one fact per instrument/axis/member/metric/period-range/
-- filing. period_start is NULL for instant facts (assets), so COALESCE
-- makes NULLs comparable (plain UNIQUE treats NULLs as distinct, which
-- would allow duplicate instants) — same pattern as
-- uq_facts_raw_identity in sql/032.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dimensional_facts_identity
    ON instrument_dimensional_facts(
        instrument_id, axis, member_qname, metric,
        COALESCE(period_start, '0001-01-01'::date),
        period_end, source_accession
    );

CREATE INDEX IF NOT EXISTS idx_dimensional_facts_read
    ON instrument_dimensional_facts(instrument_id, axis, period_end DESC);

COMMIT;
