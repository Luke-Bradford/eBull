-- 063_sec_facts_concept_catalog.sql
--
-- SEC company-facts concept catalogue (#451 Phase A). Follow-up to
-- the #448 operator directive: every structured upstream field
-- lands in SQL; ``data/raw/sec_fundamentals/`` (~11 GB of
-- companyfacts JSON) stops being a necessary audit substrate once
-- every concept is queryable in SQL.
--
-- Current state (pre-#451): ``financial_facts_raw`` stores XBRL
-- facts gated on the ``TRACKED_CONCEPTS`` allowlist (~36 us-gaap
-- concepts + 3 dei concepts). Every other concept in the issuer's
-- companyfacts.json (segment reporting, deferred-tax breakdown,
-- lease liabilities, operating-leases detail, etc.) is silent —
-- available on disk but unqueryable from SQL.
--
-- Two changes land together:
--   1. This catalogue table ``sec_facts_concept_catalog`` storing
--      (taxonomy, concept) metadata: label, description, first-seen
--      / last-seen timestamps. Populated opportunistically as the
--      ingester sees new concepts.
--   2. Extractor widens to emit facts for EVERY concept (not just
--      the editorial TRACKED_CONCEPTS subset). TRACKED_CONCEPTS
--      stays as the canonical-alias map used by the
--      ``financial_periods`` projection logic, but the raw fact
--      table now carries the full richness of each filing.
--
-- Phase B (#466 follow-up) flips the retention policy on
-- ``sec_fundamentals`` raw persistence to compact-out once SQL
-- coverage is verified end-to-end.

CREATE TABLE IF NOT EXISTS sec_facts_concept_catalog (
    id              BIGSERIAL   PRIMARY KEY,
    -- XBRL taxonomy namespace. ``us-gaap`` for the standard
    -- accounting taxonomy; ``dei`` for document-and-entity cover-
    -- page facts. Issuer-specific extensions land under the
    -- issuer's own namespace prefix (rare in our universe).
    taxonomy        TEXT        NOT NULL,
    -- Raw XBRL tag, e.g. ``Revenues``,
    -- ``OperatingLeaseLiabilityNoncurrent``,
    -- ``ShareBasedCompensation``. Case-preserved verbatim from the
    -- source JSON so lookups match the tag string on
    -- ``financial_facts_raw.concept`` exactly.
    concept         TEXT        NOT NULL,
    -- Human-readable label as it appears on the concept's ``label``
    -- field in the companyfacts JSON (e.g. "Revenues", "Operating
    -- Lease Liability, Noncurrent"). Used by the UI to render
    -- concept names without hard-coding each tag in Python.
    label           TEXT,
    -- Full SEC taxonomy description — usually a paragraph explaining
    -- what the concept measures, unit expectations, and reporting
    -- notes. Preserved verbatim for audit and for rendering in the
    -- instrument-page financial-facts explorer.
    description     TEXT,
    -- Unit types observed for this concept across all filings
    -- ingested to date: ``USD``, ``USD/shares``, ``shares``,
    -- ``pure``, etc. Stored as a TEXT[] so a concept reporting in
    -- multiple units over time accumulates the full set.
    units_seen      TEXT[]      NOT NULL DEFAULT '{}'::TEXT[],
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (taxonomy, concept)
);

CREATE INDEX IF NOT EXISTS idx_sec_facts_concept_catalog_taxonomy
    ON sec_facts_concept_catalog (taxonomy);

COMMENT ON TABLE sec_facts_concept_catalog IS
    'Per-concept metadata for every XBRL tag the SEC fundamentals '
    'ingester has seen. Populated opportunistically alongside the '
    'fact-level upsert so a UI / query tool can render concepts '
    'with their human-readable labels + descriptions without '
    'hitting the raw companyfacts JSON on disk.';

COMMENT ON COLUMN sec_facts_concept_catalog.units_seen IS
    'Accumulated set of unit types observed for this concept '
    '(``USD``, ``USD/shares``, ``shares``, ``pure``, etc.). A '
    'concept can legitimately report in multiple units across '
    'filings; the array lets callers know which units to expect '
    'without re-scanning financial_facts_raw.';
