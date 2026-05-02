-- 100_ncen_filer_classifications.sql
--
-- Issue #782 — derive ``institutional_filers.filer_type`` beyond
-- the curated ETF list (#742) by ingesting Form N-CEN annual fund
-- census filings. Each fund's N-CEN carries an
-- ``investmentCompanyType`` field (``N-1A`` / ``N-2`` / ``N-3`` /
-- ``N-4`` / ``N-5`` / ``N-6``) that maps cleanly to our enum:
--
--   * ``N-1A`` (open-end management company — mutual fund or ETF)
--     -> ``INV`` by default; the curated ETF seed list overrides
--     this when the CIK is on the ETF list.
--   * ``N-2`` (closed-end fund / BDC) -> ``INV``.
--   * ``N-3`` / ``N-4`` / ``N-6`` (variable insurance contracts)
--     -> ``INS``.
--   * ``N-5`` (small business investment company) -> ``INV``.
--
-- Broker-dealer (``BD``) classification is NOT addressable from
-- N-CEN — broker-dealers file Form ADV / FOCUS reports instead.
-- That's a separate ticket; v1 stays at the N-CEN-derivable
-- subset.
--
-- Schema decisions:
--
--   * Identity / dedupe = ``cik`` PK. Each filer has at most one
--     active N-CEN classification at a time; re-running the
--     classifier on a newer N-CEN UPSERTs in place.
--   * ``investment_company_type`` stores the raw SEC code
--     (``N-1A``, ``N-2``, etc.) for audit; the derived
--     ``filer_type`` is the operator-facing enum value applied to
--     ``institutional_filers``.
--   * ``derived_filer_type`` mirrors the
--     ``institutional_filers.filer_type`` CHECK enum so the
--     compose function in :mod:`app.services.institutional_holdings`
--     can read this column and map directly without a translation
--     step.
--   * ``accession_number`` + ``filed_at`` track the N-CEN that
--     produced this classification — re-running the classifier
--     against a newer accession (e.g. issuer changed structure)
--     promotes the row in place.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated
-- in the same PR per the prevention-log entry.

CREATE TABLE IF NOT EXISTS ncen_filer_classifications (
    cik                       TEXT PRIMARY KEY,
    investment_company_type   TEXT NOT NULL,
    derived_filer_type        TEXT NOT NULL
        CHECK (derived_filer_type IN ('ETF', 'INV', 'INS', 'BD', 'OTHER')),
    accession_number          TEXT NOT NULL,
    filed_at                  TIMESTAMPTZ NOT NULL,
    fetched_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Hot path for the per-classifier-run reader: walk the most
-- recently classified filers first.
CREATE INDEX IF NOT EXISTS idx_ncen_filer_classifications_fetched_at
    ON ncen_filer_classifications (fetched_at DESC);

-- Per-derived-type scan for the ops monitor's "how many INS
-- filers have we classified" coverage chip.
CREATE INDEX IF NOT EXISTS idx_ncen_filer_classifications_filer_type
    ON ncen_filer_classifications (derived_filer_type);
