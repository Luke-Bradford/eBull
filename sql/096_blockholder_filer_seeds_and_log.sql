-- 096_blockholder_filer_seeds_and_log.sql
--
-- Issue #766 PR 1 — operator-curated blockholder seed list and
-- per-accession ingest tombstone log. Mirrors migration 091
-- (institutional_filer_seeds + institutional_holdings_ingest_log).
--
-- ── blockholder_filer_seeds ────────────────────────────────────
--
-- Same logic as the 13F-HR seed list: rather than walk every CIK
-- that has ever filed a 13D/G (tens of thousands of one-shot family
-- trusts and small holdcos), the operator curates the few dozen
-- names that move the needle for tradable mid-cap and small-cap
-- coverage:
--
--   * Activist hedge funds (Icahn, Pershing, Elliott, Starboard,
--     ValueAct, Trian, Engaged, etc.)
--   * Founder / founder-family holdcos (Lauder family trusts on
--     EL, Wachowski-on-NXT-style positions, Soros-on-X-style
--     positions, etc.)
--   * Other ≥5%-holders the operator wants to track explicitly
--
-- ``label`` is informational — the canonical filer name is fetched
-- from primary_doc.xml on first ingest and stored on
-- blockholder_filers.name. Keeping it here helps the operator
-- audit the seed list without joining to ingest results.
--
-- ``active`` lets the operator pause a noisy / problematic filer
-- without dropping the row (preserves audit trail of prior ingests).

CREATE TABLE IF NOT EXISTS blockholder_filer_seeds (
    cik         TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_blockholder_filer_seeds_active
    ON blockholder_filer_seeds (cik)
    WHERE active = TRUE;


-- ── blockholder_filings_ingest_log ─────────────────────────────
--
-- Per-accession attempt tombstone. Same rationale as
-- ``institutional_holdings_ingest_log``: the ingester (PR 2) needs
-- a record of *attempts* so empty / failed accessions don't get
-- re-fetched on every run.
--
-- Common reasons an accession produces zero canonical rows:
--   * Reporter cover page references prior amendments rather than
--     restating ownership numbers (legal but yields no usable row
--     for the aggregator).
--   * Issuer CUSIP unresolved against ``external_identifiers`` (the
--     same gap #740 tracks for 13F-HR).
--   * Persistent 404 on the per-accession primary_doc.xml.
--
-- Without this table, ``already_ingested`` would be derived from
-- ``blockholder_filings.accession_number``, which misses every
-- accession that didn't write a row. Re-runs would then fetch the
-- same archive forever, burning SEC bandwidth and log noise.

CREATE TABLE IF NOT EXISTS blockholder_filings_ingest_log (
    accession_number   TEXT PRIMARY KEY,
    filer_cik          TEXT NOT NULL,
    submission_type    TEXT,
    status             TEXT NOT NULL
        CHECK (status IN ('success', 'partial', 'failed')),
    rows_inserted      INTEGER NOT NULL DEFAULT 0,
    rows_skipped       INTEGER NOT NULL DEFAULT 0,
    error              TEXT,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_blockholder_filings_ingest_log_filer
    ON blockholder_filings_ingest_log (filer_cik, fetched_at DESC);
