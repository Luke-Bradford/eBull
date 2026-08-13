-- 091_institutional_filer_seeds.sql
--
-- Issue #730 PR 2 — operator-curated list of institutional filer
-- CIKs to ingest 13F-HR holdings for. Per Option C in the
-- implementation plan: rather than try to ingest all ~5,000
-- institutional managers per quarter, the operator picks the top
-- ~100-200 names that move the needle (Vanguard, BlackRock,
-- Fidelity, Berkshire, etc.) — this curated set covers ~80% of
-- institutional AUM with a fraction of the SEC bandwidth.
--
-- The PR 3 filer-type classifier extends this with an ETF-CIK list
-- so the ownership card (#729) can split institutions vs ETFs.
--
-- ``label`` is informational — the canonical filer name is fetched
-- from primary_doc.xml on first ingest and stored on
-- institutional_filers.name. Keeping it here helps the operator
-- audit the seed list without joining to ingest results.
--
-- ``active`` lets an operator pause a noisy / problematic filer
-- without dropping the row (preserves audit trail of prior ingests).

CREATE TABLE IF NOT EXISTS institutional_filer_seeds (
    cik         TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- ``notes`` is free-form (e.g. "top US ETF issuer", "Buffett's
    -- holdco", "added per operator review 2026-05-03"). NULL OK.
    notes       TEXT
);

CREATE INDEX IF NOT EXISTS idx_filer_seeds_active
    ON institutional_filer_seeds (cik)
    WHERE active = TRUE;


-- ── institutional_holdings_ingest_log ──────────────────────────
--
-- Per-accession attempt tombstone. The ingester (#730 PR 2) needs
-- this to avoid re-fetching every accession on every run when the
-- accession produced zero canonical rows — common cases:
--   * Empty 13F-HR (filer reported "exempt list" or cancellation).
--   * Every holding's CUSIP unresolved (the #740 backfill gap).
--   * Persistent 404 on index.json or one of the XML attachments.
--
-- Without this table, ``already_ingested`` is derived from
-- ``institutional_holdings.accession_number``, which misses any
-- accession that didn't write a holding row. Re-runs then fetch
-- the same archive forever, burning SEC bandwidth + log noise.
--
-- Codex pre-push review caught this on PR review.

CREATE TABLE IF NOT EXISTS institutional_holdings_ingest_log (
    accession_number   TEXT PRIMARY KEY,
    filer_cik          TEXT NOT NULL,
    period_of_report   DATE,
    status             TEXT NOT NULL
        CHECK (status IN ('success', 'partial', 'failed')),
    holdings_inserted  INTEGER NOT NULL DEFAULT 0,
    holdings_skipped   INTEGER NOT NULL DEFAULT 0,
    error              TEXT,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_holdings_ingest_log_filer
    ON institutional_holdings_ingest_log (filer_cik, fetched_at DESC);
