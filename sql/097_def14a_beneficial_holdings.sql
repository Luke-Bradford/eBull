-- 097_def14a_beneficial_holdings.sql
--
-- Issue #769 PR 1 of N — schema for SEC DEF 14A beneficial-ownership
-- table parser. DEF 14A is the proxy statement filed annually by
-- every Section 12-registered issuer. Item 12 of the proxy (Schedule
-- 14A) carries a beneficial-ownership table listing every officer +
-- director + 5%+ holder along with their share count and percent of
-- class as of a recent record date.
--
-- Why this matters operationally:
--
--   * Form 4 is event-driven — an officer who is granted shares on
--     appointment and never sells them generates no Form 4 events,
--     and our cumulative running total starts at zero. Form 3 (#768)
--     covers the appointment baseline; DEF 14A is the canonical
--     annual reconciliation point for both.
--   * 13D/G blockholders (#766) are also reconciled via Item 12 —
--     the proxy lists 5%+ holders independently of the holders'
--     own filings, so a missing 13D/G ingest surfaces as a DEF 14A
--     row without a matching ``blockholder_filings`` chain.
--   * Drift between Form 4 cumulative and the DEF 14A snapshot >5%
--     is the operator's signal that ingest coverage is broken (or
--     that an unreported transaction exists).
--
-- Schema decisions:
--
--   * ``instrument_id`` is nullable — DEF 14A's issuer CIK is
--     resolved via ``external_identifiers`` post-parse; rows whose
--     CIK is unmapped are persisted with NULL so the audit trail
--     stays intact and a later mapping backfill can promote them.
--   * ``holder_role`` is a free-text label (``'officer'``,
--     ``'director'``, ``'principal'``, ``'officer:CFO'``, etc.) —
--     the parser derives this from the table's section heading or
--     row label. No CHECK constraint: roles vary by issuer and a
--     restrictive enum would silently drop unfamiliar labels at
--     ingest time.
--   * ``shares`` is NUMERIC(24, 4) to match
--     ``insider_transactions.shares`` so cross-source cumulative
--     queries stay arithmetic-clean.
--   * ``percent_of_class`` is NUMERIC(8, 4) for the same reason as
--     ``blockholder_filings`` (#766) — SEC allows 4 decimals.
--   * ``as_of_date`` is the record date the table reports against.
--     Frequently the issuer's record date for the upcoming annual
--     meeting (typically 60-90 days before the meeting). NULL when
--     the parser cannot find an explicit date string in the
--     surrounding section.
--   * Identity / dedupe is ``(accession_number, holder_name)`` —
--     the same accession with the same holder is the same row. The
--     unique constraint deliberately does NOT include
--     ``holder_role`` because the role tag is parser-derived
--     (heuristic) — re-parsing the same accession with improved
--     role inference would otherwise insert a duplicate row.
--     Two-officers-with-the-same-full-name in one accession is
--     virtually impossible (the proxy form uses distinct legal
--     names); on the rare collision the second row UPSERTs over
--     the first which is acceptable for v1. Codex pre-push review
--     caught the prior version's role-keyed identity.
--
-- _PLANNER_TABLES in tests/fixtures/ebull_test_db.py is updated in
-- the same PR per the prevention-log entry.

CREATE TABLE IF NOT EXISTS def14a_beneficial_holdings (
    holding_id        BIGSERIAL PRIMARY KEY,
    instrument_id     BIGINT REFERENCES instruments(instrument_id),
    accession_number  TEXT NOT NULL,
    issuer_cik        TEXT NOT NULL,
    holder_name       TEXT NOT NULL,
    holder_role       TEXT,
    shares            NUMERIC(24, 4),
    percent_of_class  NUMERIC(8, 4),
    as_of_date        DATE,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Idempotent re-ingest. Identity is ``(accession_number,
-- holder_name)``. Holder role is heuristic-derived and excluded
-- from the unique key so re-parsing with improved role inference
-- promotes the existing row via UPSERT instead of inserting a
-- duplicate. Codex pre-push review caught this on PR review.
CREATE UNIQUE INDEX IF NOT EXISTS uq_def14a_holdings_accession_holder
    ON def14a_beneficial_holdings (accession_number, holder_name);

-- Hot path for the per-instrument reader (PR 3 reconciliation
-- view): walk holdings for one instrument across the most recent
-- as_of dates first.
CREATE INDEX IF NOT EXISTS idx_def14a_holdings_instrument_as_of
    ON def14a_beneficial_holdings (instrument_id, as_of_date DESC);

-- Hot path for the drift detector (PR 2): walk every snapshot for
-- one issuer + holder ordered by date so the latest position is
-- a single index lookup.
CREATE INDEX IF NOT EXISTS idx_def14a_holdings_issuer_holder
    ON def14a_beneficial_holdings (issuer_cik, holder_name, as_of_date DESC);


-- ── def14a_ingest_log ──────────────────────────────────────────
--
-- Per-accession attempt tombstone. Same rationale as the
-- institutional-holdings and blockholder-filings ingest logs: an
-- accession that produces zero canonical rows (no recognisable
-- beneficial-ownership table, malformed HTML, every holder name
-- unparseable) must still be marked attempted so the next run
-- skips it instead of re-fetching the primary doc forever.
--
-- Common reasons for zero rows:
--
--   * Section heading present but the table-finder picked the wrong
--     <table> block (header mismatch on a non-standard issuer
--     layout).
--   * Issuer files DEF 14A as a notice-only statement (e.g. annual
--     meeting only ratifies auditor; no governance changes; some
--     small-cap issuers omit the ownership table when there's no
--     5% holder and only a sole executive).
--   * Persistent 404 on the primary doc.

CREATE TABLE IF NOT EXISTS def14a_ingest_log (
    accession_number   TEXT PRIMARY KEY,
    issuer_cik         TEXT NOT NULL,
    status             TEXT NOT NULL
        CHECK (status IN ('success', 'partial', 'failed')),
    rows_inserted      INTEGER NOT NULL DEFAULT 0,
    rows_skipped       INTEGER NOT NULL DEFAULT 0,
    error              TEXT,
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_def14a_ingest_log_issuer
    ON def14a_ingest_log (issuer_cik, fetched_at DESC);
