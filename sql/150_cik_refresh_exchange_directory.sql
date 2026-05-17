-- Migration 150: cik_refresh_exchange_directory — parsed snapshot of
-- company_tickers_exchange.json (G8, Phase 2 PR 4 of US-ETL plan).
--
-- Populated by daily_cik_refresh (Stage 7 sibling enrichment, mirrors
-- Stage 6 cik_refresh_mf_directory pattern at sql/149:278-290).
--
-- Ticker-grain. The SEC payload emits MULTIPLE rows per CIK for share-
-- class siblings (GOOG/GOOGL), preferred-series tickers (BAC has 17
-- variants), and ADR + OTC siblings (BABA / BABAF / BBAAY). PK MUST be
-- (cik, ticker) to preserve every (ticker, exchange) mapping the
-- payload carries. Empirical: 2026-05-17 live payload has 7,996 unique
-- CIKs across 10,353 rows; 1,446 CIKs have multiple ticker variants.
--
-- Snapshot semantics: "observed-ever". UPSERT advances last_seen on
-- every observed row; rows SEC drops from the payload remain in the
-- table with an older last_seen. Consumers needing a freshness gate
-- filter on last_seen >= cutoff. No DELETE / mark-stale in v1 — add
-- when a consumer needs strict authority over the live cohort.

BEGIN;

CREATE TABLE IF NOT EXISTS cik_refresh_exchange_directory (
    -- 10-digit zero-padded canonical form; CHECK enforces the
    -- invariant at the DB level (mirrors cik_raw_documents at
    -- sql/109:42-44) so a direct-SQL writer cannot bypass the
    -- application-layer normaliser and silently split the cache.
    cik          TEXT NOT NULL
        CHECK (cik ~ '^[0-9]{10}$'),
    ticker       TEXT NOT NULL,
    name         TEXT,
    -- SEC's exchange enum — stored verbatim. Observed values in live
    -- payload: 'Nasdaq', 'NYSE', 'OTC', 'CBOE', NULL. Nullable: 215
    -- rows in the 2026-05-17 sample emit no exchange. No CHECK
    -- constraint so a new SEC enum value lands without a migration.
    exchange     TEXT,
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (cik, ticker)
);

COMMENT ON TABLE cik_refresh_exchange_directory IS
    'Parsed snapshot of company_tickers_exchange.json keyed by (cik, ticker). Populated by daily_cik_refresh (Stage 7 sibling enrichment, G8). Ticker-grain — single CIK may have multiple rows (share-class siblings, preferred-series tickers, ADR+OTC siblings). Observed-ever semantics: last_seen advances on UPSERT; rows SEC drops remain.';

-- Per-CIK rollup lookup for future operator-visible "list all tickers
-- for this issuer" reads. Composite PK already covers (cik) prefix
-- queries via the index scan but a dedicated single-column index
-- keeps unrelated query plans honest under EXPLAIN.
CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_cik
    ON cik_refresh_exchange_directory (cik);

-- Exchange-bucket reporting / classification reads.
CREATE INDEX IF NOT EXISTS idx_cik_refresh_exchange_directory_exchange
    ON cik_refresh_exchange_directory (exchange);

COMMIT;
