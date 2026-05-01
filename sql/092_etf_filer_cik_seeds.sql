-- 092_etf_filer_cik_seeds.sql
--
-- Issue #730 PR 3 — operator-curated set of CIKs known to be ETF
-- issuers. The 13F-HR ingester (#730 PR 2) cross-references each
-- discovered filer against this list and writes ``'ETF'`` to
-- ``institutional_filers.filer_type``; CIKs not on the list default
-- to ``'INV'`` (general institutional manager) at write time.
--
-- Why a separate table from ``institutional_filer_seeds``:
--   * The ETF list is much larger (~3,000 US ETF issuers vs ~100-200
--     top filers we actively ingest). Many ETF-issuer CIKs are NOT
--     in our active-ingest seed list — but if a 13F-HR filer is
--     also an ETF issuer (e.g. an ETF sub-adviser that files its
--     own 13F), the type should still be 'ETF', not 'INV'.
--   * The two seed lists are managed under different cadences. The
--     ingest seeds change as the operator picks new filers; the ETF
--     list refreshes on a quarterly cadence when SEC publishes the
--     official ETF/RIC registrant list.
--
-- Schema mirrors institutional_filer_seeds for the operator-side
-- admin pattern (cik PK, label, active, notes). Future work:
-- ingest the SEC's official RIC list to auto-populate this table —
-- that's #730 PR 4 stretch / its own follow-up depending on shape.

CREATE TABLE IF NOT EXISTS etf_filer_cik_seeds (
    cik         TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    notes       TEXT
);

-- No additional index — ``classify_filer_type`` is a point-lookup
-- on ``cik`` and the PK index already covers it. Adding a partial
-- index on ``(cik) WHERE active = TRUE`` is dead weight (the
-- planner picks the PK index for the eq-predicate path and ignores
-- a smaller-but-redundant secondary).
