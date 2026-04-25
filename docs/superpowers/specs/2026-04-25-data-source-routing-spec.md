# Data-source routing — purge orphans, gate panels, map exchanges

Date: 2026-04-25
Author: Luke / Claude
Status: Draft v3 (post-Codex round 2; pending operator sign-off)

## Goal

Eliminate cross-source data leaks on the instrument page (BTC
showing SEC filings; LRC inheriting "no SEC profile pending"
copy from a never-linked CIK). Make data sourcing follow the
asset class / exchange of each instrument:

- US equities → SEC EDGAR (already wired).
- Crypto → no SEC, ever.
- Non-US equities → no SEC; per-region source TBD (Companies
  House for UK is in scope as a follow-up; EU / Asia are
  research tickets, not code in this spec).

Plus the prerequisite that makes per-region routing possible: a
proper exchange-id → exchange-name / country / asset-class
mapping. We currently store opaque numeric exchange ids and have
no semantic interpretation, so the router cannot reason about
what an instrument is.

## Why

Operator screenshot (2026-04-25): BTC instrument page renders
SEC content + a "filings" tab with 63 filings linked to it.
BTC is a crypto coin with no SEC CIK. PR #496 cleaned the
bogus CIK from `external_identifiers` (47 crypto instruments
purged) but every SEC fact table that keys on `instrument_id`
without checking `external_identifiers` still has the orphan
rows — `filing_events`, `financial_periods`,
`dividend_history`, `insider_filings`, `eight_k_filings`,
`instrument_business_summary_sections`. Database audit confirms
~32,500 orphan filings and ~5,200 orphan SEC fact rows on the
47 crypto instruments.

Frontend renders these panels from those tables directly,
without consulting `external_identifiers`. Result: SEC content
on a non-SEC instrument.

Operator's broader frame:

- Want each region routed to its appropriate data source.
- Crypto must never reach for SEC.
- Need a compiled list of eToro exchanges with country /
  asset class so the router can do its job.

## Non-goals (locked)

- Adding new data sources for crypto / EU / Asia. This spec
  files research tickets only — actual implementations land
  in their own PRs once we know the source per region.
- Changing the SEC ingester's symbol→CIK mapping. PR #496
  already scoped `daily_cik_refresh` to US exchanges
  (`'2','4','5','6','7','19','20'` per `app/workers/scheduler.py:1123`).
  This spec assumes that filter is correct and only cleans
  up + prevents downstream consequences.
- Backfilling identity fields (`country`, `currency`) on the
  `instruments` table from a non-eToro source. The exchanges
  metadata endpoint is the only canonical source per the
  settled "eToro = source of truth for tradable universe"
  decision (`docs/settled-decisions.md`).
- Watchlist live prices, day-change arrows, sparklines (out of
  scope for the visibility-driven plan; same here).

## Current state (anchors)

### SEC ingester scope

- `app/workers/scheduler.py:1123` —
  `AND exchange IN ('2', '4', '5', '6', '7', '19', '20')`
  filter on `daily_cik_refresh`. Crypto (exchange `8`) +
  unknown / non-listed are excluded since #496.
- `sql/065_purge_bogus_crypto_sec_ciks.sql` — DELETE on
  `external_identifiers` + `instrument_sec_profile` for
  crypto. Did NOT touch downstream SEC tables.
- `app/services/filings.py:60` —
  `_resolve_identifier(conn, instrument_id, 'sec', 'cik')`.
  When `external_identifiers` has no row, `refresh_filings`
  skips. Correct gate, but only enforced going forward —
  pre-#496 ingest already wrote rows.

### Orphan SEC tables (audit 2026-04-25)

```text
filing_events                            32500
financial_periods                         3793
dividend_history                          1336
insider_filings                            407
eight_k_filings                             43
instrument_business_summary_sections        19
instrument_sec_profile                       0  (cleaned in #496)
```

Cluster-wide check:

```sql
SELECT COUNT(DISTINCT fe.instrument_id)
FROM filing_events fe
WHERE NOT EXISTS (
    SELECT 1 FROM external_identifiers ei
    WHERE ei.instrument_id = fe.instrument_id
      AND ei.provider = 'sec'
      AND ei.identifier_type = 'cik'
);
-- 47 instruments
```

### FK chain

- `insider_filings` (parent) ← CASCADE → `insider_filers`,
  `insider_transaction_footnotes`, `insider_transactions` (per
  `sql/057_insider_transactions_richness.sql:147,201,419`).
- `eight_k_filings` (parent) ← CASCADE → `eight_k_items`,
  `eight_k_exhibits` (per `sql/061_eight_k_structured_events.sql:98,145`).
- `filing_events` (parent) ← CASCADE → `filing_documents` (per
  `sql/062_filing_documents.sql:36`).

So a delete on the parents cascades to their children. We do
NOT need to write per-child DELETEs; PostgreSQL handles them.
We DO need to verify every cascade chain is in place — a
missing CASCADE would orphan children.

### Frontend SEC panels

- `frontend/src/components/instrument/SecProfilePanel.tsx`
- `frontend/src/components/instrument/InsiderActivityPanel.tsx`
- `frontend/src/components/instrument/DividendsPanel.tsx`
- `frontend/src/components/instrument/ResearchTab.tsx` —
  composes financials, key stats; reads from the summary
  endpoint already gated on `_has_sec_cik`.
- `frontend/src/pages/InstrumentPage.tsx` — Filings tab,
  reads `/instruments/{symbol}/filings` (TODO: confirm
  endpoint, may inherit gating).
- `frontend/src/components/instrument/RightRail.tsx` — drives
  per-instrument tab visibility.

### Exchanges endpoint (eToro)

- Public API: `GET /api/v1/market-data/exchanges` returns
  `{exchangeID, exchangeDescription}`. Per
  https://api-portal.etoro.com/api-reference/market-data/retrieves-a-list-of-exchanges-supported-by-the-platform-along-with-basic-descriptive-data.md.
- Currently never called by eBull. No `exchanges` table.

## Architecture invariants (post-spec)

1. **No SEC content for instruments without a current SEC CIK
   in `external_identifiers`.** Every API endpoint that returns
   SEC-derived data joins or filters on the CIK link. Frontend
   panels gate on a single per-instrument hint
   (`has_sec_cik`) included in the summary response or a
   sibling endpoint.
2. **DB is the boundary, not the API.** Orphan rows in
   `filing_events` / `financial_periods` etc. without a current
   CIK are deleted, not just hidden by the API. A future code
   regression that forgets the API gate cannot leak data that
   no longer exists at the row level.
3. **Exchange semantics live in one place.** A new `exchanges`
   table (`exchange_id`, `description`, `country`,
   `asset_class`) is the single source the router reads from.
   No more hard-coded id lists in scheduler / SQL — the SEC
   filter migrates to a join on `asset_class = 'us_equity'`.
4. **Per-region source decisions are out of scope here.** The
   exchanges table just enables the routing; *where* to fetch
   data for each region is a separate research ticket.

## PRs (sequenced)

### PR 1 — Purge orphan SEC data (migration 066)

Branch: `fix/<n>-purge-orphan-sec-data`

Single SQL migration. For every SEC-derived table keyed on
`instrument_id`, DELETE rows where the instrument lacks a
current `(provider='sec', identifier_type='cik')` row in
`external_identifiers`. CASCADE handles children automatically.

**Tables in scope — SEC-only base tables** (delete predicate:
instrument has no `(provider='sec', identifier_type='cik')`
row in `external_identifiers`). Parent → cascaded children:

- `filing_events` → `filing_documents` (per
  `sql/062_filing_documents.sql:36`)
- `insider_filings` → `insider_filers`, `insider_transactions`,
  `insider_transaction_footnotes` (per
  `sql/057_insider_transactions_richness.sql:147,201,419`)
- `eight_k_filings` → `eight_k_items`, `eight_k_exhibits` (per
  `sql/061_eight_k_structured_events.sql:98,145`)
- `instrument_business_summary_sections`
- `instrument_business_summary` (per
  `sql/055_instrument_business_summary.sql:21`)
- `dividend_events` (per `sql/054_dividend_events.sql:25`)
- `financial_facts_raw` (per
  `sql/032_financial_data_enrichment_p1.sql:31`) — SEC-only
  per the schema comment ("wide period rows per source"
  applies to `financial_periods_raw`, not this one which has
  no `source` column; Codex round 2 finding 2).
- `instrument_sec_profile` (already cleaned in #496; defensive
  re-purge so a future re-run is idempotent)

**Views — NO direct DELETE** (Codex round 2 finding 1; round 3
correction). The migration must NOT issue DELETE against
views; PostgreSQL will reject. Both dividend views derive from
`financial_periods` — verified at
`sql/050_dividend_history_views.sql:46-64` (`FROM financial_periods fp`)
and `sql/050_dividend_history_views.sql:76-145` (same source
across the recent_quarters / ttm / latest / streaks CTEs):

- `dividend_history` is a VIEW over `financial_periods`
  filtered to `period_type IN ('Q1','Q2','Q3','Q4')` with
  non-zero dps / dividends_paid.
- `instrument_dividend_summary` is a VIEW over
  `financial_periods` (NOT `dividend_events`) computing TTM,
  latest, streak aggregates.

So the views recompute correctly only when the multi-source
predicate purges the SEC-sourced rows from `financial_periods`
(see "Tables in scope — multi-source" below). Purging
`dividend_events` alone does not affect these views — that's
a separate base table consumed by the calendar-style endpoints
(`get_upcoming_dividends`, `app/services/dividends.py:177`),
not the historical/summary views.

**Tables in scope — multi-source, predicate filters on
`source` column** (these tables explicitly carry
`source IN ('sec', 'fmp', …)` per their schema, so a blanket
"no SEC CIK" delete would purge legitimate non-US / FMP data;
predicate is `source IN ('sec', 'sec_xbrl',
'sec_companyfacts') AND <no current SEC CIK>`):

- `financial_periods_raw` (`source TEXT NOT NULL` at
  `sql/032_financial_data_enrichment_p1.sql:119`)
- `financial_periods` (`source TEXT NOT NULL` at
  `sql/032_financial_data_enrichment_p1.sql:188`)

**Out of PR 1 scope — needs a separate rebuild path**:

- `fundamentals_snapshot` (`sql/001_init.sql:29`) is a
  cross-source cache without a `source` column — neither the
  blanket "no SEC CIK" predicate nor a `source`-filtered
  predicate applies safely (Codex round 2 finding 2). Defer
  cleanup to a follow-up ticket: either add a `source`
  column in a new migration, or trigger a recompute pass
  after PR 1 lands so stale cache rows on no-CIK instruments
  get rebuilt from authoritative state. Filed as Risks
  section item.

**Tables to investigate during PR 1 implementation**:

- `sec_facts_concept_catalog` — name suggests SEC-only;
  verify schema before adding to scope.
- `sec_entity_change_log` — same.

**Required investigation before writing the migration**:

- Grep `sql/` for every `REFERENCES instruments` and every
  occurrence of `provider TEXT` / `source TEXT`. Enumerate
  every candidate table; classify each as SEC-only or
  multi-source. A table that mixes sources (carries a
  `source` / `provider` column) gets the multi-source
  predicate, not the blanket "no SEC CIK" predicate.
- Audit query results published in the PR description so the
  reviewer can verify the row counts vs the table list.

**Acceptance**:

- Crypto instruments (exchange `8`): zero rows in any of the
  above tables.
- Non-crypto instruments WITH a current SEC CIK: row counts
  unchanged.
- Migration is idempotent — re-running on a clean DB is a
  zero-row delete.
- Regression test in `tests/` migrates a fixture DB with a
  bogus pre-#496 row, runs the migration, asserts the row is
  gone and a legitimately-linked row stays.

### PR 2 — Gate SEC + filings panels at API and frontend

Branch: `fix/<n>-gate-sec-panels`

**Backend** (Codex round 1 finding 3 — multiple live handlers
do NOT currently gate on `_has_sec_cik`; they happily return
data joined to orphan rows pre-PR-1):

- `app/api/instruments.py:773` `get_instrument_8k_filings` —
  add `_has_sec_cik` gate; 404 when missing.
- `app/api/instruments.py:1080` `get_instrument_dividends` —
  add gate.
- `app/api/instruments.py:1197` `get_instrument_insider_summary` —
  add gate.
- `app/api/instruments.py:1297` `get_instrument_insider_transactions` —
  add gate.
- `app/api/instruments.py:1375` `_has_sec_cik` — confirm helper
  is reused by all the above and the existing summary endpoint.

**Two separate gates** (Codex round 1 finding 4 — `has_sec_cik`
is too narrow for source-agnostic surfaces):

1. `has_sec_cik: bool` — for SEC-specific panels:
   `SecProfilePanel`, `InsiderActivityPanel`, `DividendsPanel`
   (today's source IS SEC-only), the SEC business-summary
   section.
2. `has_filings_coverage: bool` — for **provider-agnostic**
   filings surfaces. Today this is `EXISTS(filing_events ...)
   for the instrument` since SEC is the only provider, but the
   field name is forward-compatible with Companies House
   filings landing later. Used by:
   - The Filings tab in `InstrumentPage.tsx:360`
   - The right-rail "recent filings" widget in
     `RightRail.tsx:64`

   Both surfaces are already phrased "SEC EDGAR or Companies
   House" in the UI copy; gating them on a SEC-specific field
   would bake in a follow-up the moment UK filings ship.

**Backend response shape** — extend
`InstrumentSummary.source` (or add a sibling `coverage` block;
operator's call). Two new boolean fields exposed at the API
boundary; frontend reads exactly those.

**Frontend** — each of the four SEC-specific panels gates on
`has_sec_cik`; the Filings tab + right-rail filings widget
gate on `has_filings_coverage`.

**Acceptance**:

- BTC / LRC / ETH instrument pages render: identity, live
  price (post #504), placeholder for "no coverage". No
  SEC-tagged tabs and no Filings tab (provider-agnostic gate
  also off because `filing_events` is empty post-PR-1).
- AAPL: unchanged — SEC panels render, Filings tab renders.
- Per-handler API regression tests asserting 404 / null on
  no-CIK instruments.
- Per-panel widget tests asserting hidden state when each
  gate is off.

### PR 3 — Seed exchanges metadata

Branch: `feat/<n>-exchanges-metadata`

New SQL table:

```sql
CREATE TABLE exchanges (
    exchange_id   TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    -- Derived columns; populated by the seed job from
    -- description heuristics + manual override (see Risks).
    country       TEXT,
    asset_class   TEXT,  -- 'us_equity' | 'crypto' | 'eu_equity' | …
    seeded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

New job (`exchanges_metadata_refresh`, weekly cadence — eToro
adds exchanges rarely): calls
`GET /api/v1/market-data/exchanges`, upserts every row.

Initial `country` / `asset_class` derivation: a manual seed
file checked into `sql/067_exchanges_seed.sql` based on the
descriptions returned. The seed file is the operator's
declaration of truth; the job fills in `description` updates
but does NOT touch `country` / `asset_class` once seeded
unless explicitly cleared. Drift in the operator-curated
mapping is a deliberate operator-gated change.

Update `app/workers/scheduler.py:1123` filter from
`exchange IN ('2','4','5','6','7','19','20')` to
`exchange IN (SELECT exchange_id FROM exchanges WHERE asset_class = 'us_equity')`.
Removes the magic-numbers list from the scheduler in favour
of the table.

**Acceptance**:

- `exchanges` populated with every id eToro returns (~35-40
  rows on dev).
- Seed file pins `country` + `asset_class` for every known
  id; future-dev's seed-list contains a sentinel test that
  fails if an unknown exchange id appears in `instruments`.
- `daily_cik_refresh` query reads from the table; smoke run
  lists the same instrument set as the prior magic-numbers
  filter.

### Tickets to file (research, no code in this spec)

Each as its own GitHub issue, labelled `research`:

1. **Crypto data source decision** — CoinGecko (free, has
   metadata + history), DefiLlama (DeFi only), or operator's
   pick. What do we need beyond live price? News, "fundamentals"
   (supply, on-chain metrics), or just price?
2. **UK Companies House seeding** — interface exists from
   #15 but not seeded. What's the path to populate
   `external_identifiers` for UK-listed instruments?
3. **EU equities filings strategy** — ESMA, BaFin, AMF,
   national portals. Which exchanges does eToro list in EU?
   What's the realistic free / official source per country?
4. **Asia equities filings strategy** — TSE / HKEX / SGX
   disclosure portals. Same questions as EU.

Open these tickets after Codex sign-off on this spec; do not
block PR 1-3 on them.

## Resolved findings

### Round 1

| # | Finding | Resolution in v2 |
|---|---------|------------------|
| 1 | PR 1 missed `financial_facts_raw`, `instrument_business_summary`, `dividend_events` | Added to scope list (SEC-only group). |
| 2 | PR 1 listed multi-source tables (`financial_periods`, `_raw`, `fundamentals_snapshot`) under blanket "no SEC CIK" predicate — would purge legitimate non-SEC data | Split scope into "SEC-only" + "multi-source"; multi-source predicate filters on `source IN ('sec', ...)` AND no SEC CIK. |
| 3 | PR 2 understated backend work: 4 SEC handlers don't gate on `_has_sec_cik` | Enumerated them by file:line; explicit gating tasks added. |
| 4 | `has_sec_cik` too narrow for source-agnostic Filings tab + RightRail | Two gates: `has_sec_cik` (SEC-specific panels) + `has_filings_coverage` (provider-agnostic filings surfaces). |

### Round 2

| # | Finding | Resolution in v3 |
|---|---------|------------------|
| 1 | `dividend_history` + `instrument_dividend_summary` are VIEWS (per `sql/050_dividend_history_views.sql:46,76`); migration cannot DELETE them | Moved to a dedicated "Views — NO direct DELETE" section. |
| 2 | `financial_facts_raw` has no `source` column → SEC-only, mis-grouped under multi-source. `fundamentals_snapshot` also has no `source` column → can't use either predicate | `financial_facts_raw` moved to SEC-only group. `fundamentals_snapshot` declared out of PR 1 scope; deferred to follow-up that adds a `source` column or triggers a recompute. Logged in Risks. |

### Round 3

| # | Finding | Resolution in v3 |
|---|---------|------------------|
| 1 | "Views — NO direct DELETE" section pinned the wrong lineage — `dividend_history` and `instrument_dividend_summary` derive from `financial_periods`, not `dividend_events` | Section corrected with verified `FROM financial_periods` cite at `sql/050_dividend_history_views.sql:46-64,76-145`. The views recompute via the multi-source predicate purging SEC-sourced `financial_periods` rows. `dividend_events` is a separate base table feeding the upcoming-dividends calendar (`get_upcoming_dividends`, `app/services/dividends.py:177`), not the views. |

## Risks and mitigations

1. **Cascade gaps**. If a child table on a SEC-derived parent
   lacks `ON DELETE CASCADE`, the migration will fail or leave
   orphans of orphans. PR 1's investigation step enumerates
   every FK before writing the DELETE; a regression test on a
   fixture DB walks the whole tree post-purge.
2. **Mixed-source tables**. `dividend_history` could in theory
   hold non-US dividends in future. Today every row is SEC-derived
   (XBRL `us-gaap:DividendsCommonStockCash`). The migration's
   predicate is "no SEC CIK on the instrument" — so a future
   row sourced from elsewhere would survive only if its
   instrument carries some other identifier. We add a comment
   on the migration noting the predicate's assumption.
3. **`exchanges` derivation drift**. Operator-curated
   `country` / `asset_class` columns can drift from eToro
   reality if eToro re-uses ids. Mitigation: PR 3's seed file
   carries an explicit `last_audited_at` timestamp + the
   sentinel test that fails on unknown ids.
4. **Frontend gate regression**. A new SEC-derived panel
   added later might forget the `has_sec_cik` gate. Mitigation:
   the gate field is part of the standard `InstrumentSummary`
   response — the panel can't render without consulting the
   summary anyway.
5. **PR 1 over-deletes** on a future schema change. We assert
   the specific predicate ("no SEC CIK") + scope to known
   tables (no DROP TABLE, no DELETE without WHERE). Risk is
   bounded.

## Migration / rollback

- PR 1: `sql/066_purge_orphan_sec_data.sql` is forward-only
  (delete). Rollback = restore from backup before applying.
  No code changes.
- PR 2: pure additive on the API summary; frontend gate is
  conditional render. Rollback = revert the squash commit.
- PR 3: new table + seed + scheduler filter swap. Rollback =
  drop the table + revert the scheduler filter to the magic
  numbers list.

## Success criteria (overall)

- BTC / LRC / ETH instrument page: no SEC tabs, no SEC
  content, just price + identity + (eventually) crypto-source
  tabs once the research ticket lands.
- `daily_cik_refresh` reads its scope from `exchanges`,
  not a hardcoded list.
- Operator can audit every exchange id eToro returns and
  decide where its instruments should source data.
- No row in `filing_events` etc. without a current SEC CIK.
