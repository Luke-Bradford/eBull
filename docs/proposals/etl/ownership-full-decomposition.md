# Ownership card — full decomposition + provenance/current/history triad

**Status:** draft v2, post-Codex review.
**Parent epic:** #788 (ownership card production-trustworthy).
**Supersedes:** none — extends 2026-05-03-ownership-tier0-and-cik-history-design.md.
**Author:** Claude (Opus 4.7) on 2026-05-04 after operator audit found the existing rollup was reporting AAPL institutional ownership at 5.94% vs ground-truth 50-65% (10× under-count).

## Revision history

- **v1** — initial spec.
- **v2** — Codex spec review (2026-05-04) found 2 critical + 3 high + 3 medium issues. Fixes:
  - Phase 0 outcome was "AAPL 5.94% → 50%+ without new ingest"; that claim was wrong because RC-2 (only 14 filer seeds) caps the institutional total. Phase 0 reframed as **partial recovery** of already-ingested rows; trusted institutional totals require Phase 2.
  - Dedup chain `Form 4 > 13D/G > DEF 14A > 13F` was conflating direct vs beneficial ownership. Replaced with **two-axis dedup**: a `source` axis (priority chain) and an `ownership_nature` axis (`direct | indirect | beneficial | voting | economic`). Dedup only within compatible axes — Cohen's 13D/A `beneficial` and his Form 4 `direct` are different facts and both render.
  - Target decomposition mixed incompatible share bases. Each category now declares a `denominator_basis` (`shares_outstanding`, `shares_outstanding_plus_unvested`, `registration_subset_of_outstanding`, `borrow_artifact`) and category-eligibility rules are stated up-front. Treasury and unvested RSUs ship as memo lines, not pie wedges. DRS is a registration overlay on existing categories, not a separate owner class.
  - `_latest` natural key was `(instrument_id, source_holder_id)` — doesn't fit issuer-level categories (treasury / DRS / restricted have no holder). Each category now declares its own natural key.
  - History design "snapshot only on overwrite" missed initial observations. Replaced with **immutable observations table** (one row per ingested filing fact, never updated) plus a **materialized `_current` snapshot** rebuilt from observations. History queries hit observations directly.
  - Bitemporal naming made imprecise: `valid_from / valid_to` were system-time, `period_of_report` was valid-time. Renamed to `known_from / known_to` (system) and `period_start / period_end` (valid).
  - Provenance block was SEC-shaped (`source_accession NOT NULL`, `edgar_url`). FINRA, derived rows, and per-issuer 8-K updates don't fit. Generalised to `source_document_id` / `source_url` with `source_accession` nullable.
  - DoD said "within ±5% of gurufocus / marketbeat". Contradicts settled-decisions free-regulated-source-only (#532). Acceptance now grounded on SEC-derived fixture cases; gurufocus / marketbeat are smoke-comparison only.
  - Coverage banner gets four new explicit states: `complete_source_universe`, `partial_identifier_coverage`, `stale_category`, `issuer_does_not_disclose`.
  - EdgarTools hybrid posture confirmed, with the additional requirement: **pin versions + add golden-file parser tests** so library churn surfaces in CI.
  - History tables: do not per-issuer shard first. Partition by time/category. BRIN index decision deferred until measured against realistic N-PORT fixtures.
  - DRS v1 scope: only store an `issuer_discloses_drs` flag + the disclosure narrative; defer structured extraction to v2.

## Problem statement

The shipped ownership card under-counts institutional ownership by an
order of magnitude on every name we audited:

| Symbol | eBull reports | Trusted sources (gurufocus / marketbeat / wallstreetzen) | Gap |
|---|---:|---:|---:|
| AAPL inst | 5.94% (7 filers) | 50-65% (Vanguard 9.25%, BlackRock 6-8%) | 10× under |
| AAPL retail residual | 94% | ~35% | wildly inflated |
| GME inst | 3.09% (4 filers) | 35-37% | 11× under |
| GME insider | 8.76% | 10.74% (Cohen beneficial 16.77%) | half |
| GME DRS | not tracked | ~25% (~75M shares historically) | entire category missing |
| GME retail | 88% | ~42% | wildly inflated |

The "Public / unattributed" residual in the chart hides at least
seven distinct categories that we either fail to ingest or fail to
resolve to instruments after ingest. The chart is therefore
operator-untrustworthy on its core question: *who actually owns this
issuer?*

## Three states the operator needs (the design constraint)

For every figure that lands on the chart, the database must answer:

1. **Provenance** — *where + when did this number come from*. Source
   filing accession, source field/concept, filed_at, period_of_report,
   ingest run id, source URL. Operator must be able to click any
   wedge / table cell and see the originating SEC filing.
2. **Current** — *what is the latest, deduped, non-stale value right
   now*. No mixing of stale 13F (45-135d lag) with fresh Form 4 (0-2d
   lag) without an explicit "as of" age delta per category. No
   double-counting across filing channels. No half-baked partial
   states surfaced as authoritative.
3. **History** — *how have these figures shifted over time*. Per
   category, per holder, per instrument: month-over-month or
   filing-over-filing series so the operator can see "Vanguard added
   2M AAPL last quarter", "GME DRS climbed 5M shares between Q1 and
   Q2", or "insider net-selling accelerated after the strategy
   change".

Today's data model partially supports (1) (accession + filed_at on
most tables) and (2) (single-snapshot rollup endpoint), but (3) is
ad-hoc — there is no purpose-built per-category time-series surface.

## Root causes uncovered in the audit

### RC-1 — CUSIP resolver race

`unresolved_13f_cusips` has 12,312 rows. **119 of them already have
matching `external_identifiers`** (AAPL, BAC, JPM, KO, WMT, V, DIS,
UNH, MRK, ADBE, etc.) — every Fortune-100 name. They sit unresolved
because the 13F holdings ingest ran *before* the CUSIP backfill landed
and no resolver sweep was run after.

Timing trace for AAPL:
- 13F holdings parsed: `2026-05-02 22:35-22:38`
- AAPL CUSIP added to `external_identifiers`: `2026-05-02 22:59:29`
- No re-sweep since. 325 observations stranded.

### RC-2 — Universe seed cap

`institutional_filers` has **14 rows**. Reality: AAPL is held by
~6,500 13F-HR filers each quarter; SPY by ~3,000. Even after fixing
RC-1, the institutional slice tops out at "what these 14 filers
report". Vanguard last filing on file: `2026-01-29` (4 months stale).
BlackRock last: `2024-06-30` (18 months stale).

### RC-3 — Universe extids cap

128 of 12,379 instruments (1.0%) have CUSIPs in
`external_identifiers`. Even with RC-1 + RC-2 fixed, 99% of our
tradable universe can't accept incoming 13F rows because the
issuer-side CUSIP is unknown.

### RC-4 — Categories never ingested

| Category | Source | DB state today |
|---|---|---|
| Treasury shares | XBRL `TreasuryStockShares` concept | NULL across 4,591 rows in `instrument_share_count_latest` |
| DEF 14A consolidated bene table | DEF 14A annual proxy | 0 rows in `def14a_beneficial_holdings` (rewash spec #827 registered, never run) |
| 13D/G blockholders surfaced | rollup query | GME has 2 rows in `blockholder_filings`, rollup endpoint reports 0 — query bug or filter regression |
| DRS / direct-registered | 10-K disclosures (issuer-specific text) | not ingested |
| Mutual fund <$100M AUM | SEC N-PORT / N-CSR | not ingested |
| Restricted / locked-up RSUs | 10-K Note 14 + DEF 14A vesting tables | not ingested |
| Short interest | FINRA bimonthly CSV / API | not ingested |

## Target chart decomposition

Each category declares its `denominator_basis` so totals never mix
incompatible share bases. Three bases:

- **`shares_outstanding`** — issued + tradeable shares per XBRL DEI
  `EntityCommonStockSharesOutstanding`. Default basis. Denominator
  for the pie chart.
- **`shares_outstanding_plus_unvested`** — outstanding + unvested
  RSUs / restricted awards. RSUs are NOT outstanding until they
  vest, but they will dilute the operator's holding. Memo line, not
  a pie wedge.
- **`registration_subset_of_outstanding`** — a re-classification of
  outstanding shares by registration form (DRS book vs DTC street
  name). NOT additive on top of the pie. Renders as an overlay
  badge per holder row when the issuer discloses it.
- **`borrow_artifact`** — short interest. Sold-but-borrowed shares
  are still owned by the lender; counting them as a category would
  double-count. Memo overlay only.

```
PIE WEDGES (sum to ≤ shares_outstanding):
├─ insiders + officers + directors    (Form 3/4/5 + DEF 14A bene table) [direct, indirect]
├─ blockholders >5% beneficial        (13D/G)                            [beneficial]
├─ institutional 13F-HR (≥$100M AUM)  (full 13F filer universe)          [voting/economic]
├─ mutual funds <$100M AUM            (N-PORT / N-CSR)                   [voting/economic]
├─ company treasury                   (XBRL TreasuryStockShares)         [issuer-held — NOT in shares_outstanding under SEC convention; render as separate slice OR memo line, decided per-issuer by what the XBRL filing says]
└─ retail free float                  (computed residual)                [unclassified]

MEMO OVERLAYS (NOT pie wedges):
- ESOP plan totals                    (DEF 14A fund tables)              [usually duplicates institutional N-PORT — render as overlay tag on the fund row, NOT a separate slice]
- DRS / direct-registered             (10-K Note when disclosed)         [registration-subset of outstanding — overlay badge per holder, NOT a category]
- unvested RSU / restricted           (10-K Note 14 + DEF 14A vesting)   [pre-issuance, dilution memo]
- short interest                      (FINRA bimonthly + RegSHO daily)   [borrow artifact]
```

### Dedup model — TWO axes, not one chain

The v1 single-chain `Form 4 > 13D/G > ...` was wrong because Form 4
reports DIRECT holdings while 13D/G reports BENEFICIAL ownership
(direct + indirect via funds, family trusts, control entities).
Cohen's GME case: Form 4 says ~38M direct; his 13D/A says ~75M
beneficial (RC Ventures + family). These are DIFFERENT FACTS, both
true, both worth surfacing.

Replace single chain with two axes:

**Axis 1 — `source` priority chain** (resolves which filing wins
when two filings of the SAME nature describe the same holder):

```
Form 4 > Form 3 > 13D/G > DEF 14A bene > 13F-HR > N-PORT / N-CSR
```

**Axis 2 — `ownership_nature`** (a holder may have multiple
co-existing rows of different natures):

```
direct       — physical / record-name holdings (Form 4, Form 3)
indirect     — held via family trusts, control entities (Form 4 indirect, DEF 14A bene)
beneficial   — voting + investment power per Rule 13d-3 (13D/G)
voting       — voting authority disclosed (13F-HR voting_authority)
economic     — investment authority / portfolio holding (13F-HR, N-PORT)
```

**Dedup rule:** dedup ONLY within compatible natures. Cohen's
`(beneficial, 13d, 75M)` and `(direct, form4, 38M)` BOTH render —
the 13D number flows into the blockholders slice (beneficial), the
Form 4 number flows into the insiders slice (direct).

Cross-category overlap rules (revised):
- Insiders + blockholders: a CIK may produce one `direct` row
  (insiders slice) AND one `beneficial` row (blockholders slice).
  Both render. Reconciliation note in tooltip when same CIK.
- Blockholders + institutions: Berkshire's 13F (`economic`) and a
  hypothetical 13D (`beneficial`) — both render with explicit
  nature labels.
- ESOP funds: render as a TAG on the institutional N-PORT row when
  the DEF 14A names that fund as the issuer's plan trustee. Not a
  separate slice.

## Source matrix (every category → primary + backup)

| Category | Primary source | Backup source | Update freq | License |
|---|---|---|---|---|
| Insiders | SEC Form 4 XML | SEC Form 3 (initial), Form 5 (annual) | event-driven (≤2d after txn) | public |
| Insiders consolidated | SEC DEF 14A bene table | — | annual | public |
| Blockholders | SEC 13D/G | — | event-driven (≤10d after threshold cross) | public |
| Institutional ≥$100M | SEC 13F-HR XML | — | quarterly (45d after Q-end) | public |
| Mutual funds <$100M | SEC N-PORT XML | SEC N-CSR | monthly (60d after M-end) / semiannual | public |
| Treasury | XBRL `TreasuryStockShares` | 10-K cover narrative fallback | quarterly | public |
| DRS | 10-K Note disclosure (textual) | issuer 8-K updates | annual + ad-hoc | public |
| ESOP | DEF 14A | 10-K Note 14 | annual | public |
| Restricted / RSU | DEF 14A vesting tables + 10-K Note | — | annual | public |
| Short interest (memo) | FINRA Equity Short Interest API | regsho.finra.org daily | bimonthly + daily volume | public |

All sources are free, regulated, and align with the settled-decisions
"free regulated-source-only (#532)" rule. No paid wrappers, no
scraped feeds.

## Build vs adopt — EdgarTools decision

`dgunning/edgartools` (MIT, actively maintained 2026) is the canonical
open-source SEC EDGAR Python library. It covers every form we need:
13F-HR, 13D/G, Form 3/4/5, DEF 14A bene tables + ESOP + RSU, N-PORT,
N-CSR, 10-K, XBRL DEI. Stateless, depends only on `lxml`, `pyarrow`,
`pandas` (we already have pandas in extras). License is MIT.

Two posture options:

**A — adopt as a dependency.** Replace our hand-rolled 13F / 13D/G /
Form 4 parsers with EdgarTools' parsers. Estimated 60-70% of the
ingest service code goes away. Risk: external dependency for
business-critical ingest path, version pin discipline matters,
behaviour changes between releases could silently shift downstream.

**B — adopt as a reference parser.** Keep our own parsers. Use
EdgarTools in a parallel-shadow ingest job that compares its output
to ours per filing — flags divergences for operator review. Lower
delivery risk; we still own the contract; library churn doesn't break
us.

**C — hybrid.** Adopt for the categories we don't have today (N-PORT,
N-CSR, DEF 14A bene table extraction, 10-K Note text scraping),
keep our own for what we already ship and is working (Form 4, 13F
xml, 13D/G).

**Recommendation: C** (proposed for Codex push-back). Rationale:
- shipping-cost: N-PORT alone is ~2,500 fund families × monthly. Building
  our own parser for that would be ~6 weeks of work; using EdgarTools
  is days.
- risk-isolation: we keep ownership of the production ingest paths
  that already work and have invested test coverage.
- migration ramp: if EdgarTools proves reliable on the new
  categories, we revisit "adopt fully" for existing pipelines later.

## Data model design

### Provenance block (uniform, source-neutral)

Every ownership row carries:

```sql
source                  text        not null  -- 'form4'|'form3'|'13d'|'13g'|'def14a'|'13f'|'nport'|'ncsr'|'xbrl_dei'|'10k_note'|'finra_si'|'derived'
source_document_id      text        not null  -- SEC accession, FINRA file_id, or derived synthetic id
source_accession        text                  -- SEC accession when applicable; null for FINRA / derived rows
source_field            text                  -- e.g. XBRL concept, table id within filing
source_url              text                  -- click-through to source document (edgar_url, finra archive url, etc.)
filed_at                timestamptz not null  -- when the source document was published
period_start            date                  -- valid-time start (e.g. quarter start for 13F)
period_end              date        not null  -- valid-time end (period_of_report; the financial period this fact applies to)
known_from              timestamptz not null  -- system-time start (when we first observed this fact)
known_to                timestamptz            -- system-time end (when this fact was superseded; null = current)
ingest_run_id           uuid        not null  -- ties row to a specific batch run for replay
```

`source_accession NOT NULL` was wrong — FINRA and derived rows
don't have one. `source_document_id` is the universal identity;
`source_accession` is SEC-specific metadata. `valid-time` (the
period the fact describes) and `system-time` (when we knew it) are
named separately and explicitly.

### Two-layer storage: immutable observations + materialised current

**Layer 1 — observations** (per-category, append-only, immutable):
`ownership_<category>_observations`. One row per ingested filing
fact. Never updated. Holds the full provenance block. This is the
source of truth for history queries.

**Layer 2 — `_current` snapshot** (per-category, mutable, rebuilt
from observations): `ownership_<category>_current`. One row per
natural key per category. Rebuilt by a deterministic `refresh_<cat>_current(instrument_id)`
function that reads observations and applies the dedup model
(source priority + ownership_nature axes). Rollup endpoint reads
from `_current` for fast chart queries.

This replaces the v1 "snapshot only on overwrite" history scheme,
which would have missed the FIRST observation of any fact. Now
every ingested fact lands in observations regardless of whether it
displaces a prior `_current` row.

### Per-category natural keys

The v1 universal `(instrument_id, source_holder_id)` key didn't fit
issuer-level categories (treasury / DRS / restricted have no
holder). Each category declares its own natural key:

| Category                         | Natural key                                                                  | Notes |
|----------------------------------|------------------------------------------------------------------------------|-------|
| `insiders_observations`          | `(instrument_id, holder_cik, ownership_nature, source, source_document_id)`  | one row per filing per (CIK, nature) |
| `insiders_current`               | `(instrument_id, holder_cik, ownership_nature)`                              | latest per nature for that CIK |
| `blockholders_observations`      | `(instrument_id, reporter_cik, ownership_nature, source, source_document_id)`| 13D/G amendments distinct rows |
| `blockholders_current`           | `(instrument_id, reporter_cik, ownership_nature)`                            | latest amendment per (CIK, nature) |
| `institutions_observations`      | `(instrument_id, filer_cik, period_end, source_document_id)`                 | one row per quarter per filer |
| `institutions_current`           | `(instrument_id, filer_cik)`                                                 | latest filing |
| `funds_observations`             | `(instrument_id, fund_series_id, period_end, source_document_id)`            | one row per N-PORT period per fund series |
| `funds_current`                  | `(instrument_id, fund_series_id)`                                            | latest N-PORT |
| `treasury_observations`          | `(instrument_id, period_end, source_document_id)`                            | one row per filing reporting treasury |
| `treasury_current`               | `(instrument_id)`                                                            | latest |
| `drs_observations`               | `(instrument_id, disclosure_date, source_document_id)`                       | only when issuer discloses |
| `drs_current`                    | `(instrument_id)`                                                            | latest |
| `esop_observations`              | `(instrument_id, plan_name, period_end, source_document_id)`                 | one row per named plan per proxy |
| `esop_current`                   | `(instrument_id, plan_name)`                                                 | latest per plan |
| `restricted_observations`        | `(instrument_id, award_class, period_end, source_document_id)`               | RSU / PSU / option award classes |
| `restricted_current`             | `(instrument_id, award_class)`                                               | latest per award class |
| `short_interest_observations`    | `(instrument_id, settlement_date, source_document_id)`                       | bimonthly settlement snapshots |
| `short_interest_current`         | `(instrument_id)`                                                            | latest settlement |

### History queries

Operator question "show me Vanguard's AAPL position over the last
two years" runs against `institutions_observations`:

```sql
SELECT period_end, shares, market_value_usd, source_url
FROM ownership_institutions_observations
WHERE instrument_id = 1001
  AND filer_cik = '0000102909'
  AND period_end >= '2024-05-04'
ORDER BY period_end ASC;
```

No materialised history table needed — observations IS history.

### Partitioning + indexes

Per Codex push-back, do NOT shard by issuer first. Partition each
`*_observations` table by `period_end` (range, quarterly bucket).
This isolates write hotspots (current quarter) from history scans
and keeps per-partition index sizes manageable.

Indexes per partition:
- btree `(instrument_id, period_end DESC)` — chart timeseries query
- btree `(filer_cik / holder_cik / reporter_cik, period_end DESC)` — per-holder timeseries
- BRIN `(known_from)` — system-time scans (rare, defer until measured)

BRIN vs btree on `period_end` deferred until we have a realistic
N-PORT fixture (~50M rows/year projected) and can measure.

### Rollup snapshot

The existing `/instruments/{symbol}/ownership-rollup` endpoint stays
the operator-facing contract but its underlying query rewrites to
read from the eight `_latest` tables in one snapshot read.

Adds two new fields to the response:
- `categories_freshness`: per-category as-of date (already exists in
  parts; codify uniformly)
- `category_provenance`: per-category list of source filings
  contributing, with edgar_urls

### Diff endpoint (new)

`GET /instruments/{symbol}/ownership-history?category={cat}&from={d}&to={d}`

Returns a time-bucketed series suitable for charting. One bucket per
filing event for that category, with running deduped totals. This is
the operator's "how did this shift over time" surface.

## Migration plan

### Phase 0 — partial recovery of already-ingested rows (1-2 days)

Codex correctly flagged my v1 claim that Phase 0 alone moves AAPL
to 50%+. It does not. Phase 0 only recovers what we already paid
to ingest from the existing 14-filer seed set. Trusted institutional
totals require Phase 2 (universe expansion).

1. **Sweep `unresolved_13f_cusips`**: for every row whose CUSIP now
   matches an `external_identifiers` row (~119 names), mark
   `resolution_status = 'resolved_via_extid'` and trigger rewash on
   `last_accession_number` so the holdings land in
   `institutional_holdings`.
2. **Fix the rollup query** that drops 13D/G blockholders (GME case:
   2 rows ingested, 0 surfaced).
3. **Extract `TreasuryStockShares`** from XBRL DEI in the existing
   parser — likely a 1-line concept addition.
4. **Run the registered DEF 14A rewash spec (#827)** so
   `def14a_beneficial_holdings` populates.

Realistic outcome: AAPL institutional moves from 5.94% (7 of 14
filers' resolved holdings) to maybe 8-12% (most of the 14 filers'
holdings). Vanguard alone, fully recovered + freshened, would add
~9% — but their last filing on file is 4 months stale and BlackRock
is 18 months stale, so those caps still bind. Residual shrinks
from 94% to maybe 80-85%.

The "chart looks broken" visual partially improves — Cohen's GME
beneficial slice will jump because dedup model lets both his
direct and beneficial rows render — but the AAPL-class
under-coverage requires Phase 2.

### Phase 1 — schema unification + provenance block (1 week)

5. Define the shared provenance composite + new `_latest` / `_history`
   tables. Migration writes existing data through into the new shape
   (no data loss; adds the provenance fields where missing).
6. Rewrite the rollup endpoint to read from `_latest` tables.
7. Add the `_history` write-trigger (any update to `_latest` snapshots
   the prior row to `_history`).

### Phase 2 — universe expansion (#790 already on roadmap, 2-3 weeks)

8. Discover all 13F-HR filers from SEC quarterly directory (~7,500).
9. Schedule quarterly ingest sweep — every 13F filer's holdings file.
10. Expand `external_identifiers` CUSIP coverage to all instruments
    via SEC company-tickers + share-class lookup (cusip_resolver.py
    is ~half-built for this).

### Phase 3 — N-PORT ingest (2 weeks)

11. Adopt EdgarTools for N-PORT / N-CSR parsing.
12. New ingest pipeline writes to `ownership_funds_latest`.
13. Rollup query adds the `funds` slice.

### Phase 4 — DEF 14A bene-ownership table (1 week)

14. Extend the existing DEF 14A ingest to extract the consolidated
    "Security Ownership of Certain Beneficial Owners and Management"
    table. Captures officer total, director total, ESOP plan totals,
    >5% holder list — all in one canonical issuer-published source.
15. Writes to `ownership_esop_latest`, augments
    `ownership_insiders_latest`, augments
    `ownership_blockholders_latest`.

### Phase 5 — DRS + restricted (textual NLP, 2-3 weeks)

16. Per-issuer 10-K Note text extraction. Not every issuer discloses
    DRS — start with a curated allowlist (GME and the meme-stock
    cohort that publish the figure quarterly), expand as patterns
    emerge.
17. Writes to `ownership_drs_latest`, `ownership_restricted_latest`.

### Phase 6 — FINRA short interest (3 days)

18. Bimonthly + daily short volume ingest.
19. New table `short_interest_latest` + `short_interest_history`.
20. Chart adds memo overlay (not part of pie).

### Phase 7 — chart redesign (after data is real)

21. Once Phases 0-2 land, the residual is real retail (~30-40%, not
    94%). Sunburst rendering issue mostly resolves.
22. Polish: subtle hatching for the residual, hover tooltip on the
    gap, "% known coverage" callout.

## Open questions (for Codex review)

1. **History table cardinality.** N-PORT ingest at ~2,500 funds ×
   monthly × 12,000 instruments = ~360M rows/year if every fund holds
   every instrument. Realistically ~50M/year. Is one giant
   `ownership_funds_history` table the right shape, or do we
   per-issuer-shard? BRIN index assumption needs validation.
2. **Bitemporal vs valid-time-only.** Do we need system-time
   (when-we-knew-it) AND valid-time (when-it-was-true), or is
   valid-time alone enough for ownership? Bitemporal is correct but
   doubles the schema complexity.
3. **EdgarTools posture (A/B/C).** Codex push-back welcome — is the
   hybrid C the right call, or should we go all-in on EdgarTools to
   simplify maintenance?
4. **CUSIP backfill strategy.** Adding CUSIPs for 12k instruments —
   do we use SEC company-tickers JSON (covers ~13k US-listed names),
   issue-level XBRL filings (covers everyone who's ever filed), or
   accept partial coverage and tombstone the rest?
5. **Insider/blockholder overlap dedup.** Today's chain
   (`Form 4 > 13D/G > DEF 14A`) means Cohen's 13D/A is dropped in
   favour of his Form 4. But the 13D/A captures BENEFICIAL ownership
   (RC Ventures + family trusts + etc.) which Form 4 (direct holdings
   only) misses. The current chain under-counts beneficial holders
   when Form 4 wins. Should beneficial-vs-direct be a separate axis
   from source priority?
6. **DRS scope.** Computershare (the dominant US transfer agent)
   doesn't publish DRS counts — issuer must self-disclose. Most
   issuers don't. Is investing in DRS extraction worth it for v1, or
   defer to a "this issuer publishes DRS" flag and link out?
7. **Short interest as memo line.** Short interest isn't a
   "category" of ownership — shorted shares are still owned by
   someone (the lender). Surfacing it as a separate overlay rather
   than a wedge — agree, or is there a cleaner model?
8. **Coverage banner with the new model.** Today's "unknown_universe"
   banner triggers when per-category universe estimates are NULL.
   With Phase 2 (full 13F universe) we'll have real per-category
   universe sizes. Does the banner state machine need updating, or is
   the existing red/amber/green/unknown_universe scale sufficient?

## Non-goals

- Historical reconstruction of pre-ingest state. We start tracking
  history from the day Phase 1 lands.
- International equities (LSE, ASX, etc.). This spec is US-only;
  per-region equivalents land separately.
- Daily rebalancing. Weekly sweep is sufficient; intraday is
  out-of-scope.
- Replacing eToro as execution / quote source of truth. Ownership
  data is research-side only.

## Definition of done

Acceptance is grounded on SEC-derived fixture cases per the
free-regulated-source-only posture (settled-decisions #532).
Gurufocus / MarketBeat / WallStreetZen are smoke-comparison only
— never a primary acceptance gate.

1. **SEC fixture parity:** AAPL ownership rollup, summed across
   every SEC-filed 13F-HR for the trailing quarter, matches the
   sum produced by an EdgarTools golden-file replay within ±0.1%
   (rounding tolerance). The fixture is checked into the repo so
   regressions trip CI.
2. **Two-axis dedup correctness:** GME rollup renders Cohen's
   beneficial 13D/A AND his Form 4 direct as separate rows in the
   appropriate slices, with reconciliation tooltip linking them.
   Sum equals the 13D/A beneficial figure (75M-class), not the
   Form 4 direct figure.
3. **Provenance click-through:** every wedge links to its
   `source_url`. Test asserts every rendered slice has a non-null
   `source_url` resolved from observations.
4. **History query:** operator can ask "show me Vanguard's AAPL
   position over the last 8 quarters" and get a chart sourced from
   `ownership_institutions_observations`. SEC archive accession
   per data point matches.
5. **Coverage banner accuracy:** when AAPL has 100% of the
   13F-HR universe ingested for the most recent quarter, banner is
   `complete_source_universe`. When 13F is fresh but DEF 14A is
   stale by >12 months, banner is `stale_category`. Test fixtures
   exercise each banner state.
6. **Pre-flight gates:** lint, ruff format, pyright, pytest, smoke
   test, FE typecheck + test:unit (when FE touched). All pass.
7. **CI green; review bot APPROVE; Codex agreement on
   rebuttal-only rounds.**

Smoke comparisons (run manually pre-merge, NOT a CI gate):
- AAPL institutional total within ±10% of gurufocus snapshot of
  same week.
- GME Cohen beneficial within ±5% of marketbeat snapshot.
- These are sanity checks against an independent compiler, not
  acceptance contracts. If our SEC-derived total disagrees with
  gurufocus, our SEC math is what we ship — divergence is a
  research note.

## Coverage banner — new state machine

| State                          | Trigger                                                                 | Variant | Operator action |
|--------------------------------|-------------------------------------------------------------------------|---------|-----------------|
| `no_data`                      | XBRL `shares_outstanding` not on file                                    | error   | trigger fundamentals sync |
| `unknown_universe`             | category universe size unknown (pre-Phase 2)                             | warning | wait for #790 |
| `partial_identifier_coverage`  | universe known, but >5% of universe filers can't be resolved to issuer (CUSIP gap) | warning | trigger CUSIP backfill |
| `stale_category`               | category as_of older than threshold (13F: 180d; Form 4: 30d; DEF 14A: 18mo) | warning | trigger per-category sync |
| `issuer_does_not_disclose`     | category requires self-disclosure (DRS), issuer never has               | info    | none — render "—" |
| `complete_source_universe`     | every known filer in universe has a current filing, all categories fresh | success | none |
