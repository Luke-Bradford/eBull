# Data retention rubric — per-source caps for SEC ingest

> Created: **2026-05-19** during Phase C T9-POST operator drive (post-#1208 cleardown plan).
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
>
> Status: **EVOLVING** — original spec merged 2026-05-19 (#1235) after Codex 1a + 1b + 1c + 1d. PR2 (#1237) + PR5 NUMERIC fix (#1236) shipped. PR1 revision in flight (this commit) reframes the spec's drop-policy from "DELETE pre-cap rows per PR" to "ingest-side caps only + one operator-driven pre-wipe + clean re-run at the end" — caps don't touch existing rows, and the wipe is whole-DB + operator-driven, not per-source.

## 0. Status snapshot (2026-05-19 17:00 UTC, mid-drain)

```text
                  table family                  | total size
------------------------------------------------+-----------
 financial_facts_raw (XBRL, partitioned)        |     23 GB
 filing_events (submissions manifest)           |    4.3 GB
 ownership_institutions_current (write-through) |    2.8 GB
 ownership_funds_current (write-through)        |    2.5 GB
 ownership_institutions_observations            |    2.5 GB
 ownership_funds_observations                   |    1.6 GB
 ownership_insiders_observations                |   316 MB
 institutional_holdings (raw 13F)               |    25 MB
 def14a_beneficial_holdings (raw DEF14A)        |    17 MB
 ownership_def14a_observations                  |    24 MB
 ownership_treasury_observations                |    14 MB
 insider_transactions                           |   3.7 MB ← under-ingested
 ownership_blockholders_observations            |   3.4 MB
 ownership_esop_observations                    |   3.0 MB
                                          db_size = 43 GB
```

```text
universe   = 12,417 instruments
US filers  =  5,174 (42% — `external_identifiers.identifier_type='cik'`)
non-US     = unknown (country column 100% NULL — #1233 §2)
filings    = 5,792,877 events spanning 1993–2026 (33y)
```

## 1. Problem statement

Bootstrap currently ingests **all SEC sources for all CIK-holding instruments at unbounded depth**. The 2026-05-19 T9-POST drive surfaced three concrete costs:

1. **Wall-clock.** Each per-accession bootstrap stage caps at `max_runtime_seconds=3600` (#1234). The 1h cap fires before natural drain on at least three stages we measured. The candidate set is too big for one pass; operator has to re-trigger to fully cover.
2. **Storage.** 43 GB of dev DB. `financial_facts_raw` at 23 GB alone is half. Steady-state growth is unbounded.
3. **SEC rate-limit budget.** 10 req/s shared. Universe-wide × 33y depth × per-accession HTTP for the non-bulk sources = days of wall-clock.

We pull data that **never feeds a current-as-of-today thesis or chart, and we pull it for sources at unbounded depth**. The premise of this spec: every cap is justified against a plausible downstream consumer (chart, AI prompt, alert, valuation model, future report). Caps are applied **at ingest time** so the table doesn't grow with noise; existing rows are untouched until the single operator-driven pre-wipe + clean re-run at the end (§6.3). Schema columns are preserved throughout — the product is reporting-incomplete by design, and columns the parser could fill stay populated even if no current consumer reads them.

### Why pre-wipe

This spec is the **gating work for the clean re-run**. The operator intends to wipe + re-ingest end-to-end to validate the system, measure timings, and refine. Without per-source caps the wipe + re-run is the same multi-day exercise we just finished. Caps land first, wipe second, clean re-run measures the new ceiling.

## 2. Non-goals

- Schema redesign of observations tables — `#788` two-layer model is settled.
- Removing sources entirely — every source listed below stays; only depth + retention shape changes.
- Frontend chart redesign — separate epic (#585 et al.).
- Replacing SEC with non-SEC sources — out of scope; SEC remains the spine.

## 3. Two-axis model

Cap decisions follow two axes:

### 3.1 Signal half-life

How fast does the data stop being predictive?

| Half-life | Examples | Bound type |
| --- | --- | --- |
| **Hours-days** | Real-time price, breaking 8-K Item 5.02 | Alert window |
| **Weeks-months** | Insider Form 4 buy/sell, 13D/G new filer | Thesis input window |
| **Quarters-year** | 13F-HR holdings, DEF 14A blockholders, fundamentals | Chart trend window |
| **Multi-year** | XBRL revenue/EPS trends, CAGR | Valuation depth |
| **Decade+** | Business model, sector classification | Reference only |

### 3.2 Payload weight per instrument

- **Light**: 1 row per filing (Form 3, business summary).
- **Medium**: 5-15 rows per filing (DEF 14A blockholder tables).
- **Heavy**: 100s of rows per filing (13F-HR holdings; N-PORT fund portfolios).
- **Bulk**: 1000s of rows per CIK per year (XBRL companyfacts).

### 3.3 Downstream consumer surface

Every cap must be justified against a plausible downstream consumer:

- **Chart panel** — currently-rendering surface on the instrument detail page.
- **AI prompt section** — thesis-writer, thesis-critic, ranking-engine, valuation-analyst, news-sentiment.
- **Alert trigger** — future surface (cluster insider buy, new 5%+ blockholder, material 8-K).
- **Valuation model input** — backtest history depth, ranking factor.
- **Future report** — any panel / prompt / chart not yet built but plausibly within scope; v1 is reporting-incomplete by design.

**Important: this is an ingest-side discipline, not a row-deletion rule.** Caps gate what new ingests *write*. Existing rows are NEVER deleted on a per-source basis to align with this list. The product is reporting-incomplete; columns the parser could fill but no current consumer cites stay populated because tomorrow's report may want them. Schema columns are never removed during cap-shaping work.

The only purge in this spec is the single **pre-wipe** event in §6.3 — operator-driven, whole-DB, run once before a clean-bootstrap re-run. After the wipe, the clean ingest pulls bounded data under the new caps. There is no piecemeal post-merge `DELETE` per PR.

## 4. Per-source rubric

Each subsection follows the shape: **raw shape → current volume → signal half-life → consumers → ingest depth cap → retention horizon cap → notes**.

### 4.1 Companyfacts XBRL (`financial_facts_raw`)

- **Raw shape**: per-CIK XBRL line items (revenue, EPS, margins, cash, debt, …) with `period_start`, `period_end`, `concept`, `value`, `unit`.
- **Current volume**: 16.4M rows, 23 GB. Largest single table by far.
- **Half-life**: **slow** — multi-year. 10y trend matters for valuation; CAGR models need 5y+.
- **Consumers**: PE/PS/margin time-series charts; ranking engine factor inputs; valuation analyst inputs; AI thesis valuation context.
- **Ingest depth cap**: **20y rolling window** (`period_end >= NOW() - 20y`) applied at the parser. Already landed via PR2 (#1237). Survives the pre-wipe + clean re-run as the steady-state cap.
- **Field-level cap**: many XBRL concepts are textual annotations or auditor disclosures, not numbers. Whitelist of ~50 numeric concepts + ~3 DEI concepts is enforced at ingest. Already landed via PR2 (#1237). Schema column for `concept` is preserved — operator can widen the whitelist later without a migration.
- **Existing rows**: untouched. The pre-wipe (§6.3) and subsequent clean re-run will land the bounded set; the in-place 16.4M-row table stays put until then.
- **Why this matters**: half the DB. Post-wipe + clean re-run, the 20y + whitelist combo projects to ~7 GB for this table.

### 4.2 Filing events (`filing_events`)

- **Raw shape**: per-accession manifest entry with `filing_date`, `filing_type`, `source_url`, `primary_document_url`, `items` (8-K item codes), `raw_payload_json`. **Single table holds metadata for every SEC filing_type** — 10-K, 10-Q, 8-K, Form 3/4/5, DEF 14A, 13F-HR, 13D/G, N-PORT, N-CSR, N-CEN, Form D, Form 144, NT 10-Q, S-1/3/4/8/11, 424B, etc.
- **Current volume**: 5.79M rows, 4.3 GB, 1993–2026.
- **Half-life**: **slow** for navigation (drilldown link source); fast for "recent activity" displays.
- **Consumers**: drilldown links from chart pages; AI thesis recent-events context; audit trail; 8-K event timeline chart (filtered view, not a separate table).
- **Ingest depth cap**: **10y rolling window** at ingest (every discovery writer — Atom, daily-index reconcile, first-install drain, per-CIK poll, targeted rebuild). The "8-K 2y for chart" referenced in §5.1 is a **query filter** that the consumer applies, not a parser-side cap (Codex 1a §1).
- **Raw payload posture**: `raw_payload_json` is preserved in the schema. The strip-after-parse work is a **separate** ticket (#1014 raw-payload retention) so the ingest cap here doesn't conflate with the payload question.
- **Existing rows**: untouched. The pre-wipe (§6.3) + clean re-run will land the bounded set under the new cap.
- **Why this matters**: 4.3 GB. Post-wipe + clean re-run under the 10y cap projects to ~1 GB (60-70% row reduction at clean ingest time, schema intact).

### 4.3 Form 4 (insider transactions, `insider_transactions`)

- **Raw shape**: per-filing record of insider transaction (officer/director/10%+ holder) — date, shares, price, direction (buy/sell), insider role.
- **Current volume**: 7,777 rows, 3.7 MB. **Under-ingested** — should be hundreds of thousands at universe scale × full history.
- **Half-life**: **fast** — last 90d is the alert signal; last 12mo is the thesis signal; 5y old = decoration.
- **Consumers**: insider buy/sell timeline chart; AI thesis "insiders bought $X in last 90d"; future cluster-buy alerts. **`ownership_insiders_current`** (29 MB) is the cumulative post-transaction holdings rollup derived from observations.
- **Ingest depth cap**: **3y** from today, per CIK, at the parser. Rows outside the window aren't fetched.
- **Cumulative ownership invariant**: `ownership_insiders_current` is rebuilt deterministically from `ownership_insiders_observations` + Form 3 baseline rows via `refresh_insiders_current()`. PR4 verification step: pin a steady-state test that the recompute continues to aggregate pre-existing observations alongside post-cap rows without regression. No synthetic opening-balance anchor is written — see post-wipe semantics below.
- **Post-wipe semantics for cumulative state**: a whole-DB wipe (§6.3) deliberately resets `ownership_insiders_current` along with `ownership_insiders_observations`. The clean re-ingest under the 3y cap rebuilds cumulative state going forward from "no opening balance" — i.e. the post-wipe `ownership_insiders_current` reflects only trades observed inside the 3y window. **Pre-3y cumulative position is lost by design.** Operator accepts this as the trade-off for a bounded clean re-run; if pre-3y opening balance is later wanted, it requires a separate one-shot "deep history" sweep outside the cap (operator-driven, like `POST /jobs/sec_rebuild/run` with explicit depth override). This clause is the canonical contract — an earlier draft additionally proposed a synthetic opening-balance anchor row, but PR4 (Codex 1a/1b/1c/1d) confirmed it would contradict the loss-accepted clause; the synthetic-anchor proposal is retired.
- **Cohort bound**: only ingest for `is_tradable=TRUE` instruments (PR1 §6.2). Form 4 for delisted = ingest-budget noise.
- **Existing rows**: untouched until pre-wipe (§6.3).
- **Why this matters**: not size, but ingest-budget — universe-wide × 33y Form 4 ingest is the multi-day cost. 3y cap = ~90% reduction in candidate set.

### 4.4 Form 3 / Form 5 (initial / annual insider summary)

- **Raw shape**: registry entry of insider-company pair (Form 3 = initial filing, Form 5 = annual catch-up of any missed Form 4s).
- **Current volume**: low (no dedicated table for Form 3 visible; data threaded into insider_transactions).
- **Half-life**: **latest only matters** — Form 3 is the initial filing, supplanted by ongoing Form 4s; Form 5 is annual catch-up.
- **Consumers**: insider registry table (who is registered as an insider for this issuer).
- **Ingest depth cap**: **latest per insider-company pair** at the parser. The post-wipe clean re-run naturally lands one row per pair under this rule.

### 4.5 13F-HR institutional holdings (`institutional_holdings`, `ownership_institutions_observations`)

- **Raw shape**: per-filer quarterly snapshot of all 13F-reportable positions (>$100M AUM filers). Holdings: CUSIP, value, shares, put/call.
- **Current volume**: 105k raw holdings; 3.86M observations rows; 2.5 GB obs + 2.8 GB current = **5.3 GB combined**.
- **Half-life**: **medium** — 4-quarter trend matters for momentum; 8 quarters (2y) for backtests; beyond = decoration.
- **Consumers**: stacked institutional ownership % chart; concentration metric in ranking; AI thesis "Vanguard increased position by 8%".
- **Ingest depth cap**: **8 quarters (2y)** observations at the parser + always-current snapshot.
- **Cohort bound**: **already done #1010** — `last_13f_hr_at` 380d recency cap on filer cohort (11,205 → 8,681 filers).
- **`ownership_institutions_current` size oddity**: 2.8 GB is huge for a "current snapshot" — investigation needed. Either stores wide rows with embedded payload, or write-through is dumping more than current state. Separate audit ticket (PR12).
- **Existing rows**: untouched until pre-wipe (§6.3).
- **Why this matters**: combined 5.3 GB. Post-wipe + clean re-run under 8q + PR12 audit projects to ~1-2 GB (depending on what `current` really stores).

### 4.6 N-PORT fund holdings (`ownership_funds_observations`)

- **Raw shape**: same shape as 13F-HR but for mutual fund filers (vs institutional advisers). Quarterly per fund.
- **Current volume**: 3.68M obs rows, 1.6 GB obs + 2.5 GB current = **4.1 GB combined**.
- **Half-life**: same as 13F.
- **Consumers**: funds slice of institutional ownership chart; potentially fold into 13F view if redundant.
- **Ingest depth cap**: **8 quarters** at the parser, same as 13F + always-current snapshot.
- **Cohort bound**: same recency-based filter pattern as #1010, applied to N-PORT filer registry (`last_nport_at`).
- **Existing rows**: untouched until pre-wipe (§6.3).
- **Why this matters**: similar to 13F — combined 4.1 GB; same proportional projection post-wipe + clean re-run.

### 4.7 DEF 14A blockholders (`def14a_beneficial_holdings`, `ownership_def14a_observations`)

- **Raw shape**: 5%-holders / officers-and-directors table from annual proxy statement. Holder name, shares, percent_of_class, role.
- **Current volume**: 47k raw rows, 17 MB; 40k obs rows, 24 MB; combined ~50 MB.
- **Half-life**: **slow state, but only LATEST matters** — DEF 14A is the annual snapshot; the prior year's snapshot is decoration.
- **Consumers**: top-5-holders pie chart; AI thesis "Top 5 institutional holders are…"; executive-comp slice (separate epic).
- **Ingest depth cap**: **latest 2 proxies per filer** (current + one prior for change tracking) at the parser. Older filings not fetched.
- **NUMERIC overflow bug**: #1228 — fix already landed via PR5 fold-in (#1236).
- **Existing rows**: untouched until pre-wipe (§6.3).
- **Why this matters**: not storage (small); ingest-budget. DEF 14A is HTML scrape — 1h per pass with deadline cap; 5y of proxies × 5,174 filers = un-drainable in one pass. 2-proxy cap = ~80% reduction in candidate set.

### 4.8 13D/G blockholders

- **Raw shape**: per-filing notice when an entity crosses 5% ownership threshold or files an exempt declaration.
- **Current volume**: 0 ingested (table exists; pipeline not yet active).
- **Half-life**: **fast for new-filing alert**, **slow for current state** (current 13D/G filers = decoration table).
- **Consumers**: top concentrated holders panel; AI thesis "new blockholder X filed Y ago"; future alert on new 13D crossing.
- **Ingest depth cap**: **3y historical** at the parser + current state always.
- **Existing rows**: 13D/G table is empty today (pipeline dormant); the cap shapes the first ingest.

### 4.9 8-K events (filtered view of `filing_events`)

- **Raw shape**: 8-Ks live as `filing_events` rows with `filing_type='8-K'` + `items` column listing 8-K item codes (1.01, 5.02, 8.01, …). **No separate observation table** — Codex 1a §1 caught the earlier draft's contradiction (had this section as a separate 2y retention cap conflicting with `filing_events` 10y).
- **Current volume**: count of 8-K rows = subset of `filing_events.filing_type='8-K'`. Bootstrap stage S20 (`sec_8k_events_ingest`) wrote 456 events in this cycle.
- **Half-life**: **very fast** — 90d alert window; 2y chart timeline; 5y old = historical-only.
- **Consumers**: 8-K event timeline chart (queries `filing_events WHERE filing_type='8-K' AND filing_date >= NOW() - 2y`); AI thesis recent-events context (90d slice); future material-item alerts (Item 5.02 CEO departure, 8.01 strategic announcement).
- **Retention horizon**: **none separate** — inherits §4.2 `filing_events` 10y. Chart applies the 2y filter at query time. Alert path applies the 90d filter. Both windows fit inside the parent 10y store.
- **Why this matters**: spec self-consistency. The original "2y 8-K retention" was a category error; 8-Ks are filing_events rows.

### 4.10 Business summary (10-K Item 1)

- **Raw shape**: one text blob per CIK per fiscal year — the "Business" section of the 10-K.
- **Current volume**: 10,744 rows.
- **Half-life**: **slow state, latest only** — business model doesn't change rapidly; the latest 10-K subsumes prior.
- **Consumers**: instrument page text panel; AI thesis "company is in business of…" context.
- **Ingest depth cap**: **latest 10-K per CIK** at the parser. Post-wipe clean re-run naturally lands one row per CIK.

### 4.11 Treasury / ESOP / blockholder slices

Aggregated under DEF 14A discovery (treasury share counts, ESOP plan holdings, blockholder identities). Small volumes (3-30 MB each).

- **Half-life**: slow-state.
- **Consumers**: capital structure panel; buyback / dilution context.
- **Ingest depth cap**: latest 2 proxies per filer (same as DEF 14A) at the parser.

### 4.12 N-CSR / N-CSRS (fund certified shareholder reports)

- **Raw shape**: registered fund trust annual + semi-annual reports. Per-trust filings with portfolio holdings appendix.
- **Current volume**: not yet exercised at universe scale in this drive. Ingest path lives in `app/jobs/sec_first_install_drain.py:512-815` (bootstrap_n_csr_drain). Existing `horizon_days=730` (2y) cap already in code.
- **Half-life**: **medium** — funds report semi-annually; 4 semi-annual snapshots = 2y of position changes per trust.
- **Consumers**: funds slice augmentation (N-PORT alone misses some trusts that file only N-CSR); AI thesis context for fund-held instruments.
- **Ingest depth cap**: **730 days (2y) — already in code** (`bootstrap_n_csr_drain` `horizon_days=730`). Retain as-is.
- **Cohort bound**: fund trusts only (sourced from `cik_refresh_mf_directory`). Issuer-scoped seed excludes N-CSR per `sec_first_install_drain.py:167`.

### 4.13 N-CEN (annual fund census, classification only)

- **Raw shape**: annual N-CEN filing per registered investment company. Contains `investmentCompanyType` field SEC uses to classify the filer (open-end fund, closed-end fund, UIT, ETF, …).
- **Current volume**: small; one row per investment-company CIK per year.
- **Half-life**: **slow** — classification updates annually; latest N-CEN per CIK is sufficient.
- **Consumers**: `app/services/ncen_classifier.py` filer-type classification feeds 13F-HR vs N-PORT routing; influences institutional vs funds ownership lane decision.
- **Ingest depth cap**: **latest N-CEN per CIK** at the parser. Post-wipe clean re-run lands one row per CIK.
- **Why this matters**: small storage, but **load-bearing for filer classification** — Codex 1a caught the omission. Spec must acknowledge this source even though it's lightweight.

### 4.14 Metadata-only forms (Form D, Form 144, NT 10-Q, S-1/3/4/8/11, 424B)

These SEC filings are **discovered via the submissions manifest** and persisted as `filing_events` rows with no parser-derived observation table. Codex 1a §9 — earlier draft was silent on them; explicit acknowledgment here.

- **Raw shape**: index entry only (`filing_type`, `filing_date`, `source_url`).
- **Half-life**: varies — Form 144 last 90d is the insider-sale-intent signal; S-1/424B are one-shot per offering; NT 10-Q is a late-filing notice (90d window).
- **Consumers**: drilldown links; AI thesis "recently filed S-1" context; future Form 144 alert (intent-to-sell signal complementing Form 4 actuals).
- **Ingest depth cap**: covered by `filing_events` 10y cap.
- **No separate parser cap required** unless a future ticket adds an observation table (e.g. Form 144 intent-to-sell extraction for the alerts epic — out of scope here).

## 5. Downstream consumer map

Mapping each surface to the sources it needs. Used to validate the caps don't break a consumer.

### 5.1 Charts (currently rendering)

| Chart | Required sources | Required depth |
| --- | --- | --- |
| PE/PS/margin time-series | financial_facts_raw | 10-20y |
| Insider buy/sell timeline | insider_transactions (Form 4) | 3y |
| Institutional ownership % stacked area | ownership_institutions_observations + N-CEN classification | 8 quarters obs; latest N-CEN |
| Funds ownership slice | ownership_funds_observations (N-PORT + N-CSR augmentation) | 8 quarters obs; 2y N-CSR |
| Top blockholders pie | def14a_beneficial_holdings + ownership_def14a_current | Current snapshot |
| Treasury / buyback timeline | ownership_treasury_observations | 8 quarters |
| 8-K event timeline | filing_events filtered `WHERE filing_type='8-K'` | 2y query window (no separate retention) |
| Filings drilldown | filing_events | 10y hot |
| Business summary panel | business_summaries | Latest only |
| Filer-type chip (institutional vs fund) | N-CEN classification | Latest N-CEN per CIK |

### 5.2 AI prompt sections

| Section | Sources | Depth |
| --- | --- | --- |
| Thesis-writer "company overview" | business_summaries, sector/industry | Latest |
| Thesis-writer "valuation context" | financial_facts_raw (PE/PS/EPS/margin trends) | 10y |
| Thesis-writer "ownership context" | latest 13F changes, latest DEF 14A blockholders | 2 quarters delta, current state |
| Thesis-writer "recent events" | filing_events (8-K), insider_transactions | 90d |
| Thesis-critic adversarial | same as thesis-writer | same |
| Ranking-engine factors | financial_facts (growth, profitability); 13F concentration | 5y for trends; current snapshot for state |
| Valuation-analyst | financial_facts_raw (revenue, EPS, FCF series) | 10-15y |
| News-sentiment (out of SEC scope) | n/a | n/a |

### 5.3 Future alerts

| Alert | Source | Cadence |
| --- | --- | --- |
| Cluster insider buying | insider_transactions (Form 4) | 30d window |
| New 5%+ blockholder | 13D/G | live |
| Material 8-K item (5.02, 8.01) | filing_events (8-K items) | live |
| Material 13F change | ownership_institutions_observations delta | quarterly |

## 6. Cross-cutting decisions

### 6.1 Jurisdiction filter (#1233 §2)

`instruments.country` is 100% NULL — universe sync doesn't populate it from eToro metadata. Downstream: rankings can't filter "US-equity-only" without inferring from CIK presence (proxy, not authoritative).

**Decision**: universe sync populates `instruments.country` from eToro's exchange / ISIN prefix. Backfill existing rows. SEC bootstrap entry points filter on `country='US'` explicitly (the CIK-presence filter implicitly does this today, but explicit is clearer + survives refactors).

### 6.2 Active-status filter (#1233 §3)

`instruments.is_tradable` exists. Every SEC bootstrap entry point should filter `WHERE is_tradable = TRUE` — delisted instruments consume bootstrap budget for no operator value.

**Decision**: audit + lint guard. Cron stages add the filter; bootstrap stages add the filter; the few legitimate "ingest delisted for back-history" paths require explicit override flag.

### 6.3 No piecemeal drops — one operator-driven pre-wipe + clean re-run

The original draft proposed per-PR `DELETE` of pre-cap rows. **Revoked.** Per-PR row-deletion conflates two concerns: (a) gate what new ingests write (ingest-side cap, safe + reversible), (b) reshape an existing table (destructive, hard to roll back, risks erasing data a future report may want).

**Decision: caps are ingest-side only.** No PR in this spec issues `DELETE FROM <table>` against pre-cap rows. Existing rows stay until the single pre-wipe event below.

**Pre-wipe event** (operator-driven, whole-DB, one-shot):

1. Operator triggers a controlled wipe of the dev DB (`TRUNCATE` or DB re-creation) **after** PR1-PR12 land and all caps are merged.
2. Bootstrap re-runs from a clean DB. Ingest is bounded by every cap in §4.
3. Final DB size measures the post-wipe steady-state under the new caps.

This single event replaces the dozen per-PR `DELETE` instructions. Reasons:

- It's already on the operator's roadmap ("we will purge all reporting data once we're ready to test bootstrap timing"). The cap-shaping PRs are the precondition for that wipe, not a parallel concern.
- Same outcome as in-place delete (clean ingest under new caps yields the same survivor set) without the risk of dropping rows a half-finished report still references.
- Schema columns are preserved through the wipe — the wipe is `DELETE FROM` / `TRUNCATE`, not `DROP COLUMN` / `DROP TABLE`. Parsers fill what they always filled into the same columns.

**Same-DB cold archive remains out of scope** — Codex 1a §3 was right that same-DB archive doesn't reduce DB size. If a future epic wants cold-archive, ship as **separate database** or **S3 parquet snapshots**, not same-DB tables.

### 6.4 Two-layer storage (current + observations)

Per `#788` decomposition, keep the two-layer model:

- **`*_current`** = write-through latest snapshot, optimised for "latest state of X" reads.
- **`*_observations`** = append-only historical observations, partitioned by quarter.

Caps apply to **`*_observations`**; `*_current` is always latest.

**Open question**: `ownership_institutions_current` is 2.8 GB — far larger than expected for "current snapshot". Either stores wide rows with embedded payload, or write-through dumps more than latest. **Separate audit ticket.** Not blocking this spec.

## 7. Implementation sequence

Land per-source PRs in this order. Each PR is **ingest-side cap only** — no PR issues `DELETE FROM <table>` against pre-cap rows (§6.3). Schema columns are preserved across every PR.

- **PR1 — IN PROGRESS.** Cross-cutting (#1233 §2 + §3). Populate `instruments.country` from `exchanges.country` join + backfill migration; audit + add `is_tradable=TRUE` filter to every SEC stage entry point; lint guard greps `INSERT INTO instruments` for `is_tradable` in the column list (extends prevention-log §"`INSERT INTO instruments` fixtures must supply `is_tradable`" from tests/ to app/ + tests/). **Bundles spec revision** (this commit).
- **PR2 — SHIPPED (#1237).** Companyfacts XBRL concept whitelist (~50 numeric us-gaap + ~3 DEI concepts) + 20y rolling cap at the parser. Every write path (bulk + steady-state).
- **PR3 — pending.** Filing events 10y rolling cap at the parser. Applied uniformly across every filing_type via every discovery writer. Schema columns + existing rows untouched.
- **PR4 — IN PROGRESS.** Form 4 / 4-A 3y ingest cap at every writer chokepoint (legacy filing_events SELECTs, manifest-worker `_parse_form4` pre-fetch gate, bulk-dataset Form-4-only filter). Cumulative-rollup invariant pinned by steady-state test; synthetic opening-balance anchor NOT written (§4.3 amendment, this commit). Recency cohort bound inherited from PR1 `is_tradable=TRUE` filter (no insider-filer cohort table; Form 4 walked per-issuer-CIK via filing_events). Includes a parity lint guard catching new chokepoints that omit the predicate.
- **PR5 — partially shipped (#1236 NUMERIC fix).** DEF 14A latest-2-proxies cap at the parser. NUMERIC overflow #1228 already folded.
- **PR6 — pending.** 13F-HR 8-quarter ingest cap at the parser. (#1010 cohort bound already in place.)
- **PR7 — pending.** N-PORT 8-quarter cap (mirror of PR6). N-PORT recency cohort bound (`last_nport_at`) per #1010 pattern.
- **PR8 — pending.** N-CSR/N-CSRS validate existing 2y horizon. Doc-only unless drift found.
- **PR9 — pending.** N-CEN latest-only cap at the parser. Verify `ncen_classifier` reads latest.
- **PR10 — pending.** Form 3/5 latest-only at the parser + business summary (10-K Item 1) latest-only at the parser.
- **PR11 — pending.** 13D/G activate dormant pipeline with 3y historical + current-state cap at the parser.
- **PR12 — pending.** `ownership_*_current` size audit + remediation (no row-deletion — schema audit only; if wide-row write bug found, fix the writer; existing rows reshape happens via the pre-wipe).

After PR1-PR12 land, the operator triggers the **pre-wipe + clean re-run** (§6.3) — a whole-DB controlled wipe + clean bootstrap re-ingest under all caps. The clean re-run measures the new ceiling.

## 8. Acceptance

Measured after PR1-PR12 land + the operator-driven pre-wipe + clean bootstrap re-run (§6.3). The clean re-run lands the bounded set under all caps; the numbers below are the projection for that final state, not for any in-place delete pass.

1. **Wall-clock**: full bootstrap drain (every bootstrap stage including N-CSR S25 fund-trust drain) completes in **a single business day (10-12h)** from fresh DB (was multi-day pre-caps).
   - **Why not < 8h**: SEC 10 req/s budget is shared across DEF14A HTML, Form 4, 13D/G, N-PORT, N-CSR, companyfacts per-CIK fetches. Even with caps, the per-accession candidate set remains large (5,174 US filers × multi-source + fund-trust universe for N-CSR). 10-12h is the realistic floor; sub-10h is a Phase 2 optimization (parallel SEC fetcher pools, CDN cache warm-up).
2. **DB size**: post-clean-rerun `pg_database_size('ebull')` measured (excludes WAL — Postgres WAL lives outside per-database size accounting; tracked separately at the filesystem level via `pg_stat_wal` / `pg_ls_waldir()` if needed). Provisional breakdown:
   - `financial_facts_raw`: current 16.4M rows / 23 GB. Apply (a) 20y rolling cap → keep rows where `period_end >= NOW() - 20y` ≈ 60% of rows survive (proxy: 20y/33y); (b) whitelist of ~50 numeric concepts → assumed 50% row reduction inside the surviving 20y slice. Net: 16.4M × 0.60 × 0.50 ≈ 4.9M rows × ~1.5 KB/row (raw + tuple header + partitions) ≈ **~7 GB** including indexes + partition overhead.
   - `filing_events`: current 5.79M rows / 4.3 GB. Apply 10y cap → ~30% rows survive (10y/33y). Net (no payload strip — deferred to #1014): 5.79M × 0.30 × ~750 B/row ≈ **~1.3 GB**; **~1.5 GB with indexes**. (With #1014's payload strip applied: row width drops ~50% → **~0.7 GB**; **~1 GB with indexes**.)
   - `ownership_institutions_current` + `ownership_funds_current`: **conditional on PR12 audit** — current 5.3 GB combined. Post-PR12 (audit + remediate wide-row writes) assumed ≤ 1 GB combined.
   - `ownership_institutions_observations` + `ownership_funds_observations` (8 quarters): current 4.1 GB combined across all-time partitions. 8-quarter cap retains 8 of ~64 partitions populated → ≈ 4.1 GB × 8/64 ≈ **~0.5 GB**.
   - `ownership_insiders_observations` (3y from current 316 MB across ~64 partitions): 316 MB × 12/64 ≈ **~60 MB**.
   - DEF 14A + treasury + ESOP + blockholders + N-CSR + N-CEN + raw insider tables: **~150 MB** combined.
   - Indexes + bloat overhead: ~20% headroom on top of base table sizes.
   - **Honest totals**:
     - **PR12 unsolved**: 7 + 1 + 5.3 + 0.5 + 0.06 + 0.15 ≈ **14 GB raw**, **~17 GB with 20% overhead**.
     - **PR12 solved**: 7 + 1 + 1 + 0.5 + 0.06 + 0.15 ≈ **9.7 GB raw**, **~12 GB with 20% overhead**.
   - Operator acceptance tiers (all measured AFTER the §6.3 pre-wipe + clean re-run; the *tier* selected for acceptance depends on what state PR12 reached at wipe time):
     - **v1 bar — `<20 GB hot`**: PR12 audit complete + caps merged but remediation partial / deferred (current snapshots remain near 5.3 GB combined under whatever the audit identifies as the cause). ~57% reduction from current 43 GB. Acceptable for v1 ship.
     - **Phase 2 stretch — `<15 GB hot`**: PR12 fully solved (current snapshots ≤ 1 GB combined). ~65% reduction.
     - **Ambitious stretch — `<10 GB hot`**: only achievable by also tightening companyfacts (e.g. 10y cap instead of 20y; concept whitelist trimmed to ~20 numeric concepts). Not in PR1-PR12 scope — proposed as a Phase 3 ticket if Phase 2 lands and storage remains a concern.
3. **Chart pages**: every chart in §5.1 renders correctly for the standard panel (AAPL, GME, MSFT, JPM, HD) — Cypress / Playwright golden path.
4. **AI thesis**: thesis-writer produces same-or-better quality output for the standard panel (manual eval; small comparison set).
5. **No regression**: existing PRs + smoke tests pass without modification (no consumer-shape changes).
6. **Operator override**: each cap has a documented "manual rebuild for ad-hoc deep dive" path (per-instrument or per-source override via `POST /jobs/sec_rebuild/run`).

## 9. Open questions for review

- **Valuation depth**: 15y XBRL or 20y? Trade-off: backtest depth vs storage. Bias to 20y unless ranking engine survey says 15y suffices.
- **Insider horizon**: 3y or 5y for Form 4? Trade-off: cluster-buy signal richness vs ingest budget. Bias to 3y. Resolution depends on PR4 verification of `ownership_insiders_current` rollup semantics.
- **8-K chart depth**: 2y or 3y? Trade-off: visual continuity vs storage. Bias to 2y. (Query-window only; doesn't affect retention.)
- **`ownership_*_current` size oddity (PR12)**: biggest unknown affecting §8 DB-size target. Three-tier acceptance: v1 bar `<20 GB`, Phase 2 stretch `<15 GB` (PR12 solved), ambitious `<10 GB` (PR12 + companyfacts further tightening — Phase 3 if needed).
- **Wipe gating**: §6.3 + §7 specify that PR1-PR12 must all land before the operator-driven pre-wipe + clean re-run. Acceptance §8 is measured AFTER the clean re-run, so all tiers are evaluated against the same post-wipe state — no ambiguity in the gate.
- **No piecemeal deletes**: §6.3 removed the per-PR `DELETE` instructions. Every cap in §4 is ingest-side only; existing rows are untouched until the single pre-wipe event. The trade-off is that the in-place 43 GB dev DB stays that size until the wipe — which is fine because the wipe is already on the operator's roadmap and is the only honest way to measure post-cap steady-state.
- **Form 144 alert**: out of this spec, but the metadata is there in `filing_events` if a future ticket activates it.

## 10. Out of scope

- **Non-SEC sources** (eToro market data, news sentiment, FINRA short interest) — separate retention rubric.
- **`#788` ownership decomposition** — settled; this spec uses existing tables.
- **Frontend chart redesign** — separate epic.
- **AI prompt template changes** — separate skill update. Caps are upstream of prompt assembly.

## 11. Codex review gate

Per #1208 cadence:

- **Codex 1a** on this spec (after first commit).
- **Revise** based on findings.
- **Codex 1b** on revised spec.
- **Revise**.
- **Codex 1c if needed**.
- Spec lands as DOC PR (no code yet).
- Implementation plan = separate doc (`docs/superpowers/plans/2026-05-19-data-retention-impl.md`), Codex 1a/1b on that.
- PRs 1-12 each follow standard #1208 cadence.

## 12. Handover for next session

State as of this commit:

- PR1 bundles cross-cutting code (country populate + is_tradable filter audit + lint guard) AND this spec revision (drop-policy → ingest-side-caps-only).
- PR2 (#1237) + PR5 NUMERIC fix (#1236) already shipped.
- PR3-PR12 remain — every one is ingest-side cap only, no row deletion. The pre-wipe is the single operator event at the end.

```text
After PR1 merges, the next session picks up PR3
(filing_events 10y rolling cap at the parser).

PR3 scope:
- Identify every filing_events discovery writer (Atom fast-lane, daily-index
  reconcile, first-install drain, per-CIK poll, targeted rebuild).
- Apply 10y depth cap at the parser layer so every writer respects it.
- Preserve raw_payload_json (#1014 is the separate slice).
- No DELETE of pre-10y rows. They survive until the §6.3 pre-wipe.

FIRST ACTIONS:
1. Read CLAUDE.md working order + the revised §3.3 / §4.2 / §6.3 / §7.
2. Confirm PR1 merged. Confirm #1233 still OPEN as the umbrella.
3. Branch `feature/1233-pr3-filing-events-10y-cap`.
4. Codex 1a on plan, then Codex 2 pre-push, then PR.
```
