# Data retention rubric — per-source caps for SEC ingest

> Created: **2026-05-19** during Phase C T9-POST operator drive (post-#1208 cleardown plan).
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
>
> Status: **READY FOR PR** — Codex 1a + 1b + 1c + 1d complete (all blocking + warning findings resolved). Empirical numbers captured mid-drain so the volumes here are a lower bound (S21 13F sweep still running; ownership rollups not yet refreshed).

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

We pull data that **never feeds a current-as-of-today thesis or chart**. The premise of this spec: every row in hot storage must justify itself against a downstream consumer (chart panel, AI prompt section, alert trigger, valuation model). If no current consumer cites it, it doesn't ingest — or it gets dropped post-retention horizon. (Same-DB cold archive is out per §6.3.)

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

Every row in hot storage must feed at least one of:

- **Chart panel** — currently-rendering surface on the instrument detail page.
- **AI prompt section** — thesis-writer, thesis-critic, ranking-engine, valuation-analyst, news-sentiment.
- **Alert trigger** — future surface (cluster insider buy, new 5%+ blockholder, material 8-K).
- **Valuation model input** — backtest history depth, ranking factor.

Rows that match no current consumer are candidates for drop (no same-DB cold archive — see §6.3).

## 4. Per-source rubric

Each subsection follows the shape: **raw shape → current volume → signal half-life → consumers → ingest depth cap → retention horizon cap → notes**.

### 4.1 Companyfacts XBRL (`financial_facts_raw`)

- **Raw shape**: per-CIK XBRL line items (revenue, EPS, margins, cash, debt, …) with `period_start`, `period_end`, `concept`, `value`, `unit`.
- **Current volume**: 16.4M rows, 23 GB. Largest single table by far.
- **Half-life**: **slow** — multi-year. 10y trend matters for valuation; CAGR models need 5y+.
- **Consumers**: PE/PS/margin time-series charts; ranking engine factor inputs; valuation analyst inputs; AI thesis valuation context.
- **Ingest depth cap (proposed)**: **20y** rolling window (`period_end >= NOW() - 20y`). No separate completeness SLA — the 20y cap *is* the completeness rule.
- **Retention horizon cap (proposed)**: **20y in hot table**; older → **drop** (Codex 1a §3 — same-DB cold archive doesn't reduce DB size; if backtests later need pre-20y, re-ingest via `POST /jobs/sec_rebuild/run` with explicit horizon override).
- **Field-level cap**: many XBRL concepts are textual annotations or auditor disclosures, not numbers. Filter on a whitelist of numeric concepts at ingest time. Drop or quarantine the rest. Estimated 30-50% row reduction.
- **Why this matters**: half the DB. Even a 20% cut here = 4+ GB.

### 4.2 Filing events (`filing_events`)

- **Raw shape**: per-accession manifest entry with `filing_date`, `filing_type`, `source_url`, `primary_document_url`, `items` (8-K item codes), `raw_payload_json`. **Single table holds metadata for every SEC filing_type** — 10-K, 10-Q, 8-K, Form 3/4/5, DEF 14A, 13F-HR, 13D/G, N-PORT, N-CSR, N-CEN, Form D, Form 144, NT 10-Q, S-1/3/4/8/11, 424B, etc.
- **Current volume**: 5.79M rows, 4.3 GB, 1993–2026.
- **Half-life**: **slow** for navigation (drilldown link source); fast for "recent activity" displays.
- **Consumers**: drilldown links from chart pages; AI thesis recent-events context; audit trail; 8-K event timeline chart (filtered view, not a separate table).
- **Ingest depth cap (proposed)**: **10y** rolling window applied uniformly across every filing_type. The "8-K 2y for chart" referenced in §5.1 is a **query filter**, not a separate retention cap (Codex 1a §1 — earlier draft contradicted itself by treating 8-K as a separate retention shape).
- **Retention horizon cap (proposed)**: **10y hot**, pre-2016 → drop. Operator re-ingests older slices on demand via `POST /jobs/sec_rebuild/run` with `since=<date>`.
- **Raw payload**: `raw_payload_json` should be dropped from hot table after first parse; payload retention belongs in #1014 raw-payload retention slice. Estimated 40-60% size reduction on its own.
- **Why this matters**: 4.3 GB. Drop pre-2016 = ~60-70% row reduction; payload-strip = additional 40-60% per-row reduction; ~3 GB saved.

### 4.3 Form 4 (insider transactions, `insider_transactions`)

- **Raw shape**: per-filing record of insider transaction (officer/director/10%+ holder) — date, shares, price, direction (buy/sell), insider role.
- **Current volume**: 7,777 rows, 3.7 MB. **Under-ingested** — should be hundreds of thousands at universe scale × full history.
- **Half-life**: **fast** — last 90d is the alert signal; last 12mo is the thesis signal; 5y old = decoration.
- **Consumers**: insider buy/sell timeline chart; AI thesis "insiders bought $X in last 90d"; future cluster-buy alerts. **`ownership_insiders_current`** (29 MB) is the cumulative post-transaction holdings rollup derived from observations.
- **Ingest depth cap (proposed)**: **3y** from today, per CIK.
- **Retention horizon cap (proposed)**: **3y hot** on `insider_transactions` + `ownership_insiders_observations`, pre-3y → drop.
- **Codex 1a §6 — cumulative ownership concern**: dropping pre-3y observations is safe **only if** `ownership_insiders_current` is maintained as a write-through cumulative ledger (not recomputed from observations on each refresh). Verification step in PR4: confirm `ownership_insiders_current` survives an observations truncate; if it recomputes from observations, freeze cumulative balance at the 3y boundary as an opening position so dropping older rows is non-destructive.
- **Cohort bound**: only ingest for `is_tradable=TRUE` instruments. Form 4 for delisted = noise.
- **Why this matters**: not size, but ingest-budget — universe-wide × 33y Form 4 ingest is the multi-day cost. 3y cap = ~90% reduction in candidate set.

### 4.4 Form 3 / Form 5 (initial / annual insider summary)

- **Raw shape**: registry entry of insider-company pair (Form 3 = initial filing, Form 5 = annual catch-up of any missed Form 4s).
- **Current volume**: low (no dedicated table for Form 3 visible; data threaded into insider_transactions).
- **Half-life**: **latest only matters** — Form 3 is the initial filing, supplanted by ongoing Form 4s; Form 5 is annual catch-up.
- **Consumers**: insider registry table (who is registered as an insider for this issuer).
- **Ingest depth cap (proposed)**: **latest per insider-company pair**.
- **Retention horizon cap (proposed)**: keep latest; drop older — they add no signal beyond what's already in Form 4 history.

### 4.5 13F-HR institutional holdings (`institutional_holdings`, `ownership_institutions_observations`)

- **Raw shape**: per-filer quarterly snapshot of all 13F-reportable positions (>$100M AUM filers). Holdings: CUSIP, value, shares, put/call.
- **Current volume**: 105k raw holdings; 3.86M observations rows; 2.5 GB obs + 2.8 GB current = **5.3 GB combined**.
- **Half-life**: **medium** — 4-quarter trend matters for momentum; 8 quarters (2y) for backtests; beyond = decoration.
- **Consumers**: stacked institutional ownership % chart; concentration metric in ranking; AI thesis "Vanguard increased position by 8%".
- **Ingest depth cap (proposed)**: **8 quarters (2y)** observations + always-current snapshot.
- **Cohort bound**: **already done #1010** — `last_13f_hr_at` 380d recency cap on filer cohort (11,205 → 8,681 filers).
- **Retention horizon cap (proposed)**: **8 quarters hot** in observations; current snapshot always. Pre-8q → drop (per §6.3 no same-DB cold archive).
- **`ownership_institutions_current` size oddity**: 2.8 GB is huge for a "current snapshot" — investigation needed. Either stores wide rows with embedded payload, or write-through is dumping more than current state. Separate audit ticket.
- **Why this matters**: combined 5.3 GB. Cut to 2y observations = ~2-3 GB saved (depending on what `current` really stores).

### 4.6 N-PORT fund holdings (`ownership_funds_observations`)

- **Raw shape**: same shape as 13F-HR but for mutual fund filers (vs institutional advisers). Quarterly per fund.
- **Current volume**: 3.68M obs rows, 1.6 GB obs + 2.5 GB current = **4.1 GB combined**.
- **Half-life**: same as 13F.
- **Consumers**: funds slice of institutional ownership chart; potentially fold into 13F view if redundant.
- **Ingest depth cap (proposed)**: **8 quarters** same as 13F.
- **Cohort bound**: same recency-based filter pattern as #1010, applied to N-PORT filer registry.
- **Retention horizon cap (proposed)**: **8 quarters hot**, current snapshot always.
- **Why this matters**: similar to 13F — combined 4.1 GB; same proportional cut.

### 4.7 DEF 14A blockholders (`def14a_beneficial_holdings`, `ownership_def14a_observations`)

- **Raw shape**: 5%-holders / officers-and-directors table from annual proxy statement. Holder name, shares, percent_of_class, role.
- **Current volume**: 47k raw rows, 17 MB; 40k obs rows, 24 MB; combined ~50 MB.
- **Half-life**: **slow state, but only LATEST matters** — DEF 14A is the annual snapshot; the prior year's snapshot is decoration.
- **Consumers**: top-5-holders pie chart; AI thesis "Top 5 institutional holders are…"; executive-comp slice (separate epic).
- **Ingest depth cap (proposed)**: **latest 2 proxies per filer** (current + one prior for change tracking).
- **Retention horizon cap (proposed)**: **latest 2 proxies hot**; older → drop. Current snapshot always.
- **NUMERIC overflow bug**: #1228 affects this source. Fix lands before clean re-run.
- **Why this matters**: not storage (small); ingest-budget. DEF 14A is HTML scrape — 1h per pass with deadline cap; 5y of proxies × 5,174 filers = un-drainable in one pass. 2-proxy cap = ~80% reduction.

### 4.8 13D/G blockholders

- **Raw shape**: per-filing notice when an entity crosses 5% ownership threshold or files an exempt declaration.
- **Current volume**: 0 ingested (table exists; pipeline not yet active).
- **Half-life**: **fast for new-filing alert**, **slow for current state** (current 13D/G filers = decoration table).
- **Consumers**: top concentrated holders panel; AI thesis "new blockholder X filed Y ago"; future alert on new 13D crossing.
- **Ingest depth cap (proposed)**: **3y historical**, current state always.
- **Retention horizon cap (proposed)**: **3y hot**, older → drop.

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
- **Ingest depth cap (proposed)**: **latest 10-K per CIK only**.
- **Retention horizon cap (proposed)**: keep latest; drop prior.

### 4.11 Treasury / ESOP / blockholder slices

Aggregated under DEF 14A discovery (treasury share counts, ESOP plan holdings, blockholder identities). Small volumes (3-30 MB each).

- **Half-life**: slow-state.
- **Consumers**: capital structure panel; buyback / dilution context.
- **Ingest depth cap**: latest 2 proxies per filer (same as DEF 14A).
- **Retention horizon cap**: latest 2 hot.

### 4.12 N-CSR / N-CSRS (fund certified shareholder reports)

- **Raw shape**: registered fund trust annual + semi-annual reports. Per-trust filings with portfolio holdings appendix.
- **Current volume**: not yet exercised at universe scale in this drive. Ingest path lives in `app/jobs/sec_first_install_drain.py:512-815` (bootstrap_n_csr_drain). Existing `horizon_days=730` (2y) cap already in code.
- **Half-life**: **medium** — funds report semi-annually; 4 semi-annual snapshots = 2y of position changes per trust.
- **Consumers**: funds slice augmentation (N-PORT alone misses some trusts that file only N-CSR); AI thesis context for fund-held instruments.
- **Ingest depth cap (proposed)**: **730 days (2y) — already in code**, retain as-is.
- **Retention horizon cap (proposed)**: **2y hot** matching ingest; older → drop.
- **Cohort bound**: fund trusts only (sourced from `cik_refresh_mf_directory`). Issuer-scoped seed excludes N-CSR per `sec_first_install_drain.py:167`.

### 4.13 N-CEN (annual fund census, classification only)

- **Raw shape**: annual N-CEN filing per registered investment company. Contains `investmentCompanyType` field SEC uses to classify the filer (open-end fund, closed-end fund, UIT, ETF, …).
- **Current volume**: small; one row per investment-company CIK per year.
- **Half-life**: **slow** — classification updates annually; latest N-CEN per CIK is sufficient.
- **Consumers**: `app/services/ncen_classifier.py` filer-type classification feeds 13F-HR vs N-PORT routing; influences institutional vs funds ownership lane decision.
- **Ingest depth cap (proposed)**: **latest N-CEN per CIK only**.
- **Retention horizon cap (proposed)**: keep latest; drop prior. No value in N-CEN history beyond current classification.
- **Why this matters**: small storage, but **load-bearing for filer classification** — Codex 1a caught the omission. Spec must acknowledge this source even though it's lightweight.

### 4.14 Metadata-only forms (Form D, Form 144, NT 10-Q, S-1/3/4/8/11, 424B)

These SEC filings are **discovered via the submissions manifest** and persisted as `filing_events` rows with no parser-derived observation table. Codex 1a §9 — earlier draft was silent on them; explicit acknowledgment here.

- **Raw shape**: index entry only (`filing_type`, `filing_date`, `source_url`).
- **Half-life**: varies — Form 144 last 90d is the insider-sale-intent signal; S-1/424B are one-shot per offering; NT 10-Q is a late-filing notice (90d window).
- **Consumers**: drilldown links; AI thesis "recently filed S-1" context; future Form 144 alert (intent-to-sell signal complementing Form 4 actuals).
- **Ingest depth cap**: covered by `filing_events` 10y.
- **Retention horizon cap**: covered by `filing_events` 10y.
- **No separate parser retention required** unless a future ticket adds an observation table (e.g. Form 144 intent-to-sell extraction for the alerts epic — out of scope here).

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

### 6.3 Drop, don't archive (Codex 1a §3 revision)

Earlier draft proposed same-DB `*_archive_pre_NNNN` cold tables. Codex 1a flagged: same-DB archive doesn't reduce DB size, contradicting the storage-target acceptance.

**Decision**: pre-cap rows **drop** in v1. No same-DB cold archive. If a backtest later demands pre-cap depth, operator re-ingests via `POST /jobs/sec_rebuild/run` with explicit `since=<date>` horizon override — SEC bulk endpoints are the source of truth; we don't need to be the archive.

Future epic: if cold-archive becomes desirable, ship as **separate database** or **S3 parquet snapshots**, not same-DB tables. Out of scope for this spec.

### 6.4 Two-layer storage (current + observations)

Per `#788` decomposition, keep the two-layer model:

- **`*_current`** = write-through latest snapshot, optimised for "latest state of X" reads.
- **`*_observations`** = append-only historical observations, partitioned by quarter.

Caps apply to **`*_observations`**; `*_current` is always latest.

**Open question**: `ownership_institutions_current` is 2.8 GB — far larger than expected for "current snapshot". Either stores wide rows with embedded payload, or write-through dumps more than latest. **Separate audit ticket.** Not blocking this spec.

## 7. Implementation sequence

Land per-source PRs in this order, each gated by Codex 1a/1b + bot review:

1. **PR1 — Cross-cutting (#1233 §2 + §3).**
   - Populate `instruments.country` from universe sync.
   - Audit + add `is_tradable=TRUE` filter to every SEC stage entry point.
   - Lint guard: CI grep `INSERT INTO instruments` must include `is_tradable`.

2. **PR2 — Companyfacts XBRL concept whitelist + 20y depth cap.**
   - Whitelist of ~50 numeric XBRL concepts (revenue, EPS, margins, …).
   - Drop or quarantine non-whitelisted rows at ingest.
   - 20y depth cap on bootstrap entry (`period_end >= NOW() - 20y`).
   - Pre-20y rows → drop (no same-DB archive per §6.3).

3. **PR3 — Filing events 10y hot + payload strip.**
   - 10y depth cap on hot table.
   - Pre-10y rows → drop (no same-DB archive per §6.3).
   - Drop `raw_payload_json` from hot table (#1014 raw-payload retention).

4. **PR4 — Form 4 3y depth cap + cumulative-rollup verification.**
   - **Verification step FIRST** (Codex 1a §6 / 1b PARTIAL): confirm `ownership_insiders_current` is maintained as a write-through cumulative ledger, NOT recomputed from `ownership_insiders_observations` on each refresh. If recomputed, freeze opening cumulative balance at the 3y boundary as a write-through anchor row so dropping older observations is non-destructive. Acceptance fails until this verification is recorded in the PR description.
   - 3y depth cap on `insider_transactions` ingest.
   - Drop pre-3y rows from `insider_transactions` + `ownership_insiders_observations`.
   - Recency cohort bound on Form 4 sweep (mirror of #1010 pattern).

5. **PR5 — DEF 14A latest-2-proxies cap + #1228 NUMERIC fix.**
   - Bundle the NUMERIC overflow fix from #1228.
   - Latest-2-proxies-per-filer cap on ingest.
   - Drop older from `def14a_beneficial_holdings` + `ownership_def14a_observations`.

6. **PR6 — 13F-HR 8-quarter observations cap.**
   - 8-quarter depth on `ownership_institutions_observations`.
   - Drop pre-8q rows; `current` snapshot always.
   - (`#1010` cohort bound already in place.)

7. **PR7 — N-PORT 8-quarter cap (mirror of PR6).**
   - Same shape applied to funds.
   - Add N-PORT recency cohort bound (`last_nport_at`) per #1010 pattern.

8. **PR8 — N-CSR/N-CSRS retain existing 2y horizon + observations cap.**
   - Validate existing `horizon_days=730` in `bootstrap_n_csr_drain`.
   - Document in spec; no code change unless audit finds drift.

9. **PR9 — N-CEN latest-only.**
   - Drop pre-latest N-CEN per CIK.
   - Verify `ncen_classifier` reads from latest-only, not history.

10. **PR10 — Form 3/5 latest-only + business summary latest-only.**
    - Form 3/5: keep latest per insider-company pair.
    - Business summary: drop prior; latest 10-K only.

11. **PR11 — 13D/G 3y historical activation.**
    - Activate the dormant 13D/G ingest pipeline.
    - 3y depth cap; current state always.

12. **PR12 — `ownership_*_current` size audit + remediation.**
    - Investigate why `ownership_institutions_current` is 2.8 GB + `ownership_funds_current` is 2.5 GB. Either truncate-narrow the rows, or move bulky columns to observations.
    - Cross-cutting; runs after PR6+PR7 land.

After PR1-PR12 land + bootstrap drain re-runs cleanly under the new caps, **wipe + clean re-run** to validate. Measure end-to-end wall-clock + final DB size.

## 8. Acceptance

Clean re-run after caps land (revised per Codex 1a §2 + §7):

1. **Wall-clock**: full bootstrap drain (every bootstrap stage including N-CSR S25 fund-trust drain) completes in **a single business day (10-12h)** from fresh DB (was multi-day pre-caps).
   - **Why not < 8h**: SEC 10 req/s budget is shared across DEF14A HTML, Form 4, 13D/G, N-PORT, N-CSR, companyfacts per-CIK fetches. Even with caps, the per-accession candidate set remains large (5,174 US filers × multi-source + fund-trust universe for N-CSR). 10-12h is the realistic floor; sub-10h is a Phase 2 optimization (parallel SEC fetcher pools, CDN cache warm-up).
2. **DB size**: post-clean-rerun `pg_database_size('ebull')` measured (excludes WAL — Postgres WAL lives outside per-database size accounting; tracked separately at the filesystem level via `pg_stat_wal` / `pg_ls_waldir()` if needed). Provisional breakdown:
   - `financial_facts_raw`: current 16.4M rows / 23 GB. Apply (a) 20y rolling cap → keep rows where `period_end >= NOW() - 20y` ≈ 60% of rows survive (proxy: 20y/33y); (b) whitelist of ~50 numeric concepts → assumed 50% row reduction inside the surviving 20y slice. Net: 16.4M × 0.60 × 0.50 ≈ 4.9M rows × ~1.5 KB/row (raw + tuple header + partitions) ≈ **~7 GB** including indexes + partition overhead.
   - `filing_events`: current 5.79M rows / 4.3 GB. Apply 10y cap → ~30% rows survive (10y/33y); apply payload strip → row width drops ~50%. Net: 5.79M × 0.30 × 0.50 × ~600 B/row ≈ **~0.5 GB**; **~1 GB with indexes**.
   - `ownership_institutions_current` + `ownership_funds_current`: **conditional on PR12 audit** — current 5.3 GB combined. Post-PR12 (audit + remediate wide-row writes) assumed ≤ 1 GB combined.
   - `ownership_institutions_observations` + `ownership_funds_observations` (8 quarters): current 4.1 GB combined across all-time partitions. 8-quarter cap retains 8 of ~64 partitions populated → ≈ 4.1 GB × 8/64 ≈ **~0.5 GB**.
   - `ownership_insiders_observations` (3y from current 316 MB across ~64 partitions): 316 MB × 12/64 ≈ **~60 MB**.
   - DEF 14A + treasury + ESOP + blockholders + N-CSR + N-CEN + raw insider tables: **~150 MB** combined.
   - Indexes + bloat overhead: ~20% headroom on top of base table sizes.
   - **Honest totals**:
     - **PR12 unsolved**: 7 + 1 + 5.3 + 0.5 + 0.06 + 0.15 ≈ **14 GB raw**, **~17 GB with 20% overhead**.
     - **PR12 solved**: 7 + 1 + 1 + 0.5 + 0.06 + 0.15 ≈ **9.7 GB raw**, **~12 GB with 20% overhead**.
   - Operator acceptance tiers (all measured AFTER the post-PR1-PR12 wipe + clean re-run — the wipe gate per §7 is unconditional on PR1-PR12 having merged; the *tier* selected for acceptance depends on what state PR12 reached):
     - **v1 bar — `<20 GB hot`**: PR12 merged with audit complete but remediation partial / deferred (current snapshots remain near 5.3 GB combined). ~57% reduction from current 43 GB. Acceptable for v1 ship.
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
- **Wipe gating**: clean re-run gate per §7 specifies PR1-PR12 must all land before wipe. Acceptance §8 is measured AFTER the clean re-run, so both tiers are evaluated against the same post-wipe state — no ambiguity in the gate.
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

## 12. Handover for next session (impl plan kickoff)

```text
Pick up the impl-plan write-up for the data retention rubric spec at
docs/superpowers/specs/2026-05-19-data-retention-rubric.md.

Spec is post-Codex (1a + 1b + 1c + 1d, all clean). After spec PR
merges, write the impl plan covering
PRs 1-12 with per-PR scope + LOC estimate + Codex gate + acceptance
per PR.

Wipe + clean re-run gated on PR1-PR12 landing.

FIRST ACTIONS:
1. Read CLAUDE.md working order + this spec end-to-end.
2. Confirm #1233 + #1228 + #1234 still OPEN.
3. Branch `feature/1233-data-retention-rubric-spec` is already created
   (this session's branch).
4. If Codex 1b not yet run: run it. Else: commit spec; push; create PR;
   iterate to APPROVE; merge.
5. Next session = impl plan.
```
