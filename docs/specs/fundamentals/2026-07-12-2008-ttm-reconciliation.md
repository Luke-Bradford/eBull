# #2008 — TTM reconciliation: fundamentals_snapshot vs instrument_valuation

**Status:** spec. **Issue:** #2008. **Precedents:** #682 (fy/fp comparative re-stamp), #1835 (frame is not a period key), #1823 (restated comparative in same bucket), #1839 (non-adjacent period guard), #558 (DEI as-of lift).

## Problem

Issue-reported: 16/48 quotes-gated overlap names disagree >25% on `revenue_ttm`
between the latest `fundamentals_snapshot` row and `instrument_valuation`.

**Full-population re-measure (dev, 2026-07-12):** 3,169 names have snapshot
revenue > 0 AND a complete-TTM view row; **1,399 (44%) disagree by >25%**.
Additionally **749 of 4,065 `is_complete_ttm=TRUE` windows carry
`revenue_ttm = NULL`** silently. Both sides are wrong, in different ways.

## Root causes (all empirically confirmed 2026-07-12)

### Path A — `fundamentals_snapshot` (SEC companyfacts JSON-side selection, `app/providers/implementations/sec_fundamentals.py`)

- **D1 — first-tag-wins across tag migration.** `_get_entries` (:1051) returns
  entries for the FIRST revenue tag having any entries. Issuers migrated tags
  (ASC 606 adoption: `Revenues`/`SalesRevenueNet` →
  `RevenueFromContractWithCustomer*`, 2018-19; later some switched
  Excluding↔Including AssessedTax). A dead tag's stale history shadows the
  live tag. Live-companyfacts receipts: NVDA picks `RFCWC-ExcludingAssessedTax`
  whose entries END 2022-01-30 → snapshot 26,914M = **FY2022 annual** (live tag
  `Revenues` runs to 2026). Same class: TER (FY2020), FIX (FY2020),
  NWPX (FY2017 via `SalesRevenueNet`), PKE (FY2023), GE (partial-population tag).
- **D2 — `_ttm_from_quarters` (:1108) unsound.** No dedup by period (same
  quarter counted twice via comparative re-reports: MU → 88.26B vs true ≈79B);
  no recency bound and no adjacency check (AMSC: sums four quarters from
  **2017-2018** = 56.55M, presented as current TTM).
- **D3 — annual preferred over fresher quarters.** `_get_ttm_value` (:1140)
  returns the latest 10-K annual even when up to 3 fresher quarters exist —
  up to ~15 months stale; on fast growers this is >2x (MU: FY-Aug-2025 37.4B
  vs true TTM ≈79B).
- **D4 — `as_of_date` lies.** `_determine_as_of_date` anchors on the CASH
  tag's latest instant — unrelated to the revenue period actually selected.
  NVDA's FY2022 revenue carries as_of 2026-04-26; GE's latest row sits at
  as_of 2017-12-31. Violates settled decision "as_of_date means financial
  statement period end date" (docs/settled-decisions.md §Fundamentals
  snapshot semantics) because the flows do not belong to that statement.

### Path B — `instrument_valuation` ← `financial_periods_ttm` ← `financial_periods`

- **D5 — pre-#1835 derivation rot, no backfill.** Old YTD-disambiguation
  required `frame IS NOT NULL`. SEC frames are a cross-sectional
  last-filed comparability tag: a later comparative/8-K re-report TAKES the
  frame, and the original quarter fact (fy/fp-stamped, frame→NULL) got
  dropped → flow columns NULLed on re-derivation. #1835 (`5737b256`) fixed
  the filter but shipped **no re-derivation backfill**;
  `daily_financial_facts` normalizes only touched CIKs, so every quiet
  instrument keeps rotten rows (CAT Q1-Q3'25 revenue NULL while
  `financial_facts_raw` holds correct 90-day facts, fy/fp intact).
- **D6 — NULL-blind TTM aggregation (structural).** `financial_periods_ttm`
  (sql/032:209) does `SUM(revenue)` over the latest-4 quarter rows;
  SQL `SUM` skips NULLs and `is_complete_ttm = COUNT(*)=4` counts ROWS.
  A window with NULL members yields a 1-3 quarter sum labeled complete:
  CAT view 17,415M = exactly its one non-NULL quarter; AMZN 529,390M =
  exactly 3 quarters. Also no adjacency check on the 4 rows (same class as
  #1839).

**Confirmation of the heal path:** scoped `normalize_financial_periods` re-run
on CAT+AMZN with current code → CAT 70,755M (= 16,569+17,638+19,133+17,415,
exact), AMZN 742,776M (Q4'25 derivation restored); both now within 5% of
their (legitimate) FY-annual snapshot values.

## Source rules

- **SEC Frames API label:** cross-sectional "one fact per filer, last filed"
  comparability tag (data.sec.gov/api/xbrl/frames) — NOT a per-issuer period
  key. Any selection that keys on it rots as re-reports steal the label
  (#1835, `.claude/skills/data-sources/sec-edgar.md`).
- **fy/fp re-stamping:** every comparative fact in a filing carries the
  FILING's fy/fp (#682) — grouping must canonicalise on `period_end`.
- **Tag migration:** revenue concept moved with ASU 2014-09 / ASC 606
  adoption; the only sound cross-tag rule is per-period recency-based
  selection (what `_derive_periods_from_facts` + `_TAG_TO_COLUMN` priority
  already do), never "first tag with any entries" globally.
- **TTM window:** trailing twelve months = 4 *adjacent* fiscal quarters
  ending at the anchor; quarter duration window [60,120]d, annual [335,395]d
  (settled, `_FLOW_DURATION_DAYS`, #1835). Non-adjacent windows are not TTM
  (#1839). **Adjacency guard = window span (newest_end − oldest_end) ≤ 330d**
  — the repo-settled bound with worked rationale (consecutive windows span
  ~273-275d even on 53-week calendars; one missing quarter pushes the 4-row
  window to ~364-365d; 330 separates cleanly):
  `app/services/fcf_yield.py::_QUARTERLY_SQL` + its spec
  `docs/specs/fundamentals/2026-06-26-fcf-yield-trend.md`. This spec adopts
  the SAME 330d bound everywhere (view + snapshot derivation) and both must
  stay in sync.
- **SQL:** `SUM` ignores NULL members — completeness must be asserted
  per column (`COUNT(col)`), not per row.
- **Capex NULL→0 in FCF** is the repo-settled treatment (sql/201 `fcf_ttm`,
  `fcf_yield.py::_QUARTERLY_SQL`, legacy provider `_build_latest_snapshot`)
  — capex omitted-when-immaterial is common XBRL practice; OCF itself stays
  strict 4/4. Kept for consistency (a differing snapshot treatment would
  manufacture snapshot-vs-view FCF drift).
- **`debt` = total debt** (`COALESCE(long,0)+COALESCE(short,0)`, NULL when
  both NULL) — matches the repo-settled convention in
  `financial_periods_ttm`-derived ratios (sql/032/201 `debt_equity_ratio`,
  `enterprise_value`). Conscious change from the legacy provider's
  `LongTermDebt`-only feed; consumers (scoring `debt`/`net_debt`, thesis
  block B) get a consistent, slightly higher figure.
- **Filing deadlines** (freshness windows below): Form 10-Q due 40/40/45
  days after quarter end by filer category (Exchange Act Rule 13a-13,
  Form 10-Q General Instruction A.1); Form 10-K due 60/75/90 days
  (Form 10-K General Instruction A.2).
- Official-source anchors for the ingest rules above: SEC Frames API doc
  (data.sec.gov/api/xbrl/frames — aggregates "one fact for each reporting
  entity that is last filed" per calendrical period → the label migrates to
  the most recent re-report); EDGAR companyfacts stamps `fy`/`fp` with the
  FILING's fiscal context on every fact incl. comparatives (empirically
  pinned in #682); FASB ASU 2014-09 (Topic 606, effective fiscal years
  beginning after 2017-12-15 for public entities) drove the
  `Revenues`/`SalesRevenueNet` → `RevenueFromContractWithCustomer*` tag
  migration wave.

## Design

**Question-the-model outcome:** Path A is a second, worse implementation of
period selection that the normalized pipeline already does correctly (with
the #682/#558/#1835/#1823 lessons baked in), fed by a redundant full
companyfacts HTTP sweep. Fix = make `fundamentals_snapshot` DERIVED from
`financial_periods` (single source of truth); retire the JSON-side selection.

### 1. Snapshot write-through (code)

New `_write_snapshots_from_periods(conn, instrument_id)` called inside
`normalize_financial_periods` after the canonical merge (same per-instrument
transaction, mirrors the treasury write-through step). For every quarter
anchor (`period_type IN (Q1..Q4)`, `superseded_at IS NULL`,
`normalization_status='normalized'`), upsert one snapshot row:

- `as_of_date` = `period_end_date` (restores the settled anchor semantics).
- Flow TTM fields (`revenue_ttm`, and the inputs to `gross_margin`,
  `operating_margin`, `fcf`, `eps`) = strict sum over the 4 trailing
  quarters ending at the anchor — present only when all 4 rows exist,
  are adjacent (window span ≤ 330d, the settled fcf_yield bound — see
  Source rules), and the column is non-NULL in all 4.
  `fcf = operating_cf_ttm − |capex_ttm|` (capex NULL→0 per settled
  treatment — see Source rules; OCF strict 4/4);
  `eps` = 4-quarter `eps_diluted` sum.
- Balance-sheet fields from the anchor row: `cash`, `debt` = total debt
  per Source rules (NULL when both components NULL),
  `net_debt = debt − cash` (both present), `shares_outstanding`,
  `book_value = shareholders_equity / shares_outstanding`.
- Rewash: DELETE the instrument's snapshot rows not in the anchor set
  (purges legacy cash-anchored dates carrying garbage — NVDA class).

Retire: `refresh_fundamentals`, `refresh_fundamentals_history`,
provider snapshot builders (`get_latest_snapshot*`, `get_snapshot_history*`,
`_build_latest_snapshot`, `_build_history_snapshots`, `_get_ttm_value`,
`_latest_annual_value`, `_ttm_from_quarters`, `_latest_point_in_time`,
`_determine_as_of_date`, `_fundamentals_are_fresh`, snapshot tag tuples) and
the two sweep call sites (`daily_research_refresh` scheduler.py:2839,
`fundamentals_sync` :4175). Freshness now rides `daily_financial_facts`
(touched-CIK normalize). `FundamentalsProvider` protocol methods with no
remaining implementor/caller are deleted (grep-verify at impl; FMP surfaces
dropped in sql/080).

### 2. TTM view hardening (sql/220)

Recreate `financial_periods_ttm`:

- **strict flow columns** (statement-core / recurring lines: revenue,
  cost_of_revenue, gross_profit, operating_income, net_income,
  research_and_dev, sga_expense, depreciation_amort, interest_expense,
  income_tax, sbc_expense, operating_cf, investing_cf, financing_cf,
  eps_basic_ttm, eps_diluted_ttm):
  `CASE WHEN COUNT(*)=4 AND (MAX(period_end_date)-MIN(period_end_date)) <= 330 AND COUNT(col)=4 THEN SUM(col) END`
  — window-shape guard INSIDE every flow CASE (Codex ckpt-1: readers that
  do not gate on `is_complete_ttm` must not see non-adjacent sums), plus
  per-column strictness (absence of a core line = extraction gap, not 0);
- **sporadic flow columns** (absence means "did not occur": capex,
  dividends_paid, dps_declared, buyback_spend — issuers emit these facts
  only in quarters where the event happens; a strict 4/4 rule would NULL
  a legitimate single-quarter special-dividend TTM, and capex NULL→0 is
  already settled per Source rules): window-shape guard only, summed over
  present members;
- `is_complete_ttm = COUNT(*)=4 AND (MAX(period_end_date)-MIN(period_end_date)) <= 330`
  (adjacency, #1839 class, 330d bound per Source rules);
- stock/latest columns unchanged. Column list/order/types identical to
  sql/032 → `CREATE OR REPLACE`; the dependent `instrument_valuation`
  (sql/201) reads through unchanged (#1664 suppression preserved).

### 3. Backfill (executes with the PR, not queued)

One full-universe `normalize_financial_periods(conn)` run (heals D5 rot AND
writes all snapshot anchors via step 1). Script-invoked
(`scripts/backfill_2008_ttm.py`, thin wrapper), runs on dev before merge;
figures recorded on the PR. The script ALSO purges snapshot rows for
instruments absent from `financial_facts_raw` (provider-era garbage with
no periods backing and no future writer — Codex ckpt-1 High). Environment
note: dev IS the deployment today (demo-first, single dev DB); fresh
environments self-heal because `fundamentals/bootstrap.py` already runs
full-universe `normalize_financial_periods` (snapshots ride along).

### 4. Standing invariant

`scripts/dq_audit.py` gains `ttm_snapshot_view_mismatch`: count of names
where latest snapshot `revenue_ttm` and complete-TTM view `revenue_ttm`
(both present) differ >25%, plus count of `is_complete_ttm` windows with
NULL `revenue_ttm` whose member rows all carry revenue (rot detector).
Expected steady-state: 0 / 0.

**Write-time flag (issue ask) — REBUTTED-as-specified:** with a single
writer deriving snapshot FROM the view's own source table, a write-time
cross-check of the two paths is tautological. The invariant that can still
break (regression: someone reintroduces a second writer / edits one side)
is exactly what the dq_audit check catches.

## Blast radius / consumers (verified by grep)

- `scoring.py:1126` (quality family, LIMIT 5 history) + `:1221` presence
  gate; `thesis.py:637` (context block B, LIMIT 5); `api/instruments.py:3505`
  (research tab) — read shapes unchanged, data corrected. **No scoring code
  change → no model_version bump** (data fix; append-only score history
  absorbs it on next compute_rankings).
- `instrument_valuation` legacy CTE (fs fallback) — still functions; its
  inputs are now period-derived.
- `content_predicates.py::fundamentals_content_ok` /
  `freshness.py::fundamentals_is_fresh` currently require a snapshot row
  with `as_of_date >= current_calendar_quarter_start` — **already red on
  dev today under the EXISTING writer** (2026-07-12: all 5,349 SEC-CIK
  tradables "missing"; max as_of universe-wide = 2026-06-30 vs quarter
  start 07-01 — structurally unsatisfiable for up to ~6 weeks every
  quarter). Fix in this PR: replace the calendar-quarter content rule
  with a write-through CONSISTENCY probe — instruments with normalized
  quarter periods must have snapshot rows (the write-through makes them
  atomic; a gap means it broke). The job-liveness audit STAYS on
  `daily_research_refresh` — the job the fundamentals-layer refresh
  adapter dispatches (`sync_orchestrator/adapters.py::refresh_fundamentals`);
  audit-job and refresh-job must stay aligned so a stale layer can clear
  its own liveness (Codex ckpt-2). Snapshot data is produced by the
  scheduled `fundamentals_sync → daily_financial_facts` write-through —
  the same producer/consumer split as pre-#2008 (the snapshot was already
  made by fundamentals_sync phase-1b under the default dedupe flag, not by
  the layer refresh), so no new heal-path regression.
- `ops_monitor.py:117` fundamentals layer probe (`MAX(as_of_date)`,
  3-day threshold) — same structural falsehood (12d stale on dev today).
  Repoint to `SELECT MAX(fetched_at) FROM financial_periods_raw`
  (advances on every daily normalize of touched CIKs — honest pipeline
  liveness), threshold stays 3d.
- `fundamentals_sync` orchestrator stage — repoint to normalize-missing
  instead of sweep (or fold away if the bootstrap stage already covers it;
  decide at impl with the stage's content predicate).

## Verification — RESULTS (dev, 2026-07-12, post-backfill)

- **Full-pop re-scan (mandatory gate):** revenue >25% mismatch
  **1,399 → 0**; complete-TTM windows with NULLed revenue sum
  **749 → 0** (3,047 overlap). `dq_audit` `ttm_snapshot_view_mismatch`
  reads `0 / 0` live.
- **Backfill:** `scripts/backfill_2008_ttm.py --apply` — purged 8 orphan
  snapshot instruments, normalized 5,215 instruments (55,726 canonical
  periods) in 179s; snapshot write-through fired per anchor.
- **Smoke panel:** AAPL/GME/MSFT/HD snapshot == view to the dollar
  (ratio 1.000); JPM NULL both sides (banks report interest-based
  revenue, not the tracked `Revenues`/`RFCWC` tags — unchanged from
  before, honest-absent).
- **Cross-source (stockanalysis.com, quarterly):** NVDA TTM
  **$253,491M** (ours = $253,491M, exact; old snapshot was $26,914M =
  stale FY2022 annual); CAT TTM **$70,755M** (ours = $70,755M, exact;
  old snapshot $67,589M = FY annual, old view $17,415M = one quarter).
- **Tests:** 3 db-tier write-through + 2 db-tier valuation-view = 5
  passed; fast tier 4,919 passed / 14 skipped; smoke 147 passed.

## Verification (Definition-of-done ETL clauses)

- Smoke panel AAPL / GME / MSFT / JPM / HD: latest snapshot row + view row
  agree (≤25%, expect ≈0 gap); figures recorded on PR.
- Cross-source: NVDA + CAT TTM revenue vs one independent source
  (stockanalysis.com / macrotrends), figure + source on PR.
- Backfill executed on dev + full-pop re-scan: **0 unexplained >25%
  mismatches** (explained = documented genuine restatements, per name);
  749-NULL-window count → ~0 (remaining = genuinely missing quarters).
  This gate is mandatory and non-waivable — sample panels are smoke
  only, never the safety signal.
- Operator-visible: `/instruments/{symbol}` research fundamentals +
  `/instrument/AMSC` thesis context no longer cite 56.55M; AMSC next thesis
  regen consumes ≈299M.
- Tests: pure-logic table tests for the strict-window snapshot derivation
  (complete / missing-quarter / non-adjacent / NULL-member / restated dup);
  one db-tier test pinning view strict sums + adjacency flag; existing
  provider-snapshot tests deleted with the code.
