# Fundamentals discrete-quarter D&A coverage — YTD de-cumulation + concept widening (#2036)

Status: proposal (pre-implementation).
Parent: #2021 fvb_v3 (`docs/proposals/valuation/2026-07-15-fair-value-band-ev-ebitda.md` §2 filed this follow-up).
Goal: `depreciation_amort_ttm` (strict sql/220) coverage grows so the fvb `ev_ebitda` leg
grows ~330 → ~1,000 names with **zero band-code changes**. Same mechanism incidentally
recovers `interest_expense_ttm` (+~188) and `income_tax_ttm` (+~216).

## 1. Source rule

1. **10-Q cash-flow statements are YTD-only by regulation.** Reg S-X Rule 10-01:
   interim *income* statements cover "the most recent fiscal quarter [and] the period
   between the end of the preceding fiscal year and the end of the most recent fiscal
   quarter" (17 CFR 210.10-01(c)(2)) — i.e. discrete quarter AND YTD. Interim
   *cash-flow* statements cover ONLY "the period between the end of the preceding
   fiscal year and the end of the most recent fiscal quarter" (17 CFR 210.10-01(c)(3))
   — YTD, no discrete quarter. So every cash-flow-statement XBRL duration fact in a
   10-Q (D&A, operating_cf, capex, dividends_paid, …) spans FY-start → quarter-end:
   Q1 ≈ 91d (YTD ≡ discrete), Q2 ≈ 182d, Q3 ≈ 273d. The discrete quarter is
   recoverable ONLY by subtraction: `Qn = YTD_n − YTD_{n−1}` within one fiscal year.
2. **FY = YTD + Q4** is the already-settled treatment (#682 `_canonical_merge` /
   Q4 = FY − Q1 − Q2 − Q3; prevention-log "Reconstructing a per-period series from SEC
   duration facts… residual-fill it, don't drop the overlapping annual", #1635).
   De-cumulation is the same identity applied at Q2/Q3 instead of only Q4.
3. **D&A concept semantics — from the us-gaap element documentation** (authoritative
   definitions shipped in companyfacts; quoted from `sec_facts_concept_catalog` on dev):
   - `DepreciationDepletionAndAmortization`: "The AGGREGATE expense … that allocates
     the cost of tangible assets, intangible assets, or depleting assets" → TOTAL.
   - `DepreciationAmortizationAndAccretionNet`: "The AGGREGATE NET AMOUNT of
     depreciation, amortization, and accretion … added back to net income when
     calculating cash provided by … operations" → TOTAL (alias-safe).
   - `AmortizationOfIntangibleAssets`: intangibles-only expense → COMPONENT.
   - `Depreciation` (companyfacts doc: depreciation of PP&E) → COMPONENT.
   Which component set reproduces a tagged total is verified empirically on the full
   CY2024 AND CY2023 frames populations (§2.3), not assumed from the taxonomy.
4. **Income statements in 10-Qs carry BOTH discrete-quarter and YTD facts**
   (Rule 10-01(c)(2) above), so income-statement columns normally never need the fill;
   the de-cumulation identity itself (`Qn = YTD_n − YTD_{n−1}`) holds for ANY additive
   duration measure and the fill is fill-only-when-None, so applying it generically
   across `_FLOW_COLUMNS` cannot displace a reported discrete fact. EPS is the one
   non-additive member; it is included under the SAME documented approximation
   precedent as the existing Q4 = FY − ΣQ derivation (<5% typical error, comment at
   `_derive_periods_from_facts` Q4 loop).

## 2. Full-population verification (dev DB + SEC, 2026-07-15)

Cohort = profitable non-financials with `operating_income_ttm` present and
`depreciation_amort_ttm` NULL on strict `financial_periods_ttm` (issue's 671 on
07-15 data; **733** at scan time — member-side input drift, same class structure).

### 2.1 YTD-attribution class — 486/733 recoverable from ALREADY-STORED facts

- `financial_facts_raw` DOES hold the YTD facts (emit path is gated by
  `_ALL_TRACKED_TAGS`, and the two mapped D&A concepts are tracked): 486/733 cohort
  names have mapped-concept D&A facts with 150–290d duration and `period_end` in the
  last 500d. **484/486** have a valid subtraction chain (a same-`period_start` prior
  cumulative 60–120d earlier) — chain-validity verified on the ENTIRE target cohort
  (the population this change exists to fix), not a sample. For the generic fill on
  other columns/issuers the safety argument is structural, not census-based: the fill
  triggers only where the column is None today (can only ADD a value where absence is
  the status quo) and only when anchor + adjacency + unit guards all pass (§3.1).
- AAPL worked check (facts on dev): Q1 90d = 3,080; Q2-YTD 181d = 5,741; Q3-YTD 272d
  = 8,571; FY 363d = 11,698 → de-cumulated Q2 = 2,661, Q3 = 2,830, Q4 (existing FY−ΣQ
  derivation) = 3,127. Sums exactly to FY.
- The blocker is `_derive_periods_from_facts`'s `_FLOW_DURATION_DAYS` guard (#1835):
  Q-labelled facts outside 60–120d are DROPPED before grouping. Correct as a
  mislabel guard; wrong as a discard — the dropped 182d/273d facts are the YTD
  cumulatives the de-cumulation needs.
- Same-mechanism side gains (flow-generic fix), measured over ALL 3,039 names with
  `operating_income_ttm` present (not the D&A cohort): `interest_expense_ttm` NULL on
  2,481, of which 188 have stored recent YTD facts (recoverable); `income_tax_ttm`
  NULL on 708, 216 recoverable. (The remaining interest/tax nulls are concept-coverage
  gaps — out of scope here.)

### 2.2 Concept-map class — 183 names, tags verified by fetching ALL 183 companyfacts

Issue premise partially FALSIFIED: `financial_facts_raw` holds only 78 tracked
concepts (`extract` docstring says "emit all" but every caller passes
`allowed_tags=_ALL_TRACKED_TAGS`), so raw-store absence says nothing about issuer
tagging. Fetched companyfacts for **all 183** no-mapped-concept cohort names
(0 errors), counting D&A-family concepts with USD duration facts (60–400d) ending in
the last 500d:

| concept | names (of 183) |
| --- | --- |
| `Depreciation` | 156 |
| `AmortizationOfIntangibleAssets` | 140 (already tracked → `intangible_amortization`) |
| `FinanceLeaseRightOfUseAssetAmortization` | 62 |
| `DepreciationAmortizationAndAccretionNet` | 24 |
| `OtherDepreciationAndAmortization` | 13 |
| none at all | 1 (ALC — IFRS taxonomy filer) |

MSFT: `Depreciation` + `AmortizationOfIntangibleAssets` + FLROU. GME: mostly
`OtherDepreciationAndAmortization` (excluded v1 — stays da-null, band keeps pe/ps legs).

### 2.3 Component-sum rule — calibrated on the full CY2024 + CY2023 frames populations

SEC frames API (`us-gaap/<concept>/USD/CY{2024,2023}`) — every filer tagging the
`DepreciationDepletionAndAmortization` total plus `Depreciation` (frames = last-filed
per filer, calendar-FY framing; two adjacent years cover time-stability):

| candidate | year | n | median \|res\|/total | within 5% | under >5% | over >5% |
| --- | --- | --- | --- | --- | --- | --- |
| `Depreciation`+`AmortizationOfIntangibleAssets` (both tagged) | 2024 | 1,325 | 0.0% | 67.7% | 23.0% | 9.3% |
| same | 2023 | 1,465 | 0.5% | 67.3% | 21.8% | 10.9% |
| `Depreciation` alone (AmI untagged) | 2024 | 542 | 2.9% | 49.6% | 45.8% | 4.6% |
| same | 2023 | 579 | 3.5% | 52.5% | 42.1% | 5.4% |
| + `FinanceLeaseRightOfUseAssetAmortization` | 2024 | 1,867 | 2.2% | 59.5% | — | — |
| same | 2023 | 2,044 | 2.0% | 59.8% | 25.0% | 15.1% |

Rule fixed by this calibration:
- **Sum = `Depreciation` + `AmortizationOfIntangibleAssets` (when present).**
  `FinanceLeaseRightOfUseAssetAmortization` EXCLUDED — it degrades fit (typically
  already inside the issuer's `Depreciation`).
- **Require `Depreciation` present** to emit the sum (AmI-only would omit all
  depreciation — unbounded understatement).
- Residual tail is understatement-dominant (23%/46% under vs 9%/5% over) →
  EBITDA understated → the fvb target leg reads LOW and the ≤0-conversion fail-closed
  drop catches extremes; the rare overstatement direction (the dangerous one for a
  valuation band) is ≤9.3% of names beyond +5%. Accepted; documented here.

## 3. Fix shape (no schema change)

All in `app/services/fundamentals/__init__.py::_derive_periods_from_facts` +
`app/providers/implementations/sec_fundamentals.py::TRACKED_CONCEPTS`. `financial_periods`
has no new columns; sql/220 and the band read paths are untouched.

1. **Generic YTD de-cumulation (all `_FLOW_COLUMNS`).** When the #1835 duration guard
   drops a Q-labelled duration fact, retain it in a side-map instead of discarding.
   After PeriodRow assembly, for each quarterly row and each `_FLOW_COLUMNS` column
   still None: fill `Qn = cum(end_n) − cum(end_{n−1})` where BOTH cumulatives share
   `period_start` (the FY anchor) AND the same `unit` AND come from the same concept,
   and `end_n − end_{n−1}` ∈ [60, 120]d (one quarter). Cumulative candidates = the
   dropped YTD facts PLUS admitted Q1-duration facts (Q1 YTD ≡ discrete). Per-group
   discipline mirrors the existing canonical rules: period_end == the row's canonical
   end (#682 comparative-restamp guard), concept priority then `filed_date DESC`
   first-write-wins. Fill-only (never overwrites a discrete fact); rows are NOT
   created from YTD data alone (cohort names all have quarterly rows — that's what
   `operating_income_ttm` non-null means). EPS included, same approximation precedent
   as the existing Q4 derivation (§1.4). Existing Q4 = FY − ΣQ derivation then
   completes Q4 with no changes.
2. **Alias widening.** Append `DepreciationAmortizationAndAccretionNet` to
   `TRACKED_CONCEPTS["depreciation_amort"]` as lowest-priority alias (total-semantics
   concept; +24 names).
3. **Component-sum fallback.** `Depreciation` is stored raw-only via a NEW
   `RAW_ONLY_CONCEPTS: frozenset[str]` in `sec_fundamentals.py`, unioned into
   `_ALL_TRACKED_TAGS` but NOT added to `TRACKED_CONCEPTS` — `_TAG_TO_COLUMN` is
   mechanically built from `TRACKED_CONCEPTS` (fundamentals `__init__.py` ~L692), so a
   tuple entry would wrongly enter the `depreciation_amort` priority pick; the
   raw-only split is the load-bearing mechanism (Codex ckpt-1 High #1) and is pinned
   by a test. Internal shape: component values live ONLY in a derive-local
   `dict[(fy, fp), dict[concept, value]]` populated with the same canonical-end +
   filed-DESC + de-cumulation discipline as columns (concept name as pseudo-key);
   never `setattr` on `PeriodRow`, never visible to `_upsert_period_raw`, and
   `Depreciation` facts do NOT anchor period boundaries (excluded from
   `mapped_facts`, which is `_TAG_TO_COLUMN`-driven already). After the fill step,
   when `row.depreciation_amort` is None and the row's `Depreciation` value exists:
   `depreciation_amort = Depreciation + (row.intangible_amortization or 0)`
   (AmI arrives via its existing tracked column, de-cumulated by step 1).
4. Docstring fix: `_extract_facts_from_section` "emit all by default" claim is stale
   (§2.2) — correct it.

Consumers checked: `financial_periods_ttm` (sql/220) strict gate simply starts passing;
`fair_value_band` reads the view; `fundamentals_snapshot` rewrites via the same
normalize path. `_upsert_period_raw`/`_canonical_merge_instrument` see ordinary column
values (no new writer, no ON CONFLICT change).

## 4. Tests (pure-logic, no DB)

Table-driven on `_derive_periods_from_facts` (existing test module pattern):
- AAPL-shaped fixture: Q1 discrete + Q2/Q3 YTD + FY → asserts 2,661/2,830/3,127 exact.
- Comparative-restamp fixture (#682 class): same YTD fact under two fy contexts →
  only the canonical-end row fills.
- Broken chain (missing Q2 YTD) → Q3 NOT filled (no fabrication).
- Anchor mismatch (period_start differs) → no fill.
- Discrete fact present → fill does not overwrite.
- Component sum: Depreciation+AmI; Depreciation-only; AmI-only → None; total-concept
  present → priority pick wins, no sum.
- Alias: DAAccretionNet maps at lowest priority.
- Raw-only invariant: `Depreciation` ∈ `_ALL_TRACKED_TAGS` and ∉ `_TAG_TO_COLUMN`
  (the most likely implementation mistake — Codex ckpt-1).
- Unit guard: YTD pair with mismatched units → no fill.
- `Depreciation`-only facts in a group do not anchor a PeriodRow.
One integration check rides the existing db-tier fundamentals test (no new DB test file).

## 5. Backfill runbook (Definition-of-done clauses 8–12)

Companyfacts is NOT manifest-driven — `sec_rebuild` does not apply. Path:

1. **Jobs-daemon restart FIRST** (operator or verified-safe): nightly
   `fundamentals_sync` re-runs `normalize_financial_periods` with OLD derive code and
   would clobber the correction (#2008 retired-writer class,
   `feedback-retired-writer-restart-before-backfill`).
2. Re-fetch companyfacts for the 183 concept-class names (scoped
   `refresh_financial_facts`; ~183 SEC requests ≈ 20 s at the shared throttle) so
   `Depreciation` / DAAccretionNet facts land in raw.
3. `normalize_financial_periods(conn, None)` full universe (pure DB re-derive,
   idempotent-replacing; ~10–15 min).
4. `fair_value_band` refresh via the daemon job; then re-check the #2022 §6.5
   acceptance gate with the grown ev-leg population (same-day controlled comparison
   only — #2021 lesson: 2-day baselines show phantom leg drift).
5. Verify: smoke panel AAPL/MSFT/HD (+2 cohort names) `depreciation_amort_ttm`
   non-null and `/instruments/{symbol}/financials` renders; cross-source AAPL FY2025
   D&A 11,698M vs stockanalysis/macrotrends; ev-leg count from
   `fair_value_band` legs (expect ~330 → ≥800).

## 6. Out of scope

- `OtherDepreciationAndAmortization` + long-tail concepts (13 names, GME) — revisit
  only if a consumer needs them; noted here deliberately.
- IFRS filers (ALC).
- Interest/tax CONCEPT widening (only their YTD recovery rides along).
- Creating quarterly rows from pure-YTD data (no cohort demand).
