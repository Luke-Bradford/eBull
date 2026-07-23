# DRS + restricted/RSU issuer disclosures (#844, #788 Phase 5)

Status: spec (codex ckpt-1 findings incorporated). Issue: #844. Parent: #788.

## Goal

Two operator-visible overlay metrics on the ownership panel, both issuer-disclosed only:

1. **Unvested award count** (RSU/PSU, company-wide) — memo line below the chart,
   e.g. "151.6M unvested RSUs" (AAPL). Never a pie wedge (RSUs are not
   outstanding until vested); absolute count only (see "DenominatorBasis" note).
2. **DRS / registered-share split** (curated cohort) — overlay chip,
   e.g. "66.2M shares (15%) registered with transfer agent @ 2026-03-18" (GME).

## Source rule

1. **Unvested RSU/PSU counts:** ASC 718-10-50-2(c)(2)(i)/(ii) (the SEC
   taxonomy's own reference for the tag) mandates the nonvested-award
   rollforward in the share-based-compensation note. iXBRL-tagged
   `us-gaap:ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber`
   (uom=shares, instant), dimensioned by
   `ShareBasedCompensationArrangementsByShareBasedPaymentAwardAwardTypeAndPlanNameAxis`
   (FSNDS `segments` label `AwardType=<member>`). Served structurally by DERA
   **Financial Statement and Notes Data Sets** (FSNDS, monthly
   `num.tsv` + `dim.tsv` + `sub.tsv`,
   `https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/{YYYY}_{MM}_notes.zip`).
   Note-level facts are reachable through NO other pipeline we have:
   companyfacts strips dimensional facts (sec-edgar §7.17) and plain FSDS is
   face-statements-only (new gotcha — extracted to sec-edgar skill in this PR).
2. **Award-type axis is non-additive BY NAME:** the axis mixes award *types*
   with *plan names* — a plan member and an award-type member can tag the same
   units. Σ-over-members is therefore forbidden (overlapping-by-design, the
   #1916 class). Read rule below never sums.
3. **Issue premise correction (acceptance 2):** the issue sourced the AAPL RSU
   memo from the "DEF 14A vesting table". Reg S-K Item 402(f) (Outstanding
   Equity Awards at Fiscal Year-End) is **per-NEO only** — no company-wide
   unvested total exists in DEF 14A. Corrected source = 10-K note, per rule 1.
4. **DRS / registered split:** Reg S-K Item 201(b)(1) mandates only the
   *approximate number of holders of record* (10-K Item 5). The
   registered-vs-street **share** split is voluntary narrative — no XBRL tag,
   no mandated location (extraction therefore searches the whole primary doc,
   not an Item-5 anchor). Issuer-disclosed-only with a curated cohort (issue
   scope + original Codex pushback). Absence ≠ zero; non-cohort issuers
   surface no DRS figure at all.
5. **Options excluded from v1** (conscious tradeoff): the source tag covers
   equity instruments *other than* options; options disclose
   outstanding/exercisable (different tags, different dilution semantics).
   Memo copy names the member ("unvested RSUs"), never "all unvested equity".

## Full-population verification

### RSU route selection (probes 2026-07-23, scripts in session scratchpad)

| Route | Full-pop probe | Verdict |
| --- | --- | --- |
| companyfacts non-dimensional | 2,985 / 20,087 CIK files ever carried the tag; only ~460 with latest-end ≥ 2025 (AAPL's non-dimensional tagging stopped 2014) | REJECTED |
| FSDS quarterly `num.txt` | 10 filings (2025q4) / 12 (2026q1) — face-statement product, notes absent | REJECTED |
| FSNDS monthly `num.tsv` | **409 filings (2025_10) / 1,388 (2026_03)**; AAPL 10-K `0000320193-25-000079`: **151,574,000** @ 2025-09-30 (`AwardType=RestrictedStockUnitsRSU`); GME 10-K `0001326380-26-000013`: **2,257,883** @ 2026-01-31 | **ADOPTED** |

### Member-sum additivity (falsified — drives the read rule)

Per (adsh, latest ddate), filings carrying BOTH a default (no-dimension) total
and AwardType member rows: 2025_10 → 11/34 disagree; 2026_03 → **58/91
disagree** (>0.5% relative). Σ-members is unsafe on the full population.

Read-rule coverage under the no-sum policy (latest ddate per adsh, iprx=0,
qtrs=0, uom=shares):

| month | filings | default_total | single_std_member | rsu_member_of_many | suppressed |
| --- | --- | --- | --- | --- | --- |
| 2025_10 | 409 | 55 | 104 | 95 | 155 (38%) |
| 2026_03 | 1,388 | 138 | 577 | 264 | 409 (29%) |

62-71% of tag-bearing filings render with zero fabrication; the rest are
honest absences (multi-member sets with no default and no RSU member, or a
single NON-standard member whose scope is unknowable).

### DRS cohort corpus (34 era-filings fetched + scanned 2026-07-23)

| Issuer | Disclosing | Era | Shape |
| --- | --- | --- | --- |
| GME | every 10-K/10-K/A/10-Q from `0001326380-23-000019` (10-K filed 2023-03-28) through `0001326380-26-000025` (10-Q 2026-06-11) — 15/15 | 2023-03 → present | "approximately X million … held by Cede & Co … and approximately Y million shares … held by registered holders with our transfer agent[, Computershare]" (order varies; per-sentence as-of date present in 10-Qs) |
| AMC | `0001411579-25-000073` (10-Q 2025-11-05), `…26-000016` (10-K), `…26-000051` (10-Q) — 3/3 since start; ZERO in 2017-2025-08 filings | 2025-11 → present | "…were held by N registered holders with our transfer agent and approximately Z … held by Cede & Co …" ("million" sometimes omitted; registered-holder count inline) |

Latest verified figures: GME 66.2M (15%) registered / 382.4M (85%) Cede,
177,522 record holders @ 2026-03-18; AMC ~2.0M (0.4%) registered by 14,021
holders / ~527.5M (99.6%) Cede @ 2026-02-18. The issue's "~75M historically"
(GME) has decayed to 66.2M — every stored row carries `as_of_date`; the read
path serves the latest disclosure, never a remembered constant.

Two extraction landmines the corpus surfaced (both encoded as fixture tests):

- **Decimal split:** "382.4 million" — naive sentence-split on "." severs the
  figure. Extraction runs regex windows over normalized whole-text, never
  sentence-splits.
- **iXBRL word fragmentation:** GME 10-Qs render "approxim ately",
  "sh ares", "o utstandi ng" after naive tag-strip. Normalization replaces
  *inline* tags (span, ix:*, a, b, i, font) with "" and *block* tags
  (p, div, td, tr, br, li) with " " before whitespace collapse.

## Design

### RSU — structured (FSNDS)

- **Bulk download:** add FSNDS monthly archives to
  `sec_bulk_download.build_archives` — rolling 12 months, newest marked
  optional (publication lags month close; mirrors the FSDS #1423 posture).
  Archives deleted after ingest (same lifecycle as FSDS quarterlies).
- **Loader** `app/services/fsnds_notes_facts.py` (sibling of
  `fsds_dimensional_facts.py`): stream `num.tsv`; keep rows with the nonvested
  tag AND `uom='shares'` AND `qtrs='0'` (instant — DERA readme) AND
  `version` prefix `us-gaap/` (DERA versions the taxonomy per row,
  e.g. `us-gaap/2024` — never equality-match) AND `iprx=0` (primary
  presentation; if an
  (adsh, ddate, dimh) has only iprx>0 rows, take the lowest iprx —
  DERA defines iprx as the disambiguator for otherwise-identical facts);
  10-K/10-K/A adsh set from `sub.tsv`; resolve `dimh` against `dim.tsv`
  (preload only hashes seen for the tag); accept the empty-segment (default)
  row and single-axis `AwardType=<member>` cells; reject cross-dimensional
  cells (exact-set rule, same posture as FSDS `_classify_fsds_segments`);
  sibling-CIK fanout (data-engineer fan-out rule).
- **Store:** `instrument_dimensional_facts`. Migration: `axis` CHECK +=
  `'award_type'`, `metric` CHECK += `'nonvested_awards'`; `DimensionalAxis`
  Literal += `'award_type'`; concept→metric + per-route metric maps extended.
  Default-total rows store member_qname/member_label = the domain default
  (`AwardTypeDomain` / label "All award types"). Instant fact
  (`period_start` NULL, `period_end` = ddate). Existing identity index,
  convergence guard + advisory lock reused as-is (the #554 per-filing path may
  later replace bulk rows per accession unchanged).
- **Read rule (no summing, in priority order per instrument's latest 10-K
  accession, `is_subtotal` excluded):**
  1. default-total row present → render it, label "unvested awards";
  2. exactly one AwardType member AND member ∈ standard us-gaap award-type
     member set (`RestrictedStockUnitsRSU`, `RestrictedStock`,
     `PerformanceShares`, `PhantomShareUnitsPhantomStockUnits`,
     `StockAppreciationRightsSARS`, `DeferredStockUnits`) → render with
     member label;
  3. multiple members incl. `RestrictedStockUnitsRSU` → render the RSU member
     alone, labelled "unvested RSUs" (definitionally that member's count;
     scope visible in the label);
  4. else → no memo (honest absence).
  Staleness: accession's `period_end` older than the existing 548-day
  denominator-staleness bound → suppress.
- **Overlay only:** contributes nothing to pie/residual/concentration
  (prevention-log additive-vs-overlay rule).
- **DenominatorBasis note (issue supersession):** the issue's
  `denominator_basis='shares_outstanding_plus_unvested'` predates the current
  `DenominatorBasis` contract (`pie_wedge|institution_subset|proxy_disclosure`,
  `ownership_rollup.py` + API + FE types). The memo renders an **absolute
  count** — no percentage, no denominator claim — so v1 adds NO new basis
  value and touches no existing contract. If a %-of-diluted rendering is ever
  wanted, that's a separate contract migration.

### DRS — curated text

- **Allowlist:** `DRS_DISCLOSURE_CIKS` single-source constant
  ({GME `0001326380`, AMC `0001411579`} at v1); expand as issuers surface.
- **Forms:** 10-K, 10-K/A, 10-Q (corpus: GME's freshest figures are
  quarterly). Manifest-parser hook for allowlisted CIKs only.
- **Extraction:** normalized whole-doc text (inline/block tag rule above);
  regex family covering both corpus shapes (Cede-first / registered-first,
  "million" suffix optional, parenthesised percent optional, optional
  "as of <date>" per sentence, optional inline registered-holder count).
  Captures: registered_shares, registered_pct, street_shares, street_pct,
  holders_of_record, as_of_date (falls back to period end when the sentence
  carries no date — AMC's shape dates the outstanding sentence, not the
  split). Fail-open: no match → no row + one log line, never blocks the
  filing's other parsers.
- **Sanity:** when both sides present, registered + street within 2% of
  the disclosed outstanding (or each side's pct within 3pp of its implied
  share) else drop the row (log, don't guess).
- **Store:** `ownership_drs_observations` — instrument_id (sibling fanout),
  source_accession, form_type, filed_at, as_of_date, registered_shares,
  registered_pct, street_shares, street_pct, holders_of_record,
  parser_version; UNIQUE (instrument_id, source_accession).
- **Read path + staleness policy (ckpt-1 finding 7):** latest row per
  instrument by (as_of_date, filed_at) → `drs` overlay object on the rollup
  payload (shares, pct, as_of_date, accession). Chip renders ONLY when
  `as_of_date` is within **400 days** (annual 10-K cadence + slack); staler
  rows surface only in the category-coverage drilldown as
  "issuer disclosure stale (as of <date>)". A parser miss on a newer filing
  therefore degrades to visible staleness, never a silently-wrong "current"
  figure. Non-cohort instruments: no DRS object at all — no fabricated
  "0 DRS" state. The 5-state coverage banner machine (settled #840/#923) is
  untouched.

### FE

- Memo line (unvested awards) + DRS chip on the ownership panel, reusing the
  existing overlay/memo row pattern. Change-coupled FE-QA: eyeball GME
  (DRS chip + RSU memo) and AAPL (RSU memo, no DRS chip) live pages.

## Backfill

- FSNDS rolling-12 fetch + loader run over each month (10-K rows only) —
  records the universe-overlap figure in the PR (definition-of-done clause 8).
- DRS: run the parser over the full cohort era corpus (GME 15 + AMC 3
  filings) — the corpus IS the full population; hit table goes in the PR.
- Smoke panel (clause 8): GME, AAPL, AMC + MSFT/JPM for
  no-DRS/no-regression checks; operator figure via
  `/instruments/{symbol}/ownership-rollup`.

## Out of scope (unchanged from issue + new)

- Universe-wide DRS inference; Computershare bulk feed; short interest (P6).
- Unvested options (tradeoff above).
- FSNDS `txt.tsv` text facts; 10-Q RSU facts (annual grain matches FSDS).
- Per-NEO vesting detail from DEF 14A (exec-comp owns it).
- Per-member multi-line memo for the suppressed 29-38% (future extension).

## Risks / sizing

- FSNDS monthlies are 100-700MB each; loader streams; disk transient.
- Award-member vocabulary variance is absorbed by the standard-member
  allowlist + suppress default — never a fabricated total.
- GME/AMC may reword — fail-open extraction + 400d staleness policy degrade
  to visible staleness, never a wrong number.
