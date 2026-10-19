# #3903: the valuation view's dividend yield on the total/ME form

**Ticket:** #3903 (remainder item 1; refs #3850, #3841). **Status:** spec v2. Codex checkpoint 1 round 1 raised 30
findings (UNSOUND); "Checkpoint record" gives each disposition.

`instrument_valuation.dividend_yield` divides a per-share dividend sum by a price. The sum crosses share bases and
derived quarters, and it misses most issuers that report dividends only as a cash total. This spec moves the column
onto the #3850 market cap and the TTM cash dividends paid, read only from a window whose four quarters are all
observed. It follows #3911's P/E (`docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`).

## Problem

Figures are printed by `PYTHONPATH=. uv run python scripts/measure_3903_dividend_premise.py` (dev DB, run
2026-10-19 21:26Z, code `c15afb10`, companyfacts.zip SHA-256 file `12bad36a…`). Its database reads share one
REPEATABLE READ snapshot; section numbers refer to its output. The published-cap population fell through the evening
(2,555 rows at 21:03Z, 2,393 at 21:25Z): the candle refresh writes `price_daily_revision` rows, and
`instrument_market_cap_pit` refuses those steps as `basis_stale` until the next build
(`sql/499_market_cap_pit.sql:78-81`, by design). HD, FAST, KO, JPM and MSFT published a cap at 21:03Z and not at
21:26Z. Counts below are the 21:26Z run's; "proposed" is the construction below, evaluated over stored columns
(`proposed_yield`).

sql/506 computes `dividend_yield = dps_declared_ttm / metric_price * 100` when both are positive, in the complete-TTM
arm only (`sql/506_instrument_valuation_pe_total_form.sql:140-142`), and suppresses it for an ADR of unknown ratio
(`:253`). `dps_declared_ttm` sums the window's quarterly `dps_declared` over its present members
(`sql/220_ttm_strict_flow_sums.sql:141-143`). Each member is on its own filing's share basis; eToro's price is
back-adjusted for every split (#3841 "Problem").

1. **Defective per-share members** (section 11). EFC's DPS members are 390, 390, 390, 0.39, and today's view
   publishes a yield of 10,046%. CEG's are 0.3878, −0.7756, 0.4265, 0.4265 (yield 0.17% today; 0.57% proposed);
   PAYC's 0.375, −0.75, 0.375, 0.375.
2. **Payers with no per-share fact.** HD, KO, FAST and O have no `dps_declared` member, so today's view publishes no
   yield, while their stored cash dividends paid are HD $9.221B, KO $10.971B, FAST $1.056B and O $2.996B TTM.
3. **Accuracy against an independent rebuild** (section 5). On the 554 rows where both forms publish and the
   companyfacts rebuild of the common-only TTM resolves, today's yield is within 5% of the rebuilt yield on 358 and
   within [0.8, 1.25] on 430; the proposed yield on 546 and 548.

**Populations** (section 1), all 3,941 complete-TTM rows:

| today | proposed | rows |
|---|---|---|
| published | published | 587 |
| published | NULL: cap refused | 403 (`basis_stale` 167, `no_left_anchor` 146, `no_pit_row` 72, `basis_unresolved` 14, `dual_class` 4) |
| published | NULL: fewer than four observed members | 32 |
| published | NULL: no observed member | 40 |
| published | NULL: a negative member | 3 |
| NULL | published (positive) | 267 |
| NULL | published (zero) | 10 |
| NULL | NULL: cap refused | 1,161 |
| NULL | NULL: fewer than four observed members | 50 |
| NULL | NULL: no observed member | 1,383 |
| NULL | NULL: a negative member | 5 |

**Discrepancy inventory** (section 8). Over the 587 rows publishing both, today ÷ proposed is in [0.8, 1.25] for
458. The script asserts on every row that today ÷ proposed = (V/P) × (DPS·N/D), with V the session close, P
`current_price` (equal to `metric_price` on every row with a published cap, since a curated ADS ratio refuses the
cap), N = `market_cap_live / valuation_price` and D the four members' sum; V/P per row is printed beside the second
factor. Each of the 129 rows outside the band is tagged with the first observational predicate it matches
(`inventory_cause`); a tag is a candidate, not a verified cause:

| precedence | predicate | rows |
|---|---|---|
| 1 | weighted diluted shares spread ≥ 1.5× | 5 |
| 2 | a negative DPS member | 11 |
| 3 | largest positive DPS member over 50× the smallest positive one | 2 |
| 4 | a quarter with a positive paid member and a NULL DPS member | 86 |
| 5 | stored paid sum outside [0.95, 1.05] of the rebuilt common-only TTM | 4 |
| 6 | window span over 365 days | 1 |
| — | untagged (listed in section 8) | 20 |

## Source rule

| decision | rule | source |
|---|---|---|
| dividend yield | `div12m_me` = Σ over the last 12 months of (per-share distribution on the current share basis × shares) ÷ ME; NULL when the security has fewer than 12 months of history | JKP code, `bkelly-lab/ReplicationCrisis` at `67174c7f170b`: `GlobalFactors/market_chars.sas:122-131` (`div_sum = %apply_to_lastn(x=div_tot*shares, _n=&n., func=sum); div&n.m_me = div_sum/me; if count < &n. then div&n.m_me = .`), `:86-95` (`count` = months since the security's first observation) |
| a period with no distribution | zero, and zero is a published value | `GlobalFactors/project_macros.sas:423` (CRSP `div_tot = (ret-retx)*lag(prc)*(cfacshr/lag(cfacshr))`, zero when price and total return agree), `:566-569` (Compustat, "Dividend Adjustment (set to zero if missing)", `div_tot = coalesce(div, 0)*fx_div`) |
| the dividend total | cash dividends to common shareholders of the parent: us-gaap `PaymentsOfDividendsCommonStock`, "Amount of cash outflow in the form of ordinary dividends to common shareholders of the parent entity." **The construction reads the stored `dividends_paid` members, a proxy** (below) | companyfacts `description` (section 12, verbatim) |
| sign | the payment concepts are cash outflows reported as positive amounts; a negative member is not a payment | the same descriptions ("cash outflow") |
| market equity | the #3850 `market_cap_live`, the #3841 PIT cap at the session | #3850 spec "Source rule" |
| a column the view cannot compute truthfully | NULL | #1664; sql/201 |

### Observed zero, source absence and unresolved extraction

JKP's zero is an observed zero: its source has a value for every month of the security's life, and the CRSP form is
zero exactly when no distribution was made. Our quarter carries three states, and only the first is a value:

- **observed:** the stored member is a number (zero included);
- **source absent:** the member is NULL and no tracked payment concept has a fact ending at the quarter end;
- **unresolved or missed:** the member is NULL and a fact ends there.

sql/220 sums the members it has, reading any NULL as no dividend (`:17-22`, "absence means 'did not occur'"). Section
3 tests that reading on the 96 windows with a nonzero member and a NULL member (171 NULL members, classified by
`classify_member` against `financial_facts_raw`, USD only, with the quarter's own start and end): 114 are source
absent, 4 derive to zero, **22 derive to a nonzero payment the normaliser did not store** (22 windows; section 3
prints each signed amount and the corrected sum, every one positive), and 31 are unresolved. A NULL member is
therefore not reliably a zero for this column. The construction reads a yield only from four observed members, and
leaves `dividends_paid_ttm`'s sporadic sum unchanged for its other readers. This refuses the 50 windows whose NULL
members are all source absent, which are probably correct; they return when the normaliser stores an observed zero
(filed, "Not in scope").

### The numerator is a proxy

The normaliser fills `dividends_paid` from the first tracked concept with a value, in the order of
`app/providers/implementations/sec_fundamentals.py:255-261`: `PaymentsOfDividends`, then
`PaymentsOfDividendsCommonStock`, then three partnership concepts. us-gaap describes `PaymentsOfDividends` as "Cash
outflow in the form of capital distributions and dividends to common shareholders, preferred shareholders and
noncontrolling interests" (section 12).

- **The rule's own check** (section 4). `priority_value` applies that priority to companyfacts, per quarter with the
  quarter's own boundaries (`quarter_value`: the latest-filed fact spanning the quarter, or a year-to-date fact less
  the same-start fact ending the day before the quarter; the candidate whose newest operand was filed last wins). Of
  5,685 stored non-NULL members in windows with a nonzero member, 5,533 agree within 0.5%, 60 differ, 86 are
  unresolved and 6 absent. Agreeing members came from `PaymentsOfDividendsCommonStock` 3,254, `PaymentsOfDividends`
  2,249 and partnership concepts 30. 45 quarters had a NULL `period_start_date` and took a substituted start
  (`bounds`).
- **Composition** (section 5). `common_ttm` rebuilds each quarter separately: `PaymentsOfDividendsCommonStock`; else a
  partnership concept; else `PaymentsOfDividends` less the filed `PaymentsOfDividendsPreferredStockAndPreferenceStock`
  and `PaymentsOfDividendsMinorityInterest`. A quarter with no fact ending at its end is zero; a window with an
  unresolved quarter is excluded (115), as is a non-positive rebuilt total (7). `DividendsPreferredStock` and
  `DividendsPreferredStockCash` are declared amounts ("paid and unpaid"), so they are not subtracted; 39 windows
  declare preferred dividends on the generic branch with no paid-preferred fact, and are flagged. Over all 1,342
  resolvable complete-TTM windows, stored ÷ rebuilt is in [0.95, 1.05] for 1,301; over the 809 windows rebuilt
  wholly from `PaymentsOfDividendsCommonStock` (independent of the generic concept), 781. Over the 810
  proposed-published windows, 798; every window outside [0.95, 1.05] is listed with both amounts.
- **Special distributions.** `PaymentsOfDividendsCommonStock` covers ordinary dividends; JKP's `ret − retx` carries
  every distribution. `PaymentsOfCapitalDistribution` ("excluding ordinary dividends. Includes special dividends")
  has an amount in a window quarter for 6 windows (section 6). It is not tracked, so neither form includes it.
- **Timing.** A cash-flow figure is by payment date over four fiscal quarters; JKP's is by ex-date over 12 months.
  No stored ex-date source exists to measure the difference: 3 of 854 proposed-published positive rows have an 8-K
  `dividend_events` ex-date in the 365 days to today (section 10). Section 8 prints the window span distribution:
  1,276 windows span 365 days, 44 span 366–371, 6 span 395–730 days and 1 spans 0 (stored quarter starts
  that do not match the quarter; the window's shape guard tests only end dates).
- **Security history.** JKP requires 12 months of the security's own history. The view's window requires four
  filed quarters, which does not establish listing history, and `price_daily` cannot stand in: 107
  proposed-published rows have under 365 days of stored bars (section 9), mostly small banks whose stored series
  starts at our ingestion horizon. Not implemented; a stated departure.

**Why ship the proxy.** Item 3 of "Problem": against the independent rebuild, the proposed yield is within 5% on 546
of 554 rows where today's is within 5% on 358. The 12 proposed-published windows outside [0.95, 1.05] of the rebuild
(section 5: WTRG, SSNC, BFH, PFLT, GBDC, AMSF, PSEC, CWBC, NLY, XRX, PRI, ASTH) are the residual, listed and filed.

**Alternatives rejected.** 8-K ex-dates (3 of 854 covered); Σ DPS × that quarter's count (the DPS member is the
defective operand); restating per-share DPS (no split register, #3841 "Problem").

## Construction

### Two views (sql/507)

1. **`financial_periods_ttm`** (sql/220's body) gains one column, appended last:
   `dividends_paid_ttm_observed` = `SUM(dividends_paid)` when the window passes sql/220's shape guard (`COUNT(*) =
   4`, span ≤ 330 days) and `COUNT(dividends_paid) = 4` and `MIN(dividends_paid) >= 0`; else NULL. Every existing
   column keeps its expression and position. `CREATE OR REPLACE VIEW` permits an appended column. The build greps
   every reader of the view (`peer_comparison.py`, `fair_value_band.py`, the valuation views) for `SELECT *`.
2. **`instrument_valuation`** (sql/506's body):
   - `new_pipeline` drops its `dividend_yield` expression and carries `ttm.dividends_paid_ttm_observed` in its place;
     the legacy arm carries `NULL::numeric`. The final SELECT lists its outputs explicitly and does not emit it.
   - Final SELECT: `dividend_yield = dividends_paid_ttm_observed / market_cap_live * 100` when `is_complete_ttm`,
     `dividends_paid_ttm_observed < 'Infinity'` and `market_cap_live > 0`; else NULL. Zero publishes as zero
     ("a period with no distribution"). `dividends_paid` is `NUMERIC(20,4)` and cannot hold ±Infinity (#3911
     Amendment 1, finding 2); `NaN` sorts above every number, so `MIN >= 0` passes it and the upper guard refuses
     the sum.
   - Output columns, their order and types are unchanged.
3. **Consequences.** `dividend_yield` becomes count-based under #3850's invariant: NULL whenever `market_cap_reason`
   is set. The FPI suppression (sql/506 `:253`) is subsumed by the `fpi_adr` reason and removed. It is priced at
   `valuation_price`. `dps_declared_ttm`, `dividends_paid_ttm` and every other column are unchanged.
4. **Comments.** sql/507's header and both `COMMENT ON VIEW`s state the rule; the valuation comment says the legacy
   arm's `pb_ratio` is the last per-share column.

### Expected movement

From the populations table: kept 587; gained 267 positive and 10 zero; lost 403 to a refused cap, 72 to an
unobserved member (32 with some members, 40 with none; of the 75 lost with a published cap, 27 file
`PaymentsOfOrdinaryDividends`, which is not tracked, in a window quarter — section 7) and 3 to a negative member.

### Readers

- **Scoring.** No reader: `app/services/scoring.py` does not read `dividend_yield` (grep at `c15afb10`).
- **Thesis.** `_WRITER_SYSTEM` (`app/services/thesis.py:1344-1349`, `:1448-1457`) and the field comment
  (`:862-863`) price dividend yield at `current_price` and allow re-rating arithmetic "the multiple's own price ×
  (your target multiple / the context's current multiple)". The build moves dividend yield into the `valuation_price`
  set, defines it as TTM cash dividends paid ÷ market cap, and states that a yield re-rates inversely: price × (the
  current yield ÷ the target yield). `_PROMPT_VERSION` becomes v10. The thesis window is listed and repaired as in
  the P/E spec ("Deploy order and the thesis window"), with v9 in place of v8.
- **`ranking_pot` exposure.** `ranking_pot_rebalance` reads `dps_declared_ttm`, `dividend_yield` and `current_price`
  from the view (`app/services/ranking_pot_rebalance.py:703-712`) into `NameFacts`
  (`app/services/ranking_pot.py:145-149`). `ranking_pot_exposure.dividend_yield` (`ranking_pot_exposure.py:103-119`)
  returns None for a NULL or negative `dps_ttm`, 0 for a zero one, and otherwise yield × price ÷ 100 ÷ the close on
  D. The module states "Descriptive only: nothing here enters a decision, a look or a gate"
  (`ranking_pot_exposure.py:3`). All three files are hashed by `ranking_pot_policy.POLICY_MODULES` (`:55`, `:62`, `:69`)
  and `ranking_pot_v2_policy` (`:47`, `:49`, `:53`) and are not edited. After the change the product is TTM cash paid ÷ the PIT count ×
  (`current_price` ÷ `valuation_price`), and the name is not covered when the proposed yield is NULL. v1 is halted
  pre-trade; v2 is not frozen. Exposure tables computed before and after the deploy differ in definition; a readout
  spanning both must say so. The build posts this on #2842 and #3592 and runs both pot policy checks.
- **Not readers.** The instrument page and `ranking_ablation_reader` read `instrument_dividend_summary.ttm_yield_pct`
  (`app/api/instruments.py:4352`, `app/services/ranking_ablation_reader.py:412-416`). The two yields can disagree
  until that surface moves; the build notes it on #3904.

## Acceptance (corpus rung)

`scripts/measure_3903_dividend_acceptance.py`, in the P/E acceptance's form: control sql/506's bodies, treatment
sql/507's, each created as temporary views, read in one REPEATABLE READ transaction that is rolled back. It runs
after the 08:20Z PIT build and before the evening candle refresh, when `basis_stale` refusals are fewest.

1. **Wiring, full population.** Ordered column names and types of both views (catalog) equal between arms, except
   the appended `dividends_paid_ttm_observed`. Row sets equal. Every published `dividend_yield` equals the sum of the
   window's four `dividends_paid` members ÷ `market_cap_live` × 100, recomputed in `Decimal` from `financial_periods`
   within relative 1e-12; every NULL has a cause from `proposed_yield`; every other column of both views equals the
   control exactly.
2. **A/B.** Section 1's table re-printed; differences from "Expected movement" explained by data that changed.
3. **The rebuild gate, full population.** Section 5 over the treatment: the proposed yield within 5% of the rebuilt
   common-only yield on at least as many rows as today's, over the rows where both publish and the rebuild resolves.
   Every proposed-published window outside [0.95, 1.05] of the rebuild is listed with both amounts and filed as one
   input ticket; the row stays published, because the comparison shows the proposal closer to the source on the
   population and the residual is named. The same rule applies to item 4's rows: none is failed or excused for
   sample membership.
4. **Independent values.**
   - **NULL and zero checks:** GME (no observed member) NULL; BJRI (a negative member) NULL; each of the 10
     published-zero rows has four observed zero members.
   - **External yields**, positive only, against a published TTM dividend yield (stockanalysis, else
     companiesmarketcap), recording the external price, date and TTM DPS. Mandatory, in order: MSFT, JPM, HD, AAPL
     (panel payers), O, FAST, KO (gained), CEG (negative DPS member), EFC (scale), and the three untagged section 8
     rows with the largest |log(today ÷ proposed)|. A name whose cap is refused at the run is reported with its
     reason and replaced by the next proposed-published positive row by market cap not already listed.
   - **Reconciliation, an exact identity.** With external price P_x and TTM DPS E, and N_f the filing's latest
     weighted basic count: ours ÷ external = (P_x / V) × (N_f / N) × (D / (N_f · E)). Asserted numerically per row.
   - **Pass:** |ours ÷ external − 1| ≤ 15%. Otherwise each factor outside [0.95, 1.05] that the PR accounts for with
     a cited filing figure (the count factor by the cover or balance-sheet count; the payout factor by a filed
     preferred or NCI payment, a dividend change inside the window, or a payment-date shift) is divided out, and the
     residual must lie within ±15%. Price is never accounted for. ±15% and ±5% are chosen by construction.
5. **Readers.** `ranking_pot_exposure.dividend_yield` called with `NameFacts` built from each arm's rows for the
   latest v1 and v2 snapshot universes: names changed, gained or lost coverage. A printed record; it gates nothing,
   because nothing reads it for a decision.
6. **DB tests (synthetic):** published; four observed zeros → 0; a NULL member among positive ones → NULL while
   `dividends_paid_ttm` stays the three-member sum; a negative member → NULL; `NaN` → NULL; a refused cap → NULL
   with its reason; `fpi_adr` NULL before and after; `dual_class` and `ads_ratio` rows whose sql/506 yield is
   positive → NULL with the reason; a legacy row → NULL in both; and a split window: DPS members 25.00, 1.00, 1.00,
   1.00 with `dividends_paid` 100M each, close 50 and PIT count 100M, which sql/506 publishes at 28 / 50 × 100 = 56%
   and sql/507 at 400M / 5,000M × 100 = 8%.
7. **Latency.** Per-instrument and bulk reads of both views, both arms; above 2× blocks (#3850's bound).
8. **Corpus evidence** (CLAUDE.md "Corpus changes"): the smoke panel; item 4 as the cross-source check; backfill
   none (views); live re-check after deploy; the thesis-window listing.

## Rollback

`sql/rollback/507_…sql` restores sql/506's `instrument_valuation` body. `financial_periods_ttm` keeps the appended
column, unused: removing a column needs `DROP VIEW … CASCADE` over both views, a larger change than the column. A DB
test applies forward then rollback in one transaction and asserts that `instrument_valuation`'s ordered columns,
types and every row equal sql/506's on a fixture covering each NULL cause and each suppression.

## Not in scope

Filed by the build PR:
- **Normaliser** (a corpus change): store an observed zero for a source-absent quarter of a payer; derive the 22
  missed members; track `PaymentsOfOrdinaryDividends` and `PaymentsOfCapitalDistribution`; read
  `PaymentsOfDividendsCommonStock` before `PaymentsOfDividends`.
- **Input residual:** the 12 proposed-published windows outside [0.95, 1.05] of the rebuild.
- **Non-payers.** JKP's yield for a non-payer is zero. The view publishes zero only from four observed zero members;
  a window with no observed member (1,383 + 40 rows) stays NULL until the normaliser stores observed zeros.

These stay on #3903: legacy `pb_ratio` and the derived-Q4 per-share rule. The instrument page's yield is #3904.

## Checkpoint record

Round 1 (30 findings, UNSOUND):

| # | finding | v2 |
|---|---|---|
| 1 | P1: measurement used `abs()` and admitted negatives | `proposed_yield` is the exact construction; all figures regenerated |
| 2 | one concept per window | `common_quarter` selects per quarter |
| 3 | unresolved read as zero | three states; windows with an unresolved quarter excluded and counted |
| 4 | generic fallback not common-only | declared-preferred concepts explained and flagged (39); independent common-only subset reported |
| 5 | quarter interval not checked | `quarter_value` takes the quarter's start and end |
| 6 | vintage mixing | newest-operand candidate wins; stated |
| 7 | classifier boundaries and units | quarter bounds, USD only, partial facts no longer accepted |
| 8 | "missed" direction | signed amounts and corrected sums printed; all positive |
| 9 | zero / absence / extraction conflated | "Observed zero, source absence and unresolved extraction"; four-observed rule; zero publishes |
| 10 | special distributions | measured (6 windows); stated |
| 11 | timing measured by span only | restated; no ex-date source (3 of 854); span distribution printed, anomalies named |
| 12 | 12-month history | stated departure with its measurement |
| 13 | annual supplier inventory | replaced by per-quarter supplier provenance in the rule check |
| 14 | accuracy on a selected population | all complete-TTM windows, and the published subset |
| 15 | rebuild unvalidated | section 4, the rule's own check |
| 16 | tag labels | observational predicates; predicate 4 checks the quarter |
| 17 | lost-yield causes | section 7 intersects the actual lost rows with window-quarter concepts |
| 18 | witnesses not printed | CEG, PAYC, EFC, BJRI named; amounts printed |
| 19 | tight-band list | every window outside [0.95, 1.05] listed with amounts |
| 20 | populations not exhaustive | full table |
| 21 | 8-K future dates | bounded to today |
| 22 | price attribution | identity asserted per row with V/P printed; `metric_price` equality stated |
| 23 | P1: input policy by sample | the four-observed rule removes the missed class; item 3 is one population-wide gate |
| 24 | pass rule | residual after dividing out accounted factors must be within ±15% |
| 25 | NULL/zero, coverage | separate checks; deterministic replacement; run time fixed |
| 26 | pot exposure semantics | implementation-level description; frozen, descriptive, v1 halted; notes |
| 27 | reader evidence | line-cited implementation; acceptance calls the function |
| 28 | yield re-rating inverted | prompt states the inverse |
| 29 | schema and rollback | catalog comparison; rollback compares every row on a fixture |
| 30 | split fixture | exact amounts and expected values |
