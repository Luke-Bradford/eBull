# #3903: the valuation view's dividend yield on the total/ME form

**Ticket:** #3903 (remainder item 1; refs #3850, #3841). **Status:** spec v3. Codex checkpoint 1: round 1 raised 30
findings, round 2 re-graded them and added 24 (both UNSOUND); "Checkpoint record" gives each disposition.

`instrument_valuation.dividend_yield` divides a per-share dividend sum by a price. The sum crosses share bases and
derived quarters, and it misses most issuers that report dividends only as a cash total. This spec moves the column
onto the #3850 market cap and the TTM cash dividends paid, read only from a window whose four quarters are all
observed. It follows #3911's P/E (`docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`).

## Problem

Figures are printed by `PYTHONPATH=. uv run python scripts/measure_3903_dividend_premise.py` (dev DB, run
2026-10-19 21:41Z, code `a5557f47`, companyfacts.zip SHA-256 file `12bad36a…`). Its database reads share one
REPEATABLE READ snapshot; section numbers refer to its output. The published-cap population falls through the evening
(an earlier run at 21:03Z printed 2,555; this one 2,360): the candle refresh writes `price_daily_revision` rows, and
`instrument_market_cap_pit` refuses those steps as `basis_stale` until the next build
(`sql/499_market_cap_pit.sql:78-81`, by design). "Proposed" is the construction below evaluated over stored columns
(`proposed_yield`).

sql/506 computes `dividend_yield = dps_declared_ttm / metric_price * 100` when both are positive, in the complete-TTM
arm only (`sql/506_instrument_valuation_pe_total_form.sql:140-142`), and suppresses it for an ADR of unknown ratio
(`:253`). `dps_declared_ttm` sums the window's quarterly `dps_declared` over its present members
(`sql/220_ttm_strict_flow_sums.sql:141-143`). Each member is on its own filing's share basis; eToro's price is
back-adjusted for every split (#3841 "Problem").

1. **Defective per-share members** (section 11). EFC's DPS members are 390, 390, 390, 0.39: today's view publishes a
   yield of 10,046%, the proposal 13.66%. CEG's are 0.3878, −0.7756, 0.4265, 0.4265 (0.17% today, 0.57% proposed);
   PAYC's 0.375, −0.75, 0.375, 0.375.
2. **Payers with no per-share fact.** HD, KO, FAST and O have no `dps_declared` member, so today's view publishes no
   yield; their stored cash dividends paid are $9.221B, $10.971B, $1.056B and $2.996B TTM.
3. **Against an independent rebuild** (section 5). On the 544 rows publishing a positive yield both ways whose
   companyfacts common-only rebuild resolves, today's yield is within 5% of the rebuilt yield on 351 and within
   [0.8, 1.25] on 423; the proposed yield on 536 and 538. This is evidence on those 544, not on every publication
   (see "Input policy").

**Populations** (section 1), all 3,941 complete-TTM rows (every one has a sql/220 window):

| today | proposed | rows |
|---|---|---|
| published | published | 578 |
| published | NULL: cap refused | 413 (`basis_stale` 177, `no_left_anchor` 146, `no_pit_row` 72, `basis_unresolved` 14, `dual_class` 4) |
| published | NULL: fewer than four observed members | 32 |
| published | NULL: no observed member | 39 |
| published | NULL: a negative member | 3 |
| NULL | published (positive) | 264 |
| NULL | published (zero) | 9 |
| NULL | NULL: cap refused | 1,168 |
| NULL | NULL: fewer than four observed members | 50 |
| NULL | NULL: no observed member | 1,380 |
| NULL | NULL: a negative member | 5 |

**Discrepancy inventory** (section 8). Over the 578 rows publishing a positive yield both ways, today ÷ proposed is
in [0.8, 1.25] for 451. The script asserts on every row that today ÷ proposed = (V/P) × (DPS·N/D), with V the session
close, P `current_price` (equal to `metric_price` wherever a cap publishes, since a curated ADS ratio refuses the
cap), N = `market_cap_live / valuation_price` and D the four members' sum. V/P lies in [0.936, 1.049] and outside
[0.95, 1.05] on 2 rows; it is printed per discrepant row. Each of the 127 rows outside the band is tagged with the
first observational predicate it matches (`inventory_cause`; a candidate, not a verified cause): shares spread
≥ 1.5× 5; a negative DPS member 11; a positive DPS member over 50× the smallest positive one 2; a quarter with a
positive paid member and a NULL DPS member 84; stored paid sum outside [0.95, 1.05] of the rebuild 4; span over
365 days 1; untagged 20 (listed).

## Source rule

| decision | rule | source |
|---|---|---|
| dividend yield | `div12m_me` = Σ over the last 12 months of (per-share distribution on the current share basis × shares) ÷ ME; NULL when the security has fewer than 12 months of history | JKP code, `bkelly-lab/ReplicationCrisis` at `67174c7f170b`: `GlobalFactors/market_chars.sas:122-131` (`div_sum = %apply_to_lastn(x=div_tot*shares, _n=&n., func=sum); div&n.m_me = div_sum/me; if count < &n. then div&n.m_me = .`), `:86-95` (`count` = months since the security's first observation) |
| a period with no distribution | zero, a published value. CRSP's zero is arithmetic on observed returns (`project_macros.sas:423`, `div_tot = (ret-retx)*lag(prc)*(cfacshr/lag(cfacshr))`); Compustat's is **imputed** for a missing value (`:566-569`, "Dividend Adjustment (set to zero if missing)", `coalesce(div, 0)`) | as cited |
| the dividend total | cash dividends to common shareholders of the parent: us-gaap `PaymentsOfDividendsCommonStock`, "Amount of cash outflow in the form of ordinary dividends to common shareholders of the parent entity." **The construction reads the stored `dividends_paid` members, a proxy** (below) | companyfacts `description` (section 12, verbatim) |
| market equity | the #3850 `market_cap_live`, the #3841 PIT cap at the session | #3850 spec "Source rule" |
| a column the view cannot compute truthfully | NULL | #1664; sql/201 |

### Local rules, stated as departures

- **Four observed members.** JKP imputes zero for a missing Compustat dividend. We do not: our NULL member is
  sometimes a payment the normaliser failed to store. Section 3 classifies the 171 NULL members of the 1,464 active
  windows (a window with a nonzero member) against `financial_facts_raw`, USD, with the quarter's own boundaries
  (`classify_member`): 114 have no tracked fact ending at the quarter end, 4 derive to zero, **22 derive to a nonzero
  payment the normaliser did not store** (22 windows, 13 with a published cap; each amount printed, all positive),
  and 31 are unresolved. "No tracked fact" is absence from the tracked USD facts, not proof that nothing was paid.
  The construction therefore publishes only from four observed members (zero included) and leaves
  `dividends_paid_ttm`'s sporadic sum (sql/220 `:17-22`) unchanged for its other readers. This also refuses the 48
  windows (26 with a published cap) whose NULL members all have no fact, which are probably correct.
- **No negative member.** The payment concepts describe a cash outflow, and every stored member of a correct filing
  is non-negative; a negative member is treated as a data defect, a local display rule with no JKP counterpart.
  It refuses 8 rows (3 published today). 20 windows have a negative member (section 2).
- **Zero publishes as zero**, from four observed members summing to zero: 9 rows, 7 of which the rebuild also puts
  at zero and 2 (JACK, RFIL) where it has an unresolved quarter (section 5).
- **No listing-history test.** JKP's 12-month history is not implemented and not measured: the view has no listing
  date, and `price_daily`'s first bar is our ingestion horizon, not the listing (107 proposed publications have a
  stored series under 365 days, section 9). Four filed quarters are required instead.
- **Quarter intervals.** sql/220's shape guard tests end dates only. 25 proposed publications have a member whose
  stored interval is NULL or outside 80–100 days (section 8; several are 16-week first quarters, e.g. KR, ACI); 7
  active windows span under 330 or over 371 days (FLXS 0, GOLD.US 456, IBKR 396, JRSH 546, LNN 730, OLP 638, YUMC
  395), of which FLXS publishes. The construction accepts four consecutive stored quarters by end date, as every
  other TTM column does.

### The numerator is a proxy

The normaliser fills `dividends_paid` from the first tracked concept with a value, in the order of
`app/providers/implementations/sec_fundamentals.py:255-261`: `PaymentsOfDividends`, then
`PaymentsOfDividendsCommonStock`, then three partnership concepts. us-gaap describes `PaymentsOfDividends` as "Cash
outflow in the form of capital distributions and dividends to common shareholders, preferred shareholders and
noncontrolling interests" (section 12).

- **Reconstruction rule** (`quarter_value`). Per concept and quarter, boundaries matched within 3 days: the
  latest-filed dated fact spanning the quarter; else, per year-to-date start, the latest-filed fact ending at the
  quarter end less the latest same-start fact ending the day before the quarter **filed no later than it**, so a
  later restatement of the earlier period is never subtracted from an older cumulative. Undated facts make a quarter
  unresolved, never absent. 45 window quarters with a NULL `period_start_date` take the previous member's end + 1
  day (31) or end − 90 days (14) (`bounds`).
- **The rule's own check** (section 4). `priority_value` applies the normaliser's priority to companyfacts. Of 5,685
  stored non-NULL members of active windows, 5,520 agree within 0.5%, 64 differ, 97 are unresolved and 4 have no
  fact. The concept of the matching candidate is `PaymentsOfDividendsCommonStock` 3,247, `PaymentsOfDividends` 2,243,
  partnership 30; a match does not prove which concept the normaliser stored.
- **Composition** (section 5). `common_ttm` rebuilds each quarter: `PaymentsOfDividendsCommonStock`; else a
  partnership concept; else `PaymentsOfDividends` less `PaymentsOfDividendsPreferredStockAndPreferenceStock` and
  `PaymentsOfDividendsMinorityInterest`, where an unresolved deduction makes the quarter unresolved. A quarter with no
  fact is zero. Declared preferred dividends ("paid and unpaid") are not subtracted; 24 compared publications declare
  them with no nonzero paid-preferred value and are flagged. Of 851 proposed publications, 793 are compared (51 have
  an unresolved quarter, 7 are zero in both); stored ÷ rebuilt is in [0.95, 1.05] for 781. Of the 487 compared
  publications rebuilt from `PaymentsOfDividendsCommonStock` in all four quarters, 476. Every published window
  outside [0.95, 1.05] is listed with both amounts and its per-quarter branch.
- **Special distributions.** JKP's `ret − retx` carries every distribution; `PaymentsOfDividendsCommonStock` is
  ordinary dividends. `PaymentsOfCapitalDistribution` ("excluding ordinary dividends. Includes special dividends")
  has an amount in a window quarter for 28 windows, 6 active and 0 proposed publications (section 6). Whether a
  stored generic amount includes a special distribution is not established.
- **Timing.** Cash paid by payment date over four fiscal quarters, against JKP's ex-date distributions over 12
  months. No stored ex-date source covers the population (3 of 842 positive publications have an 8-K
  `dividend_events` ex-date in the 365 days to today, section 10), so the difference is not measured.

### Input policy

The view cannot see filing facts, so it publishes by the construction alone. One rule applies to every row, whether
or not it is in an external sample:

- **A stored member that disagrees with the filing rule is an input defect, not a construction error.** 35 proposed
  publications have one (section 5 lists them). They are filed as one normaliser ticket with the concept-priority
  and missed-member defects ("Not in scope"). The rows stay published: today's form reads the same defective
  normaliser output through other members, and the rebuild comparison (Problem item 3) shows the proposal closer to
  the filings on the compared population.
- **A construction error blocks.** A proposed publication further from the rebuilt yield than today's and outside
  [0.95, 1.05] must trace to a stored member that differs from the rule, or to the concept priority (the generic
  concept stored where the common-only one is filed). An untraced row fails acceptance. At 21:41Z there are 3: PRI
  (stored Q4 168,936,000 against the rule's 33,288,000) and SSNC (stored Q4 3,900,000 against 65,500,000), both
  member defects; XRX, whose members match the generic concept (29,000,000 against the common-only 15,000,000; XRX
  files a preferred payment), the concept priority. None is untraced.

**Alternatives rejected.** 8-K ex-dates (3 of 842 covered); Σ DPS × that quarter's count (the DPS member is the
defective operand); restating per-share DPS (no split register, #3841 "Problem").

## Construction

### Two views (sql/507)

1. **`financial_periods_ttm`** (sql/220's body) gains one column, appended last:
   `dividends_paid_ttm_observed` = `SUM(dividends_paid)` when the window passes sql/220's shape guard (`COUNT(*) =
   4`, end dates within 330 days), `COUNT(dividends_paid) = 4` and `MIN(dividends_paid) >= 0`; else NULL. Every
   existing column keeps its expression and position; `CREATE OR REPLACE VIEW` permits an appended column. The build
   greps every reader of the view for `SELECT *`.
2. **`instrument_valuation`** (sql/506's body):
   - `new_pipeline` drops its `dividend_yield` expression and carries `ttm.dividends_paid_ttm_observed` in its place;
     the legacy arm carries `NULL::numeric`. The final SELECT lists its outputs explicitly and does not emit it.
   - Final SELECT: `dividend_yield = dividends_paid_ttm_observed / market_cap_live * 100` when `is_complete_ttm`,
     `dividends_paid_ttm_observed < 'Infinity'` and `market_cap_live > 0`; else NULL. `dividends_paid` is
     `NUMERIC(20,4)` and cannot hold ±Infinity (#3911 Amendment 1, finding 2); `NaN` sorts above every number, so
     `MIN >= 0` passes it and the upper guard refuses the sum.
   - Output columns, their order and types are unchanged.
3. **Consequences.** `dividend_yield` becomes count-based under #3850's invariant: NULL whenever `market_cap_reason`
   is set. The FPI suppression (sql/506 `:253`) is subsumed by the `fpi_adr` reason and removed. It is priced at
   `valuation_price`. Every other column is unchanged.
4. **Comments.** sql/507's header and both `COMMENT ON VIEW`s state the rule; the valuation comment says the legacy
   arm's `pb_ratio` is the last per-share column.

### Expected movement

Kept 578; gained 264 positive and 9 zero; lost 413 to a refused cap, 71 to an unobserved member and 3 to a negative
member. Of the 74 lost with a published cap (section 7), 26 file `PaymentsOfOrdinaryDividends`, which is not
tracked, in a window quarter.

### Readers

- **Scoring.** No reader: `app/services/scoring.py` does not read `dividend_yield` (grep at `a5557f47`).
- **Thesis.** `_WRITER_SYSTEM` (`app/services/thesis.py:1344-1349`, `:1448-1457`) and the field comment
  (`:862-863`) price dividend yield at `current_price` and allow re-rating "the multiple's own price × (your target
  multiple / the context's current multiple)". The build moves dividend yield into the `valuation_price` set,
  defines it as TTM cash dividends paid ÷ market cap, and states: a yield re-rates inversely, price × (current yield ÷
  target yield), and only when both are finite and positive; a zero yield is a non-payer and is not a re-rating
  basis. `_PROMPT_VERSION` becomes v10. The thesis window is listed and repaired as in the P/E spec ("Deploy order and
  the thesis window"), with v9 in place of v8.
- **`ranking_pot` exposure.** `ranking_pot_rebalance` reads `dps_declared_ttm`, `dividend_yield` and `current_price`
  (`app/services/ranking_pot_rebalance.py:703-712`) into `NameFacts` (`app/services/ranking_pot.py:145-149`).
  `ranking_pot_exposure.dividend_yield` (`ranking_pot_exposure.py:103-119`) returns: None for a NULL or negative
  `dps_ttm` or an unusable close; 0 for a zero `dps_ttm`, whatever the view says; None for a positive `dps_ttm` with a
  NULL view yield; else yield × `current_price` ÷ 100 ÷ the close on D. Under sql/506 that product is DPS ÷ close.
  Under sql/507 it is (TTM cash paid ÷ N) × (`current_price` ÷ `valuation_price`) ÷ close: the cash-total form,
  scaled by the quote's drift from the session close. Payers with no DPS stay uncovered. The module states
  "Descriptive only: nothing here enters a decision, a look or a gate" (`ranking_pot_exposure.py:3`); the three files
  are hashed (`ranking_pot_policy.py:55`, `:62`, `:69`; `ranking_pot_v2_policy.py:47`, `:49`, `:53`) and not edited.
  v1 is halted pre-trade; v2 is not frozen and must state its exposure definition before it freezes. Exposure rows
  carry their snapshot date, so the deploy date is the version boundary; the build posts it on #2842 and #3592.
  Acceptance item 5 exercises every combination above.
- **Not readers.** The instrument page and `ranking_ablation_reader` read `instrument_dividend_summary.ttm_yield_pct`
  (`app/api/instruments.py:4352`, `app/services/ranking_ablation_reader.py:412-416`). The two yields can disagree
  until that surface moves; the build notes it on #3904.

## Acceptance (corpus rung)

`scripts/measure_3903_dividend_acceptance.py`, in the P/E acceptance's form, in one REPEATABLE READ transaction that
is rolled back. **Control:** sql/220's `financial_periods_ttm` and sql/506's `instrument_valuation`, as deployed.
**Treatment:** sql/507's two bodies created as temporary views, the valuation body rewritten to read the temporary
TTM view by name; the script asserts from `pg_depend` that each valuation arm depends on its own TTM arm. It runs
after the 08:20Z PIT build and before the evening candle refresh.

1. **Wiring, full population.** Ordered column names and types of both views (catalog) equal between arms, except
   the appended column. Row sets equal. Every treatment `dividend_yield` equals an independent evaluation from
   `financial_periods` (sql/220's window, shape guard and `is_complete_ttm` applied explicitly, then
   `proposed_yield`) within relative 1e-12, NULL decisions and causes included; every other column of both views
   equals the control exactly.
2. **A/B.** Section 1's table re-printed; differences from "Expected movement" explained by data that changed.
3. **Input policy, full population.** Section 5 over the treatment: the publications that disagree with the rule,
   listed and filed; every publication further from the rebuild than today and outside [0.95, 1.05], traced or
   failed.
4. **Independent values.**
   - **NULL and zero checks:** GME (no observed member) NULL; BJRI (fewer than four) NULL; every zero publication
     enumerated with its four members.
   - **External yields**, positive only, against a published TTM dividend yield (stockanalysis, else
     companiesmarketcap), recording the external price, date and TTM DPS. Mandatory by category: panel payers MSFT,
     JPM, HD, AAPL; gained cash-only payers O, FAST, KO; a negative DPS member, CEG; a DPS scale outlier, EFC; and the
     three untagged section 8 rows with the largest |log(today ÷ proposed)|. A name whose cap is refused at the run
     is reported with its reason and replaced by the largest proposed-published name of the **same category**; a
     category with none is reported as unavailable. Panel names are not replaced.
   - **Reconciliation, an exact identity.** With external price P_x and TTM DPS E, and N_f the filing's latest
     weighted basic count: ours ÷ external = (P_x / V) × (N_f / N) × (D / (N_f · E)). Asserted per row.
   - **Pass:** |ours ÷ external − 1| ≤ 15%. Otherwise only these are divided out, each with its cited figure, and the
     residual must be within ±15%: the count factor, only when the PIT row's `shares_reported` equals the filed cover
     or balance-sheet count at its context date within 0.5% (#3911 Amendment 1, rule 2); the payout factor, only when
     every stored member agrees with the rule (section 4) and the difference is definitional (a filed preferred or NCI
     payment, a dividend change inside the window, a payment-date shift). A payout factor traced to a stored member
     that disagrees with the rule is an input defect under "Input policy": listed and filed, the same as anywhere in
     the population. Anything else fails. Price is never divided out. ±15%, ±5% and 0.5% are chosen by construction.
5. **Readers.** `ranking_pot_exposure.dividend_yield` called on `NameFacts` from each arm's rows for the latest v1 and
   v2 snapshot universes, reported per combination of `dps_ttm` (NULL, zero, positive) and treatment yield (NULL,
   zero, positive): names changed, gained or lost. A printed record; nothing reads it for a decision.
6. **DB tests (synthetic):** published; four observed zeros → 0; a NULL member among positive ones → NULL while
   `dividends_paid_ttm` stays the three-member sum; a negative member → NULL; `NaN` → NULL; a refused cap → NULL with
   its reason; `fpi_adr` NULL before and after; `dual_class` and `ads_ratio` rows whose sql/506 yield is positive →
   NULL with the reason; a legacy row → NULL in both; a split window: DPS 25.00, 1.00, 1.00, 1.00 with
   `dividends_paid` 100M each, close 50 and PIT count 100M, published by sql/506 at 28 / 50 × 100 = 56% and by
   sql/507 at 400M / 5,000M × 100 = 8%.
7. **Latency.** Per-instrument and bulk reads of both views, both arms; above 2× blocks (#3850's bound).
8. **Corpus evidence** (CLAUDE.md "Corpus changes"): the smoke panel; item 4 as the cross-source check; backfill
   none (views); live re-check after deploy; the thesis-window listing.

## Rollback

A revert PR applies `sql/rollback/507_…sql`, which restores sql/506's `instrument_valuation` body, together with the
reverted `thesis.py` (`_PROMPT_VERSION` back to v9). The thesis window applies in reverse, as in the P/E spec: theses
written with v10 between the rollback migration's apply time and the reverted jobs child's PID change are listed and
regenerated. `financial_periods_ttm` keeps the appended column, unused: removing it needs `DROP VIEW … CASCADE` over
both views. A DB test applies forward then rollback in one transaction and asserts that `instrument_valuation`'s
ordered columns, types and every row equal sql/506's on a fixture covering each NULL cause and each suppression.

## Not in scope

Filed by the build PR:
- **Normaliser** (a corpus change): derive the 22 missed members; store an observed zero for a payer's quarter with
  no payment fact; track `PaymentsOfOrdinaryDividends` and `PaymentsOfCapitalDistribution`; read
  `PaymentsOfDividendsCommonStock` before `PaymentsOfDividends`; the 35 publications whose stored member disagrees with
  the rule.
- **Non-payers.** JKP's yield for a non-payer is zero. The view publishes zero only from four observed zeros; a
  window with no observed member stays NULL until the normaliser stores observed zeros.

These stay on #3903: legacy `pb_ratio` and the derived-Q4 per-share rule. The instrument page's yield is #3904.

## Checkpoint record

Round 1 (30 findings) and round 2's grade of each; v3's action where round 2 left it open:

| # | finding | round 2 | v3 |
|---|---|---|---|
| 1 | measurement used `abs()` | resolved | NaN ordering fixed (finding 44) |
| 2 | one concept per window | resolved | |
| 3 | unresolved read as zero | partial | unresolved deductions make the quarter unresolved |
| 4 | generic fallback not common-only | partial | the wholly-common subset now requires four common quarters |
| 5 | quarter interval | partial | tolerance stated; substitutions counted; interval anomalies listed ("Quarter intervals") |
| 6 | vintage mixing | unresolved | contemporaneous subtraction (prior filed no later than the cumulative) |
| 7 | classifier | partial | undated facts kept as unresolved; "no tracked fact" named as such |
| 8 | missed direction | resolved | |
| 9 | zero / absence | partial | JKP's Compustat zero stated as imputed; four-observed stated as a local departure |
| 10 | specials | partial | measured over every window; the blanket claim removed |
| 11 | timing | partial | span table complete; difference stated as unmeasured |
| 12 | listing history | partial | stated as unmeasured; `price_daily` disclaimed |
| 13 | supplier provenance | partial | "matching candidate"; provenance not claimed |
| 14 | selected population | partial | every publication accounted for in section 5 |
| 15 | rebuild validation | partial | the rule check's scope stated; branches reported per quarter |
| 16–22 | | resolved | |
| 23 | P1: input policy | partial | "Input policy": one rule, population-wide |
| 24 | pass rule | partial | factors divided out only on verified inputs |
| 25 | replacement, zero count | partial | same-category replacement; zero rows enumerated |
| 26 | pot semantics | partial | every combination stated and tested; drift factor stated; version boundary is the deploy date |
| 27–28, 30 | | resolved | |
| 29 | rollback | partial | thesis reversal and reverse window added |

Round 2's new findings:

| # | finding | v3 |
|---|---|---|
| 31 | JKP zero is imputed | stated |
| 32 | sign rule source | stated as a local rule; affected rows counted |
| 33 | P1: unresolved deductions | propagated |
| 34 | wholly-common subset | requires four common quarters |
| 35 | P1: vintage mixing | contemporaneous subtraction |
| 36 | quarter validity | measured and accepted as a stated departure |
| 37 | "source absent" too strong | renamed "no tracked fact"; undated facts unresolved |
| 38 | window categories | printed separately, with cap eligibility |
| 39 | span counts | complete table |
| 40 | specials | over every window; claim removed |
| 41 | P1: selected-population claims | every publication accounted for; claims limited to the compared rows |
| 42 | P1: comparative gate | replaced by "Input policy": per-row trace or fail |
| 43 | rule check overreach | renamed; scope stated |
| 44 | NaN ordering | finiteness checked first |
| 45 | two-view control binding | `pg_depend` assertion |
| 46 | oracle preconditions | stated in `proposed_yield`; applied explicitly in acceptance |
| 47 | P1: factor division | only verified inputs are divided out (#3911 Amendment 1 rule) |
| 48 | pot coverage combinations | stated and tested |
| 49 | quote-dependent pot semantics | stated; deploy date as version boundary; v2 must restate |
| 50 | zero-yield re-rating | guarded |
| 51 | rollback thesis | added |
| 52 | replacement category | same-category |
| 53 | counter denominators | each counter's population printed |
| 54 | claims not emitted | the earlier run is labelled as such; listing-history claim removed |
