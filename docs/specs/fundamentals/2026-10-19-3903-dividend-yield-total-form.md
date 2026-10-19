# #3903: the valuation view's dividend yield on the total/ME form

**Ticket:** #3903 (remainder item 1; refs #3850, #3841). **Status:** spec v1, for Codex checkpoint 1.

`instrument_valuation.dividend_yield` divides a per-share dividend sum by a price. The sum crosses share bases and
derived quarters, and it misses most issuers that report dividends only as a cash total. This spec moves the column
onto the #3850 market cap and TTM dividends paid, the form #3911 used for P/E
(`docs/specs/fundamentals/2026-10-19-3903-pe-total-form.md`).

## Problem

Figures are printed by `PYTHONPATH=. uv run python scripts/measure_3903_dividend_premise.py` (dev DB, run
2026-10-19 21:13Z, code `b5a7f8fd`, companyfacts.zip SHA-256 file `12bad36a…`). Its database reads share one
REPEATABLE READ snapshot; section numbers below refer to its output. The PIT cap moved during the session: four runs
between 21:03Z and 21:13Z put published-cap rows at 2,555, 2,520, 2,503 and 2,471, as the evening candle refresh
wrote `price_daily_revision` rows and `instrument_market_cap_pit` refused those steps as `basis_stale` until the next
build (`sql/499_market_cap_pit.sql:78-81`, by design). HD, FAST and KO published a cap at 21:03Z and not at 21:13Z.
Counts here are the 21:13Z run's.

sql/506 computes `dividend_yield = dps_declared_ttm / metric_price * 100` when both are positive, in the complete-TTM
arm only (`sql/506_instrument_valuation_pe_total_form.sql:140-142`), and suppresses it for an ADR of unknown ratio
(`:253`). `dps_declared_ttm` sums the window's quarterly `dps_declared` over its present members
(`sql/220_ttm_strict_flow_sums.sql:141-143`). Each member is on the share basis of its own filing, a fiscal Q4 is
derived as FY − nine months, and eToro's price is back-adjusted for every split (#3841 "Problem").

1. **Derived and mis-scaled per-share members.** CEG's window is DPS 0.3878, −0.7756, 0.4265, 0.4265; PAYC's is
   0.375, −0.75, 0.375, 0.375 (the negative member is a derived Q4). EFC's is 390, 390, 390, 0.39. Section 6 tags
   12 disagreeing rows with a negative DPS member and 2 with a member over 50× the window's smallest.
2. **Payers with no per-share fact.** HD, KO, FAST and O have no `dps_declared` member in their windows, so they
   publish no yield today, while their cash-flow statements report dividends paid: HD $9.221B, KO $10.971B, FAST
   $1.056B, O $2.996B TTM (section 8).
3. **Per-share sums across a share-basis change.** 19 DPS windows have weighted diluted shares spreading ≥ 1.5×
   (section 6; the P/E inventory's predicate).

**Populations** (section 1). Of 3,941 complete-TTM rows, 2,471 have a published cap.

| today | total form | rows |
|---|---|---|
| published | published | 673 |
| NULL | published | 314 |
| published | NULL (no positive `dividends_paid_ttm`) | 50 |
| NULL | NULL | 1,434 |
| published | cap refused | 342 (`no_left_anchor` 146, `basis_stale` 106, `no_pit_row` 72, `basis_unresolved` 14, `dual_class` 4) |

**Discrepancy inventory** (section 6). Over the 673 rows publishing both, today ÷ total lies in [0.8, 1.25] for 519.
The ratio factors exactly as (V/P) × (DPS·N/D), with V the session close, P `current_price`, N the PIT count
(`market_cap_live / valuation_price`) and D `dividends_paid_ttm`; V/P lies outside [0.95, 1.05] on 2 rows, so the
disagreement is in the second factor. Each of the 154 rows outside the band is tagged with the first predicate it
matches, in this precedence (`inventory_cause`). A tag is a **candidate cause**:

| precedence | predicate | rows |
|---|---|---|
| 1 | weighted diluted shares spread ≥ 1.5× | 5 |
| 1a | a negative DPS member | 12 |
| 1b | a DPS member over 50× the window's smallest | 2 |
| 2 | fewer non-NULL DPS members than paid members | 96 |
| 3 | fewer non-NULL paid members than DPS members | 10 |
| 4 | stored `dividends_paid_ttm` outside 5% of the companyfacts common-only TTM (section 7) | 6 |
| 5 | window span over 365 days | 1 |
| — | untagged | 22 |

Predicate 2 is the "absent member" observation recorded in the P/E spec's "Not in scope": the DPS sum reads an
absent quarter as no dividend, and here the cash-flow statement shows a payment in that quarter. The 22 untagged
rows are listed in section 6 with their factors.

## Source rule

| decision | rule | source |
|---|---|---|
| dividend yield | `div12m_me` = Σ over the last 12 months of (per-share distribution on the current share basis × shares) ÷ ME. A month without a distribution contributes zero; the figure is missing only when the security has fewer than 12 months of history | JKP code, `bkelly-lab/ReplicationCrisis` at `67174c7f170b`: `GlobalFactors/market_chars.sas:122-131` (`div_sum = %apply_to_lastn(x=div_tot*shares, _n=&n., func=sum); div&n.m_me = div_sum/me; if count < &n. then div&n.m_me = .`), `:86-96` (`count` = months since the security's first observation), `GlobalFactors/project_macros.sas:423` (CRSP `div_tot = (ret-retx)*lag(prc)*(cfacshr/lag(cfacshr))`), `:566-569` (Compustat "Dividend Adjustment (set to zero if missing)", `div_tot = coalesce(div, 0)*fx_div`) |
| the dividend total | cash dividends to common shareholders of the parent: us-gaap `PaymentsOfDividendsCommonStock`, "Amount of cash outflow in the form of ordinary dividends to common shareholders of the parent entity." **This spec uses the stored `dividends_paid_ttm`, a proxy** (below) | companyfacts `description` field, printed verbatim in section 4 |
| absence of a quarter's fact | zero: the sporadic class of sql/220 (`:17-22`), consistent with JKP's "set to zero if missing" | sql/220, spec `docs/specs/fundamentals/2026-07-12-2008-ttm-reconciliation.md` §Source rules |
| market equity | the #3850 `market_cap_live`, the #3841 PIT cap at the session | #3850 spec "Source rule" |
| non-positive or non-finite dividends | no yield (NULL) | sql/506's `> 0` display convention, unchanged; the finiteness guard as in #3911 |
| a column the view cannot compute truthfully | NULL | #1664; sql/201 |

JKP's numerator is distributions by ex-date over 12 calendar months, including special dividends (`ret − retx`
carries every distribution). A cash-flow figure is by payment date over the window's four fiscal quarters.

### The numerator is a proxy

`dividends_paid_ttm` sums `financial_periods.dividends_paid` over the window's present members (sql/220). The
normaliser fills that column from the first tracked concept present, in the order of
`app/providers/implementations/sec_fundamentals.py:255-261`: `PaymentsOfDividends` first, then
`PaymentsOfDividendsCommonStock`, then three partnership concepts. us-gaap describes `PaymentsOfDividends` as "Cash
outflow in the form of capital distributions and dividends to common shareholders, preferred shareholders and
noncontrolling interests" (section 4, verbatim), so where it supplies the figure the numerator can include preferred
and NCI payments. Measured deviations:

- **Composition** (section 7). `common_ttm` rebuilds the common-only TTM from companyfacts over the same window
  quarters: `PaymentsOfDividendsCommonStock` if filed; else the partnership concepts; else `PaymentsOfDividends` less
  the filed `PaymentsOfDividendsPreferredStockAndPreferenceStock` and `PaymentsOfDividendsMinorityInterest`. Over
  983 resolvable paid-active windows, stored ÷ rebuilt lies in [0.95, 1.05] for 927 and in [0.8, 1.25] for 932.
  Of the windows whose latest annual tracked fact is `PaymentsOfDividends` (405), 33 file a nonzero preferred or NCI
  concept in the same accession (section 4 lists each with its share of the supplier figure; C's
  `DividendsPreferredStock` is 20.7%).
- **Members.** 987 published windows have a nonzero paid member; 931 have all four. The 99 NULL members of the
  other 56 are classified against `financial_facts_raw` (`classify_null_member`): 19 are derivable from the filed
  facts and nonzero (18 windows, a missed fact: ALNT, BAH, CSPI, CSWC, CW, ESOA, GAP, GIS, JMSB, LNN, MCB, NBBK,
  NSSC, PEG, RWAY, SKYD, TWIN, USPH); 1 is derivable and zero; 79 are not derivable (63 with no fact ending at the
  quarter end, 16 with one). The 18 windows understate the TTM; 17 of them fall below 0.8 of the rebuilt common-only
  TTM in section 7.
- **Sign** (section 2). Window members: 5,541 positive, 256 zero, 22 negative. Two published rows have a negative
  TTM (BJRI, COCH) and publish no yield under the `> 0` guard; 9 have a positive TTM beside a negative member.
- **Timing.** 36 paid-active windows span more than 365 days (52/53-week calendars; 3 over 371). KO's Q4 2025
  member is $4,388M against $2,108–2,281M in its other quarters; this spec does not establish why (its cap is refused at 21:13Z).

**Why ship the proxy.** Today's form is wrong by construction in the cases above and absent for payers that tag no
per-share fact. The proxy's measured deviation from the common-only total is within 5% on 927 of 983 windows. A
dollar total has no share basis, so the only basis left in the ratio is the cap's, which #3841 checks.

**Alternatives measured and rejected.**
- **8-K `dividend_events`** (ex-date and DPS, the closest to JKP's ex-date numerator): 6 of 985 paid-active
  published instruments have an ex-date in the last 365 days (section 9).
- **Σ DPS × that quarter's share count:** the DPS member is the defective operand (1a, 1b) and covers fewer issuers.
- **Restating per-share DPS:** needs a split register, which does not exist (#3841 "Problem").

## Construction

### The view (sql/507)

`CREATE OR REPLACE VIEW instrument_valuation`, sql/506's body with these changes. Output columns, their order and
their types are unchanged.

1. **Complete-TTM arm:** `new_pipeline` drops its `dividend_yield` expression and carries `ttm.dividends_paid_ttm`
   instead; the legacy arm carries `NULL::numeric` in that position. The column is internal: the final SELECT lists
   its outputs explicitly and does not emit it.
2. **Final SELECT:** `dividend_yield = dividends_paid_ttm / market_cap_live * 100` when `is_complete_ttm`,
   `dividends_paid_ttm > 0`, `dividends_paid_ttm < 'Infinity'` and `market_cap_live > 0`; else NULL. The unit stays a
   percent. `dividends_paid` is `NUMERIC(20,4)` and cannot hold ±Infinity (#3911 Amendment 1, finding 2); the upper
   guard refuses `NaN`.
3. **Consequences:**
   - `dividend_yield` becomes a count-based column under #3850's invariant: NULL whenever `market_cap_reason` is set.
     The separate FPI suppression (sql/506 `:253`) is subsumed by the `fpi_adr` reason and is removed. Curated
     `ads_ratio` and `dual_class` instruments are NULL with their reason; none of the 4 `ads_ratio` rows publishes a
     yield today (section 1).
   - It is priced at `valuation_price`, the session close, not `current_price`.
   - `dps_declared_ttm` and every other column keep sql/506's expressions.
4. **Comments.** sql/507's header and `COMMENT ON VIEW` state the formula, the price base, and that the legacy arm's
   `pb_ratio` is the last per-share column.

### Expected movement (section 1)

| outcome | rows | cause |
|---|---|---|
| kept | 673 | |
| gained | 314 | no positive DPS sum today (O; HD, FAST and KO when their caps publish) |
| lost: no positive `dividends_paid_ttm` | 50 | 45 windows with a DPS and no paid member: 24 file `PaymentsOfOrdinaryDividends`, which is not tracked (section 5) |
| lost: PIT coverage or curated suppression | 342 | the cap refusals in the population table |

### Readers

- **Scoring.** No reader: `app/services/scoring.py` does not read `dividend_yield` (grep at `b5a7f8fd`).
- **Thesis.** `_WRITER_SYSTEM` (`app/services/thesis.py:1344-1349`, `:1454-1457`) and the field comment
  (`:862-863`) name dividend yield as priced at `current_price`. The build moves it into the market-cap set, priced at
  `valuation_price`, defined as TTM dividends paid ÷ market cap, and not DPS ÷ price. `_PROMPT_VERSION` becomes v10.
  The thesis window is listed and repaired as in the P/E spec ("Deploy order and the thesis window"), with v9 in
  place of v8.
- **`ranking_pot` exposure (frozen modules).** `ranking_pot_rebalance` reads `dps_declared_ttm`, `dividend_yield`
  and `current_price` from the view; `ranking_pot_exposure.dividend_yield` takes yield × price ÷ 100 as the DPS per
  tradable unit when `dps_declared_ttm > 0`, for exposure reporting only (`app/services/ranking_pot.py:145-149`).
  These files are hashed by `ranking_pot_policy.POLICY_MODULES` and are not edited. After the change that product is
  TTM dividends paid ÷ the PIT count × (`current_price` ÷ `valuation_price`), and NULL for a refused cap. The build PR
  posts the semantic change on #2842 and #3592 and runs both pot policy checks to confirm no hash moved.
- **Not readers.** The instrument page and `ranking_ablation_reader` read `instrument_dividend_summary.ttm_yield_pct`
  (`app/api/instruments.py:4352`, `app/services/ranking_ablation_reader.py:412-416`), not the view. Until that surface
  is moved the two yields are different constructions and can disagree; the build PR notes it on #3904.

## Acceptance (corpus rung)

`scripts/measure_3903_dividend_acceptance.py`, in the P/E acceptance's form: control sql/506's body, treatment
sql/507's, each created as a temporary `instrument_valuation`, read in one REPEATABLE READ transaction that is rolled
back.

1. **Wiring, full population.** Row sets equal. Every published `dividend_yield` equals `dividends_paid_ttm /
   market_cap_live * 100` recomputed in `Decimal` within relative 1e-12; NULL decisions exact; every other column
   equals the control exactly.
2. **A/B.** Section 1's movement re-printed over the two arms; a difference from "Expected movement" is explained by
   data that changed since the premise run.
3. **Readers.** The pot exposure product over the v1 and v2 universes, both arms: names whose product changes, and
   names moving between covered and not covered. No decision input reads it, so this is a printed record, not a gate.
4. **Independent values.**
   - **Expected NULL:** GME (no paid member); AAPL while its cap is refused; BJRI (negative TTM).
   - **External yield** against a published TTM dividend yield (stockanalysis or companiesmarketcap), recording the
     external price, date and TTM DPS, for: the panel names (AAPL, GME, MSFT, JPM, HD) whose cap publishes at the run; O, FAST, KO (gained);
     CEG, PAYC (derived-Q4 members); EFC (scale); and the three untagged rows with the largest |log(today ÷ total)|.
   - **Reconciliation, an exact identity.** With external TTM DPS E and price P_x: ours ÷ external =
     (P_x / V) × (N_f / N) × (D / (N_f · E)), where N_f is the filing's latest weighted basic share count. The script
     asserts the identity numerically per row before any factor is interpreted.
   - **Pass:** |ours ÷ external − 1| ≤ 15%; or each factor outside [0.85, 1.15] is matched to a filing figure the PR
     cites — the count factor to the cover or balance-sheet count; the payout factor to the filing's preferred or NCI
     payment, a dividend change inside the window, or a payment-date shift. A factor that exposes a wrong treatment
     input (`dividends_paid_ttm` or the PIT count) fails the row and is filed. ±15% is chosen by construction.
   - **Treatment-input check, flagged population.** The 18 missed-member windows and every section 7 row outside
     [0.95, 1.05] are listed with the stored and rebuilt TTMs and filed as one ticket. The stored `dividends_paid`
     members already feed `instrument_dividend_summary.ttm_dividends_paid` through `dividend_history`
     (`sql/050_dividend_history_views.sql:56`, `:97`), so such a defect is pre-existing; the PR does not block on it.
5. **DB tests (synthetic):** published; `dividends_paid_ttm` 0, negative, NULL and `NaN` → NULL; a refused cap → NULL
   with its reason; an `fpi_adr` row, NULL before and after; a `dual_class` row and an `ads_ratio` row whose sql/506
   yield is positive → NULL with the reason; a legacy-arm row → NULL in both; a window summing DPS across a 25-for-1
   split, which sql/506 publishes at about 25× and sql/507 at the total form.
6. **Latency.** Per-instrument and bulk reads, both arms; a regression above 2× blocks (#3850's bound).
7. **Corpus evidence** (CLAUDE.md "Corpus changes"): the smoke panel; item 4 as the cross-source check; backfill none
   (a view); live re-check after deploy; the thesis-window listing.

## Rollback

`sql/rollback/507_…sql` holds sql/506's body. A revert PR applies it with the reverted `thesis.py`; the thesis window
applies in reverse. A DB test applies forward then rollback and asserts the column list and, on a fixture, that the
rolled-back `dividend_yield` and its FPI suppression equal sql/506's.

## Not in scope

Filed by the build PR:
- **Untracked and mis-ordered concepts** (a corpus change): `PaymentsOfOrdinaryDividends` (24 of 45 DPS-only
  windows) is not tracked; `PaymentsOfDividends` is read before `PaymentsOfDividendsCommonStock`. Fixing either is a
  normaliser change with a backfill.
- **Missed members:** the 18 windows whose NULL member is derivable and nonzero.
- **Non-payers.** JKP's yield for a non-payer is zero. The view keeps NULL: `dividends_paid_ttm` is NULL for 1,444
  published-cap rows, and the view cannot tell a never-payer from an issuer whose payments the normaliser missed.

These stay on #3903: legacy `pb_ratio` and the derived-Q4 per-share rule (P/E spec "Not in scope"). The instrument
page's yield is #3904.
