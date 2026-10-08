# #3621 — avoidance filters measured as exclusions on the #3609 panel

Status: **draft, revised after Codex checkpoint 1 round 3 (§"Checkpoint log").** No filtered book (U_F), flagged
book (B_F), differential or MAX fidelity return has been computed on any month. One unfiltered book was seen
before this spec: step 2's equal-weight top-1,000 universe is U(top 1,000) (§"Samples and design history"). Programme: `docs/research/2026-10-04-strategy-research-sweep.md` §4 item 5.
Standards: `.claude/skills/quant/research-process.md`. Panel and book machinery: #3609 step 1 and step 2
(`docs/research/2026-10-04-3609-step1-factor-panel.md`, `docs/research/2026-10-06-3609-step2-factor-book.md`).

## Question

On the #3609 panel, net of our costs, what does excluding the names each avoidance filter flags do to an
equal-weight book's return and drawdown, per size segment, and which names does it remove?

The filters: MAX/lottery (Bali, Cakici & Whitelaw 2011), raw price below $5, and fewer than 36 months of archive
history (the house "IPO < 3 years" flag of `market-segments.md`, after Loughran & Ritter 1995). Short interest
(Drechsler & Drechsler 2014) is slice 5. Retail attention (Barber, Huang, Odean & Schwarz 2022) is not built
(premise 4).

**What kind of study this is.** A non-claiming, preregistered enumeration, like step 1's fidelity study
(`trial_register.py`, `3609-step1-fidelity-v1`). It claims no premium and no non-inferiority. Its verdicts only
decide which filter sets a later long-book spec **may cite** as support for an exclusion. Premise 3 explains why no
claim is made.

**What the answer can say.**
- **ELIGIBLE (filter set F, population P):** F's measured net effect on P's equal-weight reference book met
  §"Decision rule". A later long-book spec may cite this result for F under §"Inheritance contract".
- **NOT ELIGIBLE:** a later spec may not cite this study as support for F on P.
- **Existing defaults are not changed by either verdict.** `market-segments.md` already says to exclude sub-$5
  names by default, young names and high-MAX names from longs. Those rest on the published record. This study's
  verdicts are recorded beside them as evidence about this implementation on our panel; a NOT ELIGIBLE verdict
  obliges the next long-book spec drawing from P to say how it treats that default, and does not repeal it.
- **Step 3's MAX requirement stands.** Step 2 says step 3's forward construction "must include the cap and the MAX
  filter" (step 2 §"Exceptions to the skills"). This spec does not amend that. It supplies the MAX filter's frozen
  definition (§"Source rules"), and its MAX verdicts and fidelity result are evidence that step 3's own declared
  backtest must report. Step 2 failed its screen (#3609, 2026-10-08), so no step 3 is currently scheduled.
- **Never:** a strategy admission, a change to the 2026-10-08 demo screen, or a relaxation of the evidence bar for
  any book that adopts a filter.

**Estimand.** Step 1's restricted population (linked 10-K/10-Q filers our archive prices), formations 2014-09..
2024-07. Nothing is generalised to CRSP or to today's eToro universe. The books are references, not tradable
strategies; historical eToro eligibility is not checked.

## Inheritance contract

What a later long-book spec may cite, and on what terms:
- **The whole flag construction, unchanged:** §"Source rules" including the breakpoint population (all admitted
  panel names at M), the missing-value treatment and the screens, applied to a book drawn from a tested
  population P. P is the book's **opportunity universe before stock selection**: a book that selects and weights
  holdings from P can cite P's result, and its own backtest tests those holdings and weights. An opportunity
  universe narrower than P, a mixture of populations, or a recomputed breakpoint universe is a construction change:
  it may cite this study only as background, not as an ELIGIBLE result.
- **Selection is carried forward, not discharged.** The register counts this study's 62 searches (§"Registration"),
  but counting is not selection control. A later book that adopts any set must list this study's full candidate
  set (5 sets × 6 populations) and verdicts in its own design history, and its PBO/CSCV or nested evaluation
  (`research-process.md` §"Every study is registered") must treat the filter choice as one of its selections.
- **Reuse is recorded.** This study reads stages A and B. A later book backtested on the same months after adopting
  a filter from here reuses already-inspected data again; its results there are retrospective, and its declaration
  carries the actual access and design-history inventory. Any admission that requires a
  sealed confirmation must use a sample neither this study nor that book's design has read.
- **Costs.** The verdicts are spread-only (§"Known limits"). A later book assesses its own notional-dependent costs.

## Premises (measured)

All figures in premises 1 and 2 come from one command: `uv run python -m scripts.measure_3621_filter_premise`
(stage-A artefact `2026-10-07-0e8dba6e-stageA`, manifest sha256 `e5087210…`, verified by
`read_verified_artefact`). The script implements §"Source rules"' flag function in full, both screens included.
The build's slice 1 re-implements it as a tested module and must reproduce these counts on stage A.

**1. Where the filters bite (counts only, no return read).** Names flagged per stage-A formation (80 formations,
2014-09..2021-04). Medians over the 80; the script prints min, max and the per-formation table.

| population | admitted | MAX | price < $5 | archive < 36 m | any | any / admitted (min–max) |
|---|---|---|---|---|---|---|
| micro | 1,344.5 | 230.5 | 528.5 | 277 | 755.5 | 50.9%–72.2% |
| small | 784.5 | 47 | 17 | 123.5 | 173.5 | 16.8%–29.3% |
| large | 595 | 17 | 1 | 53 | 67.5 | 8.3%–19.9% |
| mega | 331.5 | 4 | 0 | 13 | 16 | 2.1%–9.0% |
| top 1,000 | 1,000 | 23 | 2.5 | 74.5 | 97 | 6.9%–15.4% |
| rest | 2,025 | 279 | 540 | 388.5 | 909 | 39.1%–54.7% |

MAX missing values per formation (max over the 80): screened windows (flagged) at most 5 in micro and rest, 1 in
small, large and top 1,000, 0 in mega; windows with fewer than 15 returns (not flagged) at most 18 in micro and
rest, 2 in small, 1 in large, mega and top 1,000; windows with 10 or more zero returns (not flagged) at most 55 in
micro, 57 in rest, 5 in small, 2 in large and top 1,000, 0 in mega. The median zero-heavy count is 35 in micro.

These are counts on stage A only. They say where the exclusions fall, not what they cost or earn.

**2. Published series to 2014-09 (JKP US, monthly, capped value weight).** JKP publishes each factor already signed
by its Table 9, so a positive return is the expected direction. For all three the sign is −1
(`docs/research/3609-jkp-table9-signs.csv`), so the series are low MAX minus high, young minus old and low price
minus high. The plain t is mean / (sd / √n), before dependence.

| factor | months | mean / month | sd | annualised IR | t |
|---|---|---|---|---|---|
| `rmax1_21d`, 1926-02..2014-09 | 1,064 | 0.00278 | 0.04668 | 0.206 | 1.94 |
| `rmax1_21d`, 2011-02..2014-09 (after BCW) | 44 | 0.00281 | 0.02953 | 0.329 | 0.63 |
| `age`, 1926-02..2014-09 | 1,064 | −0.00087 | 0.03033 | −0.100 | −0.94 |
| `prc`, 1926-01..2014-09 | 1,065 | 0.00182 | 0.04402 | 0.144 | 1.35 |
| `prc`, 1990-01..2014-09 | 297 | 0.00026 | 0.03682 | 0.024 | 0.12 |

Readings, limited to these series:
- **MAX:** low-MAX names beat high-MAX names on non-micro, capped value weights, at t 1.94 over the long history.
  The post-publication estimate is positive, but 44 months give t 0.63.
- **`age`:** negative, so old firms beat young ones, the direction the seasoning filter assumes, at t −0.94. JKP's
  `age` is months since first Compustat or CRSP appearance, not our archive seasoning.
- **`prc`:** low-priced non-micro stocks earned slightly more, not less. The case for the sub-$5 exclusion is cost
  and microstructure, not return prediction: most anomalies are strongest in micro caps and fail value-weighted
  (Hou, Xue & Zhang 2020), and our cost band below $5 is the widest (`cost_model.py`). The net measurement here is
  what tests that case.
- JKP's factors are built on non-micro names, so none of them speaks to micro caps, where premise 1 shows most
  exclusions fall. The artefact's JKP series end at 2021-05.

**3. Why no claim is made.** No substantively justified non-inferiority margin, meaning a tolerable harm fixed
before power, has been established for these exclusion differentials. No published effect size applicable to these exact
exclusion differentials has been identified and adopted for this declaration, so no claim is proposed. For scale only: JKP's capped MAX factor at its post-publication IR of 0.329 would
need `((1.645 + 0.842) / 0.329)²` ≈ 57 independent years to reach 80% power for one configuration. That is a
different estimand from these equal-weight differentials. The study is therefore non-claiming, as step 1's
fidelity study was, and its screen is a point-estimate screen, never significance.

**4. Data for the two filters not built here.**
- **Short interest.** FINRA's bimonthly file covers exchange-listed names only from the 2021-07 settlement files
  (`data-sources/finra.md`, the pre-June-2021 note), so it lies wholly inside stage B. As stored it is not point in
  time: `filed_at` is the settlement date and FINRA publishes days later (`quant/data-map.md`). It is not in the
  panel artefacts. Slice 5 captures it with a publication-calendar lag and has its own spec addendum and
  registration.
- **Retail attention.** Two proxies were examined.
  - Barber et al. 2022's return effect is measured on Robinhood user-count herding events (Robintrack,
    2018-05..2020-08), a series we do not hold.
  - Abnormal volume (Barber & Odean 2008) measures who buys. Gervais, Kaniel & Mingelgrin 2001 find high abnormal
    volume is followed by higher returns, the opposite of an avoidance signal.

  The Robinhood proxy cannot be built from what we hold. Abnormal volume could be built from the panel's daily
  volume, but it is not selected, because the cited evidence points the other way for an avoidance rule. So no
  attention filter is built. Whether MAX captures part of the same behaviour is not
  measured here.

## Source rules

| filter | governing source | rule as published | our implementation |
|---|---|---|---|
| MAX value | JKP replication code, `bkelly-lab/ReplicationCrisis` at `67174c7f`: `GlobalFactors/main.sas` (`roll_apply_daily(... __n=1, __min=15 ...)`, `_21d` set) and `GlobalFactors/market_chars.sas` (`roll_apply_daily`, `rmax1 = max(ret)`; `zero_obs < 10`) | the largest daily total return in the stock's calendar month; at least 15 returns with non-missing excess return; stock-months with 10 or more zero returns dropped | the largest daily total return over the SPY sessions of s(M)'s calendar month up to s(M) (s(M) is the month's last SPY session); at least 15 returns; fewer than 10 zero returns |
| MAX threshold | Bali, Cakici & Whitelaw, NBER WP 14804 (the working paper of the 2011 JFE article), §II.B, p. 7: decile portfolios "formed by sorting the NYSE/AMEX/NASDAQ stocks" on the previous month's MAX, the effect in decile 10 | deciles over the whole sample of stocks, not NYSE breakpoints; no tie convention stated | top decile over all admitted panel names with a value at M (our sample in place of CRSP's); ties frozen below |
| price | `research-process.md` §"Returns"; `market-segments.md` | raw price, never split-adjusted; sub-$5 excluded by default | raw close at s(M) below `factor_book_path.PRICE_FLOOR` |
| seasoning | `market-segments.md`: "IPO < 3 years … Exclude from longs" (it cites Loughran & Ritter 1995 and Brav & Gompers 1997 for the underperformance; no paper locator is verified here) | listing age under 36 months | house proxy: `factor_book_path.archive_seasoned` false, first admitted archive bar later than 36 months before s(M) |
| size | `market-segments.md`: NYSE breakpoints | the house NYSE set with overlaid market cap | JKP's NYSE cutoffs at M, as step 2 substituted (below) |

Details:
- **MAX returns and screens: substitutions for JKP's availability rules, stated.** JKP keeps days with a market
  factor return (`mktrf` non-missing, its trading-day test) and counts returns with a non-missing excess return.
  We use SPY sessions as trading days and count total returns between usable bars on adjacent SPY sessions. That
  replaces JKP's excess-return availability test with total-return availability, a house substitution; no RF
  coverage or day-level equivalence with JKP's sample is claimed. JKP's `zero_obs` counts zero local-currency returns over all the stock's trading days in the month,
  before the excess-return filter; ours counts exact zeros among our counted returns, a substitution that can
  differ where a series has unusable bars. JKP takes `max(ret)` after a descending rank filter (`ret_rank <= 5`),
  which drops a stock-month whose highest return is tied across ten or more days; we take the plain maximum, which
  differs only in that case, and slice 1 has a fixture for it. Both of the panel's screens apply, as in
  `factor_panel_prices.series_prices`: a return below `SCREEN_RETURN_LOW` or above `SCREEN_RETURN_HIGH`, or an
  `adj_close / close` move with |ln(ratio₁ / ratio₀)| > ln(1 + `SCREEN_RATIO_MOVE`) = ln 1.5 (a ratio change above
  ×1.5 or below ×2/3) between consecutive usable session bars with no stamp between them, screens the window when
  the pair's later bar is inside it. The ratio screen compares consecutive usable bars even across missing
  sessions; the extreme-return screen applies only to adjacent-session returns.
- **MAX missing values, in precedence order:**
  1. **Screened:** the name is flagged. Our name for this rule is "MAX plus screened-data exclusion". It is a house
     construction choice under evaluation here, not BCW's result: a screened window holds either an extreme move
     or a data defect. Whether excluding those names helps is what the separately reported screened-only
     component measures (§"Diagnostics").
  2. **Fewer than 15 returns:** no value, and not flagged.
  3. **10 or more zero returns:** no value, and not flagged.
- **MAX threshold, frozen (a house empirical-percentile convention; BCW state none).** Of the N valid values at M,
  the cutoff q is the ⌈0.9 N⌉-th smallest (1-based), and a name with a value ≥ q is flagged. With distinct values
  that flags N − ⌈0.9 N⌉ + 1 names (101 of 1,000), and ties at q add more. N = 0 at any formation refuses the run (`MAX_EMPTY`).
- **Breakpoint population.** BCW sort their whole sample (above), so the cutoff is taken over the whole admitted
  panel, not within each population: "high MAX" then means the same level in every population. BCW also report
  that excluding stocks priced below $5 leaves the value-weighted result essentially unchanged and strengthens the
  equal-weighted one (printed pp. 9–10, the paragraph on measurement issues in low-priced stocks). The departure is the sample: step 1's restricted population instead of CRSP's
  NYSE/AMEX/NASDAQ stocks.
- **Seasoning error direction.** When the archive's coverage of a series starts after the security began trading,
  its first archive bar is later than its listing. An old stock can then be flagged young: the proxy can
  over-flag, never under-flag.
- **Size segments:** JKP's `nyse_p20`, `nyse_p50` and `nyse_p80` at M, in USD millions, frozen in both artefacts.
  Micro is ME < p20, small p20 ≤ ME < p50, large p50 ≤ ME < p80, and mega ME ≥ p80. The house NYSE set has no
  effective-dated exchange history in the panel's frozen inputs, which is why step 2 made the same substitution.

## Samples and design history

- **One path, 2014-09-30 to 2024-08,** over the stage-A and stage-B artefacts of #3609 (`2026-10-07-0e8dba6e-stageA`
  and `2026-10-08-7b3169b6-stageB-2039b95f…`), with step 2's timing, statuses, termination arms, cost bands and
  step 0's stress cost.
- **Retrospective evidence, not confirmation.** Stage A is #3609's development sample, and stage B is reused
  validation that has been read (below). No sample here is sealed, and every verdict is labelled retrospective.
- **Design history** (what had been seen when this spec was written):
  - step 0's baselines on both stages;
  - step 2's value book and its references on both stages. That includes its equal-weight top-1,000 universe,
    which is this study's unfiltered top-1,000 book: step 2 printed 3.73% best case and 3.12% worst case
    annualised growth on stage B (#3609, 2026-10-08 16:50Z). Step 2's book applied the sub-$5 and seasoning entry
    rules inside its value selection;
  - step 1's fidelity results on stage A (run `b6378c7c`, `docs/research/3609-ledger.jsonl`), including the
    `rvol_21d` failure, and step 2's premise-6 stage-A exposure plan. The exposure plan ran step 2's value-book
    construction, which applies the price and seasoning entry rules inside the book. Neither exercise measured a
    standalone filter differential or any MAX quantity;
  - premise 1's stage-A counts and premise 2's published series.
- **Access.** Two accesses, kept distinct:
  - **Prior, already done:** the premise measurement (2026-10-08) read the stage-A artefact's admitted rows, daily
    bars, first bars and reference snapshots, and no holding return. It is disclosed here, not backdated into the
    ledger.
  - **Execution:** after the declaration lands (§"Registration"), a `strategy_holdout_accesses` row is written
    before the first post-declaration read of either artefact, including the stage-B daily bars used for MAX and
    slice 1's stage-A count reproduction. Fixture work precedes both.

## The books

For each population P and formation M, the **target set** is defined directly:
- **Unfiltered U(P, M):** every admitted name in P at M. None of step 2's entry or retention rules (the $5 floor,
  seasoning, composite or tercile band) applies.
- **Filtered U_F(P, M):** U(P, M) minus every name F flags at M.
- **Flagged B_F(P, M):** the names F removed. Printed only.

Each book holds its target set at equal weight after the trades at s(M). A held name outside the new target is sold
at s(M); a name that enters or re-enters the target is bought at the same s(M). The books are step 2's
`factor_book_references.reference_decisions` with `Formation.universe` set to the target set, valued by
`factor_book_path.value_path` (statuses, both termination arms, entry bands, path ends). When the target is empty, the
book sells its holdings at s(M) and then holds cash earning 0. Trade costs follow `value_path`'s timing: the cost
of formation M's trades falls in month M's return (NAV after the trades over the previous month-end NAV), as in
step 2. A book empty before and after formation M has a return of 0 for month M + 1.

**Populations with verdicts:** micro, small, large and mega (the `market-segments.md` primary partition), plus top
1,000 (step 2's book universe) and rest (every other admitted name). Six populations. **All admitted names** is
printed as the pooled diagnostic, with no verdict.

**Filter sets:** MAX; sub-$5; young; sub-$5 + young; all three. Five sets. No other combination is eligible from
this study.

## Decision rule (frozen before any new outcome; U(top 1,000) was seen, §"Samples and design history")

Annualised log growth G = (12/n) Σ ln(1 + r_t) over a window's monthly net returns, step 2's statistic. For each
(F, P), ΔG = G(U_F) − G(U).

**ELIGIBLE** iff all of:
1. **Whole path:** ΔG ≥ 0 in both termination arms, at base cost and at step 0's stress cost.
2. **Stage B alone** (2021-06..2024-08, the Form 25-checked regime): ΔG ≥ 0 in both arms at base cost.
3. **Holdings-count gate:** U_F(P, M) holds at least 10 names after the trades at every formation (step 2's
   floor). This is a cross-sectional floor, not an effective-sample measure.

**Effective-sample exception, declared.** `research-process.md` asks for a minimum effective sample per cell. This
descriptive enumeration declares none: its verdicts are point-estimate screens that claim nothing about
significance. Per cell it prints the formations with exclusions (a formation at which the filter flagged a name in U), by stage.
4. **For any set containing MAX:** the MAX fidelity check below passes.

Otherwise **NOT ELIGIBLE**, with every failed condition named. A set that flags no name at any formation is
`NO_EFFECT` and not eligible: there is nothing to adopt. Condition 2 is always evaluated from the actual stage-B ΔG:
U and U_F can enter stage B holding different names, so a set that excludes nothing there can still differ there.
A set with no formation with exclusions in stage B carries the label `no stage-B exclusions` beside its verdict;
an ELIGIBLE verdict with that label is never read as corroboration in the Form 25-checked regime.

**Non-positive wealth, by book role.** If U or U_F of a pair reaches non-positive wealth in any arm or cost
scenario, that pair is `REFUSED` with the book, arm, scenario and month. Other pairs are unaffected. Diagnostic-only
books (B_F, the screened-only book, the pooled population's books) gate nothing. For any book that reaches
non-positive wealth, whether diagnostic or one of a REFUSED pair's, every statistic of that book, and every
differential or wealth ratio using it, over a window that contains or follows the exhaustion month is printed as
`undefined (wealth exhausted at M)`. Windows that end before it, and statistics of books not involved, are printed
normally. No window is truncated, and no other pair is stopped.

The margin is zero. That is the floor the 2026-10-08 entry sets for its own point-estimate condition, because a
filter whose measured net effect on our data is negative has no case for citation. These are point estimates; no
significance is claimed.

**MAX fidelity (stage A, holding months 2014-10..2021-05, where JKP's series ends).**
- **Construction.** Our `rmax1_21d` long-short is built as step 1 built its characteristics
  (`factor_panel_fidelity.factor_month`: JKP terciles on non-micro names, value weights capped at the NYSE p80,
  sign −1). Only names with a numeric value enter. Screened, short and zero-heavy names are left out of the
  terciles; they are not given a sentinel rank.
- **Comparison.** It is compared with JKP's `rmax1_21d` through `compare_arm` and `characteristic_verdict`, with
  step 1's price-characteristic bars passed explicitly: correlation 0.90, beta 0.7–1.3, offset 0.03, the
  lead-lag rule, `MIN_LEG` 5, `MIN_PAIRS` 60 and `MAX_UNDERSIZED` 2.
- **Registry.** Step 1's characteristic registry (`CHARACTERISTICS`, `CORRELATION_BAR`) and its search count are
  not changed. MAX's configuration lives in this study's module.
- **Scope.** It checks that the reconstructed factor-return series tracks JKP's. It does not show name-level
  ranking agreement, the top-decile threshold or anything about micro caps, and the report says so.

**The verdict line** lists, for each of the 30 (F, P) pairs, ELIGIBLE, NOT ELIGIBLE (with conditions), NO_EFFECT or
REFUSED, and the MAX fidelity verdict.

## Diagnostics (printed, never gated)

**Windows.** Every window statistic is a slice of the one continuous path, with its actual returns and costs.
Windows are labelled by holding month. A formation's diagnostics (flag counts, excluded weight) belong to the
holding month it starts, except trade costs, which `value_path` books in month M (§"The books"): stage A is formations 2014-09..2021-04 for holding months 2014-10..2021-05, stage B
formations 2021-05..2024-07 for 2021-06..2024-08, and calendar years follow the holding month. A
window's drawdown starts from its opening wealth, which is its first high-water mark. The calendar years 2014
(Oct–Dec) and 2024 (Jan–Aug) are labelled partial.

For each (F, P) including the pooled population, each arm, at base and stress cost, on the whole path, stage A,
stage B and each calendar year:
- for U_F, U and B_F:
  - G;
  - arithmetic annualised return, 12 × the mean monthly return;
  - volatility, the sample sd of monthly returns × √12;
  - maximum drawdown;
- the monthly differential D_t = r(U_F) − r(U):
  - its mean and sample sd;
  - annualised IR, 12 × mean / (sd × √12), printed `undefined` when sd is 0;
  - its Newey–West t: Bartlett kernel, lag 3, null zero, descriptive only, the step 2 estimator;
  - the maximum drawdown of the wealth ratio W(U_F) / W(U);
- the number of months in the window, the number with a defined D_t, the formations with exclusions, and the
  first and last month;
- names flagged per formation (min, median, max), split by filter and, for MAX, into above-cutoff and screened,
  with the short and zero-heavy counts;
- excluded weight, the flagged count divided by |U(P, M)|: the target-weight share of U at formation M;
- turnover (traded notional, step 0's definition) and cost of U_F and U, and the difference;
- the screened-only component: U minus the screened names alone, against U, so the data-exclusion part of the MAX
  set is visible.

**Exceptions to `market-segments.md`, stated.**
- **Size × cost return cells are not built.** The cost axis enters through the band charged to each name. The
  sub-$5 filter is itself the lowest-band cut. Crossing every population with the price bands would split the
  smaller segments into cells with few names. Per formation, the counts of flagged and retained names in each size ×
  cost-band cell are printed.
- **No volatility axis,** inherited from step 2 (§"Exceptions to the skills").

**Names excluded:** one gzipped JSON line per (M, `name_key`, symbol, population, filters that flagged it), with
its sha256 in the report.

## Registration

- One trial, `3621-avoidance-filters-v1`. It is a preregistered enumeration, non-claiming in #2599's sense, with no
  `TrialDesign`, as for `3609-step1-fidelity-v1` and `3609-step2-book-v1`.
- **Searches: 62.** That is 5 filter sets × 6 populations × 2 arms = 60, plus the MAX fidelity check's 2 arms. The
  cost scenarios and windows are conditions within each configuration, never alternatives, so they add no
  searches. The pooled diagnostic carries no verdict and adds none. It is not a `hunt-` entry, so M_inh grows by
  62.
- **Ordering.** The declaration lands before any slice runs on research outcomes, the fidelity check included.
  Until then, code is tested on fixtures only.
- **Evidence pins:** this spec's sha256, the two artefact manifest digests, the step 0 manifest (stress cost), the
  construction hash of the run's modules, and the JKP code commit above.
- **Ledger:** step 2's pattern, in `docs/research/3621-ledger.jsonl`, with declared, access, run and report rows.

## Slices

1. The flag function (`rmax1_21d` with both screens and the missing-value order, the threshold, price and
   seasoning). Pure, tested on fixtures, and it must reproduce premise 1's stage-A counts.
2. Target sets, the three books per (F, P) on `value_path`, ΔG and the diagnostics. Tested on fixtures.
3. MAX fidelity through step 1's functions with this study's configuration. Tested on fixtures.
4. Declaration (register r28), access, run, report and ledger, plus the record of the verdicts in
   `market-segments.md`.
5. Short interest: a spec addendum, capture with the publication lag, and its own registration.

## Known limits

- Archive seasoning over-flags old names whose archive coverage starts late (§"Source rules").
- Condition 2 shows only that the filter did not lower growth on stage B; a set with no stage-B exclusions is
  labelled.
- The panel is retrospectively filtered (step 1 premise 4). Survivorship is unverified for 2014-09..2018 (step 2
  §"Known limits"). Condition 2 checks stage B, which lies in the Form 25-checked regime.
- **Costs are spread-only, a declared exception to `research-process.md` §"Costs".** Step 0's bands come from nine
  summer calibration dates with no stressed regime, plus step 0's stress multiplier. No gap, slippage, minimum-ticket
  or fixed-fee term is modelled. That matters most in the sub-$5 and micro cells, so every verdict is "eligible
  under the spread-only model".
- The books are equal-weight references. A later book's weighting changes how much any exclusion matters.

## Checkpoint log

**Round 1 (Codex, 2026-10-08): 40 findings, all applied.**

The first draft gated a Track B non-inferiority test, with a margin sized to what the data could power, and adopted
the filters automatically as standing rules. Several findings showed that design did not hold:
- the margin had no tolerable-harm basis (findings 1–3);
- the statistic ignored the margin's estimation error (4);
- nominal years were used as effective years (5–7);
- the eligible set was selectable from diagnostics (9);
- adoption overreached the tested construction (11–12);
- the hold-out reasoning and the declaration timing were wrong (13–15);
- survivorship conflicted with automatic adoption (33).

The study is now non-claiming. Its verdicts only make a set citable, and the declaration precedes every outcome
read.

The remaining findings and how each was applied:
- **8:** the MAX-fidelity fallback is gone, and sub-$5 + young is an enumerated set.
- **10:** step 3 is separated.
- **16:** the status line and the design history agree on what has been seen.
- **17–19:** the MAX rules were re-sourced; round 2 replaced them.
- **20:** the order statistic and ties are frozen.
- **21:** the size substitution is stated.
- **22–23:** the seasoning error direction and its label are corrected.
- **24–25:** the counts and the missing-value treatment are fixed.
- **26–27:** the premise reading is narrowed, and rest is counted.
- **28:** the partition is stated.
- **29–31:** the premise 2 readings are corrected and the figures made reproducible.
- **32:** the fidelity scope is stated.
- **34:** costs are covered.
- **35–37:** the books, degenerate cases and diagnostics are defined.
- **38:** no `TrialDesign`.
- **39:** the attention claim is narrowed.
- **40:** the slice numbering is fixed.

**Round 2 (Codex, 2026-10-08): 23 findings, all applied.** Codex judged the non-claiming framing, the zero-margin
screen and the search count defensible.
- **1:** finding 16 of round 1 is logged.
- **2–3:** step 2's MAX requirement and the `market-segments.md` defaults are stated as unchanged.
- **4–6:** §"Inheritance contract" carries the selection control, the reuse and the construction terms.
- **7–9:** the MAX rule is re-sourced from JKP's code at a pinned commit:
  - the calendar-month window (JKP's `__n=1`), not 21 sessions;
  - the 15-return minimum, which is JKP's `__min=15`;
  - the zero-return rule, newly added;
  - the source-rule table;
  - the breakpoint population is labelled a house substitution with its reason.
- **10:** the screened rule is named and reported separately.
- **11:** premise 3 is reworded.
- **12:** the premise script now implements both screens and the JKP window, so premise 1 is final, and slice 1 must
  reproduce it.
- **13:** the holdings gate is renamed and temporal coverage printed.
- **14:** the size × cost exception is stated.
- **15:** the pooled diagnostic is added.
- **16:** the cost exception is declared.
- **17:** MAX's fidelity configuration lives in this study's module, and step 1's registry is unchanged.
- **18:** the fidelity missing-value contract is set.
- **19:** the fidelity scope is reworded.
- **20:** non-positive wealth is handled by book role.
- **21:** the formulas and windows are frozen.
- **22:** the access row precedes any read.
- **23:** the design-history statement is narrowed.

**Round 3 (Codex, 2026-10-08): 18 findings, all applied.** Codex judged the framing sound, and the 62-search count
defensible.
- **1:** the status line and the decision-rule heading name U(top 1,000) as seen.
- **2:** the MAX breakpoint is now sourced from BCW's working paper. It sorts the whole sample, so the cutoff is
  taken over the whole panel.
- **3–5:** JKP's trading-day test, `zero_obs` population and rank filter are stated as substitutions, with a tie
  fixture.
- **6:** the screened rule is stated as a choice under evaluation.
- **7:** premise 3 is narrowed.
- **8:** P is the opportunity universe before selection.
- **9:** the prior and execution accesses are separated.
- **10:** step 1 and the exposure plan are inventoried.
- **11:** the reuse count is removed.
- **12:** the effective-sample exception is declared, and active months are printed.
- **13:** a stage-B no-effect pass is labelled.
- **14:** B_F exhaustion is defined by window.
- **15:** the empty target is defined.
- **16:** windows are aligned by holding month.
- **17:** the attention wording is fixed.
- **18:** the 36 months are attributed to `market-segments.md`.

**Round 4 (Codex, 2026-10-08): 8 findings, all applied.** Codex judged the framing sound.
- **1:** condition 2 always reads the actual stage-B ΔG, and the label becomes `no stage-B exclusions`.
- **2:** the RF claim is removed, and total-return availability is a stated substitution.
- **3:** the exposure plan's use of the price and seasoning rules is stated.
- **4:** the exhaustion contract covers every book.
- **5:** the percentile convention is labelled, with its cardinality.
- **6:** the ratio screen's log inequality is written out.
- **7:** cost timing follows `value_path`, with costs in month M as step 2 does. The finding's suggestion of M + 1
  contradicts `factor_book_path.py`'s timing note, so it was not taken.
- **8:** the BCW page locator is corrected.
