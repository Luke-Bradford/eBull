# #3621 — avoidance filters measured as exclusions on the #3609 panel

Status: **draft, revised after Codex checkpoint 1 round 1 (§"Checkpoint log").** No book built from this spec has
been valued on any month; the outcomes already seen are listed in §"Design history". Programme:
`docs/research/2026-10-04-strategy-research-sweep.md` §4 item 5. Standards: `.claude/skills/quant/research-process.md`.
Panel and book machinery: #3609 step 1 and step 2 (`docs/research/2026-10-04-3609-step1-factor-panel.md`,
`docs/research/2026-10-06-3609-step2-factor-book.md`).

## Question

On the #3609 panel, net of our costs, what does excluding the names each avoidance filter flags do to an
equal-weight book's return and drawdown, per size segment, and which names does it remove?

The filters: MAX/lottery (Bali, Cakici & Whitelaw 2011), raw price below $5, and fewer than 36 months of archive
history (the house "IPO < 3 years" flag of `market-segments.md`, after Loughran & Ritter 1995). Short interest
(Drechsler & Drechsler 2014) is slice 5. Retail attention (Barber, Huang, Odean & Schwarz 2022) is not built
(premise 4).

**What kind of study this is.** A non-claiming, preregistered enumeration, like step 1's fidelity study
(`trial_register.py`, `3609-step1-fidelity-v1`). It claims no premium and no non-inferiority. Its verdicts only
decide which filter sets a later long-book spec **may** inherit by citing this study. Premise 3 explains why no
powered claim is available.

**What the answer can say.**
- **ELIGIBLE (filter set F, population P):** F's measured net effect on P's equal-weight reference book met the
  screen in §"Decision rule". A later long-book spec drawing from P may adopt F as an exclusion. That book's own
  declared backtest, run with F applied, is the test of F inside that book: an equal-weight reference result does
  not carry over to a scored, concentrated or differently weighted book.
- **NOT ELIGIBLE:** a later spec may not cite this study to adopt F for P. The `market-segments.md` defaults for
  sub-$5 and young names rest on the published record and stay as written, and step 2's book already applies
  both. The skill records the result beside them, and the next long-book spec drawing from P states how it
  treats them.
- **Never:** a strategy admission, a change to the 2026-10-08 demo screen, or a relaxation of the evidence bar for
  any book that adopts a filter.
- **Step 3's MAX prerequisite.** Step 2 makes "#3621's frozen MAX filter" a prerequisite of step 3's construction
  (step 2 §"Exceptions to the skills"). This spec freezes the MAX definition and threshold (§"Source rules")
  whatever the verdicts. Whether step 3 then applies it is step 3's declared choice, tested in step 3's own
  backtest, with this study's MAX verdicts and fidelity result as its evidence. Step 2 failed its screen
  (#3609, 2026-10-08), so no step 3 is currently scheduled.

**Estimand.** Step 1's restricted population (linked 10-K/10-Q filers our archive prices), formations 2014-09..
2024-07. Nothing is generalised to CRSP or to today's eToro universe. The books are references, not tradable
strategies; historical eToro eligibility is not checked.

## Premises (measured)

All figures in premises 1 and 2 come from one command: `uv run python -m scripts.measure_3621_filter_premise`
(stage-A artefact `2026-10-07-0e8dba6e-stageA`, manifest sha256 `e5087210…`, verified by
`read_verified_artefact`).

**1. Where the filters bite (counts only, no return read).** Names flagged per stage-A formation (80 formations,
2014-09..2021-04). Medians over the 80; the script prints min, max and the per-formation table.

| population | admitted | MAX | price < $5 | archive < 36 m | any | any / admitted (min–max) |
|---|---|---|---|---|---|---|
| micro | 1,344.5 | 235 | 528.5 | 277 | 756 | 50.9%–72.3% |
| small | 784.5 | 48 | 17 | 123.5 | 174 | 16.7%–29.3% |
| large | 595 | 17 | 1 | 53 | 67.5 | 8.3%–19.3% |
| mega | 331.5 | 3.5 | 0 | 13 | 16 | 2.1%–9.0% |
| top 1,000 | 1,000 | 23 | 2.5 | 74.5 | 96.5 | 7.1%–15.0% |
| rest | 2,025 | 283 | 540 | 388.5 | 912 | 39.3%–54.7% |

- A screened MAX window (a daily return below −90% or above +300%) is flagged; there are at most 5 a month in
  any population. Windows with fewer than 15 returns are not flagged: at most 32 a month in micro, 2 in small and 1 in
  large, mega or the top 1,000.
- ⚠ **Provisional MAX counts.** The script applies the extreme-return screen but not `rvol_21d`'s ratio-move
  screen, which the build shares (§"Source rules"). The run prints the final counts.
- Inside the top 1,000 the median flagged set is 9.65% of names, mostly young ones. In micro caps every formation
  loses more than half.

**2. Published series to 2014-09 (JKP US, monthly, capped value weight).** JKP publishes these already signed by
its Table 9 so a positive return is the expected direction (`docs/research/3609-jkp-table9-signs.csv`: all three
are −1, that is low MAX minus high, young minus old, low price minus high). Plain t is mean / (sd / √n), before
dependence.

| factor | months | mean / month | sd | annualised IR | t |
|---|---|---|---|---|---|
| `rmax1_21d`, 1926-02..2014-09 | 1,064 | 0.00278 | 0.04668 | 0.206 | 1.94 |
| `rmax1_21d`, 2011-02..2014-09 (after BCW) | 44 | 0.00281 | 0.02953 | 0.329 | 0.63 |
| `age`, 1926-02..2014-09 | 1,064 | −0.00087 | 0.03033 | −0.100 | −0.94 |
| `prc`, 1926-01..2014-09 | 1,065 | 0.00182 | 0.04402 | 0.144 | 1.35 |
| `prc`, 1990-01..2014-09 | 297 | 0.00026 | 0.03682 | 0.024 | 0.12 |

Readings, limited to these series:
- **MAX:** low-MAX names beat high-MAX names on non-micro, capped value weights, at t 1.94 over the long history.
  The post-publication estimate is positive but 44 months give t 0.63.
- **`age`:** negative, so old firms beat young ones, the direction the seasoning filter assumes; t −0.94. JKP's
  `age` is months since first Compustat or CRSP appearance, not our archive seasoning.
- **`prc`:** low-priced non-micro stocks earned slightly more, not less. The case for the sub-$5 exclusion is cost
  and microstructure, not return prediction: most anomalies are strongest in micro caps and fail value-weighted
  (Hou, Xue & Zhang 2020), and our cost band below $5 is the widest (`cost_model.py`). The net measurement here is
  what tests that case.
- JKP's factors are built on non-micro names, so none of them speaks to micro caps, where premise 1 shows most of
  the exclusions fall. The artefact's JKP series end at 2021-05.

**3. Why no powered claim is made.** For the published MAX factor at its post-publication IR of 0.329, one
configuration needs `((1.645 + 0.842) / 0.329)²` ≈ 57 years to reach 80% power. That figure is for JKP's capped
long-short factor; the equal-weight exclusion differential here has its own variance and costs, and no published
effect size exists for it. A Track B non-inferiority claim would need a margin justified as tolerable harm before
power is computed (checkpoint 1, round 1, findings 2–7), and none is available that ten years could test. So the
study is non-claiming, as step 1's fidelity study was, and its screen is a point-estimate screen, never
significance.

**4. Data for the two filters not built here.**
- **Short interest.** FINRA's bimonthly file covers exchange-listed names only from the 2021-07 settlement files
  (`data-sources/finra.md`, the pre-June-2021 note), so it lies wholly inside stage B. As stored it is not point in
  time: `filed_at` is the settlement date and FINRA publishes days later (`quant/data-map.md`). It is not in the
  panel artefacts. Slice 5 captures it with a publication-calendar lag and has its own spec addendum and
  registration.
- **Retail attention.** Two proxies were examined. Barber et al. 2022's return effect is measured on Robinhood
  user-count herding events (Robintrack, 2018-05..2020-08), a series we do not hold. Abnormal volume (Barber &
  Odean 2008) measures who buys; Gervais, Kaniel & Mingelgrin 2001 find high abnormal volume followed by higher
  returns, the opposite of an avoidance signal. Neither gives a buildable filter, so none is built. Whether MAX
  captures part of the same behaviour is not measured here.

## Source rules

- **MAX: `rmax1_21d`, JKP's definition governs** (its published series is the fidelity reference): the largest
  daily total return over the 21 SPY sessions ending at s(M). BCW's MAX(1) uses the calendar month instead; the
  two differ when a month has other than 21 sessions, and this spec follows JKP.
  - **Minimum: 15 returns.** This is the house minimum `RVOL_MIN_RETURNS`, which `factor_panel_prices.py` calls
    "JKP's residual minimum, by analogy". It is a house rule, not a documented JKP `rmax1_21d` minimum.
  - A daily return exists only between usable bars on adjacent sessions, and both of `rvol_21d`'s screens apply
    (`factor_panel_prices.py`: extreme return, `SCREEN_RETURN_LOW` and `SCREEN_RETURN_HIGH`; and the unstamped
    ratio move), using `screened_at` exactly as `rvol_21d` does.
  - **Missing values.** A screened window is flagged: it holds either a lottery-sized move or a data defect, and
    either is a reason not to buy. A window with fewer than 15 returns is not flagged. Both counts are printed
    for every population and formation.
- **MAX threshold: top decile, a house rule.** BCW report decile sorts with the top decile carrying the effect;
  the breakpoint population used here, all admitted panel names with a value at M, is our substitution and is
  not claimed to be BCW's. The cutoff q is the ⌈0.9 N⌉-th smallest of the N values (1-based), and a name with a
  value ≥ q is flagged, so ties at q are flagged. N = 0 at any formation refuses the run (`MAX_EMPTY`).
- **Price below $5:** raw close at s(M) below `factor_book_path.PRICE_FLOOR`, never a split-adjusted price
  (`research-process.md` §"Returns").
- **Archive seasoning below 36 months:** `factor_book_path.archive_seasoned` is false. This is a proxy for
  listing age. When the archive's coverage of a series starts after the security began trading, its first
  archive bar is later than its listing, so an old stock can be flagged young: the proxy can over-flag, never
  under-flag. The 36 months are the house rule; Loughran & Ritter measure returns over the three years after an
  offering, which is a different event.
- **Size segments:** JKP's NYSE breakpoints at M (`nyse_p20`, `nyse_p50`, `nyse_p80`, in USD millions, frozen in
  both artefacts), the substitution step 2 made for `market-segments.md`'s NYSE set. A name with ME < p20 is
  micro, p20 ≤ ME < p50 small, p50 ≤ ME < p80 large, ME ≥ p80 mega.

## Samples and design history

- **One path, 2014-09-30 to 2024-08,** over the stage-A and stage-B artefacts of #3609 (`2026-10-07-0e8dba6e-stageA`
  and `2026-10-08-7b3169b6-stageB-2039b95f…`), with step 2's timing, statuses, termination arms, cost bands and
  stress cost.
- **Retrospective evidence, not confirmation.** Stage A is #3609's development sample, and stage B is reused
  validation that has been read (below). No sample here is sealed, and the verdicts are labelled retrospective.
- **Design history (what had been seen when this spec was written):**
  - step 0's baselines on both stages;
  - step 2's value book and its references on both stages, including its equal-weight top-1,000 universe, which
    is this study's unfiltered top-1,000 book (step 2 printed 3.73% best case and 3.12% worst case annualised
    growth on stage B, #3609, 2026-10-08 16:50Z). Step 2's book already applied the sub-$5 and seasoning entry
    rules;
  - premise 1's counts and premise 2's published series.
  - No filtered book, flagged-set return or MAX-sorted return has been computed on any month. The filter
    definitions come from §"Source rules" and were not chosen on any return.
- **Access.** The run logs a `strategy_holdout_accesses` row before it reads either artefact's holding returns,
  and the declaration precedes it (§"Registration").

## The books

For each population P and formation M, the **target set** is defined directly:
- **Unfiltered U(P, M):** every admitted name in P at M. None of step 2's entry or retention rules (the $5 floor,
  seasoning, composite, tercile band) applies.
- **Filtered U_F(P, M):** U(P, M) minus every name F flags at M.
- Each book holds its target set at equal weight after the trades at s(M). A held name outside the new target is
  sold at s(M); a name that enters or re-enters the target is bought at the same s(M). The books are step 2's
  `factor_book_references.reference_decisions` with `Formation.universe` set to the target set, valued by
  `factor_book_path.value_path` (statuses, both termination arms, entry bands, path ends).
- **Flagged book B_F(P, M):** the names F removed, valued the same way. Printed only.
- An empty target holds cash at 0 for the month.

**Populations:** micro, small, large and mega (the `market-segments.md` primary partition), plus top 1,000 (step 2's
book universe) and rest (every other admitted name). Six populations.

**Filter sets:** MAX; sub-$5; young; sub-$5 + young; all three. Five sets. These are the only sets a later spec may
cite; no other combination is eligible from this study.

## Decision rule (frozen before any outcome)

Annualised log growth G = (12/n) Σ ln(1 + r_t) over the window's monthly net returns, step 2's statistic. For each
(F, P), ΔG = G(U_F) − G(U).

**ELIGIBLE** iff all of:
1. **Whole path:** ΔG ≥ 0 in both termination arms, at base cost and at step 0's stress cost.
2. **Stage B alone** (2021-06..2024-08, the Form 25-checked regime): ΔG ≥ 0 in both arms at base cost.
3. **Information:** U_F(P, M) holds at least 10 names at every formation (step 2's minimum, fixed by construction).
4. **For any set containing MAX:** the MAX fidelity check below passes.

Otherwise **NOT ELIGIBLE**, with every failed condition named. A set that flags no name at any formation is
printed as `NO_EFFECT` and is not eligible: there is nothing to adopt. A series whose wealth becomes non-positive
refuses the run (`WEALTH_NONPOSITIVE`, as step 2).

The margin is zero, the floor the 2026-10-08 entry sets for its own point-estimate condition, because a filter
whose measured net effect on our data is negative has no case for inheritance. These are point estimates; no
significance is claimed.

**MAX fidelity (stage A, holding months 2014-10..2021-05, where JKP's series ends).** Our `rmax1_21d` long-short,
built exactly as step 1 built its characteristics (`factor_panel_fidelity.factor_month`: JKP terciles on non-micro
names, value weights capped at the NYSE p80, sign −1), against JKP's `rmax1_21d`, through `compare_arm` and
`characteristic_verdict` with step 1's price-characteristic bars (`CORRELATION_BAR` 0.90, beta 0.7–1.3,
`OFFSET_BAR` 0.03, the lead-lag rule). Scope: it checks that our `rmax1_21d` values rank non-micro names as JKP's
do. It does not validate the top-decile threshold or the micro segment, and the report says so.

**The verdict line** lists, for each of the 30 (F, P) pairs, ELIGIBLE, NOT ELIGIBLE (conditions) or NO_EFFECT,
and the MAX fidelity verdict.

## Diagnostics (printed, never gated)

For each (F, P), each arm, base and stress cost, on the whole path, stage A, stage B and each calendar year:
- G, arithmetic annualised return, volatility and maximum drawdown of U_F, U and B_F;
- the monthly differential D_t = r(U_F) − r(U): mean, standard deviation, annualised IR and Newey–West t (lag 3,
  null zero, descriptive only); its drawdown is the maximum drawdown of the wealth ratio W(U_F) / W(U);
- names flagged and weight excluded per formation (min, median, max), with the MAX screened and short counts;
- turnover (traded notional, step 0's definition) and cost of U_F and U, and the difference.

Also: size × cost-band counts of the flagged names per formation (`market-segments.md` asks for size × cost; the
sub-$5 filter is itself the cost-band cut, so returns are not crossed further), and the inherited step 2 exception
that no volatility axis is printed.

**Names excluded:** one gzipped JSON line per (M, `name_key`, symbol, population, flags), its sha256 in the report.

## Registration

- One trial, `3621-avoidance-filters-v1`: a preregistered enumeration, non-claiming in #2599's sense, with no
  `TrialDesign` (as `3609-step1-fidelity-v1` and `3609-step2-book-v1`). Searches: 5 filter sets × 6 populations ×
  2 arms = 60, plus the MAX fidelity check's 2 arms = 62. Not a `hunt-` entry, so M_inh grows by 62.
- The declaration lands before any slice runs on research outcomes. Code is tested on fixtures only until then.
  Its evidence pins this spec's sha256, the two artefact manifest digests, the step 0 manifest (stress cost), the
  construction hash of the run's modules and the frozen definitions above.
- The ledger follows step 2's pattern in `docs/research/3621-ledger.jsonl`: declared, access, run, report rows.

## Slices

1. `rmax1_21d` with both screens, and the flag function, pure, tested on fixtures.
2. Target sets, the three books per (F, P) on `value_path`, ΔG and the diagnostics, tested on fixtures.
3. MAX fidelity through step 1's functions, tested on fixtures.
4. Declaration (register r28), access, run, report, ledger, and the `market-segments.md` record of the verdicts.
5. Short interest: spec addendum, capture with the publication lag, its own registration.

## Known limits

- Archive seasoning over-flags old names whose archive coverage starts late (§"Source rules").
- The panel is retrospectively filtered (step 1 premise 4), and survivorship is unverified 2014-09..2018 (step 2
  §"Known limits"); condition 2 requires the effect on stage B, which lies in the Form 25-checked regime.
- Costs are step 0's bands, calibrated on nine summer dates with no stressed regime, plus step 0's stress
  multiplier. No gap, slippage or ticket term is modelled, which matters most for the sub-$5 and micro cells.
- Equal-weight references: a later book's weighting changes how much any exclusion matters.

## Checkpoint log

**Round 1 (Codex, 2026-10-08), 40 findings, all applied.** The first draft gated a Track B non-inferiority test
with a feasibility-sized margin and automatic standing adoption. Findings 1–7, 9, 11–15 and 33 showed the margin
had no tolerable-harm basis, the statistic ignored the margin's estimation error, nominal years were used as
effective years, and adoption overreached the tested construction. The study is now non-claiming, verdicts only
make a set citable by a later book spec that runs its own backtest, and the declaration precedes every outcome
read, the fidelity check included (15). Others: the MAX-fidelity fallback to an untested pair is removed and
sub-$5 + young is an enumerated set (8); step 3's prerequisite is separated (10); the 15-return minimum is a house
rule (17); JKP's window governs (18); the breakpoint population is a house substitution (19); the order statistic
and ties are frozen (20); the size substitution is stated (21); the seasoning error direction is corrected to
over-flag (22) and the 36 months labelled a house rule (23); MAX counts are labelled provisional (24); screened
windows are flagged (25); the premise reading is limited to the counts (26); `rest` is counted (27); the primary
partition, minimum information and the size × cost and volatility treatment are stated (28); premise 2's MAX
reading is softened and t printed (29); the 57-year figure is confined to its estimand (30); premise 2 is
reproduced by the premise script (31); fidelity's scope is stated (32); stress cost and the cost limits are in
the rule and limits (34); target sets are defined directly (35); degenerate cases are frozen (36); diagnostics
are defined (37); the register row needs no `TrialDesign` (38); the attention statement is narrowed (39); slice
numbering is reconciled and the short-interest margin removed (40).
