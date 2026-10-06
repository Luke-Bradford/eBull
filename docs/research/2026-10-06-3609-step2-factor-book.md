# #3609 step 2 — the factor composite book, backtested net of our costs

Status: **draft, blocked on an operator decision** (#3609, 2026-10-06: can a zero-capital demo test be authorised
by a Track B screen when no realistic edge is powered on our data? See premise 2). Revised after Codex checkpoint 1
round 1 (50 findings; §"Checkpoint log"). Round 2's 56 findings are not yet applied: the loop worktree holds them at
`var/research/3609_step2/ckpt1_round2.txt`. Most are construction detail; findings 1–8 are the decision above. Nothing is built. No book,
IC, spread or factor mean has been computed on any month. Programme: `docs/research/2026-10-04-strategy-research-sweep.md`
§4 item 2. Inherits from `docs/research/2026-10-04-3609-step1-factor-panel.md` §"Registration, ledger and what step 2
inherits" (amended below), and reports against `docs/research/2026-10-04-3609-step0-baselines.md`.

## Question

Does a long-only, equal-weight book of US large caps, chosen by an industry-relative composite of the characteristics
step 1 rebuilt faithfully, (a) carry the published factor exposures it is built for, and (b) beat SPY total return
and a matched random book net of our costs on a validation sample its construction never saw?

**What the answer can say.**
- **Pass:** the book is eligible for a declared forward demo test (step 3, the ranking-pot successor). The case
  for the premium rests on the published post-publication record (§"Adoption rationale"). This backtest shows that
  our implementation carries the intended exposures, and that it did not lose to SPY or to random trading net of
  our costs on the validation sample.
- **It does not prove a premium.** About ten years cannot statistically certify a realistic edge over SPY
  (premise 2), so no IR claim is made here.
- **Fail:** reported as "not demonstrated". No variant is built because of this result (§"Decision rule").

**Estimand.** Step 1's restricted population (linked 10-K/10-Q filers our archive prices), cut to its 1,000 largest
names each month. Nothing is generalised to CRSP or to today's eToro universe. Historical eToro eligibility is not
checked, so the backtest book is a reference, as B3 is in step 0; step 3 trades only the eToro-tradable intersection.

**Why this replaces v1.5.** The live ranking's weights were never backtested, and the 2026-10-04 committee found it
mis-specified (#3609 body). This book uses only characteristics that passed step 1, combined by a published recipe.

## Premises (measured)

**1. Five characteristics in three of step 1's frozen families are eligible.** Step 1's declared run (#3609
comment of 2026-10-06 01:36Z; `docs/research/3609-ledger.jsonl`, run `b6378c7c`) passed `at_gr1`, `be_me`, `gp_at`,
`ni_me` and `ocf_me`. `ope_be`, `ret_12_1` and `rvol_21d` failed the offset bar on one arm each, and no v2 is claimed.
Using step 1's family identities unchanged:
- **GP/A:** `gp_at`;
- **value:** `be_me`, `ni_me`, `ocf_me`;
- **investment:** `at_gr1`.

The profitability (`ope_be`), 12-1 momentum and low-volatility families have no eligible member, so they are absent.
Step 1 lets step 2 drop non-PASS characteristics.

**2. About ten years cannot power an IR claim.** Holding months 2014-10..2024-08 are 119, or 9.92 nominal years.
For a one-configuration Track B test, `power_check` needs a non-inferiority margin of at least 0.789 IR for 80% power:
- `critical_t` = 1.645 and `required_years` = 9.66 at δ = 0.80;
- stage B alone (3.25 years) needs δ ≥ 1.38.

Reproduce:

```
PYTHONPATH=. uv run python -c "from app.services.trial_register import *; import math; \
c=power_check(TrialDesign(EvidenceTrack.ADOPTION,0.8,'x',9.92,'x'),trials=1); \
print(c.critical_t, c.required_years, (c.critical_t+0.8416)/math.sqrt(9.92), (c.critical_t+0.8416)/math.sqrt(39/12))"
```

The skill's realistic skilled IR is 0.3–0.5 (`portfolio-construction-and-risk.md`). A margin that keeps the book at
or above SPY must not exceed the prior IR, and a margin of 0.79 or more does not. So no margin with that meaning
is feasible. Nominal years are also an upper bound on effective years. Consequences:
- Step 2 makes no IR claim and carries no `TrialDesign` (§"Registration").
- Its gate is an exposure test, which is powered because loadings are identified from co-movement, not from means,
  plus the point-estimate comparisons that `research-process.md` §Track B "Net result" and the 2026-08-23 settled
  decision require.

**3. The size rule.** No point-in-time exchange membership exists (step 1 premise 5), so NYSE breakpoints cannot be
computed at s(M). The book universe is a by-construction proxy: the top 1,000 by ME at s(M) (ties by `name_key`).

Measured over stage A's 80 formations (`PYTHONPATH=. uv run python -m scripts.measure_3609_step2_universe`, which
reads ME, `name_key`, characteristic presence and SIC only, never a return):

| quantity | min | max |
|---|---|---|
| admitted names | 2,975 | 3,156 |
| names above JKP's NYSE-median cutoff | 862 | 1,094 |
| ME of the 1,000th name ($B) | 1.74 | 3.72 |
| top-1,000 names below the NYSE median | 0 | 138 |
| above-median names outside the top 1,000 | 0 | 94 |
| above-median ME held inside the top 1,000 | 99.14% | 100% |
| top-1,000 names with ≥ 2 of the three families | 964 | 991 |
| top-1,000 names with all three | 613 | 683 |
| top-1,000 names with SIC 6221 (commodity pools' SIC) | 0 | 1 |

The proxy is close to the large-plus-mega tranche that `market-segments.md` makes the default for books traded more
than quarterly, but it is not that tranche. It matches the Russell 1000's count, not its float or reconstitution
rules. Stage B is measured in the declared run (§"Diagnostics") and the rule is not retuned.

**4. Stage B is not built.** The builder stops at 2021-05-31 (`scripts/build_3609_factor_panel.py:120`
`PRICE_BOUND`, refusal at `:1221-1222`). FSDS SUB is pinned 2012q1..2021q2 (`app/services/factor_panel_reference.py:36-39`).
No SIC-to-industry map exists in `app/`, `scripts/`, `sql/` or `tests/`.

**5. Survivorship regimes are inherited** (step 1 premise 1):
- 2014-09..2018: terminations recorded, coverage unverified;
- 2019 on: checked against a selected Form 25 set.

Stage B lies wholly in the second regime, which is one reason it carries the realised-return gate.

## Adoption rationale (Track B, per family)

Each family is a published premium with support outside its original sample. These are the skill's citations
(`strategy-menu.md`, `research-process.md`); no figure is quoted from them here.

| family | original | independent support after or outside the original sample |
|---|---|---|
| GP/A | Novy-Marx 2013 | survives in Hou, Xue & Zhang 2020; replicates in Jensen, Kelly & Pedersen 2023 |
| value (B/M, E/P, CF/P) | Fama & French; within-industry form Asness, Porter & Stevens 2000 | across asset classes, Asness, Moskowitz & Pedersen 2013; Jensen, Kelly & Pedersen 2023; `strategy-menu.md` records the 2007–2020 drawdown |
| investment (asset growth) | Cooper, Gulen & Schill 2008 | the q-factor I/A (Hou, Xue & Zhang); Jensen, Kelly & Pedersen 2023 |

**Transfer to this book is unmeasured by any source.** No cited source measures a long-only, industry-relative,
equal-weight, banded large-cap composite at eToro costs; McLean & Pontiff 2016 put average post-publication decline
at 58%. That transfer is exactly what gates G1 and G2 test.

## Source rules

| decision | rule | source |
|---|---|---|
| characteristic z-score | each month, rank the variable and standardise the ranks by their cross-sectional mean and standard deviation | Asness, Frazzini & Pedersen 2019 (QMJ), p. 9, eq. (2); AQR working-paper PDF, sha256 `761c42f9…fbb2a9`, pinned in slice 1 |
| industry-relative form | rank and standardise within industry | QMJ p. 13 ("varying the standardization universe of our z-scores … by country-industry"); `portfolio-construction-and-risk.md` ("Rank accounting signals within industry") |
| family and composite | family = z(Σ member z); composite = z(Σ family scores) | QMJ eq. (2): a component is the z-score of the sum of its members' z-scores |
| industry | Fama-French 12 from SIC; codes outside French's listed ranges are "Other" | `market-segments.md` §6; French's data library `Siccodes12.zip`, pinned in slice 1 |
| SIC as of M | step 1's accession-specific SUB `sic`, under step 1's cutoff | step 1 §"Universe at M" step 4 |
| direction | JKP Table 9 sign | `docs/research/3609-jkp-table9-signs.csv` |
| entry and hold band | enter in the top decile; hold until the name leaves the top tercile | Novy-Marx & Velikov 2016 (`portfolio-construction-and-risk.md` §Turnover control) |
| weights | equal weight | DeMiguel, Garlappi & Uppal 2009 (same skill) |
| default long exclusions | raw close < $5; under 36 months listed | `market-segments.md` ("Exclude by default"; "Exclude from longs", Loughran & Ritter 1995) |
| timing, ME, characteristics, returns, terminations | step 1, unchanged | step 1 §"Dates and stages", §"Market equity", §"Returns and holdings" |
| costs, self-financing, turnover, metric formulas, FF5+momentum regression | step 0, unchanged | step 0 §"Costs", §"Outputs", Amendment 1 |
| benchmark | step 0 B1 (SPY total return) | step 0 §"Baselines"; settled 2026-10-01 |

**Fixed by construction** (no source; each frozen in the construction-version hash):
- **The 1,000 count.**
- **The standard deviation is the population form (ddof 0).** QMJ does not state it.
- **Ties take the average rank.** A group whose ranks have zero variance gives every member z = 0.
- **Small industries.** Where an industry has fewer than 10 names with a value for a characteristic at M, those names
  alone are ranked within the whole book universe for that characteristic. The occurrences are counted and printed.
- **Missing classifications.** `sic_null` and `sic_unloaded` together form one "unclassified" industry ("count
  NULL-sector names as their own bucket", `portfolio-construction-and-risk.md`); the census keeps the two reasons
  apart.
- **Family and composite presence.** A family is present when at least one member has a value, and its score sums
  the available members. A composite needs at least 2 of the 3 families and sums those present.
- **Listing age is an archive-age proxy.** It is the series' first admitted bar on or before s(M); a ticker change
  that starts a new series reads as young.

**Exceptions to the skills, stated:**
- **High MAX / IVOL is not excluded.** `market-segments.md` says to exclude it from longs but gives no threshold, and
  #3621 measures it as a filter on this panel. Short interest is a conditioning flag, and FINRA data start 2021.
- **No sector cap.** The skill's "cap any sector at about 2× its benchmark weight" is reported, not enforced:
  - within-industry ranking with equal weight already gives industry weights close to name-count shares;
  - a cap needs a redistribution rule, which would be a further construction choice;
  - industries above 2× their cap-weighted universe weight are flagged per month;
  - step 3's forward book enforces the cap before any capital.
- **No volatility conditioning axis.** 126-session volatility is not in the panel; `rvol_21d` (21 sessions) is printed
  as the volatility diagnostic instead.

## Dates, samples and the hold-out

- **Timing.** M, s(M), the evidence cutoff and the four-month lag are step 1's. Decisions use s(M)'s close and fill at
  that close: step 0's stated market-on-close assumption, not an executable schedule.
- **Stage A:** formations 2014-09..2021-04, holding months 2014-10..2021-05 (80). **Development data.** Construction
  and the step-1 fidelity selection were decided on it.
- **Stage B:** formations 2021-05..2024-07, holding months 2021-06..2024-08 (39), equal to step 0's W2. **Reused
  validation** (`research-process.md` §Hold-out).
- **The access is logged before any stage-B read.** That covers the extended SUB files, the stage-B builder run and
  every price read after 2021-05-31: `record_holdout_access` (`app/services/result_ledger.py:1598`),
  `access_kind="evaluate"`, under this declaration's identity.
- **One continuous path, 2014-09-30 to 2024-08.** Stage A's holdings carry into stage B as they would have in real
  time; stage-B statistics are slices of that path.
- **Nothing is computed before the declaration freezes.** No book return, IC, spread, loading or factor mean is
  computed on any month first. Code is tested on synthetic fixtures only.

## Step 1's inheritance clause: the separately governed validation sample

Step 1 selected characteristics with bars that read our long-short series against JKP's over all of stage A, so step-2
claims on stage A need a nested replay or a separately governed validation sample. This spec takes the second option:
- **G2, the realised-return gate, runs on stage B only.** Stage B's selection (the five characteristics) used only
  earlier months.
- **Stage-A realised returns are printed as development,** never gated.
- **G1, the exposure gate, uses the whole path.** A loading is identified from how the book co-moves with published
  factors, and the fidelity selection fitted no book quantity. Inventory of the stage-A information used in step 2's
  design:
  - fidelity verdicts (booleans);
  - premise 3's ME and coverage counts;
  - nothing else.
- **A nested K-fold replay was considered and rejected** (checkpoint 1, findings 8 and 12–14). A stitched
  cross-validated path is not executable, holdings carried across folds leak, and its estimand differs from the
  forward book.

## The book

At each formation M, in order:
1. **Universe:** step 1's admitted names at M; the top 1,000 by ME at s(M), ties by `name_key` ascending.
2. **Eligible to enter:** in the universe, with a composite, raw close at s(M) ≥ $5 and listing age ≥ 36 months. The
   two exclusions apply to entry only. A held name that no longer meets them is still judged by step 5.
3. **Scores:** each characteristic's JKP-signed value, ranked and standardised within FF-12 industry, among universe
   names that have a value. Family z and composite z are then computed within the same industry groups. Exclusions
   in step 2 do not change the ranking population.
4. **Ordering:** universe names with a composite, by composite descending, ties by `name_key` ascending. With n such
   names, the top decile is ranks 1..⌈n/10⌉ and the top tercile ranks 1..⌈n/3⌉. Unlike step 1, which grouped tied
   runs, there is no tied-run grouping.
5. **Holdings:**
   - A held name stays while it is in the universe, has a composite and ranks in the top tercile.
   - Otherwise it is sold at s(M).
   - An eligible name not held enters if it ranks in the top decile.
6. **Weights:** equal across holdings on post-cost NAV, by step 0's one-pass self-financing rule. Weights drift within
   the month.
7. **Returns:** step 1's three statuses, under both arms.

**Position state machine:**
- A `terminal` or `coverage_exit` holding becomes cash at the end of its holding month, with no sale and no cost, and
  leaves the book.
- A sold name pays its entry band's half-spread.
- A name that re-enters is a new position with a new band.
- A position's band is fixed at entry by its raw close at s(M) (step 0's `half_spread_for` rule).
- A missing, non-positive or non-finite raw close at a trade refuses the run, since stages A and B lie inside
  Intrader's coverage.

**Path ends:**
- The initial purchase at 2014-09-30 is charged.
- At the end of 2024-08 every remaining position is sold at its band, with no rebalance first. That liquidation is
  charged but excluded from turnover (step 0).

**Insufficient book.** A formation with fewer than 10 holdings makes the run's verdict `INSUFFICIENT`. The path is
still printed, and no substitute selection is made. The 10 is fixed by construction: an equal-weight book of fewer
names is not the declared strategy.

## References and the control

**B1** is step 0's SPY total-return path (`var/research/3609_step0/20261004T214439Z/paths.json`, `"B1 SPY"`).
- Its continuing returns are used for 2014-10..2024-08.
- Step 0's saved costs carry its 2009-12 band, so entry and exit costs are recomputed from SPY's raw close at
  2014-09-30. Under step 0's band rule, one band applies to both the 2014-09 purchase and the 2024-08 sale. Both are
  charged.
- Stage-B slices take the continuing returns for 2021-06..2024-08, with the 2024-08 sale charged, as the book's are.
- After 2021, B1 is a synthetic IVV NAV history (step 0).

**Matched random control,** draws d = 0..999, with `random.Random(f"3609-step2:{d}:{M}")` seeded per formation. It
reproduces the book's trading schedule with random names:
- It starts from the book's 2014-09 holding count, drawn uniformly from the book's eligible-to-enter set.
- At each later formation:
  1. it applies the same forced removals, for its own holdings: left the universe, no composite, or became cash;
  2. it sells k randomly chosen remaining holdings, where k is the number of the book's top-tercile exits that month,
     capped at what it holds;
  3. it buys random names from the book's eligible-to-enter set, excluding its own holdings, until its count equals
     the book's holding count.
- If the eligible set runs out, it holds fewer names, and that is counted.
- Everything else matches the book: weights, costs, bands, statuses and arms.
- So the control matches the opportunity set, book size and name turnover. It does not match holding-duration
  structure exactly; realised turnover and cost are printed per draw and arm.
- Percentiles are nearest-rank on the sorted 1,000 (step 0). They describe random selection under this one history,
  not confidence intervals.

**Equal-weight universe:** all 1,000 names, rebalanced monthly, costed the same way. Used for attribution. Its
cap-weighted counterpart is printed beside B1 as the same-universe buy-and-hold that the 2026-08-23 decision names.

## Decision rule (frozen before any outcome)

The book **passes** only if G1 and G2 both hold, at base cost, under **both** termination arms:

**G1, exposures (whole path, 2014-10..2024-08).**
- Regress the book's monthly return in excess of RF on FF5 plus momentum. Use step 0's data, unit checks and
  Newey–West rule, `newey_west_lag` = ⌊4(T/100)^(2/9)⌋, on all 119 months.
- The intended loadings must each be positive with a one-sided t above z(1 − 0.05/3) = 2.128 (Bonferroni over three):
  - HML for value;
  - RMW for GP/A. RMW is operating profitability, the nearest FF factor and not the same variable;
  - CMA for investment.
- A missing factor month refuses the run.

**G2, realised net return (stage B, 2021-06..2024-08).**
1. The book's annualised net return exceeds B1's.
2. It exceeds the median (nearest-rank) of the control's 1,000 annualised net returns.

The two arms are a conjunction: both must pass.

**Printed beside the verdict, never gating:** stress cost, stage A, each calendar year, segments, attribution, and the
Newey–West effective sample. A pass that fails at stress cost, or that comes from one year, is reported so in the
verdict line.

**After a pass:** step 3 declares the forward demo test on the eToro-tradable intersection, as a claiming #2599
declaration with its own `TrialDesign` and power check. Its prediction interval and the #2500 sequential stop rule are
specified there from this run's monthly series.

**After a fail or `INSUFFICIENT`:** "not demonstrated" on #3609. Any new configuration (universe, weighting, band,
families) is a new, counted declaration, and needs a reason other than this result.

## Registration

**`DeclaredTrial` `3609-step2-book-v1`:** non-claiming (`declared_for=None`), `exactness=EXACT`, `searches=1`.
- **One configuration is run:** the book above.
- **Not counted, with reasons:**
  - The arms are a conjunction, which cannot raise the false-pass rate.
  - The control, the stress scenario and the diagnostics select nothing.
  - No alternative configuration is run, and nothing was run before the freeze.
- **Step 1's 16 searches stay counted** in M.

**Amendment to step 1's inheritance text.** Step 1 says step 2's declaration "needs a `TrialDesign` that passes
`power_check`". Premise 2 shows that no margin meaning "at or above SPY" is feasible for step 2. A `TrialDesign`
belongs to a claiming #2599 declaration, and `DeclaredTrial` refuses a design without `declared_for`. So the design
moves to step 3's claiming forward declaration, whose planned data include the forward months. Step 2 claims no IR.

**Freeze evidence:**
- this spec's sha256;
- the construction-version hashes (step 1's mechanism, covering the report, the builder and every imported module
  except `trial_register.py`);
- the stage-A artefact manifest (`ee1e8abc…`);
- step 0's run manifest;
- the reference artefact hashes: FF-12 map, QMJ PDF, extended SUB once published, and Table 9 CSV;
- the Python version, recorded because `random.Random` string seeding is version 2 (step 0).

**Ledger:** step 1's protocol (`started` before evaluation, `completed` or `failed` before printing), written to
`var/research/3609_step2/ledger.jsonl` and committed to `docs/research/3609-ledger.jsonl`.

**Diagnostics count later.** A later declaration that selects a family, cell, horizon or variant this report printed
counts every alternative printed in that dimension as searched.

**Labels on every output:**
- the verdict line names the survivorship regime of each sample;
- "retrospectively filtered construction data" (step 1 premise 4);
- "reused validation" on stage B;
- "development" on stage A.

## Diagnostics (printed, never gated)

- **Universe:** premise 3's table recomputed on all 119 formations, stage B included, with no retuning.
- **Signals,** on every month and for every family and the composite, so no estimand is selection-conditioned:
  - Spearman rank IC of the score at M against holding returns at horizons 1, 3, 6 and 12 months, with Newey–West
    errors on the overlapping horizons;
  - IC-IR;
  - quintile spreads, equal-weighted and capped-value-weighted. Quintiles are ranks 1..⌈n/5⌉ and onwards, at least 5
    names per leg. The ME cap is the universe's own 80th percentile at s(M), not NYSE's.
- **Two size cells:** the book universe, and admitted names outside it. Scores are standardised within each cell
  separately, with the same industry rule. Labelled a proxy for `market-segments.md`'s NYSE cells.
- **Segments:** the primary partition is cost band at s(M) × the two size cells. A cell is printed only with at least
  24 months of at least 30 names. FF-12 industry is printed for IC only.
- **Book, per stage, per calendar year and pooled:** step 0's formulas for these figures. No Sharpe is printed.
  - net and gross annualised return, volatility and maximum drawdown;
  - active return, tracking error, IR, beta and maximum relative drawdown against B1;
  - turnover, cost drag and book size;
  - weight held in each return status.
- **Turnover months:** months with one-way turnover above 50%, their cost, and the year's gross-minus-net
  (Novy-Marx & Velikov 2016).
- **Attribution** (`portfolio-construction-and-risk.md` §Attribution): SPY beta; universe effect (equal-weight
  universe minus B1); the full FF5+momentum loadings; selection (book minus equal-weight universe); FF-12 weights
  against the cap-weighted universe, with 2× flags.
- **Information:** first-order autocorrelation of monthly active returns, and the effective years implied by step 0's
  Newey–West lag.
- **Operations:**
  - the minimum ticket ($10) against the smallest position weight, giving the capital at which it binds;
  - assumptions carried from step 0: real settlement, fractional holdings, zero slippage, gaps and fees, and
    pre-withholding returns for the book and B1 alike.
- **Control:** 5th/50th/95th percentiles of the control's net and gross returns, turnover and cost per arm, and the
  book's percentile.

## Slices

1. **Reference data, no stage-B reads.**
   - French `Siccodes12.zip`: pinned and parsed, with a fixture test of every range boundary and of "Other".
   - The QMJ PDF, pinned by hash.
   - Code for SUB quarters through 2024q3, tested on fixtures only; the files are not fetched yet.
2. **Stage-B builder path.** Formations 2021-05..2024-07 and a 2024-08-31 price bound. The builder refuses unless:
   - the frozen declaration exists;
   - the access row exists;
   - a reference artefact carries the extended SUB.

   Stage A must replay with rows, census and every frozen input byte-identical. The manifest's provenance fields may
   differ, and stage A keeps its original pins. Tests use fixtures.
3. **The report** (`scripts/report_3609_step2.py`): scores, book, control, references, gates, diagnostics and the
   ledger. Fixture tests only, each revert-probed. Codex checkpoint 2.
4. **Declaration,** in this order: the `DeclaredTrial` (register bump), then the access record.
5. **Declared run,** from clean `main`:
   - publish the extended SUB reference artefact;
   - build and publish the stage-B artefact, capturing and hashing its inputs under one repeatable-read snapshot;
   - run the report, refusing any pin mismatch;
   - post the verdict and ledger on #3609.

## Known limits

- No IR claim: ten years cannot certify a realistic edge (premise 2). A pass is a screen plus the published record.
- G2 rests on 39 months, one regime, and point estimates.
- RMW is not GP/A. HML stands for three value variables.
- Restricted estimand; the size proxy is not NYSE breakpoints; linkage under-covers dead and illiquid names.
- Survivorship is unverified 2014-09..2018, and the panel is retrospectively filtered (step 1).
- Historical eToro eligibility is unchecked. Leveraged and inverse products are excluded by the 10-K/10-Q and
  one-security rules plus premise 3's SIC count, not by a point-in-time security-type field.
- Costs: nine summer calibration dates, no stressed regime, stock bands (step 0).
- The market-on-close fill is an assumption.

## Checkpoint log

**Round 1 (50 findings), all applied:**
- **1–6** (inference, margin, effective years, count, adoption rationale): the IR claim and `TrialDesign` move to
  step 3, with premise 2 as the reason and the amendment recorded; G1 is a powered exposure test; the count is
  enumerated; a per-family adoption table is added.
- **7:** step 1's family identities are kept.
- **8–14:** nested K-fold rejected; stage B is the separately governed validation sample for G2.
- **15–16:** survivorship and retrospective-filter labels go on every output; G2 sits in the Form 25-checked regime.
- **17–20:** the measurement is redone with the book's tie-break, grid checks and overlap counts; the unmeasured
  bank explanation is removed.
- **21–25:** QMJ is quoted and pinned; ddof, ties, the small-industry rule and SIC buckets are frozen; the price-signal
  ranking rule is removed (no price characteristic remains).
- **26–28:** the sector-cap exception is stated; default long exclusions are applied; the MAX/IVOL exception is
  stated; security type is measured and limited.
- **29–31:** `INSUFFICIENT`, presence rules, exact decile and tercile ranks.
- **32–35:** the control is rebuilt as matched random trading on the book's eligible set; the turnover claim is
  removed; percentiles are nearest-rank.
- **36–38:** B1's entry and exit are re-banded at 2014-09; path ends and the state machine are explicit.
- **39–41:** market-on-close is stated; turnover months are listed; operational assumptions are carried.
- **42–45:** stage-B reads come after the access; the slice order is fixed; the replay boundary and frozen inputs are
  enumerated.
- **46–50:** diagnostic populations, quintiles, segment minimums, formulas, IC horizons and full-grid family
  readouts are specified. The prediction interval moves to step 3.
