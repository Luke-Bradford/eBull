# #3609 step 2 — the factor composite book, backtested net of our costs

Status: **draft, blocked on an operator decision** (#3609, 2026-10-06: can a zero-capital demo test be authorised
by a Track B screen when no realistic edge is powered on our data? See premise 2). Revised after Codex checkpoint 1
rounds 1 and 2 (§"Checkpoint log"). Round 2's findings 9–56 are applied. Findings 1–8, and the gate-design parts of
3–5, are the decision above and stay open until the operator answers; §"Decision rule" is provisional until then.
Nothing is built. No book, IC, spread or factor mean has been computed on any month. Programme: `docs/research/2026-10-04-strategy-research-sweep.md`
§4 item 2. Inherits from `docs/research/2026-10-04-3609-step1-factor-panel.md` §"Registration, ledger and what step 2
inherits" (amended below), and reports against `docs/research/2026-10-04-3609-step0-baselines.md`.

## Question

Does a long-only, equal-weight book of US large caps, chosen by an industry-relative composite of the characteristics
step 1 rebuilt faithfully, (a) carry the published factor exposures it is built for, and (b) beat SPY total return
and a matched random book net of our costs on a reused validation sample (stage B) that no step-1 selection read?
Stage B has been seen before, through step 0's baselines (§"Design-history inventory").

**What the answer can say.**
- **Pass:** the book is eligible for a declared forward demo test (step 3, the ranking-pot successor). The case
  for the premium rests on the published post-publication record (§"Adoption rationale"). This backtest shows that
  our implementation carries the intended exposures, and that it did not lose to SPY or to random trading net of
  our costs on the validation sample.
- **It does not prove a premium.** About ten years cannot statistically certify a realistic edge over SPY
  (premise 2), so no IR claim is made here.
- **What a pass can never authorise:** live capital, or a forward construction other than this one. Step 3's forward
  book adds the sector cap, the MAX filter and the eToro-tradable intersection, so it needs its own declared
  implementation backtest that passes the same gates before any position (§"Decision rule"). Stage B is not
  survivorship-free in full (premise 5) and its integrity masks are retrospective (step 1 premise 4), so a pass is
  a screen on that sample, labelled so.
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
uses only ME, `name_key`, characteristic presence and SIC, and computes no outcome; it deserialises whole rows, and
the integrity check hashes whole files). It first checks the stage-A manifest's
sha256 against its pin and every frozen input and published file through `verify_artefact`
(`scripts/build_3609_factor_panel.py`), and refuses a duplicate or missing cutoff month and any non-finite or
non-positive ME or cutoff:

| quantity | min | max |
|---|---|---|
| admitted names | 2,975 | 3,156 |
| names above JKP's NYSE-median cutoff | 862 | 1,094 |
| ME of the 1,000th name ($B) | 1.74 | 3.72 |
| top-1,000 names at or below the NYSE median | 0 | 138 |
| names above the median outside the top 1,000 | 0 | 94 |
| above-median ME held inside the top 1,000 | 99.14% | 100% |
| top-1,000 names with a raw value in ≥ 2 of the three families | 964 | 991 |
| top-1,000 names with a raw value in all three | 613 | 683 |

"Above the median" is ME > cutoff and "at or below" is its complement, here and in every diagnostic. The family
counts are raw-input availability: they apply no group minimum, zero-variance rule, seasoning or price screen.
| top-1,000 names with SIC 6221 (commodity pools' SIC) | 0 | 1 |

The proxy is close to the large-plus-mega tranche that `market-segments.md` makes the default for books traded more
than quarterly **in one dimension only: value coverage**. By count it is not: up to 138 of the 1,000 names sit at or
below the NYSE median, and an equal-weight book can overweight them. The declared run prints, per formation, the
count, the book's weight and the book's traded notional at or below JKP's NYSE-median cutoff where JKP publishes one
for M (§"Diagnostics"). The proxy
matches the Russell 1000's count, not its float or reconstitution rules. Stage B is measured in the declared run and
the rule is not retuned.

**4. Stage B is not built.** The builder stops at 2021-05-31 (`scripts/build_3609_factor_panel.py:120`
`PRICE_BOUND`, refusal at `:1221-1222`). FSDS SUB is pinned 2012q1..2021q2 (`app/services/factor_panel_reference.py:36-39`).
No SIC-to-industry map exists in `app/`, `scripts/`, `sql/` or `tests/`.

**5. Survivorship regimes are inherited** (step 1 premise 1):
- 2014-09..2018: terminations recorded, coverage unverified;
- 2019 on: checked against a selected Form 25 set.

Stage B lies wholly in the second regime. That regime is not full-population survivorship-free: step 1 calls the
Form 25 set a selected, symbol-bearing population, and ticker reuse and mismatched endpoint dates remain unresolved.
The full-population survivorship requirement is therefore **unmet** for stage B too. This is why a pass authorises
nothing beyond §"Question"'s limits.

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
| family and composite | family = z(mean of member z); composite = z(mean of family scores) | QMJ p. 4 ("average them") and eq. (2): a component is the z-score of its members' z-scores combined; with every member present, sum and mean rank identically |
| industry | Fama-French 12 from SIC; codes outside French's listed ranges are "Other" | `market-segments.md` §6; French's data library `Siccodes12.zip`, pinned in slice 1 |
| SIC as of M | step 1's accession-specific SUB `sic`, under step 1's cutoff | step 1 §"Universe at M" step 4 |
| direction | JKP Table 9 sign | `docs/research/3609-jkp-table9-signs.csv` |
| entry and hold band | enter in the top decile; hold until the name leaves the top tercile | Novy-Marx & Velikov 2016 (`portfolio-construction-and-risk.md` §Turnover control) |
| weights | equal weight | DeMiguel, Garlappi & Uppal 2009 (same skill) |
| default long exclusion | raw close < $5, applied to entries and holdings | `market-segments.md` ("Exclude by default") |
| timing, ME, characteristics, returns, terminations | step 1, unchanged | step 1 §"Dates and stages", §"Market equity", §"Returns and holdings" |
| costs, self-financing, turnover, metric formulas, FF5+momentum regression | step 0, unchanged | step 0 §"Costs", §"Outputs", Amendment 1 |
| benchmark | step 0 B1 (SPY total return) | step 0 §"Baselines"; settled 2026-10-01 |

**Fixed by construction** (no source; each frozen in the construction-version hash):
- **The 1,000 count.**
- **The standard deviation is the population form (ddof 0).** QMJ does not state it.
- **Ties take the average rank.**
- **One ranking population per operation.** For each M, every operation (each characteristic, then each family,
  then the composite) ranks and standardises within one (FF-12 industry, M) group, among universe names that have
  that operation's input. There is no cross-industry fallback: ranking a small industry's names against the whole
  universe would restore the between-industry comparison the source rule removes.
- **Uninformative groups give no score.** An operation's group yields no score for any of its names when it has
  fewer than 10 inputs or its ranks have zero variance. Those names lack that input at the next operation, and the
  occurrences, names and book weight affected are counted per operation and printed. The 10 is fixed by
  construction; a z-score over fewer peers is mostly rank noise.
- **Missing classifications.** `sic_null` and `sic_unloaded` together form one "unclassified" industry ("count
  NULL-sector names as their own bucket", `portfolio-construction-and-risk.md`); the census keeps the two reasons
  apart.
- **Missing members (a custom rule).** QMJ does not state its treatment of a missing member. A family is present
  when at least one member has a score, and its input is the **mean** of the members present: normalisation by
  the count of members present, so the input stays on one member's scale. It does not make ranks neutral to
  missingness; no rule does. A composite needs at least 2 of the 3 families and takes
  the mean of those present. The declared run prints, per formation, the count and book weight of each membership
  pattern (which members and families were present).
- **Identifier-decided selections.** Where tied composites straddle a decile or tercile boundary, `name_key`
  decides. Every such selection is counted and printed.
- **Archive seasoning, not an IPO rule.** A name may enter only when its series' first admitted bar is at least 36
  months before s(M). This is an archive-seasoning rule. It does **not** implement `market-segments.md`'s "under 36
  months listed" exclusion (Loughran & Ritter 1995): no effective-dated listing history exists here, archive
  coverage need not start at listing, and a ticker change that starts a new series reads as young. It applies at
  entry only; a held series' age only grows.

**Exceptions to the skills, stated:**
Each exception below makes this a **reference construction**, not the forward one. Step 3's forward construction
must include the cap and the MAX filter and pass its own implementation backtest (§"Decision rule").
- **High MAX / IVOL is not excluded.** `market-segments.md` says to exclude it from longs but gives no threshold.
  #3621 measures and freezes that filter on this panel; the frozen filter is a prerequisite of step 3's construction.
  Short interest is a conditioning flag, and FINRA data start 2021.
- **No sector cap.** The skill's "cap any sector at about 2× its benchmark weight" is not enforced, because a cap needs
  a redistribution rule, which is a further construction choice. Within-industry ranking is **not** assumed to keep
  industry weights near name-count shares: missing scores, discrete ranks, entry exclusions and the hold band can all
  move them. The book's FF-12 weights are measured on the whole path instead (§"Diagnostics", Attribution).
- **The 2× flag is a same-universe diagnostic, not the skill's benchmark cap.** The skill's benchmark is B1, but no
  SPY constituent weights are in the panel's frozen inputs (adding them, for example from fund N-PORT holdings, would
  be a new data source with its own coverage check), so the flag compares each FF-12 weight with the reconstituted
  cap-weighted universe's (§"References"). An industry with zero universe weight and a positive book weight is
  flagged "no reference weight".
- **No volatility conditioning axis and no substitute.** `market-segments.md`'s axis is 126-session volatility, which
  the panel does not hold, and `rvol_21d` failed step 1. No volatility-conditioned result is printed; this is a known
  limit.
- **SIC across the stage boundary.** Stage A keeps its frozen classifications. Stage-B formations classify with the
  extended SUB. Population: names in the universe at both the 2021-04 and 2021-05 formations. For each, the run
  compares the two formations' SIC rows and sets two flags, which can both be set: **accession changed** (the
  accession used differs, whether newly filed or newly past the evidence cutoff) and **reference changed** (the same
  or a different accession, where the 2021-04 result was `sic_unloaded` and the 2021-05 one is not). It prints the
  count and post-trade book weight at 2021-05 for each flag combination among names whose FF-12 group changed.
  Entrants and departures are counted separately. Frozen stage-A rows are never rewritten.

## Dates, samples and the hold-out

- **Timing.** M, s(M), the evidence cutoff and the four-month lag are step 1's. Decisions use s(M)'s close and fill at
  that close: step 0's stated market-on-close assumption, not an executable schedule.
- **Stage A:** formations 2014-09..2021-04, holding months 2014-10..2021-05 (80). **Development data.** Construction
  and the step-1 fidelity selection were decided on it.
- **Stage B:** formations 2021-05..2024-07, holding months 2021-06..2024-08 (39), equal to step 0's W2. **Reused
  validation** (`research-process.md` §Hold-out).
- **The access is logged before any stage-B read.** That covers the extended SUB files, the stage-B builder run and
  every price read after 2021-05-31 (§"Registration", ledger steps 1–2).
- **One continuous path, 2014-09-30 to 2024-08, with a retrospective warm start.** The characteristic set was chosen
  on all of stage A, so stage A's holdings could not have been produced in real time. They are a declared
  development-path warm start: the path starts all-cash at 2014-09-30. The **stage boundary state** is taken at the
  end of the 2021-05 holding month, before the 2021-05 formation's trades: for each termination arm and cost
  scenario, every open position (`name_key`, value, entry formation, entry band), the cash and the NAV, and the same
  for each control draw and the equal-weight and cap-weighted references. It is written as canonical JSON (sorted
  keys) and its sha256 is printed. Stage-B statistics are slices of the path after that state.
- **Nothing is computed before the declaration freezes.** No book return, IC, spread, loading or factor mean is
  computed on any month first. Code is tested on synthetic fixtures only.

## Step 1's inheritance clause: the separately governed validation sample

Step 1 selected characteristics with bars that read our long-short series against JKP's over all of stage A, so step-2
claims on stage A need a nested replay or a separately governed validation sample. This spec takes the second option:
- **G2, the realised-return gate, runs on stage B only.** Stage B's selection (the five characteristics) used only
  earlier months.
- **Stage-A realised returns are printed as development,** never gated.
- **G1, the exposure gate, uses the whole path.** A loading is identified from how the book co-moves with published
  factors, and the fidelity selection fitted no book quantity. (Round 2, findings 3–5, contests this; it is part of
  the open operator question.)

**Design-history inventory.** Everything the designers of this spec had seen, frozen with the spec:
- **Stage A:** step 1's fidelity verdicts and its printed per-characteristic correlations, betas, tracking errors and
  mean offsets against JKP's long-short factors, for all eight characteristics and both arms (`docs/research/3609-ledger.jsonl`,
  run `b6378c7c`); premise 3's ME and coverage counts.
- **Stage B, through step 0** (`var/research/3609_step0/20261004T214439Z`): the W2 statistics of B1 SPY, the static
  ETF mixes and the random-basket draws. No characteristic-sorted book was ever computed on stage B.
- **Earlier GP/A trial** #2901 (`r6-2901-quality-gpa-2026-09-25`, 2013–2024 June formations): it failed its
  construction gate (correlation +0.193 to +0.199 against +0.20), and no arm outcome was computed or published.
- **Published results:** the adoption-rationale sources and McLean & Pontiff's 58% decline.
- **Choices fixed with that knowledge:** the 1,000 size rule (premise 3), the decile/tercile band and equal weight
  (both from the skill's cited sources), and dropping the three non-PASS families (step 1's verdicts). None was
  chosen by comparing book outcomes, because none has been computed.
- **A nested K-fold replay was considered and rejected** (checkpoint 1, findings 8 and 12–14). A stitched
  cross-validated path is not executable, holdings carried across folds leak, and its estimand differs from the
  forward book.

## The book

At each formation M, in order:
1. **Universe:** step 1's admitted names at M; the top 1,000 by ME at s(M), ties by `name_key` ascending. Any
   admitted row with a non-finite or non-positive ME refuses the run (`ME_INVALID`), as in premise 3's script; fewer
   than 1,000 admitted names refuses it (`UNIVERSE_SHORT`); both in either stage.
2. **Scores:** each characteristic's JKP-signed value, then each family, then the composite, by §"Source rules"'
   one-population-per-operation rule. Exclusions in step 3 do not change any ranking population.
3. **Eligible to enter:** in the universe, with a composite, raw close at s(M) ≥ $5 and archive seasoning ≥ 36
   months.
4. **Ordering:** universe names with a composite, by composite descending, ties by `name_key` ascending. With n such
   names, the top decile is ranks 1..⌈n/10⌉ and the top tercile ranks 1..⌈n/3⌉. Unlike step 1, which grouped tied
   runs, there is no tied-run grouping.
5. **Holdings:**
   - A held name stays while it is in the universe, has a composite, ranks in the top tercile and has a raw close at
     s(M) ≥ $5.
   - Otherwise it is sold at s(M). Each sale has one primary reason, tested in this order: left the universe;
     lost a composite; raw close below $5 (each a **forced** exit); left the top tercile (a **discretionary** exit).
     Further reasons that also hold are printed as flags and do not count. k_M is the number of discretionary exits.
   - An eligible name not held enters if it ranks in the top decile.
6. **Weights:** equal across holdings on post-cost NAV, by step 0's one-pass self-financing rule. Weights drift within
   the month.
7. **Returns:** step 1's three statuses, under both arms.

**Position state machine:**
- A `terminal` or `coverage_exit` holding follows step 1's event timing: it is realised at its `end_bar` (with
  step 0's terminal value for `terminal`), held as cash at 0 for the rest of the month, with no sale and no cost, and
  is already cash, not a holding, at the next formation.
- A sold name pays its entry band's half-spread.
- A name that re-enters is a new position with a new band.
- A position's band is fixed at entry by its raw close at s(M) (step 0's `half_spread_for` rule).
- A missing, non-positive or non-finite raw close at a trade refuses the run, since stages A and B lie inside
  Intrader's coverage.

**Path ends:**
- The initial purchase at 2014-09-30 is charged.
- At the end of 2024-08 every remaining position is sold at its band, with no rebalance first. That liquidation is
  charged but excluded from turnover (step 0).

**Insufficient book.** Holdings are counted after the formation's trades. Fewer than 10 at any formation makes the
run's verdict `INSUFFICIENT`. The path continues on the declared rules: with zero holdings the book is all cash at 0
until a later formation's entries, which re-enter normally. No substitute selection is made. Under `INSUFFICIENT`, G1
and G2 are not evaluated; the path's return, drawdown, turnover and cost are still printed, with the formations
concerned. The 10 is fixed by construction: an equal-weight book of fewer names is not the declared strategy.

## References and the control

**B1** is step 0's SPY total-return path (`var/research/3609_step0/20261004T214439Z/paths.json`, `"B1 SPY"`).
- Its continuing returns are used for 2014-10..2024-08.
- Step 0's saved costs carry its 2009-12 band, so entry and exit costs are recomputed from SPY's raw close at
  2014-09-30. Under step 0's band rule, one band applies to both the 2014-09 purchase and the 2024-08 sale. Both are
  charged.
- Stage-B slices take the continuing returns for 2021-06..2024-08, with the 2024-08 sale charged, as the book's are.
- **ETF cost exception, inherited from step 0** (§"Costs"): `research-process.md` wants ETF costs calibrated
  separately, and none exists, so B1 is charged the stock band of SPY's raw price, provisionally. B1 trades twice on
  the path, so the assumption enters through those two trades only; the report prints both charges.
- After 2021, B1 is a synthetic IVV NAV history (step 0).

**Matched random control,** draws d = 0..999. **Estimand:** random selection from the book's own opportunity set
under a target-count rule: after each formation it holds the book's count, and its random replacements are capped at
the book's discretionary exit count. It does not match total turnover, traded notional or cost, because its forced
exits and count adjustments are its own; those are printed beside the book's.

At each formation M, with n_M the book's holding count after its trades and k_M its discretionary exit count:
1. **Forced exits:** the control sells its own holdings that left the universe, lost a composite or closed below $5
   (holdings that became cash already left at their `end_bar`).
2. **Discretionary exits:** it sells min(k_M, h) holdings chosen at random, where h is its count after step 1.
3. **Down-sizing:** if it still holds more than n_M, it sells the excess, chosen at random.
4. **Purchases:** it buys names chosen at random from the book's eligible-to-enter set, excluding its holdings and
   every name it sold at this formation, until it holds n_M.

At 2014-09-30 it holds nothing, so only step 4 runs.

**Reproducibility.** One `random.Random(f"3609-step2:{d}:{M.isoformat()}")` per draw and formation. Every sampled
population is sorted by `name_key` ascending, and the calls run in the order above as `rng.sample(population, count)`.
Selections are computed once and used for both termination arms, since no status depends on the arm.

**Shortage refuses.** If the eligible set cannot fill step 4 in any draw at any formation, the run is refused
(`CONTROL_SHORT`) with the draw and formation. No feasibility is assumed in advance; the run prints, per formation,
the smallest purchase pool any draw met and the purchases it needed. No percentile is ever computed on a subset of
draws.

**Everything else matches the book:** weights, costs, bands, statuses and arms. Traded notional and cost are
reported in four categories, for the book and per draw and arm: forced exits, discretionary exits (the control's
sampled replacements), count adjustments (the control's down-sizing sales and any purchases beyond its
replacements), and the self-financing rebalance adds and trims. The four reconcile to the total, which is printed.

**Percentiles.** Control quantiles are nearest-rank on the sorted 1,000 (step 0). The book's percentile is the
mid-rank empirical CDF: (number of draws below the book + half the number equal) / 1,000. These describe random
selection under this one history; they are not confidence intervals or p-values.

**Equal-weight universe:** all 1,000 names, rebalanced monthly to equal weight, costed the same way. Used for
attribution.

**Reconstituted cap-weighted universe:** the top 1,000 at each formation, cap-weighted at s(M) and rebalanced to
those weights monthly, costed on its own turnover at step 0's bands. It is printed beside B1. It is a reconstituted
index, not a buy-and-hold portfolio; the 2026-08-23 decision's same-universe buy-and-hold has no exact counterpart
in a universe that changes each month.

## Decision rule (frozen before any outcome)

The book **passes** only if G1 and G2 both hold, at base cost, under **both** termination arms:

**G1, exposures (whole path, 2014-10..2024-08).**
- Regress the book's monthly return in excess of RF on FF5 plus momentum. Use step 0's data, unit checks and
  Newey–West rule, `newey_west_lag` = ⌊4(T/100)^(2/9)⌋, on all 119 months.
- The intended loadings must each be positive with a one-sided t above z(1 − 0.05/3) = 2.128 (Bonferroni over three):
  - HML for value;
  - RMW for GP/A. RMW is operating profitability, the nearest FF factor and not the same variable;
  - CMA for investment.
- **Refusals, checked before any coefficient is printed** (verdict `G1_REFUSED`, with the reason): a missing factor
  month; a non-finite value in the return or factor matrix; anything other than exactly one aligned book, RF and
  factor observation for each of the 119 declared months (no duplicates, no extras); a design matrix whose rank is below
  its column count (numpy `matrix_rank` at its default tolerance); zero residual variance; or any Newey–West standard
  error that is not finite and positive.

**G2, realised net return (stage B, 2021-06..2024-08).**
1. The book's annualised net return exceeds B1's.
2. It exceeds the median (nearest-rank) of the control's 1,000 annualised net returns.

The two arms are a conjunction: both must pass.

**Printed beside the verdict, never gating:** stress cost, stage A, each calendar year, segments and attribution.
Two annotations go on a `PASS` verdict line only, by frozen tests:
- **"fails at stress cost"** when G2 fails under step 0's stress cost in either arm;
- **"depends on <year>"**, for every year that qualifies, when G2 fails after removing that calendar year of stage B
  (2021 from June, 2022, 2023, 2024 to August) from the book, B1 and every control draw alike, annualising the
  remaining months. The failing arm and comparison are printed.

**Verdict order.** The run stops at the first that applies, and prints the status with its reason:
1. `REFUSED`: a data or pin refusal (`ME_INVALID`, `UNIVERSE_SHORT`, a raw-price refusal, `CONTROL_SHORT`, any
   pin or ledger mismatch). Nothing else is printed.
2. `INSUFFICIENT`: the path, with the formations concerned; G1 and G2 are not evaluated.
3. `G1_REFUSED`: the path and G2's inputs are printed; no gate verdict.
4. `PASS` or `FAIL`, by the conjunction above, with the annotations on a `PASS`.

The turnover note (§"Diagnostics", turnover above 50%) goes on every verdict line from step 2 on.

**After a pass:** step 3 specifies the forward construction: this book plus the sector cap, #3621's frozen MAX filter,
an effective-dated listing-age exclusion with a stated missing-history policy, a security-type exclusion whose
classifier (field, accepted values, unknown-type treatment) is specified and validated over every candidate
instrument, and the eToro-tradable intersection. Before any position, that construction gets its own declared implementation
backtest on this path, which must pass G1 and G2 as frozen here (step 3 must also state how it treats the absence of
historical eToro eligibility). Only then is the forward demo test declared, as a
claiming #2599 declaration with its own `TrialDesign` and power check. Its prediction interval and the #2500
sequential stop rule are specified there from the implementation backtest's monthly series.

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

**The freeze has two immutable stages; neither is edited after it is written.**

1. **The declaration** (slice 4), merged on `main` before any stage-B read. It is the `DeclaredTrial` row in
   `TRIAL_REGISTER` with a `TRIAL_REGISTER_VERSION` bump: the mechanism step 1's `3609-step1-fidelity-v1` used. It is
   not `freeze_preregistration` (`app/services/result_ledger.py`), which is for claiming declarations and refuses one
   without a `TrialDesign` (#3610). The row's `evidence` string carries:
   - this spec's sha256;
   - the construction-version hash (step 1's mechanism: the report, the builder and every imported module except
     `trial_register.py`, which is left out to avoid hashing the row that holds the hash);
   - the stage-A artefact manifest sha256, `ee1e8abc241f26596529509cd38f4692de4de8ff8700c0ac8ca19c2a412f3c1e`;
   - step 0's run manifest sha256;
   - the FF-12 map, QMJ PDF and Table 9 CSV sha256s;
   - the sha256 of `inspect.getsource(DeclaredTrial)`, which pins the register policy that `trial_register.py`'s
     exclusion from the construction hash would otherwise leave unpinned;
   - the hold-out access identity: `strategy_id="3609-step2-book"`, `strategy_version="v1"`;
   - the Python version, because `random.Random` string seeding is version 2 (step 0).
2. **The data-capture manifest** (slice 5), written after the access row and before evaluation. It holds the
   sha256s of the extended SUB reference artefact and the stage-B artefact manifest, with their input hashes. These
   cannot be in the declaration, because the files are fetched only after the access is logged.

**The declaration payload is pinned separately.** The run's `started` row records `TRIAL_REGISTER_VERSION` and the
sha256 of the row's canonical JSON (`dataclasses.asdict`, sorted keys). The report refuses if the row's payload hash
differs from the one recorded. The register version is recorded for audit, not compared: any later trial bumps it.

**The run checks its own code against the row before any stage-B step.** `record_holdout_access` does not enforce a
register row (it is called with `require_declaration=False`, so a trial without a frozen #2599 declaration passes),
so the check is the run's own. Before writing `started`, it recomputes the spec sha256, the construction hash and the
`DeclaredTrial` source hash, and refuses unless each equals the value in the row's `evidence`. The `started` row
records the three values it checked.

**Ledger: one parent run record from the first stage-B step.** Written to `var/research/3609_step2/ledger.jsonl` and
committed to `docs/research/3609-ledger.jsonl`, in this order:
1. `started`: run id, spec hash, construction hash, register version, payload hash, command. Written before
   anything below.
2. `access_recorded`: one `record_holdout_access` row with `strategy_id="3609-step2-book"`, `strategy_version="v1"`,
   `access_kind="evaluate"`, `result_version` = the run id, `accessed_by` = the operator or loop identity running it,
   and `purpose="#3609 step 2 declared run <run id>"`. It commits in its own transaction before step 3, and the
   ledger row records the returned `access_id`.
3. `sub_published`, then `stage_b_published`: each artefact's manifest sha256.
4. `data_frozen`: the data-capture manifest's sha256. The report refuses without it, or if any file differs from it.
5. `completed` or `failed`, written before any result is printed.

A run with no terminal row is an abandoned attempt; it is committed with the rest and counts as an access.

**Diagnostics count later.** A later declaration that selects a family, cell, horizon or variant this report printed
counts every alternative printed in that dimension as searched.

**Labels on every output:**
- the verdict line names the survivorship regime of each sample;
- "retrospectively filtered construction data" (step 1 premise 4);
- "reused validation; integrity masks retrospective, not strictly point-in-time" on stage B;
- "development" on stage A.

## Diagnostics (printed, never gated)

**Weights.** Unless stated, a weight is post-trade at s(M), on post-cost NAV including cash. A sold name's
"affected" weight is its pre-trade weight at s(M) on pre-trade NAV. Status weights are the post-trade weights at the
start of a holding month, grouped by the status that month ends in.

**Windows.** Every summary is printed for stage A (development), stage B (reused validation) and pooled, with the
window's defined-month count for that metric.

- **Universe:** premise 3's table recomputed on all 119 formations, stage B included, with no retuning, plus the
  book's weight and traded notional at or below JKP's NYSE-median cutoff where JKP publishes one for M.
- **Signals: one-month horizon, for the three retained families and the composite.** These are
  selection-conditioned: the families were chosen on stage A, and the label says so. The three dropped families
  have no step-2 construction and are not printed.
  - **Population:** universe names at M with the score and a step-1 holding-month return, per arm.
  - **IC_M:** Spearman correlation (average ranks for ties) between the score and the holding-month return.
    Undefined when fewer than 30 names, or either vector has zero variance.
  - **Quintile spread_M:** names ordered by score descending, ties by `name_key`; boundaries at ranks ⌈kn/5⌉ for
    k = 1..4. Spread = Q1 (highest scores) minus Q5, equal-weighted, gross. Undefined when any quintile has fewer
    than 5 names. No value-weighted spread is printed.
  - **Monthly series** of IC and spread are written to the output file.
  - **Summaries per window,** each metric on its own defined months: mean; standard deviation (ddof 1); IC-IR = mean
    / standard deviation, monthly, not annualised; a Newey–West t on the mean with step 0's lag rule. One-month
    outcomes remove mechanical overlap; Newey–West is used because the series may still be serially dependent. All
    summaries are `undefined` with fewer than 12 defined months; IC-IR is `undefined` at zero standard deviation;
    the t is `undefined` if any calendar month in the window is undefined (it is never computed over compressed
    months) or its standard error is not finite and positive.
- **Exception: no decay by horizon.** `research-process.md` lists decay by horizon among signal metrics. Step 1
  defines only a one-month holding contract (returns, statuses, terminal handling); a multi-month cohort return is
  a further construction with its own treatment of names that leave the panel, and horizons past the 2024-08-31
  read bound would be partial. It is not printed here; step 3 must supply it before a forward declaration.
- **Two size cells:** the book universe, and admitted names outside it. Scores are standardised within each cell
  separately, by §"Source rules". Labelled a proxy for `market-segments.md`'s NYSE cells.
- **Segments (primary partition: cost band at s(M) × the two size cells).** A name's cell is set at each M, so a
  name that migrates counts in its cell at that M.
  - **Each cell:** the signal block above, on its own names.
  - **The book, per cost band:** each month, the contribution Σ (start-of-month weight × holding-month return) of
    the holdings in that band, and the band's weight. The book lives in one size cell, so the size axis has no book
    split.
  - **Minimum effective sample:** 24 effective months, n_eff = n(1 − ρ)/(1 + ρ), with n the cell's defined months
    and ρ the lag-1 autocorrelation of its IC series, floored at 0 (the AR(1) effective-sample approximation, Wilks,
    *Statistical Methods in the Atmospheric Sciences*). The 24 is fixed by construction. Every cell is printed,
    with n and n_eff; a cell below the minimum is marked `insufficient`, not omitted.
  - **FF-12 industry:** a marginal partition of the book universe, not crossed with the cells; IC block only.
- **Book, per window and per calendar year:** step 0's formulas for these figures. No Sharpe is printed.
  - net and gross annualised return, volatility and maximum drawdown;
  - active return, tracking error, IR, beta and maximum relative drawdown against B1;
  - turnover (traded notional), cost drag and book size;
  - weight held in each return status.
- **Turnover above 50% a month** (`research-process.md`: net expected benefit must be shown, Novy-Marx & Velikov
  2016). For each such month: its traded notional, its cost, and the book's gross and net return minus the
  control's median, labelled realised, from one history. **Exception:** no expected-benefit model exists here. If
  any month exceeds 50%, the verdict line says "turnover above 50% in N months; net expected benefit not shown",
  and step 3 must show it for the forward construction.
- **Attribution** (`portfolio-construction-and-risk.md` §Attribution): SPY beta; universe effect (equal-weight
  universe minus B1); the full FF5+momentum loadings; selection (book minus equal-weight universe); FF-12 weights,
  monthly and averaged, against the reconstituted cap-weighted universe, with the 2× and "no reference weight" flags.
- **Information, per window:** the lag-1 autocorrelation of monthly active returns, and the ratio of the
  Newey–West standard error of the mean active return to its iid standard error. Each is `undefined` with fewer
  than 24 months, a zero iid standard error, or a Newey–West standard error that is not finite and positive. No
  effective-years figure is printed.
- **Operations:**
  - **Minimum ticket ($10).** Every trade on the path counts, including the 2024-08 liquidation and the
    self-financing adds and trims. A trade's weight is its notional over pre-trade NAV at that date. For each
    trade, the contemporaneous NAV at which it falls below $10 is $10 / weight, and the initial capital at which
    it does is that figure × NAV_0 / NAV_t. Per arm at base cost, the maximum of each over the path is printed,
    with the trade that sets it.
  - Assumptions carried from step 0: real settlement, fractional holdings, zero slippage, gaps and fees, and
    pre-withholding returns for the book and B1 alike.
- **Control:** 5th/50th/95th percentiles of the control's net and gross returns, turnover and cost per arm, and the
  book's percentile.

## Slices

1. **Reference data, no stage-B reads.**
   - French `Siccodes12.zip`: pinned and parsed, with a fixture test of every range boundary and of "Other".
   - The QMJ PDF, pinned by hash.
   - Code for SUB quarters through 2024q3, tested on fixtures only; the files are not fetched yet.
2. **Stage-B builder path.** Formations 2021-05..2024-07 and a 2024-08-31 price bound. The builder refuses unless:
   - the `DeclaredTrial` row is in the imported register, and the run's spec, construction and `DeclaredTrial`
     source hashes equal the row's (§"Registration");
   - the run's `started` and `access_recorded` ledger rows exist and the access row is committed;
   - a reference artefact carries the extended SUB.

   Stage A must replay with rows, census and every frozen input byte-identical. The manifest's provenance fields may
   differ, and stage A keeps its original pins. Tests use fixtures.
3. **The report** (`scripts/report_3609_step2.py`): scores, book, control, references, gates, diagnostics and the
   ledger. Fixture tests only, each revert-probed. Codex checkpoint 2.
4. **Declaration:** the `DeclaredTrial` row and register bump, merged on `main`.
5. **Declared run,** from clean `main`, in the ledger's order:
   - write `started`, then record and commit the access;
   - publish the extended SUB reference artefact;
   - build and publish the stage-B artefact, capturing and hashing its inputs under one repeatable-read snapshot;
   - write `data_frozen`;
   - run the report, refusing any pin mismatch;
   - post the verdict and ledger on #3609.

## Known limits

- No IR claim: ten years cannot certify a realistic edge (premise 2). A pass is a screen plus the published record.
- G2 rests on 39 months, one regime, and point estimates.
- RMW is not GP/A. HML stands for three value variables.
- Restricted estimand; the size proxy is not NYSE breakpoints; linkage under-covers dead and illiquid names.
- Survivorship is unverified 2014-09..2018, and the panel is retrospectively filtered (step 1).
- Historical eToro eligibility is unchecked.
- **Security type is unverified.** Step 1 states that commodity pools and other funds are not separable in the
  panel, and premise 3's SIC 6221 count is sometimes 1, so leveraged, inverse or pooled products may enter this
  reference book. No exclusion is claimed. Step 3 must specify and validate a classifier before its forward
  construction can claim one (§"Decision rule").
- Archive seasoning is not the IPO rule; no volatility-conditioned result exists.
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

**Round 2 (56 findings; `var/research/3609_step2/ckpt1_round2.txt` in the loop worktree):**
- **1–8, and the gate-design parts of 3–5: open.** They are the operator's evidence-bar question on #3609 (power,
  deflation, prior, search accounting, and whether G1 may use stage A). §"Decision rule" stays provisional.
- **9:** a pass cannot authorise a different construction; step 3's forward construction (cap, MAX filter,
  intersection) needs its own passing implementation backtest.
- **10–11:** a design-history inventory is frozen with the spec; stage A is a declared retrospective warm start
  with a printed, hashed boundary state.
- **12–14:** the survivorship requirement is stated as unmet for stage B, stage B is labelled not strictly
  point-in-time, and the security-type exclusion claim is withdrawn.
- **15–16:** the size-proxy claim is limited to value coverage, with count and weight below the NYSE median
  printed; the measurement script verifies the pinned artefact, its inputs and a unique, complete, finite cutoff
  grid.
- **17:** SIC transitions at the stage boundary are printed by reason; frozen stage A is not rewritten.
- **18–19:** listing age is renamed archive seasoning and the IPO claim withdrawn; the $5 rule applies to holdings.
- **20–23:** the sector-weight assumption is removed and weights measured; the 2× flag is labelled a same-universe
  diagnostic with zero-weight handling; the MAX filter is a step-3 prerequisite; the volatility substitute is
  dropped.
- **24–27:** one ranking population per operation, no cross-industry fallback; groups under 10 inputs or with zero
  variance give no score; missing members are averaged (a stated custom rule) with membership patterns printed;
  identifier-decided selections are counted.
- **28–31:** scores precede eligibility; a short universe refuses; `INSUFFICIENT` is checked after trades with
  all-cash continuation defined; terminal and coverage exits keep step 1's `end_bar` timing.
- **32–36, 49:** the control gains down-sizing, excludes same-formation sales from purchases, withdraws the
  turnover-match claim, fixes its sampling order and seeds, refuses on any shortage, and defines the book's
  percentile as a mid-rank empirical CDF.
- **37:** the cap-weighted reference is defined as a reconstituted index, not buy-and-hold.
- **38–39:** minimum ticket uses trade sizes; high-turnover months print realised benefit against the control.
- **40–48, 50:** signal diagnostics are cut to the one-month horizon for the retained families, labelled
  selection-conditioned, with exact IC, IC-IR, quintile and cell rules; the effective-years figure is replaced by a
  standard-error ratio.
- **51–52:** the one-year annotation is a leave-one-year-out test; G1's refusal states are enumerated.
- **53–56:** the freeze is two immutable stages (register row, then data-capture manifest); the declaration payload
  is hashed separately; a parent ledger record starts before the first stage-B step.

**Round 3 (40 of findings 9–56 resolved; 8 open plus 24 new, numbered 57–80; `ckpt1_round3.txt`), all applied:**
- **15:** traded notional at or below the NYSE-median cutoff is printed.
- **33, 62:** the control is a target-count rule with capped replacements; notional and cost reconcile across
  forced, discretionary, count-adjustment and rebalance categories.
- **39:** the missing expected-benefit model is a stated exception with a verdict-line note; step 3 must show it.
- **45–47, 71–73:** cells use a 24-effective-month minimum (AR(1) approximation); per-cost-band book contributions
  are printed; every summary has its undefined states; the IC t is never computed over compressed months; outputs,
  axes and windows are enumerated.
- **54–55, 64–65:** the run checks the spec, construction and `DeclaredTrial` source hashes against the register
  row before any stage-B step; the access identity and fields are fixed; `access_id` is ledgered.
- **57–59, 80:** premise-3 counts are labelled raw availability, the shortage assurance is removed, the script's
  wording and the median boundary are fixed, and invalid ME refuses in both script and run.
- **60–61, 63:** the mean rule's rationale is corrected; exits carry one primary reason by precedence; SIC
  transitions use two flags on a stated population.
- **66–69:** the boundary state schema, the exact 119-month G1 sample, the verdict order and `PASS`-only
  annotations are frozen.
- **70:** decay by horizon is a stated exception; step 3 supplies it.
- **74–76:** weight timestamps and denominators are defined; the ticket check covers every trade, with contemporaneous
  and initial-capital bases.
- **77–79:** listing age and a validated security-type classifier are step-3 prerequisites; B1's ETF cost exception
  is inherited from step 0 and stated.
