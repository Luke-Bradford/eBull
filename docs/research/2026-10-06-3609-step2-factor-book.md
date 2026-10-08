# #3609 step 2 — the factor composite book, backtested net of our costs

Status: **gate open; checkpoint 1 converged (round 25).** The evidence-bar question (#3609, 2026-10-06) is settled by `docs/settled-decisions.md`
entry 2026-10-08: a Track B book may enter a zero-capital demo test on a preregistered four-part historical screen,
and §"Decision rule" is that screen. Round 2's findings 1–8 are applied under it (§"Checkpoint log", "Gate opened").
Applying finding 4, the power calculation for the exposure test, changed the book to the value family alone
(premise 6). Revised after Codex checkpoint 1 rounds 1–21; the items queued while building are written into
§"Build-time clarifications". Slices 1–5 are merged and tested on fixtures only. No step-2 code has read a stage-B
month. On stage A, premise 6 measured exposures only; no book return, mean, alpha or growth has been printed for any
month. Programme: `docs/research/2026-10-04-strategy-research-sweep.md`
§4 item 2. Inherits from `docs/research/2026-10-04-3609-step1-factor-panel.md` §"Registration, ledger and what step 2
inherits" (amended below), and reports against `docs/research/2026-10-04-3609-step0-baselines.md`.

## Question

Does a long-only, equal-weight book of US large caps, chosen by the industry-relative composite of the three value
characteristics step 1 rebuilt faithfully (B/M, E/P, CF/P), (a) load on HML, the factor it is built to carry, and
(b) beat SPY total return and a matched random book net of our costs, on a reused validation sample (stage B) that
no step-1 or step-2 selection read? Stage B has been seen before, through step 0's baselines (§"Design-history
inventory").

**What the answer can say.**
- **Pass:** the book's computed result meets the 2026-10-08 screen's four conditions. That makes it a candidate for a
  declared forward demo test (step 3, the ranking-pot successor), not eligible for one: step 3's declaration must
  also show the entry's data requirements hold (premise 5). The case for the premium rests on the published record (§"Adoption rationale"). This
  backtest shows that our implementation carries the intended exposure, and that it did not lose to SPY or to random
  trading net of our costs on the validation sample.
- **It does not prove a premium.** About ten years cannot statistically certify a realistic edge over SPY
  (premise 2), so no IR claim is made here. The beats are point estimates, reported as a screen.
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

**1. Five characteristics passed step 1; the book uses the three value ones.** Step 1's declared run (#3609
comment of 2026-10-06 01:36Z; `docs/research/3609-ledger.jsonl`, run `b6378c7c`) passed `at_gr1`, `be_me`, `gp_at`,
`ni_me` and `ocf_me`. `ope_be`, `ret_12_1` and `rvol_21d` failed the offset bar on one arm each, and no v2 is claimed.
Step 1's family identities, unchanged:
- **value:** `be_me`, `ni_me`, `ocf_me` (the book);
- **GP/A:** `gp_at`, and **investment:** `at_gr1` (passed, not in the book: premise 6).

The profitability (`ope_be`), 12-1 momentum and low-volatility families have no eligible member. Step 1 lets step 2
drop non-PASS characteristics, and nothing requires it to use every PASS one.

**2. About ten years cannot power an IR claim at the skill's prior.** Holding months 2014-10..2024-08 are 119, or
9.92 nominal years. For a one-configuration Track B test, `power_check` needs a non-inferiority margin of at least
0.7896 IR for 80% power over 9.92 years:
- `critical_t` = 1.645 and `required_years` = 9.66 at δ = 0.80;
- stage B alone (3.25 years) needs δ ≥ 1.3792.

Reproduce:

```
PYTHONPATH=. uv run python -c "from app.services.trial_register import *; import math; \
c=power_check(TrialDesign(EvidenceTrack.ADOPTION,0.8,'x',9.92,'x'),trials=1); \
print(c.critical_t, c.required_years, (c.critical_t+0.8416)/math.sqrt(9.92), (c.critical_t+0.8416)/math.sqrt(39/12))"
```

This is conditional on a prior. The skill's 0.3–0.5 is a generic range for a skilled manager's IR
(`portfolio-construction-and-risk.md`), not a measured prior for this book, and it assumes independent months
(nominal years bound effective years from above). §"Adoption rationale"'s published value series give no reason to
expect more for a long-only large-cap value book net of costs. Under that prior, a margin that keeps the book at or
above SPY cannot reach 80% power on our data. Consequences:
- Step 2 makes no IR claim and carries no `TrialDesign` (§"Registration").
- Its gate is the 2026-10-08 screen. Its claim is the exposure test (condition 3), whose power premise 6 computes.

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
| top-1,000 names with SIC 6221 (commodity pools' SIC) | 0 | 1 |

"Above the median" is ME > cutoff and "at or below" is its complement, here and in every diagnostic. The two family
rows count the first draft's three families (GP/A, value, investment); the declared run prints the count of the top
1,000 with the value family present. Family counts are raw-input availability: they apply no group minimum, zero-variance rule, seasoning or price screen.

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

Stage B lies wholly in the second regime.

**What the requirement is, and what of it is met.** `research-process.md` defines the requirement operationally:
"a survivorship-free universe from `research_price_daily`, with delisting returns handled", and "one archive does
not certify the table". The 2026-10-08 entry keeps that requirement and names this corpus as the survivorship-free
one ("the survivorship-free corpus runs 2014-10..2024-08").
- **Met:** the universe is drawn from the archive at each formation, delisted names included, and terminations
  are recorded with step 1's terminal-return handling from 2014-09 on (step 1 premise 1).
- **Partly met: certification.** From 2019 the archive is checked against a second source, the symbol-bearing Form
  25 records. In stage B's years (2021 to 2024-08), 807 of 814 records have a series, and 16% to 25% of matched
  series a year end more than 30 days from the suspension or filing date (step 1 premise 1's table). That reference is a selected population, not every exit; ticker reuse and those
  endpoint mismatches are unresolved. For 2014-09..2018 no independent exit list exists.

So this spec does not claim a certified full-population survivorship-free stage B, and it does not decide whether
the entry's data requirement is met. **A `PASS` here is the screen's computed result, not demo eligibility.**
Eligibility is decided at step 3's declaration, which must show that the entry's data requirements hold, survivorship
and full population included, or cite a recorded policy decision that settles them. This premise is the evidence it
starts from, and the gap is labelled on the verdict line (§"Registration", Labels).

**6. Condition 3 is powered for the value family alone (development data).** Checkpoint 1 round 2, finding 4,
asked for the exposure test's power. Its planning inputs come from stage A, the development sample. For each of three
family sets, the book's scorer, decisions and base-cost path ran on stage A's pinned artefact, and the book's
monthly return in excess of RF was regressed on FF5 plus momentum with step 0's Newey–West rule, in both arms. The
regressions read the book's stage-A returns. Only loadings, standard errors, residual standard deviation, R² and
holding counts were printed; no mean return, alpha, growth or comparison was printed or used to choose. All three
sets count as searches (§"Registration"). The plan, with the script's sha256, stage A's pin and digests of every
factor and prior row read, is `docs/research/3609-step2-exposure-plan.json`, pinned by register r25.

Loading (t), best-case / worst-case arm, formations 2014-09..2021-04, 80 months, rounded from the plan file:

| family set | HML | RMW | CMA | residual sd | median holdings |
|---|---|---|---|---|---|
| GP/A + value + investment (first draft) | 0.279 (5.33) / 0.285 (5.95) | 0.143 (1.72) / 0.149 (1.84) | 0.018 (0.27) / 0.027 (0.43) | 0.81% / 0.83% | 164 |
| value | 0.488 (5.70) / 0.486 (5.80) | 0.114 (1.22) / 0.112 (1.24) | −0.206 (−2.02) / −0.197 (−1.89) | 1.06% / 1.10% | 153 |
| value + GP/A | 0.029 (0.50) / 0.045 (0.82) | 0.396 (2.84) / 0.376 (2.71) | 0.032 (0.30) / 0.020 (0.19) | 1.15% / 1.14% | 108 |

Reproduce: `PYTHONPATH=. uv run python -m scripts.plan_3609_step2_exposure` (reads stage A's pinned artefact and
factor rows to 2021-05-31 only).

- **The first draft cannot be tested with power.** Its CMA coefficient, conditional on the other factors, is 0.018
  to 0.027 (t below 0.5), and its RMW t is 1.72 to 1.84 on 80 months. Stage B has 39, so neither intended loading
  could be tested with useful power there.
- **Value + GP/A cancels value.** HML falls to 0.03: GP/A and value are negatively related (Novy-Marx 2013).
- **The value book carries HML.** Its condition 3 tests one intended loading, so the Bonferroni bar is
  z(0.95) = 1.645.
- **The declared power criterion.** The gate needs HML in both arms, so its power is the conjunction's. The
  criterion is the distribution-free (Bonferroni) bound on the conjunction, 1 − Σ (1 − arm power), at the planning
  point estimates, and it must be at least 0.80. Per arm, planned t is 3.98 / 4.05 and power 0.990 / 0.992, so the
  bound is 0.982: the criterion holds. The planner refuses if it does not.
- **Sensitivities, not criteria.** At the unadjusted one-sided 95% lower bound of each arm's stage-A loading
  (b − 1.645 s; pointwise, not adjusted for choosing among three sets or for two arms), per-arm power is 0.882 / 0.896
  and the conjunction bound 0.778. The criterion holds while each arm's planned t is at least 2.926: stage B's
  standard error may be about 1.36 times the planned one, or the loading about 26% smaller, before it fails.
- **Assumptions; the powers are approximate.** Stage B's standard error is stage A's scaled by √(80/39). That
  assumes stage B matches stage A in factor covariance, residual variance and dependence, and in the long-run
  covariance of the regression scores. The power treats the estimated standard error as known and the HAC t as
  normal; at T = 39 the 1.645 bar is asymptotic, not a demonstrated finite-sample 5% test.
- **What this selection used.** It chose a family set by whether its intended loading can be tested, from
  return-derived exposure statistics. No direct performance summary was printed or used, but exposures carry
  information about performance alongside known factor returns, so the selection is not information-free. Its
  validity rests on sample governance: condition 3 and condition 4 run on stage B, which no selection read (step 0's
  earlier stage-B exposure is inventoried in §"Design-history inventory"). The value book's other stage-A loadings
  (SMB 0.25, momentum −0.20, CMA −0.21) are what a large-cap value book carries; they are printed, not gated.

## Adoption rationale (condition 1, Track B)

The book holds one family, value. Condition 1 asks for independent post-publication support. The published
long-short series, over months before our path starts (to 2014-09-30), from the premise-6 script:

| series | relation to the original study | months | mean a year | Newey–West t |
|---|---|---|---|---|
| French HML, 1992-07..2014-09 | after Fama & French (1992) was published; same US market and construction | 267 | 3.58% | 1.36 |
| AQR large-cap value, UK, 1981-07..2014-09 | outside the US sample; inside Asness, Moskowitz & Pedersen's (2013) to 2011 | 399 | 4.25% | 1.41 |
| AQR large-cap value, Europe ex-UK, same months | as UK | 399 | 4.12% | 1.83 |
| AQR large-cap value, Japan, same months | as UK | 399 | 9.31% | 2.97 |
| AQR large-cap value, US, 1972-02..2014-09 (scale only) | overlaps the original sample | 512 | 4.10% | 1.57 |

- **What it supports:** the premium stayed positive after its US publication and in each of three markets outside
  the US. On a two-sided, unadjusted, asymptotic 5% test it is significant only in Japan; one-sided, Europe ex-UK
  (t 1.83) also clears 1.645.
- **What differs from this book:** each series is gross, long-short and value-weighted on B/M. This book is
  long-only, equal-weighted, industry-relative (Asness, Porter & Stevens 2000), a three-member composite, and net
  of our costs.
- **The prior, stated:** a small positive long-short premium with wide uncertainty. McLean & Pontiff (2016) put the
  average post-publication decline across anomalies at 58% (cited, not measured here), and `strategy-menu.md` calls
  value's edge small, regime-dependent and strongest in small caps, with a 2007–2020 drawdown. The long-only
  large-cap book's net share of the premium is smaller and unmeasured. Conditions 3 and 4 screen that transfer; no
  test here can measure the premium (premise 2).

## Source rules

| decision | rule | source |
|---|---|---|
| characteristic z-score | each month, rank the variable and standardise the ranks by their cross-sectional mean and standard deviation | Asness, Frazzini & Pedersen 2019 (QMJ), p. 9, eq. (2); AQR working-paper PDF, sha256 `761c42f9…fbb2a9`, pinned in slice 1 |
| industry-relative form | rank and standardise within industry | QMJ p. 13 ("varying the standardization universe of our z-scores … by country-industry"); `portfolio-construction-and-risk.md` ("Rank accounting signals within industry") |
| family and composite | family = z(mean of member z); composite = z(mean of family scores), which with the value family alone ranks exactly as the family | QMJ p. 4 ("average them") and eq. (2): a component is the z-score of its members' z-scores combined; with every member present, sum and mean rank identically |
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
  missingness; no rule does. The composite needs the value family, the book's only
  one. The declared run prints, per formation, the count and book weight of each membership
  pattern (which members and families were present).
- **Identifier-decided selections.** Where tied composites straddle a decile or tercile boundary, `name_key`
  decides. Every such selection is counted and printed.
- **Archive seasoning, not an IPO rule.** A name may enter only when its series' first admitted bar is at least 36
  months before s(M). This is an archive-seasoning rule. It does **not** implement `market-segments.md`'s "under 36
  months listed" exclusion (Loughran & Ritter 1995): no effective-dated listing history exists here, archive
  coverage need not start at listing, and a ticker change that starts a new series reads as young. It applies at
  entry only; a held series' age only grows.
  - **Source.** The series' first admitted bar is the earliest bar of that series admitted under the builder's
    decision-bar predicates (`_DECISION_BARS_SQL`: a quarantine-coverage row, `return_usable`, finite positive close
    and adj_close), with no lower date bound and at or before the stage's price bound. The builder freezes it per
    admitted series as `inputs/first_bars.jsonl.gz`, read in the same repeatable-read snapshot as the other inputs.
    It is a date only; no price is frozen with it. Neither the 12-month daily window (`daily_start`) nor the
    decision bars can supply it: seasoning at 2014-09-30 needs a bar on or before 2011-09-30.
  - **Shape.** One JSON line `[series_id, "YYYY-MM-DD"]` per admitted series (`inputs/admitted.jsonl.gz`) that has
    at least one qualifying bar, sorted by `series_id`. A series with none is omitted, never written as null. The
    report refuses the file on a repeated `series_id`, a series not in `admitted.jsonl.gz`, a malformed date, or a
    date after the stage's price bound, and maps each date to the row's `name_key` at M through the row's
    `series_id`.
  - The report requires every admitted name at M to have a decision bar at s(M) (its ME close must equal it), so
    every admitted name has a qualifying bar. A universe name with a composite and no first bar still refuses the
    run (`eligible_to_enter`), as a defect.

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
  every price read after 2021-05-31 (§"Registration", ledger steps 1–2). The one exception is the declaration's
  integrity-only read of the two factor snapshots, which has its own `read` access record (§"Registration").
- **One continuous path, 2014-09-30 to 2024-08, with a retrospective warm start.** The characteristic set was chosen
  on all of stage A, so stage A's holdings could not have been produced in real time. They are a declared
  development-path warm start: the path starts all-cash at 2014-09-30. The **stage boundary state** is taken at the
  end of the 2021-05 holding month, before the 2021-05 formation's trades: for each termination arm and cost
  scenario, every open position (`name_key`, value, entry formation, entry band), the cash and the NAV, and the same
  for each control draw and the equal-weight and cap-weighted references. It is written as canonical JSON (below) and
  its sha256 is printed. Stage-B statistics are slices of the path after that state.
- **Nothing else is computed before the declaration freezes.** Two registered exercises are the exceptions: premise
  6's stage-A exposure plan (loadings, standard errors, residual standard deviation, R² and holding counts of three
  family sets, from stage-A returns) and §"Adoption rationale"'s published series to 2014-09. No other book return,
  IC, spread, loading or factor mean is computed on any month first, and nothing at all on stage B. Code is tested on
  synthetic fixtures only.

## Step 1's inheritance clause: the separately governed validation sample

Step 1 selected characteristics with bars that read our long-short series against JKP's over all of stage A, and
premise 6 chose the family set on stage-A exposures. Step-2 claims on stage A would need a nested replay or a
separately governed validation sample; this spec takes the second:
- **Conditions 3 and 4 (G1 and G2) run on stage B only.** Every selection (step 1's characteristics, premise 6's
  family set, every construction choice) used earlier months.
- **Stage A is development.** Its returns and loadings are printed, never gated.

**Design-history inventory.** Everything the designers of this spec had seen, frozen with the spec:
- **Stage A:** step 1's fidelity verdicts and its printed per-characteristic correlations, betas, tracking errors and
  mean offsets against JKP's long-short factors, for all eight characteristics and both arms (`docs/research/3609-ledger.jsonl`,
  run `b6378c7c`); premise 3's ME and coverage counts.
- **Stage B, through step 0** (`var/research/3609_step0/20261004T214439Z`): the W2 statistics of B1 SPY, the static
  ETF mixes and the random-basket draws. No characteristic-sorted book was ever computed on stage B.
- **Earlier GP/A trial** #2901 (`r6-2901-quality-gpa-2026-09-25`, 2013–2024 June formations): it failed its
  construction gate (correlation +0.193 to +0.199 against +0.20), and no arm outcome was computed or published.
- **Published results:** the adoption-rationale sources, premise 6's published value series (months to 2014-09)
  and McLean & Pontiff's 58% decline.
- **Stage-A exposures (premise 6):** the three family sets' loadings, standard errors, residual standard deviations,
  R² and holding counts. No return, mean, alpha or growth of any book on any month has been printed.
- **Choices fixed with that knowledge:** the 1,000 size rule (premise 3), the decile/tercile band and equal weight
  (both from the skill's cited sources), dropping the three non-PASS families (step 1's verdicts), and the value
  family alone (premise 6, on exposures). None was chosen by comparing book returns, because none has been printed.
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
3. **Raw closes are validated first.** Every universe name and every holding at s(M) must have a finite, positive
   raw close; otherwise the run refuses (`PRICE_INVALID`), whether or not the name would trade.
   **Eligible to enter:** in the universe, with a composite, raw close at s(M) ≥ $5 and archive seasoning ≥ 36
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

**Everything else matches the book:** weights, costs, bands, statuses and arms.

**Trade categories,** for the book and per draw and arm, each with order notional and charged cost: initial
purchase (2014-09-30); forced exits; discretionary exits (the control's step-2 sales); count-adjustment sales (the
control's step-3 sales); entries (every purchase of a name not held after 2014-09-30, book or control); rebalance adds and trims;
final liquidation (2024-08). Imputed terminal realisations are listed separately; they have no order and no cost.
Order notional and cost each reconcile to their totals across the categories. Turnover follows step 0 (initial
purchase, final liquidation and terminal realisations excluded) and is printed with that exclusion stated.

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

The book's computed result **passes** the 2026-10-08 screen (`docs/settled-decisions.md`) only if all four of its
conditions hold. A `PASS` is that computed result; demo eligibility is decided at step 3 (premise 5).
Conditions 1 and 2 are met before the declaration and recorded in this spec, whose sha256 it pins. Conditions 3 and 4 are G1 and G2 below, which
must both hold at base cost under **both** termination arms.

1. **Published support:** §"Adoption rationale".
2. **Construction fidelity:** step 1's declared run `b6378c7c` passed `be_me`, `ni_me` and `ocf_me` against JKP's
   published series (premise 1). This is where the members' fidelity is shown; condition 3 tests only the book's
   exposure.
3. **Powered exposure test:** G1.
4. **Net beat on the sealed sample:** G2.

**G1, condition 3: the HML exposure on stage B (2021-06..2024-08, 39 months).**
- Regress the book's stage-B monthly return (the slice from the boundary state that G2 uses) in excess of RF on FF5
  plus momentum. Use step 0's data, unit checks and Newey–West rule, `newey_west_lag` = ⌊4(T/100)^(2/9)⌋, which is
  3 at T = 39.
- HML, the value family's intended loading, must be positive with a one-sided t above z(1 − 0.05/1) = 1.645
  (Bonferroni over the one intended loading). Its power is premise 6's. The other loadings are printed, not gated.
- **Two kinds of input failure.** A factor snapshot that fails `read_factors` (digest, unit, a non-finite value
  or float, a repeated month) is an integrity failure: the run ends `failed` before evaluation (item 22). The
  refusals below are about the evaluation window inside a verified collection.
- **Refusals, checked before any coefficient is printed** (verdict `G1_REFUSED`, with the reason): a missing factor
  month; a non-finite value in the return or factor matrix; anything other than exactly one aligned book, RF and
  factor observation for each of the 39 declared months (no duplicates, no extras: `read_factors` refuses a
  repeated month in the verified snapshot rows, and G1 takes exactly the declared months from that collection, so
  later or earlier months in the snapshot are not extras); a design matrix whose rank is below
  its column count (numpy `matrix_rank` at its default tolerance); zero residual variance; or any Newey–West standard
  error that is not finite and positive.

**G2, condition 4: realised net return (stage B, 2021-06..2024-08).**
1. The book's annualised net log growth G exceeds B1's by more than the margin over B1.
2. It exceeds the median (nearest-rank) of the control's 1,000 net G values by more than the margin over the control.

**The margins are zero over both comparators, the entry's floor.** Basis:
- A positive margin needs an expected net excess for this construction, and no source gives one. §"Adoption
  rationale"'s series are gross and long-short, and `strategy-menu.md` calls value's edge small and regime-dependent.
- Over 39 months, a margin the size of a realistic edge is far inside the sampling noise (premise 2), so it would add
  a threshold without adding information.
- The beats are point estimates, reported as a screen and never as significance. The stress-cost and year-deletion
  annotations below show how thin a pass is.

**Selection across configurations.** The governing reading: the entry's "configurations" are those evaluated on the
sealed confirmation sample, here stage B. Configurations chosen on development data (step 1's characteristics,
premise 6's family sets) are governed by separation instead: they are counted in the register, and the chosen one
is tested on stage B, which none of them read. Validation counts as separate when the selection read no month of
the confirmation sample.
- One configuration is evaluated on stage B, so PBO/CSCV, which needs at least two, is not computed, and a beat shown
  by only one of several configurations cannot arise.
- **A later configuration on this path joins the rule.** Step 3's forward construction is evaluated on the same
  stage B (§"After a pass"). That makes two, so step 3's declaration must state its PBO/CSCV treatment over both,
  and step 3 may pass only if this book passed: a beat only one of them shows is not a pass. No configuration is
  evaluated on stage B to build a PBO matrix.

The two arms are a conjunction: both must pass. The turnover veto stays as declared (verdict order step 5).

**Printed beside the verdict, never gating:** stress cost, stage A, each calendar year, segments and attribution.
Two annotations go on a `PASS` verdict line only, by frozen tests:
- **"fails at stress cost"** when G2 fails under step 0's stress cost in either arm;
- **"depends on <year>"**, for every year that qualifies, when G2 fails after removing that calendar year of stage B
  (2021 from June, 2022, 2023, 2024 to August) from the book, B1 and every control draw alike, annualising the
  remaining months. The failing arm and comparison are printed.

**Every compared series must be complete and its statistics defined,** at base and stress cost alike: the book,
B1, every control draw and both references each need exactly one finite monthly return for each of the 119 months,
and every monthly wealth factor 1 + r, as computed, must be finite and strictly positive. **Comparisons use
annualised log growth** G = (12/n) Σ ln(1 + r): G2, the control's median and ordering, the stress-cost and
year-deletion annotations, and the book's percentile all compare G values, never rounded returns, so two different
valid paths cannot tie at a displayed −100%. The annualised return exp(G) − 1 is for display only, here and in
every diagnostic that prints one: where it is not finite (`exp` overflows on a short window), the percentage prints
"outside representable range" beside the finite G, and nothing computed from G changes.

**Non-positive wealth stops the run** (declared policy, by construction). The run does **not** try to tell an
economic total loss from numerical failure: a factor that computes to ≤ 0, or an r that computes to exactly −1, in
any series (the book, B1, a reference or a control draw, either arm, base or stress cost) is `REFUSED`, reason
`WEALTH_NONPOSITIVE`, whatever its cause. This is a declared exception to stress cost being non-gating.
- **Precedence.** Series are valued month by month in calendar order. The first month in which any series meets
  this is the stopping month. Every series is valued and validated through that whole month, and every refusal
  arising in it or earlier is collected and printed with its series, draw, arm, cost scenario and month. The status
  is `REFUSED` with all the reason codes found. No later month is valued and no statistic, gate or diagnostic is
  computed.
- **Why a refusal, not a score.** A window ending at zero wealth has a defined −100% return, so this is a chosen
  conservative policy, not forced by the arithmetic. Every series here is long-only, holds cash at a zero return,
  and pays costs of a few percent of traded notional at most (step 0's bands), so its wealth reaches zero only if
  every position it holds is worth nothing at once, and a value near zero is indistinguishable from a precision
  failure without holding-level evidence that B1's inherited returns do not carry. The run stops for it to be
  investigated, and a refusal can never become a `PASS`.

A non-finite G, median or year-deletion statistic refuses the run as
`COMPARATOR_INVALID` (the book's own failure as `REFUSED` with that code too).

**The declared path is a precondition.** Before step 1's status is assigned, the run must span the declared path,
every holding month 2014-10..2024-08 once and in order; otherwise it is not the declared run and no verdict is
printed. `report_3609_step2_run.evaluate_run` enforces it before evaluation (each stage's formations must equal its
declared grid, `STAGE_GRIDS`; a mismatch ends the run `failed`, item 24), and the verdict re-checks that stage B's
39 months are present once each, in order.

**Verdict order.** The run stops at the first that applies, and prints the status with its reason:
1. `REFUSED`: a data refusal raised during evaluation (`ME_INVALID`, `UNIVERSE_SHORT`, `PRICE_INVALID`,
   `CONTROL_SHORT`, `CUTOFF_INVALID`, `COMPARATOR_INVALID`, `WEALTH_NONPOSITIVE`). The run ends `completed` with this
   status (item 18). Two other phases are not verdicts: a refusal at the report's gate, before `report_started`
   (`CAPTURE_AMBIGUOUS`, a declaration or hash mismatch), prints `REFUSED` and writes no row; an integrity failure
   after `report_started` (a pin mismatch, a stage off its grid) ends the run `failed` (items 22 and 24). The payload is: the status and reason code; the offending
   formation, `name_key` and draw; the counts that triggered it (for `CONTROL_SHORT`, the pool and the purchases
   needed); the run id and its hashes; and the labels. No path, gate or diagnostic is printed.
2. `INSUFFICIENT`: the path, with the formations concerned; G1 and G2 are not evaluated.
3. `G1_REFUSED`: the path and G2's inputs are printed; no gate verdict.
4. G1 and G2 by the conjunction above. If either fails: `FAIL`, with the failing gate and arm.
5. **Turnover veto:** if any stage-B formation (2021-05..2024-07, the trades that set stage B's holdings) has
   one-way turnover above 50% at base cost in either arm: `FAIL`, reason `TURNOVER_BENEFIT_UNSHOWN`. The 2024-08
   final liquidation is not a formation and is excluded, as step 0 excludes it from turnover: it is the backtest's
   end, not a trading decision. `research-process.md` requires the net expected benefit to be shown
   above that level, and this run has no model that shows it. Stage-A exceedances are printed, not gating.
6. Otherwise the run is a **pass candidate**. Its annotation statistics (stress-cost G2, year deletions) are
   computed and validated; a refusal among them (`COMPARATOR_INVALID`, `WEALTH_NONPOSITIVE`) makes the status
   `REFUSED`. Only then is the terminal ledger row written and `PASS` printed with its annotations. Annotations are
   never computed for a run that stopped at an earlier step.

**After a pass:** step 3 specifies the forward construction: this book plus the sector cap, #3621's frozen MAX filter,
an effective-dated listing-age exclusion with a stated missing-history policy, a security-type exclusion whose
classifier (field, accepted values, unknown-type treatment) is specified and validated over every candidate
instrument, and the eToro-tradable intersection. Before any position, that construction gets its own declared implementation
backtest on this path, which must reach `PASS` under this verdict order. Every requirement of that order is
mandatory; the only one step 3 may replace is the turnover veto, and only by a declared method that shows the net
expected benefit, reviewed at its own checkpoint 1 and itself passed (step 3 must also state how it treats the absence of
historical eToro eligibility). Only then is the forward demo test declared, as a
claiming #2599 declaration with its own `TrialDesign` and power check. Its prediction interval and the #2500
sequential stop rule are specified there from the implementation backtest's monthly series.

**After a fail or `INSUFFICIENT`:** "not demonstrated" on #3609. Any new configuration (universe, weighting, band,
families) is a new, counted declaration, and needs a reason other than this result.

## Registration

**`DeclaredTrial` `3609-step2-book-v1`:** `declared_for=None`, `exactness=EXACT`, `searches=1`.
- **Its claim is condition 3**, powered in premise 6. The register's claiming form (`declared_for` with a
  `TrialDesign`) plans an IR margin against a benchmark through `power_check`. This screen claims no IR (premise 2),
  so the row is non-claiming in the register's sense. Condition 3's power plan is frozen in this spec, whose sha256
  the row's `evidence` pins.
- **One configuration is evaluated on stage B:** the book above. Not counted, with reasons:
  - the arms are a conjunction, which cannot raise the false-pass rate;
  - the control, the stress scenario and the diagnostics select nothing;
  - no alternative configuration is evaluated on stage B, and nothing was before the freeze.
- **Counted in M elsewhere:** step 1's 16 searches (`3609-step1-fidelity-v1`) and premise 6's 3
  (`3609-step2-exposure-planning-2026-10-08`, register r25).
- **How the upstream selections meet this screen's tests.** `searches` deflates an IR test (`freeze_power_record`
  uses a trial's own count), and this screen runs none, so those counts enter global M only. The screen's tests are
  protected by separation instead. Condition 3 runs on stage B, which no selection read, at one intended loading.
  Condition 4 runs on stage B for the one configuration and claims no significance. So step 1's selection on
  fidelity and premise 6's on exposures are both tested on separate data, which is what step 1's inheritance clause
  allows.

**Amendment to step 1's inheritance text.** Step 1 says step 2's declaration "needs a `TrialDesign` that passes
`power_check`". Premise 2 shows that no margin meaning "at or above SPY" can be powered here. Under the 2026-10-08
entry the power requirement attaches to the screen's claim, condition 3 (premise 6). A `TrialDesign` belongs to a
claiming #2599 declaration, and `DeclaredTrial` refuses a design without `declared_for`, so the IR design moves to
step 3's claiming forward declaration, whose planned data include the forward months.

**The freeze has two immutable stages; neither is edited after it is written.**

1. **The declaration** (slice 4), merged on `main` before any stage-B read. It is the `DeclaredTrial` row in
   `TRIAL_REGISTER` with a `TRIAL_REGISTER_VERSION` bump: the mechanism step 1's `3609-step1-fidelity-v1` used. It is
   not `freeze_preregistration` (`app/services/result_ledger.py`), which is for claiming declarations and refuses one
   without a `TrialDesign` (#3610). The row's `evidence` string carries:
   - this spec's sha256;
   - the construction-version hash (step 1's mechanism: the report, the builder and every imported module except
     `trial_register.py`, which is left out to avoid hashing the row that holds the hash);
   - the stage-A artefact manifest sha256, `e50872104f77d4db4d16a41dd9b953bf064fa92704516c9813940b60ae8ec115`
     (`2026-10-07-0e8dba6e-stageA`): stage A republished from `main` at `0e8dba6e` with
     `inputs/first_bars.jsonl.gz` (slice 3), replacing `ee1e8abc241f26596529509cd38f4692de4de8ff8700c0ac8ca19c2a412f3c1e`.
     `--check-republish` passed, and premise 3's table reproduced unchanged under `read_verified_artefact`. **Replay identity:** the
     republished manifest's `inputs` must equal that artefact's plus the one key `inputs/first_bars.jsonl.gz`;
     `schema`, `stage`, `pinned_manifests`, `formations`, `rule_versions`, `rows.path`, `rows.count`,
     `rows.content_sha256` and `census` must be equal. Only `git_sha`, `published_at`, `spec_sha256`,
     `construction_sources`, `construction_versions` and `rows.sha256` (the compressed bytes) may differ.
     Otherwise slice 3 stops;
   - step 0's run manifest sha256;
   - the FF-12 map, QMJ PDF and Table 9 CSV sha256s;
   - for each of step 0's two factor snapshots, its `response_sha256` and its observation digest (below);
   - the **register-policy hash**: sha256 of `ast.unparse` of `trial_register.py`'s module AST with its two
     top-level assignments `TRIAL_REGISTER_VERSION` and `TRIAL_REGISTER` removed. It pins every class, constant and
     function the register's validation uses, which the construction hash leaves out to avoid hashing the row that
     holds the hash. Any later edit to that policy code makes the run refuse, and the trial must be re-declared;
   - the hold-out access identity: `strategy_id="3609-step2-book"`, `strategy_version="v1"`;
   - the Python version, because `random.Random` string seeding is version 2 (step 0).
2. **The data-capture manifest** (slice 5), written after the access row and before evaluation. It holds the
   sha256s of the extended SUB reference artefact and the stage-B artefact manifest, with their input hashes. These
   cannot be in the declaration, because the files are fetched only after the access is logged.

**Canonical JSON**, wherever a hash is taken of a state or payload: `json.dumps(obj, sort_keys=True,
separators=(",", ":"), ensure_ascii=True, allow_nan=False)`, encoded UTF-8. Collections of positions or draws are
lists sorted by (`name_key`) or (draw, `name_key`); floats use Python's shortest round-trip `repr`; dates are ISO
strings; enums are their values; tuples become lists.

**The declaration payload is pinned separately, once.** Slice 4 commits a `declared` row to
`docs/research/3609-ledger.jsonl` in the same PR as the register row: the trial id and the sha256 of the row's
canonical JSON (`dataclasses.asdict`, then the conversions above). Every attempt requires exactly one `declared` row
for `3609-step2-book-v1` and exactly one register row with that id, `declared_for=None`, `exactness=EXACT` and
`searches=1`, and refuses unless the row's payload hash equals the `declared` row's. Its `started` row records the hash
it checked and `TRIAL_REGISTER_VERSION`; the version is recorded for audit, not compared, since any later trial bumps
it. An edited payload therefore refuses on every later attempt, not only within one.

**Every consumed file is verified immediately before use** against the manifest that pins it. A mismatch refuses
the run, so an unchanged manifest beside an altered file fails.
- **The bytes checked are the bytes used.** Each consumed file (manifests included) is read once into memory,
  hashed, and parsed from those same bytes; no path is reopened after its check. A file too large to hold is first
  copied into a directory the run creates for itself (mode 0700); the copy is hashed and only the copy is read.
  Slice 3 brings `verify_artefact` and `scripts/measure_3609_step2_universe.py` under this contract, and premise 3's
  table must reproduce under it.
- **Stage A and stage B** through `verify_artefact` (manifest digest, every input and every published output), each
  against **its own expected pin map**. Stage A's is the three pins `verify_artefact` checks today
  (`scripts/build_3609_factor_panel.py:1149`), unchanged. Stage B's is those three plus `reference_3609_step2_sub`,
  the extended SUB artefact's manifest sha256 from the data-capture manifest. Slice 2's tests show each stage
  verifies under its own map, and that a stage-A artefact under stage B's map, a stage-B artefact under stage A's,
  and a substituted SUB artefact each refuse.
- **Step 0's `paths.json`** against step 0's run manifest `sha256` map.
- **Factor and RF data.** Step 0's manifest pins `reference_data_snapshots` ids (`factor_snapshots`: five-factor 39,
  momentum 40), which are row ids, not content hashes. For each id the run requires `parse_status = 'accepted'`,
  recomputes sha256 of the stored `payload` and requires it to equal the row's `response_sha256` and the value the
  declaration pins. It then reads that snapshot's `reference_data_observations` and requires the sha256 of their
  canonical JSON (a list of `[series_key, observation_date, str(value), unit]`, sorted by series key then date) to
  equal the observation digest the declaration pins. The snapshot row, its payload and its observations are read
  in one repeatable-read transaction, verified, and the regressions and RF use only that verified in-memory
  collection; no later query reads factor or RF data. Both digests are computed when the declaration is written, by
  an **integrity-only read**. Before it, the slice-4 script commits a `record_holdout_access` row with
  `strategy_id="3609-step2-book"`, `strategy_version="v1"`, `access_kind="read"`, no `result_version`,
  `accessed_by` = the identity running it, and `purpose="#3609 step 2 declaration: integrity digests of factor
  snapshots 39 and 40, no values read out"`. It then hashes the stored bytes and rows and prints only the two
  digests. The declaration PR cites the `access_id`. This is the access history `research-process.md` §Hold-out
  requires in `strategy_holdout_accesses`; step 0 already evaluated these snapshot ids over stage B's months.
  **The digests capture content at slice 4.** Step 0 pinned row ids only, so equality with the content step 0
  read is unverified, and the observation digest does not prove the rows still match the payload (no re-parse is
  done). Both are known limits.
- **Each reference artefact** (FF-12 map, QMJ PDF, Table 9 CSV) against its pinned sha256.

**The run checks its own code against the row before any stage-B step.** `record_holdout_access` does not enforce a
register row (it is called with `require_declaration=False`, so a trial without a frozen #2599 declaration passes),
so the check is the run's own. Before writing `started`, it recomputes the spec sha256, the construction hash and the
register-policy hash, and refuses unless each equals the value in the row's `evidence`. The `started` row
records the three values it checked. It also refuses unless the running Python's major and minor version equal the
declared one, which fixes `random.Random` string seeding and `json` serialisation.

**Ledger: one parent run record from the first stage-B step.** Written to `var/research/3609_step2/ledger.jsonl` and
committed to `docs/research/3609-ledger.jsonl`, in this order:
1. `started`: run id (a fresh `uuid4` hex for every attempt, so a retry is a new run), HEAD commit, spec hash,
   construction hash, register-policy hash, Python version as `"major.minor"` (the `evidence` form, e.g. `"3.14"`),
   payload hash, register version and command. The three hashes and the Python version are the values compared at
   `report_started`. The other fields are provenance and are not compared with `started`: the report re-checks the
   payload hash against the `declared` row directly, and neither equality nor inequality of the two HEADs is
   required (a capturing attempt's differ, since its ledger merges first; a reusing attempt's may coincide).
   Written before anything below.
2. `access_recorded`: one `record_holdout_access` row with `strategy_id="3609-step2-book"`, `strategy_version="v1"`,
   `access_kind="evaluate"`, `result_version` = the run id, `accessed_by` = the operator or loop identity running it,
   and `purpose="#3609 step 2 declared run <run id>"`. It commits in its own transaction before step 3, and the
   ledger row records the returned `access_id`.
3. `sub_published`, then `stage_b_published`: each artefact's manifest sha256. A reusing attempt writes
   `capture_reused` instead of rows 3 and 4 (§"Slices", capture lifecycle).
4. `data_frozen`: the data-capture manifest's sha256. The report refuses without it, or if any file differs from it.
5. `report_started`: written by the report, after the capture's ledger rows are merged and fetched, for capturing
   and reusing attempts alike: the report's HEAD commit, the binding's capturing run id and its `data_frozen`
   sha256. `started` keeps the HEAD the attempt began at; neither row is edited.
6. `completed` or `failed`, written before any result is printed.

**Abandoned runs.** A run with no terminal row is committed with the rest and classified by the database, not by
the JSONL: if the hold-out access log holds a row with the declared `strategy_id`, `strategy_version` and
`result_version` = its run id, it is an **access** (stage-B
exposure possible), whether or not `access_recorded` reached the JSONL; otherwise it is an **attempt** with no
exposure. The ledger PR states each abandoned run's class and the query that set it.

**Diagnostics count later.** A later declaration that selects a family, cell, horizon or variant this report printed
counts every alternative printed in that dimension as searched.

**Labels on every output:**
- the verdict line names the survivorship regime of each sample;
- "retrospectively filtered construction data" (step 1 premise 4);
- "reused validation; integrity masks retrospective, not strictly point-in-time" on stage B;
- "development" on stage A.

## Diagnostics (printed, never gated, except the turnover veto and input refusals)

**Weights.** Unless stated, a weight is post-trade at s(M), on post-cost NAV including cash. A sold name's
"affected" weight is its pre-trade weight at s(M) on pre-trade NAV. Status weights are the post-trade weights at the
start of a holding month, grouped by the status that month ends in.

**Windows.** Every summary is printed for stage A (development), stage B (reused validation) and pooled, with the
window's defined-month count for that metric.

- **Universe:** premise 3's table recomputed on all 119 formations, stage B included, with no retuning, plus the
  book's weight and traded notional at or below JKP's NYSE-median cutoff.
  - **Cutoffs:** every published stage-B cutoff passes the script's checks (one per month, finite and positive
    after normalisation); a duplicate or invalid one refuses the run (`CUTOFF_INVALID`). An unpublished month prints
    `unavailable` in its cutoff-dependent columns, and the coverage count is printed. A month with no admitted name
    above its cutoff prints its share column as `unavailable`. An ME sum used in a share that is not finite and
    positive refuses the run (`ME_INVALID`).
  - **Notional classes,** by precedence: no ME at s(M) (the name left the admitted population); cutoff
    unavailable; above; at or below. The 2024-08 final liquidation is its own class, not size-classified. The
    classes reconcile to the book's total order notional.
- **Signals: one-month horizon, for the value family and the composite.** With one family the two rank
  identically; both are printed because the report lists every family and the composite. They are
  selection-conditioned: the family was chosen on stage A, and the label says so. Families outside the book have no
  step-2 construction and are not printed.
  - **Population:** universe names at M with the score and a step-1 holding-month return, per arm.
  - **IC_M:** Spearman correlation (average ranks for ties) between the score and the holding-month return.
    Undefined when fewer than 30 names, or either vector has zero variance.
  - **Quintile spread_M:** names ordered by score descending, ties by `name_key`; boundaries at ranks ⌈kn/5⌉ for
    k = 1..4. Spread = Q1 (highest scores) minus Q5, equal-weighted, gross. Undefined when any quintile has fewer
    than 5 names. No value-weighted spread is printed.
  - **Monthly series** of IC and spread are written to the output file.
  - **Summaries per window,** each metric on its own defined months. For IC and spread alike: mean; standard
    deviation (ddof 1); a Newey–West t on the mean with step 0's lag rule. For IC only: IC-IR = mean / standard
    deviation, monthly, not annualised. No mean-to-deviation ratio is printed for the spread. One-month
    outcomes remove mechanical overlap; Newey–West is used because the series may still be serially dependent. All
    summaries are `undefined` with fewer than 12 defined months; IC-IR is `undefined` at zero standard deviation;
    the t is `undefined` if any calendar month in the window is undefined (it is never computed over compressed
    months) or its standard error is not finite and positive.
- **Exception: no decay by horizon.** `research-process.md` lists decay by horizon among signal metrics. Step 1
  defines only a one-month holding contract (returns, statuses, terminal handling); a multi-month cohort return is
  a further construction with its own treatment of names that leave the panel, and horizons past the 2024-08-31
  read bound would be partial. It is not printed here; step 3 must supply it before a forward declaration.
- **Two size cells:** the book universe, and admitted names outside it. Scores are standardised within (size cell,
  FF-12 industry, M), by §"Source rules", among names with the inputs, whatever their price. Cost-band reporting
  cells are assigned afterwards; an outside name whose raw close at s(M) is missing, non-finite or non-positive goes
  to a "price unavailable" reporting cell, counted per formation, and never refuses. Labelled a proxy for `market-segments.md`'s NYSE cells.
- **Segments (primary partition: cost band at s(M) × the two size cells).** A name's cell is set at each M, so a
  name that migrates counts in its cell at that M.
  - **Each cell:** the signal block above, on its own names.
  - **Per-band sub-books** (attribution series: not tradable portfolios, no cash, and they do not sum to the book;
    the label says so). A name's band at M is the cost band of its raw close at s(M); that sets cell membership.
    For band b at formation M:
    - V_b = the book's post-trade value in names of band b, and g_b = their value-weighted holding-month return;
    - C_b = every cost charged at this formation on trades in names of band b (sales of names leaving the book
      included), each at the position's entry band;
    - L_b = the 2024-08 final-liquidation cost on band-b holdings, charged in that month only (bands by s(2024-07));
    - net return = ((1 + g_b) − L_b/V_b) / (1 + C_b/V_b) − 1, evaluated in that ratio form, with each quotient,
      the numerator and the denominator required finite. This is step 0's
      one-pass rule applied to the band's capital before its costs. V_b = 0 is checked first (undefined, below).
      Otherwise the inputs are validated before evaluation: V_b, g_b, C_b and L_b finite, V_b > 0, C_b ≥ 0,
      L_b ≥ 0 and g_b ≥ −1. An input failing any of these, a factor ((1 + g_b) − L_b/V_b) / (1 + C_b/V_b) that is
      not finite and strictly positive, or a net return that computes to exactly −1, marks the month `invalid`
      (diagnostic only).
    - **Trades by band:** every order (sales of complete exits included) is assigned to the traded name's band at
      s(M). The 2024-08 final liquidation, which has no formation, is assigned to the name's band at s(2024-07), the
      membership L_b uses. That reporting band is separate from the entry band that sets each sale's charged
      half-spread. Per band and window: order notional, cost, and turnover as (buys + sells) / 2 over the **book's**
      pre-trade NAV, so band turnovers sum to the book's. Initial purchase, final liquidation and terminal
      realisations are excluded from turnover as in step 0, and printed separately.
    - A month with V_b = 0 is undefined for that sub-book. A month with fewer than 5 band-b names is marked thin;
      the 5 is fixed by construction and monthly counts are printed.
    - **Per window:** step 0's metrics (net and gross return, volatility, maximum drawdown, active return, tracking
      error, IR, beta and maximum relative drawdown against B1) and an FF5+momentum regression. The regression is
      G1's, with that window's exact months in place of stage B's 39 and its Newey–West lag from that window's count,
      and G1's numerical refusals mapped to `undefined`.
    - **Uncomputable windows:** if any month in the window is undefined, invalid or thin, every metric is printed
      as `undefined` with the reason and nothing is computed. **Computable windows** whose n_eff (below) is under
      the minimum are computed and marked `insufficient`.
    - A sub-book's undefined or insufficient output affects only that output, never the verdict.
    - The book lives in one size cell, so the size axis has no book split.
  - **Minimum effective sample, per (series, arm, metric, window)**, where a series is a signal's IC or spread or a
    sub-book's net return: 24 effective months, with n_eff = n × (iid
    variance of the mean) / (Newey–West variance of the mean), capped at n, using the same Newey–West rule as the t.
    It is an estimate that captures dependence only up to the Newey–West bandwidth, and is labelled so. It is
    `undefined` when the t is undefined (fewer than 12 defined months, any undefined calendar month in the window,
    or a standard error that is not finite and positive), and an undefined n_eff counts as below the minimum,
    never as n. The 24 is fixed by construction. Every summary below the minimum is printed and marked
    `insufficient`, not omitted.
  - **FF-12 industry:** a marginal partition of the book universe, not crossed with the cells; IC block only.
- **Other `market-segments.md` axes, stated exceptions:**
  - **ADR and share-class flags:** unavailable. The panel holds no effective-dated security-class field. Step 1's
    10-K/10-Q restriction removes 20-F and 40-F issuers and its one-security rule removes issuers with two linked
    lines, but neither classifies the admitted security, so no flag is claimed.
  - **Institutional-ownership terciles:** unavailable. `market-segments.md` makes them forward-only from 2024, and
    no flag is backfilled from later ownership data.
  - **Liquidity terciles within size:** deferred. Each printed axis counts as searched for any later declaration
    (§"Registration"), and the cost band already carries the trading-cost axis.
  - **Short-interest deciles:** deferred to #3621, which owns that filter; FINRA data start in 2021.
- **Book, per window and per calendar year:** step 0's formulas for these figures. No Sharpe is printed.
  - net and gross annualised return, volatility and maximum drawdown;
  - active return, tracking error, IR, beta and maximum relative drawdown against B1;
  - turnover (traded notional), cost drag and book size;
  - weight held in each return status.
- **Turnover above 50% a month** (`research-process.md`: net expected benefit must be shown, Novy-Marx & Velikov
  2016). For each such month: its traded notional, its cost, and the book's gross and net return minus the
  control's median in the same arm, labelled realised, from one history. Tested at base cost, per arm; N is the
  number of calendar months exceeding 50% in either arm, with the per-arm counts beside it. **Exception:** no
  expected-benefit model exists here, so the requirement cannot be met by this run. It is applied fail-closed through
  the verdict order's turnover veto (§"Decision rule", step 5), over stage-B months. **This veto is the one
  performance gate in this section.** The other exception is input validation: the refusals this section names
  (`CUTOFF_INVALID`, `ME_INVALID`) stop the run like any data refusal. Everything else here is printed only.
- **Attribution** (`portfolio-construction-and-risk.md` §Attribution): SPY beta; universe effect (equal-weight
  universe minus B1); the full FF5+momentum loadings; selection (book minus equal-weight universe); FF-12 weights,
  monthly and averaged, against the reconstituted cap-weighted universe, with the 2× and "no reference weight" flags.
- **Information, per window:** the lag-1 autocorrelation of monthly active returns (Pearson, over calendar-adjacent
  pairs), and the ratio of the Newey–West standard error of the mean active return to its iid standard error. The
  autocorrelation is `undefined` with fewer than 24 pairs or a zero-variance lagged or leading vector; the ratio is
  `undefined` with fewer than 24 months, a zero iid standard error, or a Newey–West standard error that is not
  finite and positive. No
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
   - the `DeclaredTrial` row is in the imported register, and the run's spec, construction and register-policy
     hashes equal the row's (§"Registration");
   - the run's `started` and `access_recorded` ledger rows exist and the access row is committed;
   - a reference artefact carries the extended SUB.

   Stage A must replay with rows content (`rows.content_sha256`), census and every frozen input byte-identical. The manifest's provenance fields may
   differ, and stage A keeps its original pins. Each stage verifies under its own pin map, with the cross-stage
   refusal tests §"Registration" lists. Tests use fixtures.
3. **The report** (`scripts/report_3609_step2.py`): scores, book, control, references, gates, diagnostics and the
   ledger. Fixture tests only, each revert-probed. Codex checkpoint 2.
   - **The first-bar input** (§"Source rules", archive seasoning): the builder freezes `inputs/first_bars.jsonl.gz`
     for both stages, and stage A is republished with it under the replay identity in §"Registration". Premise 3's
     table is re-run on the republished artefact and must reproduce.
4. **Declaration:** the `DeclaredTrial` row and register bump, the committed `declared` ledger row, and the two
   factor-snapshot digests from the integrity-only read, merged on `main`.
5. **Declared run,** from clean `main`, in the ledger's order:
   - write `started`, then record and commit the access;
   - publish the extended SUB reference artefact;
   - build and publish the stage-B artefact, capturing and hashing its inputs under one repeatable-read snapshot;
   - write `data_frozen`;
   - run the report, refusing any pin mismatch;
   - post the verdict and ledger on #3609.

   **Capture lifecycle.** The authority is the committed ledger on `main`, never a local JSONL.
   - **Binding.** After `data_frozen`, the run stops. Its ledger rows through `data_frozen` are merged on `main`
     before any report runs. The trial's binding is the **earliest** `data_frozen` row for it in `main`'s
     first-parent history. Git orders `main`'s commits totally, so a later row can never displace it. `main`'s branch
     protection does not require an up-to-date base (measured 2026-10-07: `strict: false`), so the rule does not rely
     on merge ordering or PR checks. A fast-tier test refusing a committed ledger with more than one `data_frozen` row
     catches a second capture at its own push or CI, and the procedure rebases a capture's ledger PR on current `main`
     before merging it. If a second row lands anyway, it is invalid by the earliest-row rule, and every later report
     refuses (`CAPTURE_AMBIGUOUS`) until the trial is re-declared.
   - **The report's ledger revision.** The report runs from a clean checkout whose HEAD equals `origin/main` after a
     fetch, and requires exactly one `data_frozen` row for this trial in the committed ledger at HEAD, its own run's
     or the one it reuses (`CAPTURE_AMBIGUOUS` otherwise). That row is then the earliest, so it is the binding, and a
     row merged after the report cannot change which capture the report used. Before reading any evaluation input,
     the report repeats every freeze check at that HEAD: the `declared` payload hash, and the spec, construction and
     register-policy hashes and Python version against the row's `evidence`. It also requires the spec,
     construction and register-policy hashes and the Python version to equal the values the attempt's own `started`
     row recorded, and refuses any mismatch. So code changed between the capture
     and the report cannot evaluate under the same run id. Only then does it write `report_started` (ledger step 5)
     with that HEAD.
   - **Reuse.** A later attempt under this declaration writes `capture_reused` (the capturing run id and its
     `data_frozen` sha256) in place of `sub_published`, `stage_b_published` and `data_frozen`. It verifies both
     artefacts against that row and never publishes or rebuilds them.
   - **Abandoned captures.** A run that recorded its access but has no committed `data_frozen` is abandoned and
     classified by §"Registration" (an access). Its outputs are never reused, and the next attempt may capture
     afresh under its own run id.
   - Replacing a bound capture needs a new declaration.

## Build-time clarifications (normative)

Interpretations fixed in code while slices 3–5 were built (#3609 handoffs, 2026-10-07 and 2026-10-08), written here
so the spec states what the code does. Numbers are the handoffs' item numbers; item 16 was withdrawn, because
§"Diagnostics", Weights, already answers it.

**Paths, references and the verdict.**
- **Stage B starts from the boundary's pre-trade NAV.** The path charges formation M's trades in return month M, so
  the 2021-05 boundary formation's cost is folded into the first stage-B month, for the book, every draw and G1's
  regression input. That cost also sits in the stage-A month 2021-05; the stage-A print says so. The turnover veto
  reads stage B's formations, 2021-05..2024-07.
- **B1's 2014-09-30 close (finding 157, item 20).** Slice 4's declaration script reads it with step 0's ETF raw-price
  rule: the Intrader SPY series' last quarantine-usable bar of 2014-09 (`total_return_reader._MONTH_END_SQL`), with
  a `bar_date BETWEEN 2014-09-01 AND 2014-09-30` bound added in SQL
  (`declare_3609_step2.bounded_month_end_sql`), and the bar must fall on 2014-09-30. The close is pinned in the
  declaration's `evidence` as `b1_close_2014_09_30=<decimal>`, finite and positive, and the report reads it from
  there (`declared_b1_close`). It is a stage-A read, so no hold-out access is recorded. Measured read-only on
  2026-10-08: series 7694, one row, close 197.020004.
- **NYSE cutoffs (item 21).** Each stage freezes its own `reference_snapshot_jkp_nyse_cutoffs` to its own price
  bound. Each formation takes its cutoff from its own stage's file. A date both files publish must agree, or the run
  refuses `CUTOFF_INVALID`. A formation its stage's file lacks prints `unavailable` (§"Diagnostics", Universe).

**Terminal ledger rows.**
- **Item 18.** A `REFUSED` verdict raised during evaluation ends the run `completed`, with status `REFUSED` and its
  code. A refusal at the report's gate, before `report_started`, writes no row.
- **Items 22 and 24.** A pin mismatch in any report input (`read_verified_artefact` and `verify_capture` raise
  `PanelError`), or a stage whose formations are not its declared grid (`ReportError`), ends the run `failed`, not
  `REFUSED`. `failed` means a defect or an integrity failure, never a verdict.

**Freeze mechanics.**
- **Item 17.** The construction hash's roots are `factor_book_declaration.CONSTRUCTION_ROOTS`: the builder, the
  report loader (`scripts/report_3609_step2.py`), the assembly (`report_3609_step2_assembly.py`), the report's entry
  point (`report_3609_step2_run.py`) and the declared run's (`run_3609_step2.py`). §"Registration"'s "the report" means
  these; a loader-rooted closure would miss the verdict.
- **Item 19.** The data-capture manifest is canonical JSON at `<RESEARCH_ROOT>/factor_book_3609_step2_capture/<run
  id>.json`, created exclusively (`scripts/capture_3609_step2.py`, `CAPTURE_SCHEMA`). It restates the run's `run_id`
  and `access_id`, the SUB artefact's manifest sha256 and file hashes, and the stage-B artefact's manifest sha256,
  pin map, input hashes and published rows. A fresh capture refuses while a committed `data_frozen` row may bind the
  trial.
- **Item 23.** The observation digest's `str(value)` is Python's `str` of psycopg's `Decimal`. On the dev DB it equals
  `numeric::text` for all 5,744 rows of snapshots 39 and 40 (compared as strings, no value printed).

**Diagnostics.**
- **Item 1.** Turnover above 50%: the book's return minus the control median is read in month M + 1, which holds
  formation M's result. Formation M's cost has its own column; M + 1's net return carries formation M + 1's cost.
- **Item 2.** n_eff's "iid variance of the mean" is Σe²/n², the Newey–West estimator at lag 0, so a series with no
  autocovariance has n_eff = n. The information ratio uses the same iid error, so n_eff = n / ratio² before the cap.
- **Item 3.** A constant series is detected by equality (peak-to-peak 0); its t, IC-IR, n_eff and information figures
  are then undefined.
- **Item 4.** FF-12 weights are averaged over the formations held into a window's months, as the status weights are.
  The `2x` flag needs the book's weight strictly above twice the reference's.
- **Item 5.** Universe windows are keyed by formation: stage A is premise 3's 80 formations, stage B is 2021-05..
  2024-07 plus the 2024-08 liquidation. The operations windows are keyed by return month.
- **Item 6.** "The count" at or below the cutoff is printed for the top 1,000 and for the book's holdings.
- **Item 7.** Notional by size class is per arm at base cost.
- **Item 8.** A universe name with an unusable close also goes to the "price unavailable" cell.
- **Item 9.** A cell or industry is reported if it appears at any formation. A month in which it is empty is
  undefined, so that window's t and n_eff are undefined under the existing rules.
- **Item 10.** A sale's reporting band when its name has no close at s(M) cannot occur: `check_closes` refuses
  `PRICE_INVALID` first. The module raises `ValueError` if it ever does.
- **Item 11.** A sub-book month is holding month M + 1 and carries formation M's costs; the book's path charges them in
  month M. Sub-book windows therefore take the costs of the formations that opened their months.
- **Item 12.** V_b is `Decision.share` of the path's post-cost NAV; g_b uses `Decision.returns` in the arm; C_b and L_b
  come from `PathResult.trades`, banded at s(M) and s(2024-07).
- **Item 13.** Sub-book windows are formation-keyed: stage B is formations 2021-05..2024-07 plus the liquidation, with
  no boundary folding, and stage A is formations through 2021-04. The book's stage-A operations also charge the
  2021-05 formation, so sub-book stage-A turnovers sum to the book's except for that formation.
- **Item 14.** n_eff and the `insufficient` mark come from the base-cost net series and apply to all of that window's
  metrics. A window is uncomputable if any month is undefined, invalid or thin in the base or the gross series.
- **Item 15.** An empty window is left out (a run starting at stage B has no stage A); a window passed with no months
  refuses `ValueError`.

## Known limits

- No IR claim: ten years cannot certify a realistic edge (premise 2). A pass is a screen plus the published record.
- G2 rests on 39 months, one regime, and point estimates.
- HML stands for three value variables. Condition 3 shows the book carries HML; each member's fidelity is condition
  2's, from step 1.
- Condition 3's power rests on stage-A planning inputs (premise 6).
- Restricted estimand; the size proxy is not NYSE breakpoints; linkage under-covers dead and illiquid names.
- Survivorship is unverified 2014-09..2018, and the panel is retrospectively filtered (step 1).
- Historical eToro eligibility is unchecked.
- **Security type is unverified.** Step 1 states that commodity pools and other funds are not separable in the
  panel, and premise 3's SIC 6221 count is sometimes 1, so leveraged, inverse or pooled products may enter this
  reference book. No exclusion is claimed. Step 3 must specify and validate a classifier before its forward
  construction can claim one (§"Decision rule").
- Archive seasoning is not the IPO rule; no volatility-conditioned result exists.
- Costs: nine summer calibration dates, no stressed regime, stock bands (step 0).
- Factor and RF content is pinned at slice 4, not at step 0; equality with what step 0 read is unverified.
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
- **1–8, and the gate-design parts of 3–5:** held for the evidence-bar question until 2026-10-08, then applied under
  the settled entry (below, "Gate opened").
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

**Round 4 (26 of 32 resolved; 6 open plus 15 new, 81–95; `ckpt1_round4.txt`), all applied:**
- **39: kept as a stated exception.** No expected-benefit model exists for this book, and inventing one would be a
  treatment without a source. The verdict line carries the note and step 3 must show the benefit (finding 85 fixes
  the count's arm and scenario).
- **45, 81–83:** the effective sample is n × iid/Newey–West variance of the mean, per (signal, arm, metric, window),
  labelled an estimate within the bandwidth; undefined counts as insufficient.
- **46:** per-cost-band net contributions, cost, notional and turnover are printed as contributions to one book.
- **55:** a register-policy hash over `trial_register.py` minus its two register assignments.
- **62, 84:** seven trade categories with reconciled notional and cost; turnover follows step 0's exclusions.
- **73:** autocorrelation pairs and degenerate cases are defined.
- **86–88:** raw closes are validated before eligibility; missing cutoffs print `unavailable`; departing names
  without ME take their own class.
- **89:** ADR and share-class flags are unnecessary by step 1's rules; liquidity terciles and short-interest deciles
  are deferred, with reasons.
- **90–91:** comparator completeness refuses as `COMPARATOR_INVALID`; the refusal payload is enumerated.
- **92–93:** abandoned runs are classed by the database access log; canonical JSON is specified.
- **94–95:** the script validates normalised cutoffs, sums and an empty reference set; the table row is moved.

**Round 5 (14 of 21 resolved; 7 open plus 11 new, 96–106; `ckpt1_round5_final.txt`), all applied:**
- **39:** the unshown turnover benefit is now fail-closed: it turns a `PASS` into `FAIL` (`TURNOVER_BENEFIT_UNSHOWN`).
- **45–46, 83, 99–100:** per-band sub-books, defined by current band with entry-band costs, are scored by step 0's
  metrics and the effective-sample rule; contributions are dropped, so no additive reconciliation is claimed.
- **84:** entries exclude the initial purchase.
- **89, 96–98:** ADR, share-class and institutional-ownership flags are declared unavailable, with reasons.
- **90, 104:** every compared series must be complete with defined annualised returns, at base and stress cost.
- **101–103:** notional classes have a precedence and a liquidation class; stage-B cutoffs are validated.
- **105–106:** the Python version is enforced; abandoned runs are matched on the full access identity, and run ids
  are fresh per attempt.

**Round 6 (14 of 18 resolved; 4 open plus 9 new, 107–115; `ckpt1_round6_final.txt`), all applied:**
- **46, 99, 107–110:** sub-books get an explicit capital and cost basis (band capital before its costs, sold names'
  costs included, final liquidation in August), undefined empty months, a 5-name thin rule, and the FF5+momentum
  regression; their gaps never touch the verdict.
- **90, 114:** total loss is a valid −100% outcome; annualisation is log-based; negative factors and non-finite
  statistics refuse.
- **103:** ME sums in coverage shares refuse as `ME_INVALID`.
- **111–113:** the turnover veto is step 5 of the verdict order, stage-B months only, named as the one gating
  diagnostic, and binds step 3 unless step 3 declares a reviewed benefit method.
- **115:** outside-universe names with no valid raw close go to a "price unavailable" cell.

**Round 7 (12 of 13 resolved; 1 open plus 8 new, 116–123; `ckpt1_round7_final.txt`), all applied:**
- **99:** per-band order notional, cost and turnover over the book's pre-trade NAV.
- **116:** sub-book regressions use their window's months and lag, not G1's 119.
- **117–118:** a total loss ends the path; the book fails (`TOTAL_LOSS`), B1 or a reference refuses, a control draw
  scores −100%.
- **119:** step 3 may replace only the turnover veto, and only by a reviewed, passing benefit method.
- **120:** scores are standardised within (size cell, industry, M) before reporting cells are assigned.
- **121–122:** the sub-book return is evaluated in ratio form with an `invalid` state; uncomputable windows print
  `undefined`, computable ones below the minimum print `insufficient`.
- **123:** every consumed file is verified against its pinned manifest immediately before use.

**Round 8 (6 of 9 resolved; 3 open plus 6 new, 124–129; `ckpt1_round8_final.txt`), all applied:**
- **117, 124–126:** the stopped-path accounting for comparators is removed. A total loss is still its own outcome,
  not invalid data, and a path ending in one is complete. The book at base cost fails (`TOTAL_LOSS`); at stress cost
  only it gets the stress annotation. B1, a reference or a control draw refuses the run (`COMPARATOR_TOTAL_LOSS`):
  a long-only series reaches a zero factor only if every holding returns −100% in one month, and a refusal cannot
  pass, so no −100% control convention, stopped-draw percentile or count-matching exception is needed.
- **121:** sub-book inputs are validated (finite, V_b > 0, costs ≥ 0, g_b ≥ −1) before the ratio is evaluated.
- **123, 127:** factor and RF snapshots are verified by content: payload sha256 and a canonical observation digest,
  both pinned in the declaration. Snapshot ids 39 and 40 are row ids, not hashes.
- **128:** stage A and stage B verify against stage-specific pin maps; slice 2 tests the cross-stage refusals.
- **129:** final-liquidation notional and cost report under the s(2024-07) band, separate from the charged entry band.

**Round 9 (8 of 9 resolved; 1 open plus 6 new, 130–135; `ckpt1_round9_final.txt`), all applied:**
- **117, 130–132:** one stop rule. A total loss in any series, arm or cost stops valuation of every series at that
  month; earlier refusals take precedence and later checks are not run. The book at base cost fails
  (`TOTAL_LOSS`); anything else refuses (`TOTAL_LOSS_STOP`), a declared exception to non-gating stress cost. The
  justification is restated as a conservative policy: a window ending at zero has a defined −100% return.
- **133:** a total loss is recognised from the holdings (every position worth exactly 0, no cash); a factor ≤ 0 or
  r rounded to −1 otherwise is numerical failure (`COMPARATOR_INVALID`). Sub-book quotients must be finite.
- **134:** the two factor-snapshot digests are computed by an integrity-only read that prints no value, on
  snapshots step 0 already evaluated over stage B; the declaration PR records it.
- **135:** slice 4 commits a `declared` ledger row with the payload hash; every attempt compares against it and
  requires exactly one matching register row.

**Round 10 (5 of 7 resolved; 2 open plus 5 new, 136–140; `ckpt1_round10_final.txt`), all applied:**
- **133, 136, 137:** the economic-versus-numerical distinction is dropped. Any factor ≤ 0, or r computing to −1, in
  any series, arm or cost refuses (`WEALTH_NONPOSITIVE`), with the whole stopping month valued and every refusal in
  it collected. No `FAIL`/`TOTAL_LOSS` branch remains, so no precedence between base and stress book losses is
  needed, and B1 needs no holding-level inputs. Sub-books apply the same factor test.
- **134:** the integrity-only read is preceded by a committed `read` access record; the stage-B access rule names
  it as the one exception.
- **138:** the digests are stated as content captured at slice 4; equality with step 0's content and
  payload-to-observation correspondence are listed as unverified.
- **139:** a pass candidate computes and validates its annotation statistics before the terminal row and `PASS`.
- **140:** IC-IR is for IC only; the spread gets mean, deviation and t.

**Round 11 (all 7 carried findings resolved; 3 new, 141–143; `ckpt1_round11_final.txt`), all applied:**
- **141:** every comparison (G2, control median and ordering, annotations, percentile) uses annualised log growth;
  exp(G) − 1 is display only, so valid paths cannot tie at a rounded −100%.
- **142:** the first `data_frozen` binds the capture to the trial; later attempts verify and reuse it, abandoned
  captures are never reused, and replacement needs a new declaration.
- **143:** the diagnostics heading and text name both exceptions: the turnover veto and input refusals.

**Round 12 (all 3 carried findings resolved; 2 new, 144–145; `ckpt1_round12_final.txt`), all applied:**
- **144:** a display exp(G) − 1 that overflows prints "outside representable range" beside the finite G.
- **145:** the committed ledger on `main` binds the capture: `data_frozen` rows are merged before any report, the
  report requires exactly one for the trial (`CAPTURE_AMBIGUOUS` otherwise), and a reuse writes `capture_reused`
  in place of the three publication rows.

**Round 21 (156 resolved; no new finding; `ckpt1_round21_final.txt`).**

**Amendment after round 19 (156, found while building slice 3c-iv; reviewed in round 20):**
- **156:** archive seasoning named no frozen source, and neither artefact holds one (the daily window starts 12
  months before the first formation). The builder now freezes each admitted series' first admitted bar, and stage A
  is republished with it under a replay identity (rows, census and existing inputs unchanged); the declaration pins
  the republished manifest.
- **Round 20 on 156 (`ckpt1_round20_final.txt`, run before the spec branch was rebased onto `main`, so it read an
  older builder):** its snapshot finding cited the pre-slice-2 autocommit path; `_publish_artefact` on `main` reads
  every input in one repeatable-read snapshot. Applied: the file's shape, omission and refusal rules; replay
  identity as rows content (`rows.content_sha256`), here and in slice 2; the manifest fields that may and may not
  differ.

**Round 19 (153–155 resolved; no new finding; `ckpt1_round19_final.txt`).** Only the evidence-bar findings remain open.

**Round 18 (152 resolved; 3 new, 153–155; `ckpt1_round18_final.txt`), all applied:**
- **153:** the two HEADs are provenance either way: a capturing attempt's differ, a reusing attempt's may coincide.
- **154:** the report's comparison with `started` names its four values explicitly; the payload hash is checked
  against `declared` only.
- **155:** the opening status matches this log.

**Round 17 (151 resolved; 1 new, 152; `ckpt1_round17_final.txt`), applied:**
- **152:** HEAD is provenance, never re-compared. The capture's ledger merges before the report, so a capturing
  attempt's report HEAD always differs from its `started` HEAD. The report re-compares only the spec, construction
  and register-policy hashes and the Python version.

**Round 16 (150 resolved; 1 new, 151; `ckpt1_round16_final.txt`), applied:**
- **151:** `started`'s field list now records the attempt's HEAD (provenance) and every value the report re-compares:
  the spec, construction and register-policy hashes, and the Python version as `"major.minor"`.

**Round 15 (145, 148 and 149 resolved; 1 new, 150; `ckpt1_round15_final.txt`), applied:**
- **150:** the report repeats every freeze check at its own HEAD and requires agreement with both the declaration
  and the attempt's `started` row before reading evaluation inputs.

**Round 14 (146 and 147 resolved; 145 carried; 2 new, 148–149; `ckpt1_round14_final.txt`), all applied:**
- **145:** PR checks can be stale (branch protection is not strict), so the binding no longer relies on merge
  order. It is the earliest `data_frozen` in `main`'s first-parent history; a later row cannot displace it and
  makes later reports refuse.
- **148:** the report's HEAD goes in a new `report_started` row; `started` keeps the attempt's own HEAD.
- **149:** each consumed file is read once, hashed and parsed from the same bytes (or from a checked private copy),
  with no reopen after the check. Slice 3 brings `verify_artefact` and the measurement script under this rule.

**Round 13 (144 resolved; 145 carried; 2 new, 146–147; `ckpt1_round13_final.txt`), all applied:**
- **145:** a uniqueness check at the report's HEAD did not stop a second capture merging later. Uniqueness is now
  enforced on acceptance: merges to `main` are serialised and a fast-tier test refuses a committed ledger with two
  `data_frozen` rows for the trial, so a second binding can never merge. The report runs at HEAD = `origin/main`
  and records it.
- **146:** a negative factor is `WEALTH_NONPOSITIVE` only; `COMPARATOR_INVALID` is for non-finite comparison
  statistics.
- **147:** each factor snapshot's row, payload and observations are read in one repeatable-read transaction, and the
  run evaluates only that verified collection, with no later factor or RF query.

**Gate opened, 2026-10-08; round 2 findings 1–8 applied** under `docs/settled-decisions.md` 2026-10-08:
- **1:** the claim is condition 3, powered in premise 6. The register row stays non-claiming in #2599's sense,
  because its claiming form is an IR design (§"Registration").
- **2:** G2 is condition 4: point estimates reported as a screen, zero margins with their basis, and the selection
  statement (§"Decision rule").
- **3:** G1 moved to stage B. Stage A is development only (§"Step 1's inheritance clause").
- **4:** power computed from stage-A planning inputs. The first draft's composite could not be powered, so the book is
  the value family alone (premise 6; code PR #3718: `FAMILIES`, `G1_LOADINGS`, `G2_MARGINS`).
- **5:** members' fidelity is condition 2 (step 1 against JKP's series per characteristic). Condition 3 claims only the
  HML exposure.
- **6:** §"Adoption rationale" gives each series' sample relation, effect, uncertainty and implementation differences,
  measured from the published series.
- **7:** premise 2 is stated as conditional on the skill's generic prior.
- **8:** the search accounting is connected to the tests (§"Registration").
- The build-time items are in §"Build-time clarifications".

**Round 22 (15 findings; `ckpt1_round22_final.txt`), all applied; checkpoint 2 on the code raised one, the same as
4:**
- **1:** first answered by the entry naming this corpus; round 23 held that insufficient (below).
- **2:** the configuration reading is stated, and step 3's construction on the same stage B joins the rule.
- **3–5:** premise 6 declares the power criterion (conjunction bound ≥ 0.80 at the point estimates), labels the
  lower bound an unadjusted sensitivity, adds the break-even t, and lists the approximation's assumptions.
- **6:** the selection is described as using return-derived exposure statistics, justified by sample governance.
- **7:** the freeze rule names the two registered pre-declaration exercises.
- **8–9:** the plan is a pinned manifest with input digests, and the planner validates its inputs and refuses an
  unpowered plan.
- **10–11:** G1's factor alignment and the declared-path precondition name the upstream checks that enforce them.
- **12:** verdict order step 1 maps each phase to its terminal row.
- **13:** the turnover veto is formation-keyed and excludes the final liquidation, with the reason.
- **14:** weak coefficients are described as such, not as unidentified.
- **15:** the significance convention is stated.

**Round 23 (`ckpt1_round23_final.txt`): 13 of round 22's findings resolved; 1, 8 and five new findings applied:**
- **22.1:** premise 5 now separates the skill's operational requirement (universe from the archive with delisting
  returns handled: met) from certification against a second source (partly met from 2019, absent before). The spec
  claims no certified full-population stage B and no waiver; the gap is labelled.
- **22.8:** the plan manifest adds the script's import-closure hash, the FF12 pin and the Python and numpy versions.
- **New 1–2:** the planner requires each prior series to be one observation a month, consecutive, through 2014-09,
  checks every unit against `FACTOR_UNIT`, and digests the units.
- **New 3:** factor-snapshot failures are integrity failures (`failed`); `G1_REFUSED` covers the evaluation window.
- **New 4:** `read_factors` refuses a value that overflows its float.
- **New 5:** premise 6's table is generated from the plan file.

**Round 24 (`ckpt1_round24_final.txt`):** new 2–5 of round 23 resolved; three items and one new finding applied:
- **22.1:** following Codex's fix, a `PASS` is the screen's computed result, not demo eligibility. Step 3's declaration
  must show the entry's data requirements hold, survivorship and full population included, or cite a recorded policy
  decision (premise 5, §"Question", §"Decision rule").
- **22.8 and the new finding:** the plan manifest was stale because the planner was edited after the plan was
  written. It is regenerated from the final planner, and `tests/test_3609_step2_plan_pin.py` now requires the
  committed plan's sha256 to equal register r25's pin and its `script_sha256` to equal the committed planner's.
- **23 new 1:** each prior series must equal its frozen monthly grid, from a first month frozen per series to 2014-09.

**Round 25 (`ckpt1_round25_final.txt`): all four round-24 items resolved; no new findings.** Checkpoint 1 has
converged on the 2026-10-08 revision.
