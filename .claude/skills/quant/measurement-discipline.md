# quant/measurement-discipline

## When to use

**Before reporting ANY quantitative finding about market data, and before
believing one you are handed.** Read it the moment you are about to write "this
signal produces X%", "the t-statistic is", "this beats buy-and-hold", or
"the backtest shows".

This file is the record of measurements **this project got wrong**, each with the
correction that caught it. Every entry cost real time. None of them were caught
by a test.

> **The rule: a measurement is not a finding until you have tried to kill it.**
> The failure mode is never "we could not compute it" — it is "we computed
> something that looked convincing and was not the thing we meant."

⚠ Companion files: `quant/strategy-evidence.md` (what to build),
`quant/data-capability.md` (what we can measure at all),
`quant/trade-lifecycle.md` (how to trade it),
`data-sources/market-structure.md` (indicator formulations).

---

## 1. ⚠⚠ The eight ways we have actually fooled ourselves

### 1.1 Pooling correlated observations as independent — the most expensive one

**What happened (2026-08-08).** Measured return autocorrelation across 7,709
series by pooling every (series, block) pair. Got 1-year momentum at
**`+0.2479`, t `+18.4`** in the `>=$100` band and wrote it into a skill as
giving the size screen *"a measured basis rather than a borrowed one"*.

**The correction.** Series in the same year move together. Recomputing the
statistic **within each calendar year** and treating the **years** as the sample
— `n` falls from millions of pairs to 22-65 years:

```text
                     pooled              year-clustered
1y  >=$100        +0.2479  t +18.4      +0.0511  t +1.6
6mo >=$100        +0.1262  (all bands)  +0.0039  t +0.1   <- ERASED
```

⚠⚠ **The 6-month momentum finding did not weaken. It vanished.** And the
headline 1-year result does not clear `t` of 2, let alone 3.

**Prior form:** the same error took one effect from **t 50.3 → 17.7 → 5.1** on
this project, across day-clustering and non-overlap corrections.

> **Rule: if observations share a time period, they are not independent. Compute
> the statistic within the period, then treat periods as the sample.** A
> t-statistic in the hundreds is not strong evidence — it is a missing
> correction.

### 1.2 Overlapping windows

A k-day return sampled daily shares `k-1` days with its neighbour. It inflates
`n` by roughly `k` and the t-statistic by roughly `sqrt(k)`.

> **Rule: non-overlapping windows only.** Cut the series into disjoint blocks.

### 1.3 The loaded query — searching for the answer you expect

**What happened (2026-08-09).** Asked to assess day-trading viability, searched
for *failure rates*. Found Barber/Odean (fewer than 1% predictably profitable)
and Chague et al. (97% of persisters lose) and reported it as the answer.

⚠⚠ **The same Barber/Lee/Liu/Odean paper also reports that the top 500 traders
ranked by PRIOR-YEAR performance earned 37.9 bps/day after fees the following
year.** Persistence. Skill that is real and identifiable ex ante. **I had the
source open and read one half of it.**

> **Rule: before searching, write down what evidence would change your mind, and
> search for THAT.** A query shaped like your expectation returns your
> expectation. ⚠ And when a paper is cited for one result, check what else is in
> it.

### 1.4 A favourable result deserves the hardest look

**What happened.** A Roll (1984) test of whether our short-horizon reversal is
just bid-ask bounce came back **3-12× the modelled spread** — i.e. real reversal
on top of the bounce, which would have rescued the one family our data still
supported.

Two confounds, both flattering, one of them mine:

- ⚠⚠ **Selection bias I built in.** Roll is undefined for positive covariance and
  **26-30% of series had `cov >= 0`**. The script averaged `sqrt(-cov)` over the
  negative subset only. **If the true covariance were zero with noise, that
  manufactures a positive spread out of nothing.** The discard rate was the tell.
- **Era mismatch.** Cost bands are calibrated on modern spreads; **20.3% of
  corpus bars predate decimalisation**, when a 1/8 tick on a \$10 stock was 1.25%.

> **Rule: when a result arrives in the direction that would justify what you
> want to do, that is the moment to audit your own code.** Recorded as
> INCONCLUSIVE rather than as support.

### 1.5 Descriptive claims are measurements too

*"Most exchange filings attach a stub"* — written from **one** filing. The
defect is the population of one, regardless of whether the sentence is true.

> **Rule: if a sentence contains "most", "usually", "every", "always" or
> "rarely" about source data, it is a measurement. Run it or delete the
> quantifier.**

---

### 1.6 ⚠⚠ The univariate instrument — testing one condition and reporting the null

**This is the biggest methodological error of the 2026-08-09 pass, and it produced
three false nulls in a row.**

Gap fade, 12-month momentum, one-day loser reversal: each tested as ONE condition,
averaged across the whole cross-section, each returning nothing after costs and
clustering, each written up as "the effect is dead".

⚠⚠ **That conclusion does not follow.** A marginal test estimates `E[r | A]`. A
trader claiming a setup is claiming `E[r | A,B,C,D,E]`. Those can have **opposite
signs**. If an edge exists only where several conditions co-occur, then measuring
each alone returns ~zero *by construction* — the firings where the others were
absent swamp the few where they were present. The instrument cannot detect the
thing, and the null it produces is about the instrument.

⚠ Three failed univariate tests are **one** piece of evidence about univariate
tests, not three about the market. Do not let them accumulate into a verdict.

**Evidence:** Gu, Kelly & Xiu (NBER w25398) — ML asset-pricing gains come materially
from **nonlinear predictor interactions**. Lo, Mamaysky & Wang (JF 2000) — technical
patterns must be made algorithmic and tested as **conditional distributions**.

**Our own proof, and it was sitting in the output:** buying a one-day −5% drop is
−437 bps at breadth 2-5 and **+91 bps at breadth 101+**. Same signal, opposite sign,
on a conditioning variable that was simply not being measured. The tell was
day-clustered and year-clustered `t` disagreeing **in sign** (−4.49 vs +1.81) — when
two clusterings of the same data disagree that way, the outcome covaries with
something the test is averaging over. **That disagreement is a finding, not a
nuisance.**

**Test for the conjunction with ONE degree of freedom, not 2^N.** Pre-specify the
conditions, then bucket the outcome by **how many are simultaneously true** and look
for a gradient. Searching all subsets on 6,700 stocks over six years manufactures a
winner every time; a monotonic gradient across alignment counts does not.

⚠ And know what a flat gradient means. After correcting the #2437 next-session
market-return leak, ours is noisy rather than monotonic: at 10 days the `vs base`
column runs −35, −28, −39, −17, −10, approximately 0, +5, +2, −6, +14, −26,
+15 bps as alignment rises from 0 to 11. The sparse 9/11 cells do not rescue the
lack of a gradient. **Equal-count confluence did not identify a buy signal or a
stable filter.** Reproduce with
`PYTHONPATH=. uv run python scripts/verify_2437_confluence.py`.

### 1.7 ⚠⚠ A level type is only real if it beats its own placebo

Before claiming price behaves differently at support, Fibonacci, pivots, round
numbers or an anchored VWAP: **construct an arbitrary level of the same shape and
measure both.** Same tolerance, same swing, same ATR unit — only the level itself
differs. Without the placebo arm, a hit rate measures the tolerance and the fact
that price is often mid-range, not the level.

**Measured 2026-08-09 (`scripts/verify_2437_confluence.py`, 2020+, 2.35M stock-days,
day-clustered):**

```text
      8 near support    16.78 (t 2.46)   40.47 (t 4.33)
     P2 fake support    18.84 (t 2.82)   42.26 (t 4.65)
    9 fib 38/50/62      14.58 (t 2.16)   38.22 (t 4.09)
P1 fake fib 29/44/71    18.82 (t 2.79)   44.45 (t 4.76)
```

⚠⚠ **Both placebos BEAT the real thing, and both real levels sit below the
unconditional baseline (44.28 bps).** Arbitrary fractions 29/44/71 outperform
38.2/50/61.8; a displaced pseudo-level outperforms an actual pivot low. This
falsifies unique unconditional directional value for THESE constructions. It does
not falsify every causal support/resistance definition or their possible value for
path geometry, invalidation, or exits.

⚠ The placebo comparison is evidence against the specified level rules, not a
universal claim about chart structure. A new construction is a new registered
trial and still needs a matched firing-rate placebo.

### 1.8 ⚠⚠ Era-split before you get excited, not as a robustness check

A long sample averages over market structures that no longer exist. Cut at real
structural events — **2001 decimalisation, 2007 Reg NMS, 2019 zero commissions** —
never arbitrary dates.

The gap-down fade measured **+156 bps at t 5.73 over 1962-2026**, and **+11.8 bps at
t 1.39 over 2020-2026**. It paid 100-180 bps a year from 2001 to 2019 and has paid
nothing since. ⚠ Pre-2001 the sign is **negative**, so the effect existed only inside
one 19-year regime, opened and closed by structural change.

⚠⚠ **The binding consequence: for anything short-horizon the usable sample is 2020+,
about six years.** That is a much weaker statistical position than the corpus size
suggests, and it is the honest one. A 64-year `t 5.73` described a market we cannot
trade in.

## 2. Traps specific to price data

### 2.1 The shared-print trap

**Never sort on a variable that terminates at price P and measure an outcome
that originates at price P.** Any error in that single print enters the sort
negatively and the outcome positively.

⚠ **Monotonicity across deciles is the signature of this artefact, not evidence
against it.**

### 2.2 The unfillable window

A measured **+43.9 bps** overnight effect lived entirely inside
`close(t) -> open(t+1)` — consumed before any fill could exist. Re-measured on
what a next-open fill actually earns, **the tradable signal was the opposite
sign.**

> **Rule: measure every outcome from the first price you could actually transact
> at.**

### 2.3 Penny-stock domination

A corpus-wide equal-weighted mean is a **micro-cap mean** — a series contributes
in proportion to its bar count, not its size or tradability. Intraday expectancy
measured negative corpus-wide; by band it is negative **only below \$5**.

> **Rule: stratify by price band and dollar volume before believing any pooled
> mean.** And prefer a per-day cross-sectional mean, then a time-series mean of
> those.

### 2.4 Adjustment-distorted levels

Back-adjusted closes are meaningless as **levels** (#2400) — serial
reverse-splitters inflate toward `3e17`. **Returns are safe; levels are not.**

> **Rule: any analysis bucketing by price level must first exclude or
> reconstruct adjustment-distorted series, and count what it excluded.**

### 2.5 A near-zero pooled result is not evidence of no effect

A pooled **−22.0 bps (t −1.54)** was two opposite regimes cancelling:

```text
$20-50     +40.0   t  2.66
$50-150   -121.8   t -7.12
>$150     -169.4   t -7.01
```

---

## 3. The multiple-testing budget

Every hypothesis tested **raises the bar for every strategy we already own.**
Measured with our own trial-Sharpe variance:

```text
independent trials    threshold SR_0    multiple of today
              10          0.083344            1.00x
             250          0.151499            1.82x
           1,000          0.173786            2.09x
         100,000          0.234420            2.81x
```

**Harvey, Liu & Zhu** put the honest hurdle at **t > 3.0**, where **only 9 of 313
factors survive**. ⚠ **Sullivan, Timmermann & White** is the cautionary tale:
Brock/Lakonishok/LeBaron's 26 technical rules looked significant until the test
accounted for the universe those 26 were *selected from* — after which **no
profitable simple rule remained** on the DJIA, S&P 500 or S&P futures.

> **Rule: declare the hypothesis and its acceptance criterion to
> `trial_register.py` BEFORE running the test.** A search that records its trials
> afterwards is indistinguishable from one that reports only its winners.

⚠ The register is a documented **floor** — under-counting `M` raises the DSR, so
every stored value is an upper bound on the honest one.

### 3.1 "Test every combination" is not exhaustive subset search

An operator asking to validate every strategy is asking for **complete coverage
of the declared catalogue**, not permission to enumerate every subset of every
indicator and retain the winner. With `N` binary conditions there are `2^N`
subsets before thresholds, windows, directions, stops and universes multiply the
search again. The winning backtest from that search is a selection artefact until
the entire search count is charged and untouched evidence confirms it.

> **Rule: CI must prove every implementation is registered and every registered
> version receives the same test matrix. Statistical combinations must be
> bounded, pre-registered interaction hypotheses with an economic rationale.**

Exhaustive tests belong on finite software state spaces — outcome classes,
ownership transitions, missing-field combinations and closed vocabularies — not
on an unbounded strategy powerset. Failed strategies and rejected combinations
remain visible in the result catalogue; hiding them undercounts the trial budget.

⚠ Allocating more capital to whichever strategy just performed best is itself a
new timing strategy. Register and backtest that allocation rule, or keep capital
changes as explicit operator decisions. Do not smuggle performance chasing into
a "picker" and then attribute its result to the underlying strategies.

---

## 4. What a finding must carry before it is reported

1. **Non-overlapping windows.**
2. **Clustered inference** — statistic within period, periods as the sample.
3. **Stratification** by price band and liquidity, with the strata shown.
4. **A fillable-window check** — measured from a transactable price.
5. **The rejected count**, not only the admitted count.
6. **A reproduce command** next to every number.
7. ⚠ **An attempt to kill it** — the null model, the artefact explanation, the
   confound. State what you tried and whether it survived.

> **A number without item 7 is a hypothesis wearing a decimal point.**

---

## 5. An autocorrelation sign is not a tradable signal

A negative coefficient says the series mean-reverts *on average*. It does not say
the reversion is **larger than the spread**, **reachable at a fill you could
get**, or **present when you need it**.

⚠ §1.4 is the proof: the identical coefficient is what a bid-ask bounce produces
with no economic content whatsoever.

> **Treat a statistical regularity as a hypothesis about a mechanism, never as a
> strategy.**
