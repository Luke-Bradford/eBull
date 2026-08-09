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

## 1. ⚠⚠ The five ways we have actually fooled ourselves

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
