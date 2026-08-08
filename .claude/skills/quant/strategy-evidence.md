# quant/strategy-evidence

## When to use

**MANDATORY before proposing, speccing or defending any trading strategy.** Read
it the moment a ticket says "new strategy", "signal", "edge", "alpha", "factor",
"backtest looks good", or proposes tuning an existing strategy.

It exists because the replication literature has already tested most of what we
would think of, and it says most of it does not work. Speccing without that
prior is how a session spends a week on a family the evidence killed in 2016.

> **The rule this file enforces:** a strategy proposal cites either (a) the
> evidence that its FAMILY survives, or (b) the mechanism and the measurement
> that will test it. **"The backtest looks good" is neither.**

⚠ Companion to `data-sources/market-structure.md`, which owns *indicator
formulations*. This file owns *whether a strategy family is worth building*.

---

## 1. ⚠⚠ The four filters, and our own three strategies scored against them

Each filter is a published result. The scores are **our own stored numbers**,
recomputed — never quoted from here without re-running.

| filter | source | s1 time-series mom | s2 cross-sectional mom | s3 mean-reversion |
| --- | --- | --- | --- | --- |
| **turnover < 50%/month** | Novy-Marx & Velikov | ✗ **600%/mo (12× over)** | ✓ **35%/mo INSIDE** | ✗ 333%/mo (6.7× over) |
| **family survives costs** | Frazzini/Israel/Moskowitz | ✓ momentum most scalable | ✓ momentum most scalable | ✗ **reversal is the MOST constrained** |
| **responds to vol scaling** | Cederburg et al. | ✓ momentum is the exception that works | ✓ same | ✗ not in the working subset |
| **construction not microcap-inflated** | Hou/Xue/Zhang | ⚠ indirect (equal-weight pooling) | ⚠ **direct hit** (cross-sectional sort, `MIN_CLOSE=1.0`) | ⚠ indirect |

**Our measured result: s2 is the only one of the three that beats buy-and-hold.**

⚠⚠ **BUT SEE §2.8 BEFORE LEANING ON THIS TABLE.** Under year-clustered inference
on our own corpus, **momentum is not statistically established at any horizon**,
while short-horizon reversal is robust in every price band. The literature ranks
momentum above reversal; our data does not reproduce that. The two are
reconcilable — a real effect you cannot trade is precisely what §2.5 says
reversal is — but **the filters below are the LITERATURE's ranking, not ours**,
and the four-arm test of §7 is what adjudicates.

⚠⚠ **The literature predicted that, ex ante, on four independent axes.** That is
the strongest evidence in this file — not that any single paper is right, but
that four unrelated results converge on the same ranking our own backtest
produced. Treat the framework as load-bearing.

Reproduce the turnover column:

```sql
SELECT strategy_id, turnover_annualised, turnover_annualised / 12 * 100 AS pct_per_month
FROM strategy_results_store
WHERE benchmark_rule = 'equal_weight_buy_and_hold_v1' AND ambiguity_arm = 'best_case'
ORDER BY turnover_annualised;
```

---

## 2. The results that should change a design decision

### 2.1 Turnover is the first-order filter — Novy-Marx & Velikov, *RFS* 29(1) (2016)

> *"Most anomalies with less than 50% turnover per month generate significant net
> spreads when designed to mitigate transaction costs; few with higher turnover
> do."*

Execution costs 20-57 bps for mid-turnover anomalies. Size, value and
profitability have the greatest capacity for new capital.

⚠⚠ **The mitigation is a design pattern we do not have: a BUY/HOLD SPREAD —
"more stringent requirements for establishing positions than for maintaining
them."** Named by the paper as *the most effective* cost-mitigation technique. It
is a small change to any strategy's exit rule and it directly attacks the filter
we fail worst on.

> **Check turnover BEFORE anything else.** It is one column, it is already
> stored, and it disqualifies faster than any backtest.

### 2.2 Most anomalies do not replicate — Hou, Xue & Zhang, *RFS* (2020)

452 anomalies. With microcaps mitigated via **NYSE breakpoints and value
weighting**, **65%** fail |t| > 1.96; **82%** fail at 2.78. *"Anomalies in
microcaps are more apparent than real."* Replicated ones have *"much smaller"*
magnitudes than published.

⚠ Ours: equal weight, no value weighting, **24% of the universe below $5**.
Directly applicable to cross-sectional sorts (s2); reaches s1/s3 as small-stock
microstructure bias via equal-weight pooling.

### 2.3 The significance bar is t > 3.0 — Harvey, Liu & Zhu, *RFS* (2016)

316 factors across 313 articles; after multiple-testing correction the hurdle is
**t > 3.0**, and **only 9 of 313 survive**.

⚠ The debate is live — Chen (*"Most claimed statistical findings … are likely
true"*) argues the other way. We adopt the strict side on asymmetry of
consequences, not on certainty.

### 2.4 Volatility scaling — Moreira & Muir (2017) vs Cederburg et al. (2020)

Moreira & Muir, *JF* 72(4): scale by inverse of last month's realised variance →
large alphas across eight factors, because *"changes in volatility are not offset
by proportional changes in expected returns."*

⚠⚠ Cederburg, O'Doherty, Wang & Yan, *JFE* 138(1), **103 strategies**: the
out-of-sample versions *"generally earn lower certainty equivalent returns and
Sharpe ratios"* than unmanaged — the spanning alphas are *"not implementable in
real time"*. **Exception: it does work for momentum, profitability and BAB.**

> **Pre-registered prediction for our four-arm test:** `inverse_vol_v1` helps s1
> and s2, not s3. ⚠ If it helps s3 most, that is a reason for suspicion, not
> celebration.

### 2.5 Real costs are lower than academia thinks — except for reversal

Frazzini, Israel & Moskowitz, ~**\$1 trillion** of live trading data, 19 markets,
1998-2011: actual costs *"less than a tenth as large as previous studies
suggest"*; capacity an order of magnitude larger; value and momentum more
scalable than size; ⚠ **"short-term reversals are the most constrained by
trading costs."**

⚠ Their costs are low because they execute like an institution. **Ours are
worse**, so every constraint here binds harder, not softer.

### 2.6 Published edges decay — McLean & Pontiff, *JF* (2016)

**26% lower out-of-sample, 58% lower post-publication**, ~32 points attributable
to publication-informed trading. ⚠ A measured average, not a universal law.

### 2.7 The gains are in interactions — Gu, Kelly & Xiu, *RFS* 33(5) (2020)

Trees and neural nets best; NN portfolio OOS Sharpe **0.77 vs 0.51**
buy-and-hold; long-short decile **1.35 VW / 1.45 EW**. Gains come from
*"nonlinear predictor interactions"*, and the dominant signals across all
methods are **momentum, liquidity and volatility**.

> Read as: **conditioning beats a better single signal**, and the three signals
> that matter are ones we already hold. ⚠ Not a licence to reach for ML — that
> multiplies the trial count before the measurement is fixed.

---

## 2.8 ⚠⚠ The horizon term structure — MEASURED HERE, and it is NOT universal

The literature's organising fact: autocorrelation is negative at days, positive
at 3-12 months, negative again at 3-5 years (Jegadeesh 1990; Jegadeesh & Titman
1993; De Bondt & Thaler 1985).

⚠⚠ **This file asserted that curve from the literature. It was then measured on
our own corpus and it is only PARTLY true — the sign at long horizons depends on
price band.** Full population, 7,709 series, non-overlapping windows,
`adj_close`, adjustment-distorted series excluded
(`scripts/verify_2437_autocorrelation_term_structure.py`, 2026-08-08):

```text
horizon        <$5        $5-20      $20-100      >=$100
   1d       -0.1181     -0.1464     -0.0578     -0.0876     reversal everywhere
   5d       -0.1023     -0.0723     -0.0537     -0.0583     reversal everywhere
   1mo      -0.0420     -0.0327     -0.0274     -0.0024     reversal everywhere
   3mo      -0.0171     -0.0033     -0.0094     +0.0703     flips
   6mo      +0.0207     +0.0214     +0.0375     +0.1262     MOMENTUM everywhere
   1y       -0.0546     -0.0364     +0.0177     +0.2479     SPLITS BY PRICE
   3y       -0.1598     -0.1063     -0.0074     +0.1838     SPLITS BY PRICE
```

**What survived:** short-horizon reversal (1d-1mo) in **every** band, and
6-month momentum in **every** band. Those two are solid.

**What did not:**

1. ⚠⚠ **Momentum at 1 year exists ONLY in the higher price bands.** `+0.2479` at
   `>=$100` against **−0.0546** at `<$5` — not weaker, *opposite sign*. In cheap
   stocks there is **no momentum regime at any horizon**; they are reversal
   throughout.
2. ⚠ **Long-horizon reversal is not universal either.** At 3 years `>=$100`
   shows **+0.1838 continuation**, the opposite of De Bondt & Thaler.

> **This is an INDEPENDENT route to the same conclusion as §2.2**, and that is
> what makes it load-bearing. Hou/Xue/Zhang say microcap results are artefacts of
> construction. This says the *underlying return process itself differs by price
> band*. Two unrelated arguments, one instruction: **momentum strategies must not
> be run across the whole universe.** s1 and s2 currently are, and 24% of that
> universe sits in the band where momentum has the wrong sign.

### ⚠⚠ AND THEN THE CLUSTERED RUN KILLED HALF OF IT

The table above pools every (series, block) pair as independent. Series in the
same year move together, so those `t` values are fiction. Re-run computing the
correlation **within each calendar year** and treating the **years** as the
sample — `n` becomes 22-65 years instead of millions of pairs:

```text
horizon        <$5           $5-20         $20-100        >=$100
   1d      -0.0812 t -7.3  -0.0689 t -5.3  -0.0252 t -3.0  -0.1018 t -7.9
   5d      -0.0747 t -7.9  -0.0664 t-11.0  -0.0534 t -8.9  -0.0893 t -8.0
   1mo     -0.0330 t -2.3  -0.0362 t -4.8  -0.0334 t -4.6  -0.0345 t -2.9
   3mo     -0.0764 t -4.8  -0.0302 t -2.1  -0.0262 t -1.6  -0.0218 t -1.2
   6mo     -0.0403 t -2.1  +0.0025 t +0.1  -0.0196 t -0.9  +0.0039 t +0.1
   1y      -0.0636 t -2.8  -0.0184 t -0.9  +0.0183 t +0.8  +0.0511 t +1.6
   3y      -0.1283 t -4.2  -0.0642 t -2.3  +0.0030 t +0.1  +0.1070 t +1.9
```

**11 of 28 cells clear |t| >= 3.0** (Harvey/Liu/Zhu's hurdle applied to our own
measurement), and they are almost all the **short** horizons.

⚠⚠ **WHAT DIED:**

1. **6-month momentum, which the pooled run showed in EVERY band, is gone.**
   `+0.1262` at `>=$100` became **`+0.0039`, t `+0.1`**. Not weakened —
   **erased**. It was an artefact of counting correlated series as independent.
2. **The 1-year momentum split — the headline finding — does not survive.**
   `+0.2479 (t +18.4)` became **`+0.0511`, t `+1.6`**. It does not clear 3.0,
   or even 2.0. ⚠ **This file previously claimed that result gave the size
   screen "a measured basis rather than a borrowed one". IT DOES NOT.** The
   direction persists (`+0.0511` vs `-0.0636` at `<$5`) but neither end is
   significant, and the size-screen argument falls back to §2.2's literature.
3. **3-year continuation in expensive names** (`+0.1070`, t `+1.9`) also fails.

**WHAT SURVIVED, robustly and in every band: short-horizon reversal.** 1-day and
5-day at `t -5` to `-11`, 1-month in three of four bands. That is the one part of
the term structure our own data establishes.

⚠⚠ **This inverts the ranking the literature gave us.** §2.1/§2.5 say momentum is
the scalable survivor and short-term reversal is the most cost-constrained.
**Our data says reversal is the robust signal and momentum is not established
here at all.** Both can be true at once — a real effect you cannot trade
profitably is exactly Frazzini/Israel/Moskowitz's point about reversal — and
momentum's absence may be a property of a microcap-heavy universe where the
`>=$100` band has the fewest series. ⚠ **But it means s1 and s2 are momentum
strategies running on a corpus where momentum is not statistically present, and
that is a finding about our universe, not about momentum.**

### The rest of what NOT to take from this

- **The 1-day row is biased negative by construction.** Consecutive
  non-overlapping blocks share a boundary print, so one bad close enters block A
  positively and block B negatively. It is the least trustworthy row even
  clustered.
- **The band cut uses median adjusted close, a LEVEL** — #2400 says adjusted
  levels are distorted. Extreme-span series are excluded and counted, but the
  assignment is imperfect. A market-cap cut is better and is what §7's test uses.
- **3-year n is 22-44 years**, which is thin for the horizon.
- ⚠ **Year-clustering is itself an approximation.** It handles cross-sectional
  dependence within a year; it does not handle dependence *across* adjacent
  years, and for a 3-year horizon the blocks straddle year boundaries anyway.

## 2.8a ⚠⚠ The size/horizon collision — a real effect we probably cannot harvest

§2.8 named the 3-5 year end as our biggest gap: lowest turnover, most likely to
survive the cost filter, and the horizon where XBRL fundamentals are the right
data. Then the measurement complicated it, and the complication is worth
recording rather than resolving by choosing a convenient paper.

**Three independent sources agree on the same fact:**

1. **Our own measurement** — 3-year autocorrelation is `-0.1598` at `<$5`,
   `-0.1063` at `$5-20`, `-0.0074` at `$20-100` and `+0.1838` at `>=$100`. The
   reversal **fades monotonically with price and reverses sign at the top.**
2. **Fama-French and the size-value literature** — value effects are
   concentrated in smaller companies and weaker among large caps.
3. The two arrived at it by completely different routes.

**So long-horizon reversal is real and it lives in small, cheap names.**

⚠⚠ **And that is exactly the population everything else in this file says to
avoid:**

| the same population is where… | source |
| --- | --- |
| anomalies are *"more apparent than real"* | Hou/Xue/Zhang §2.2 |
| trading costs bite hardest and capacity is smallest | Novy-Marx/Velikov §2.1 |
| **size is the LEAST scalable** of the surviving anomalies | Frazzini/Israel/Moskowitz §2.5 |
| our own execution is worst (retail, ~50.9 bps flat) | §2.5's caveat |

> **Working conclusion: the long-horizon reversal premium is probably REAL and
> probably UNHARVESTABLE BY US.** Not because the effect is fake — three sources
> say it is not — but because it is concentrated precisely where our costs,
> our construction bias and our capacity all fail at once.

⚠ **The obvious escape does not appear to exist.** One would hope for a middle
band where the effect survives and costs are tolerable. The measurement says
otherwise: by `$20-100` the 3-year autocorrelation is `-0.0074`, i.e. gone. The
effect does not fade gently into a tradable zone — it is concentrated in the
band we cannot trade.

**This is recorded so a future session does not spend months rediscovering it.**
⚠ It is a *working* conclusion on one measurement with small n at the 3-year
horizon (1,256-10,846 pairs per band) and unclustered inference. **Overturning it
requires a measurement, not an argument** — specifically, a cost-aware backtest
of a long-horizon reversal strategy restricted to the `$5-20` band, where the
effect is still `-0.1063` and the costs are merely bad rather than prohibitive.

## 2.8b ⚠⚠ Momentum crashes — forecastable, and the state is COMPUTABLE HERE

**Daniel & Moskowitz, "Momentum Crashes", *JFE* (2016).** Momentum carries huge
tail risk: *"short but persistent periods of highly negative returns"*. The
crashes are **partly forecastable** — they occur in **"panic" states, following
market declines and when volatility is high, contemporaneous with market
rebounds**.

⚠⚠ **14 of the 15 worst momentum returns occurred when the past two-year market
return was NEGATIVE and the contemporaneous market return was POSITIVE.** A
dynamic strategy scaling exposure on forecasts of momentum's mean and variance
*"approximately doubles the alpha and Sharpe ratio"* of static momentum.

**VERIFIED ON OUR OWN DATA, 2026-08-08** — the state is computable from SPY
(series 7694) with nothing new ingested:

```text
SPY monthly observations with a 2-year lookback   357   (1995-01 .. 2024-09)
PANIC states (2y return < 0 AND 1m return > 0)     28   (7.8% of months)
clustered in                                       2001, 2002, 2003, 2008, 2009, 2010
```

⚠ **That clustering is the validation.** The state is rare, and it lands exactly
on the dot-com bust, the GFC and their rebounds — the periods when momentum
historically crashed (March 2009 being the canonical case). A condition invented
by data-mining would not concentrate itself on precisely the episodes the theory
names. Reproduce with `scripts/` or the CTE in this section's git history.

> **This is a better-targeted gate than generic volatility conditioning**, and it
> applies to the two strategies we most want to keep. It is a *two-condition
> state*, not a threshold, and it fires in under 8% of months — so it costs
> almost nothing in normal times.

⚠ Three papers now converge on the same instruction for momentum specifically:
Moreira-Muir (scale by inverse variance), Cederburg et al. (vol management works
*for momentum*), Daniel-Moskowitz (dynamic scaling doubles Sharpe). ⚠ Coverage
caveat: SPY stops **2024-09-27** while the corpus runs to 2026-07-08, so the
state is unavailable for the last ~21 months and must be **fail-closed**, not
carried forward.

## 2.9 Most measures are redundant — Green, Hand & Zhang (2017)

94 firm characteristics tested **simultaneously** in Fama-MacBeth regressions.
Only **12** are reliably independent determinants over 1980-2014 — and
⚠⚠ **since 2003, just TWO.**

> **This is the answer to "combine every metric we have".** The combination adds
> little because the characteristics are collinear: they are mostly the same
> few signals wearing different names. And **1/N — equal weighting the signals —
> is a hard benchmark that optimised weights routinely fail to beat.**

⚠ Read as a *budget*, not a prohibition: aim for a small number of genuinely
independent signals, equal-weighted, and prove independence rather than assuming
it. A 40-feature model is 38 features of noise and a multiple-testing problem.

## 2.10 Factor timing is harder than it looks — and our vol result is the exception

**Asness, Chandra, Ilmanen & Israel, "Contrarian Factor Timing is Deceptively
Difficult", *JPM* (2017).** Timing factor exposures on valuation spreads gives
*"somewhat disappointing"* results, and — the part that matters —
**"as the baseline portfolio became more diversified with more factors, it became
progressively harder for value timing to improve performance."** Trading
simulations failed to produce economically meaningful gains in gross return or
Sharpe.

⚠⚠ **This tempers the regime-gating ambition and must not be read as refuting
it.** Two different things:

- **Volatility conditioning** — a risk/liquidity mechanism at short horizons.
  Nagel's result reproduces monotonically on our own data (56.6 / 57.0 / 79.8 /
  **184.7** bps). Evidence FOR.
- **Contrarian valuation timing of factors** — a slow bet that a cheap factor
  mean-reverts. Evidence AGAINST.

> **Rule: gate on volatility and liquidity, where the evidence is. Do not
> generalise to "regime gating works" and start timing factors on spreads.**

## 2.11 You can see part of the order flow without the tape — Amihud (2002)

**Amihud, "Illiquidity and stock returns", *JFM* 5 (2002), 31-56.** The measure
is `mean(|daily return| / daily dollar volume)` — ⚠ **daily data only, no tick
data, no order book.** It proxies price impact per unit of order flow (the
empirical cousin of Kyle's λ) and is *"a relatively strong indicator of informed
trading when trading is urgent."*

Three uses, all available to us on 25.9M bars with nothing new ingested:

1. **A conditioner** — illiquid names are where limits to arbitrage bind, which
   is where §2.6's surviving anomalies survive.
2. **A per-name cost model**, replacing the flat ~50.9 bps round trip with
   something that varies the way real costs do. ⚠ Our current flat charge
   under-penalises exactly the microcaps §2.2 says are inflating us.
3. **A priced factor in its own right** — the illiquidity premium.

⚠ It is a *proxy*, not the tape. It cannot see a specific participant, and
nothing at daily resolution can. What it measures is how much price moved per
dollar traded, which is the footprint, not the actor.

---

## 2.12 Chart patterns — how to TEST one, which is the only interesting question

Bull flags, cup-and-handle, Wyckoff accumulation, head-and-shoulders. The useful
question is not *"do they work"* — it is **"what would make one demonstrably
valid?"**, because without an answer every discussion is anecdote.

**Lo, Mamaysky & Wang, "Foundations of Technical Analysis: Computational
Algorithms, Statistical Inference, and Empirical Implementation", *JF* (2000)**
is the paper that answered it. US stocks, **1962-1996** — almost exactly our
corpus window. Their method is the template:

1. ⚠⚠ **Make the pattern ALGORITHMIC first.** They smooth the price with
   **nonparametric kernel regression**, take local extrema off the smoothed
   series, and define each pattern as a *geometric relation among those extrema*.
   **A pattern you cannot express as code cannot be tested, and a pattern
   recognised by eye is recognised with hindsight.** This step is most of the
   work and it is the step practitioners skip.
2. **Compare the CONDITIONAL return distribution to the UNCONDITIONAL one.** The
   pattern carries information if and only if the distribution of returns given
   the pattern differs from the distribution without it. ⚠ Note what this
   avoids: you do not have to guess the right trade, stop or target to find out
   whether there is information there. It is a goodness-of-fit test, not a
   backtest.
3. **Test against a null.** A pattern that appears as often, and pays as much, in
   a randomised series with the same drift and volatility is noise.

**Their finding:** several indicators *"do provide incremental information and
may have some practical value"* — ⚠⚠ **"especially for low-liquidity stocks"** —
and traditional forms such as head-and-shoulders *"may not be optimal"*.

> ⚠⚠ **THAT IS THE THIRD TIME TODAY THE SAME COLLISION HAS APPEARED.** Chart
> patterns carry information *in illiquid names*. Long-horizon reversal lives *in
> cheap names* (§2.8a). Both sit in the population where §2.1, §2.2 and §2.5 say
> costs and construction destroy the edge. **Meanwhile momentum — the one effect
> our own measurement finds strongest in EXPENSIVE names (`+0.2479` at
> `>=$100`) — is the one the cost literature calls most scalable.**
>
> The convergence is the finding: **almost everything that looks exploitable
> lives where we cannot trade it, and the exception is momentum in liquid
> names.**

### What we already have, and the one piece we do not

| step | ours | ⚠ gap |
| --- | --- | --- |
| local extrema | `detect_swings(bars, n)` — n-bar fractal | ⚠ LMW smooth with kernel regression *first*. A raw fractal on noisy prices finds different extrema than a smoothed one, and they argue the smoothing is what makes detection robust |
| geometric relations | `cluster_levels`, `fib_levels`, `find_break_and_retest` | fine |
| causality | `detect_swings` needs `2n` neighbours | ⚠⚠ **a pivot is only knowable `n` bars later** — the look-ahead that caught S-6 |
| **the null model** | ✅ **`random_entry_cohort.py`** (stage 5e-5b) | ⚠ **already built and not being used for this.** A random-entry cohort with matched holding periods IS step 3's null |

⚠ **The null model is the part worth noticing.** We built a synthetic random-entry
control for the backtester and it is exactly what a pattern test needs: *does
this pattern beat a random entry held the same length?* Any pattern proposal
that cannot clear its own random-entry cohort should not reach a backtest.

### The four papers that ARE the evidence base for technical analysis

⚠ Asked for well-written research rather than assertion. This is the arc, and it
matters that it is read in order — the third paper overturns the second.

**1. Osler (2003) — the MECHANISM for support and resistance, from order data.**
*"Currency Orders and Exchange Rate Dynamics: An Explanation for the Predictive
Success of Technical Analysis"*, **JF 58(5), 1791-1819.** The first study with
data on **individual stop-loss and take-profit orders**. Two findings that
explain two separate TA claims:

- **Take-profit orders cluster at round numbers** → why trends *reverse* at
  support and resistance.
- **Stop-loss orders cluster just BEYOND round numbers** → why moves are
  *unusually rapid* once a level breaks.

⚠⚠ **This is the strongest pro-TA evidence in the literature, and its strength is
that it is mechanistic rather than statistical.** It does not say "the pattern
backtests well" — it shows the order book actually contains the clustering that
would produce the effect. That is precisely what `find_break_and_retest` and
`classify_interaction` model, and they now have a documented reason to exist.

⚠ Caveat that must travel with it: **the order data is FX, not equities.**
Round-number clustering is documented in equities too, but the order-book
evidence is currency markets.

**2. Brock, Lakonishok & LeBaron (1992) — the best statistical case FOR.**
*"Simple Technical Trading Rules and the Stochastic Properties of Stock
Returns"*, **JF.** DJIA **1897-1986**, moving-average and trading-range-break
rules, bootstrapped against four nulls — random walk, AR(1), GARCH-M, EGARCH.
**Strong support**; the returns were not consistent with any of the four.

**3. ⚠⚠ Sullivan, Timmermann & White (1999) — which overturns it.**
*"Data-Snooping, Technical Trading Rule Performance, and the Bootstrap"*, **JF
54(5), 1647-1691.** They apply **White's Reality Check** to quantify
data-snooping bias, expand BLL's 26 rules to *"the full universe from which the
trading rules were drawn"*, and re-run on 100 years of DJIA. Result: **no
profitable simple trading rule** for the DJIA, S&P 500, or S&P 500 futures.

> ⚠⚠ **This is the single most important paper in this file for how we work.**
> BLL's rules looked significant because the test ignored how many rules were
> considered before those 26 were selected. **STW is the direct ancestor of the
> deflated Sharpe and of `trial_register.py`** — the same correction, applied to
> the same mistake, 27 years earlier. Anyone proposing a rule here is repeating
> BLL unless the trial count is declared first.

**4. Lo, Mamaysky & Wang (2000) — the method for patterns.** §2.12. Kernel
regression → extrema → geometric definition → conditional vs unconditional
distribution. *"Incremental information … especially for low-liquidity stocks."*

### What the arc adds up to

| | verdict |
| --- | --- |
| Is there a mechanism behind support/resistance? | ✅ **Yes** — order clustering, Osler, with order-book data |
| Do simple rules beat the market on the index? | ❌ **No**, once data-snooping is corrected (STW) |
| Do chart patterns carry information? | ⚠ **Some**, incrementally, mostly in illiquid names (LMW) |
| Does that make them profitable? | ⚠ **Unestablished** — the gap is costs, and §2.1 says turnover decides |

⚠ **The honest reading: TA's foundations are better than its reputation and
weaker than its marketing.** There is a real, documented micro-mechanism. There
is no evidence that simple rules survive honest multiple-testing correction on
an index. Both are true, and the resolution is that the mechanism lives at a
scale and in a population where costs are hardest — the same collision as
§2.8a and §2.12.

### The three the operator named, specifically

⚠ **"Untested" and "disproven" are different, and conflating them is how a real
edge gets discarded.**

**Bull flag / cup-and-handle.** LMW's tested set centres on head-and-shoulders
(and inverse), broadening, triangle, rectangle and double top/bottom formations.
**Flags and cup-and-handle are not among the formations the academic literature
has systematically tested.** ⚠ That is not evidence they work — it is the absence
of evidence either way, and it means anyone quoting a success rate for them is
quoting a practitioner source (Bulkowski's *Encyclopedia of Chart Patterns* is
the usual one), not a controlled study. They are **candidates for the §2.12
recipe**, not conclusions.

**Wyckoff — ⚠⚠ I called this "not a pattern, should not be tested as one" and
that was WRONG.** Corrected after the operator pushed back and the primary source
was actually read (`wyckoffanalytics.com/wyckoff-method/`). It is far more
structured than a discretionary framework, and much of it is computable.

**It defines named, chartable events with stated criteria** — accumulation: PS
(preliminary support), SC (selling climax), AR (automatic rally), ST (secondary
test), **Spring**, SOS (sign of strength), LPS (last point of support), BU
(back-up); distribution: PSY, BC (buying climax), AR, SOW, LPSY, UT (upthrust),
UTAD. Each with a definition — a Spring is *"a price move below the support level
of the TR that quickly reverses and moves back into the TR"*, an SOS is *"a price
advance on increasing spread and relatively higher volume"*.

⚠⚠ **And it carries an explicit numbered checklist — the Nine Buying Tests —
most of which we can compute today:**

| # | test | computable with |
| --- | --- | --- |
| 1 | downside price objective accomplished (P&F count) | ⚠ needs point-and-figure; we have none |
| 2 | PS, SC, ST present | ⚠ needs the event definitions coded |
| 3 | activity bullish — volume rises on rallies, falls on reactions | ✅ bars + volume |
| 4 | downtrend line penetrated | ✅ `detect_swings` + a fitted line |
| 5 | higher lows | ✅ `detect_swings` |
| 6 | higher highs | ✅ `detect_swings` |
| 7 | **stock stronger than the market** | ✅ relative strength vs SPY (#2398) |
| 8 | base forming (horizontal price line) | ✅ `cluster_levels` |
| 9 | profit potential >= 3x risk | ✅ a reward:risk filter on the level distance |

**Seven of nine are expressible with primitives we already ship.**

> **So the honest status is not "untestable" — it is "never systematically
> tested", which is a completely different and much more interesting
> statement.** This sweep found no peer-reviewed validation, and the source
> itself says proficiency *"requires considerable practice"* and that a trader
> must *"interpret the motives behind the action"* — i.e. **as practised it is
> discretionary, but as specified it is largely mechanical.**

⚠ The structure is also unusually well-suited to being tested properly, because
the Nine Tests are a **conjunctive filter**: you can measure the marginal
contribution of each condition, and find out whether the edge (if any) lives in
the whole gestalt or in two of the nine. **That is a far better experiment than
"does Wyckoff work".**

⚠ And the third law — **effort versus result**, *"high-volume bars with narrow
ranges indicating institutional unloading"* — is **Amihud illiquidity inverted**:

```text
Amihud                      =  |return| / dollar volume
Wyckoff absorption           =  high volume, small |return|  ->  LOW Amihud
Wyckoff "ease of movement"   =  low volume, big move         ->  HIGH Amihud
```

So the law Wyckoff rests on is a quantity that **does** have empirical support
(§2.11) and is computable on our 25.9M bars with no new data. ⚠ Note what this
does NOT establish: that the *interpretation* (a composite operator accumulating)
is correct. It establishes that the **observable** is real and measurable. Test
the quantity, not the story attached to it.

⚠⚠ **Lesson recorded against myself: I dismissed Wyckoff from priors without
reading the source, and the source contradicted me. The repo rule is "grep before
cite"; the same applies to dismissing something. A framework being old and
practitioner-taught is not evidence about its testability.**

### The mathematics actually involved, and which of it is grounded

| what | the maths | grounded? |
| --- | --- | --- |
| trend | SMA/EMA — linear and exponentially-weighted moving averages | ⚠ the 50/200 pair is a **convention**, not a result |
| momentum oscillators | RSI = `100 - 100/(1+RS)`, RS from **Wilder-smoothed** (recursive, causal) average gain/loss | published (Wilder 1978). ⚠ a non-causal smoother inflates results — #2260 |
| volatility | ATR (Wilder smoothing), realised σ, Bollinger `SMA ± kσ` | published; ⚠ the Squeeze is BandWidth lowest in **126 days**, not a percentile |
| oscillator | MACD = `EMA12 − EMA26`, signal = `EMA9(MACD)`; Stochastic = position in range | published periods are conventions |
| levels | single-linkage clustering of extrema, ATR-relative | ⚠ **no published formulation** — by construction, frozen in a hash |
| retracement | Fibonacci ratios | ⚠ **0.5 is not a Fibonacci ratio** — Dow Theory's halfway |
| pattern recognition | **nonparametric kernel regression** → local extrema → geometric relations | LMW (2000) — the only rigorous treatment |
| liquidity / order flow | **Amihud** `mean(|r| / dollar volume)`; Kyle's λ | published, and the closest thing to seeing flow without a tape |
| significance | correlation `t`, **clustered** standard errors, deflated Sharpe, block bootstrap | ⚠ the part everyone skips — and the part that killed our own momentum finding (§2.8) |

⚠⚠ **The last row is the one that separates analysis from decoration.** Every
formula above is arithmetic a spreadsheet can do. The inference — clustering,
multiple-testing correction, out-of-sample discipline — is what decides whether
the arithmetic means anything, and it is the only row where a mistake is
invisible in the output.

### The rule for this repo

> **A chart-pattern proposal ships with: an algorithmic definition that is causal
> at the decision bar, a conditional-vs-unconditional distribution test, and a
> random-entry cohort it beats. Absent any of the three, it is a picture.**

⚠ And note the honest ceiling even when all three pass: LMW's own conclusion is
*"incremental information"*, not a profitable strategy — the gap between the two
being costs, which §2.1 says is decided by turnover. Pattern strategies are
high-turnover by construction.

---

## 3. Family viability, with the evidence attached

| family | evidence | our data | verdict |
| --- | --- | --- | --- |
| **insider purchases** | ⚠⚠ **the best-evidenced family we can actually build.** Lakonishok & Lee (2001): heavy insider buying beats the market ~6%/12mo. Jeng, Metrick & Zeckhauser (2003): ~11.2%/yr abnormal on purchases, **sales show no effect**. Cohen, Malloy & Pomorski: **routine trades have ZERO predictive power; opportunistic trades pay 82 bps/month** (~10%/yr) — ~4× the undifferentiated signal | Form 4 tables held | **build** — ⚠ purchases only, and the routine/opportunistic split is the whole edge |
| cross-sectional momentum | survives costs, scalable, responds to vol scaling | shipped (s2) | keep, re-measure per §2.2 |
| time-series momentum | family survives, but ⚠ our turnover is 12× the bar | shipped (s1) | ⚠ fix turnover or drop |
| short-term reversal | ⚠ most cost-constrained family; ours is 6.7× the turnover bar | shipped (s3) | ⚠ hardest case in the catalogue |
| PEAD | *"largely disappeared in many segments"*, persists only where limits to arbitrage bind | event + XBRL, ⚠ **no analyst estimates** | conditional at best |
| 13F flow / crowding | ⚠ mixed. Cloning ≈ market returns; the 45-day lag is the design problem, and institutions *deliberately* delay to deter copycats | 7M rows | ⚠ crowding/unwind angle only, not cloning |
| filing text | — | ⚠ **NOT held** — `filing_documents` is a manifest of URLs, no bodies | needs a fetch pipeline first |
| short volume | — | RegSHO daily flow; ⚠ not short interest, not borrow | proxy only |

---

## 3.1 Insider purchases — the family measured against our own data

**Cohen, Malloy & Pomorski, "Decoding Inside Information", *JF* 67(3) (2012),
1009-1043.** Routine trades — an insider with a **history of trading the same
calendar month across years** — are **over half the universe** and carry
*"essentially zero"* abnormal return. Strip them out and the remainder yields
**82 bps/month value-weighted**. *"Opportunistic trades predict future news and
events at a firm level, while routine trades do not."*

⚠ Two conditioning details that invert intuition and are checkable on our
`filer_role` column: the most informed opportunistic traders are **local,
NON-EXECUTIVE insiders from geographically concentrated, poorly governed
firms** — not the CEO.

**Measured on `insider_transactions` (1,052,947 rows), 2026-08-08:**

```text
distinct insiders                                        55,001
purchases (txn_code = 'P')                               48,278
sales     (txn_code = 'S')                              243,068
insiders with >= 3 distinct PURCHASE years                  997 of 9,936 (10.0%)
```

⚠⚠ **Two things this measurement changes, and neither was obvious:**

1. **The usable population is 48,278, not 1M.** Jeng/Metrick/Zeckhauser find
   purchases carry the signal and *"insider sales showed no comparable effect"* —
   and sales outnumber purchases **5:1** here. Anyone sizing this family off the
   row count will be out by a factor of 20.
2. **Only 10% of purchasing insiders have the ≥3 years of history CMP's routine
   test needs.** ⚠ That is not necessarily fatal — CMP's routine label requires
   an *established pattern*, so an insider with no pattern is arguably
   opportunistic by construction rather than unclassifiable. **But that is an
   inference about their method, not a quote from it — verify against the paper
   before building on it**, because the alternative reading (exclude the
   unclassifiable 90%) leaves a very different population.

⚠ Data defect found while measuring this: **20 rows carry two-digit-year
`txn_date`s** (year 23 in `-24-` accessions) that `txn_date_invalid` does not
flag. Negligible as a share, material for this family specifically — a
2,000-year gap wrecks the per-insider history the classification depends on.
Filed as **#2441**.

---

## 4. The checklist before speccing any strategy

1. **Turnover first.** Above ~50%/month, the evidence says do not bother unless a
   buy/hold spread brings it down.
2. **Name the forced participant.** No mechanism → it is a pattern → §2.2 applies.
3. **Check the family** against §3 before designing the signal.
4. **Declare the trial** to `trial_register.py` *before* measuring (10 trials →
   0.0833 bar; 1,000 → 0.1738; 100,000 → 0.2344).
5. **Value-weight or size-screen**, or expect §2.2 to explain your result for you.
6. **Verify every number against our own data.** ⚠ Everything in this file is a
   prior. The repo's rule stands: *grep before cite, run it or delete the
   quantifier.*

---

## 5. Open-source landscape

- **Qlib** (Microsoft) — closest to this design; ML pipeline, cross-sectional
  stock selection, DataServer ~10× pandas on time series. **Evaluate before
  hand-rolling more infrastructure.**
- **vectorbt** — ⚠ **measured and REJECTED**, reasons in `equity_curve.py`:
  forcing `freq="1D"` imposes annualisation of exactly 365 against our ~196
  observations/year, inflating Sharpe **1.37×**. Do not revisit without new
  evidence.
- **zipline-reloaded** (original dead since Quantopian closed), **backtrader**
  (event-driven, no cross-sectional strength).
- **TradeMaster / FinRL** — RL platforms. ⚠ RL multiplies the trial-count
  problem rather than solving it.
- **Quantpedia** — 900-1,200+ strategies, a subset replicated in QuantConnect.
  Use as a **hypothesis source with mechanisms attached**, never a shopping
  list. Every one taken is a declared trial.
