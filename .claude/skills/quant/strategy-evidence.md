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
A third companion, `market-technician/SKILL.md`, owns *how to read a chart*
(the layered read protocol + per-concept evidence status, every claim tagged
PUBLISHED/MEASURED/CONVENTION/REFUTED); it defers to this file on tradability.

⚠ Storage companion: `quant/data-capability.md` §7. Every logical signal must
reach the durable daily census, but only fired signals are durable detail;
routine verdict detail is retained 90 days in drop-only partitions. Bypassing
`store_strategy_observations` both breaks census parity and recreates the
measured 8.42 GB/year failure.

---

## 0. ⚠⚠ WHAT SURVIVED — read this first, the rest of the file is mostly what did not

A day of falsification has a shape, and the shape is the finding:

> **Everything that failed was a RETURN PREDICTION. Everything that survived is
> on the VOLATILITY, COST or STRUCTURE side.**

That is not luck. It is the central asymmetry of the field:

**Volatility is predictable.** *"The conditional variance of financial returns
is time-varying and predictable"* — one of the most robust regularities in
finance: **Mandelbrot (1963)**, **Engle's ARCH (1982, Nobel)**, **Bollerslev's
GARCH**. Large changes follow large changes. Nothing here challenged it.

⚠⚠ **But "returns are not predictable" — which this file previously asserted —
is FALSE as stated** (Codex audit). **Order-flow imbalance predicts short-horizon
returns** with a linear, stable relation (§2.10b, Cont/Kukanov/Stoikov). The
accurate claim is narrower and worth stating precisely:

> **Returns are not predictable FROM DAILY PRICE HISTORY ALONE, which is the
> data most of this file tests.** They are predictable from order flow, which we
> do not receive. That is a statement about our feed, not about markets.

⚠⚠ **This is the positive result, and it is actionable WITHOUT predicting a
single return.**

### The four things worth building, in order of evidence

**1. Volatility-scaled position sizing (`inverse_vol_v1`).** Improves risk-adjusted
return using only the quantity we can actually forecast. No return prediction
required. ⚠ Scoped by §2.4 — the evidence supports it for momentum, not
universally.

**2. ATR-based stops and targets — useful, and ⚠ NOT an edge.** The direct
answer to *"what tells us the stop and the take profit"* is **the volatility, not
the signal.**

⚠⚠ **But this file previously called it "the highest value per line of code on
the board", and that was wrong** (Codex audit). **ATR gives SCALE, not
EXPECTANCY.** A stop changes the shape of the return distribution and **can
convert positive drift into negative** by cutting winners' paths short and
locking in the noise. It is infrastructure that makes risk statable — it does
not create return, and a strategy with no edge is not rescued by a good stop.

```text
stop    = entry - m * ATR14
target  = entry + n * ATR14      with  n/m >= 2  (Wyckoff Buying Test 9 says >= 3)
```

Why this is well-founded rather than a convention:

- **The distance adapts to the instrument's own scale and regime.** A flat 5%
  stop means something completely different on a 15%-vol large cap and a 90%-vol
  microcap. ATR normalises both onto one axis — the same argument
  `CLUSTER_ATR_K` already makes for level tolerance.
- **ATR is forecastable** (vol clustering) where the price target is not. You are
  setting the distance from a quantity that persists, not from a prediction.
- **It makes reward:risk a stated input** rather than an outcome, which is what
  turns a signal into a trade.

✅ **Delivered for the S-4 harness control (#2473).**
`strategy_manifest._s4_exit_levels` constructs causal ATR-at-signal brackets;
the backtest and forward resolver consume that same manifest field. Backtests
convert a same-bar double touch into declared best/worst sensitivity arms;
forward resolution records the single honest `ambiguous` terminal outcome.
This validates the exit machinery. It does **not** turn S-4 into a return signal
or capital candidate: #2478 permanently labels S-1..S-4
`harness_validation`.

**3. Opportunistic insider purchases — the best-evidenced RETURN signal we can
actually build.** Cohen/Malloy/Pomorski: routine trades **zero**, opportunistic
**82 bps/month**. ⚠ And the reason it belongs on a positive list while momentum
does not: **insiders trade rarely, so the strategy is naturally low-turnover** —
it passes the Novy-Marx/Velikov filter that killed s1 (12× over) and s3 (6.7×
over) before either was even measured. Evidence, mechanism, our data, and the
right turnover profile. See §3.1 for what the measurement says about population
size.

**4. The equity risk premium itself.** Our corrected historical benchmark
compounded at **6.3-6.6%/yr** over 58 years. The older three-control snapshot
had only S-2 above that comparator; it is not current promotion evidence. The
risk premium remains a **fallback that works**, and the honest default until a
candidate clears it. Related: the one effect that survived all four traps in the
`market-structure` research pass was the **overnight drift (~4-5 bps/day in
liquid names, in every decile)** — which is the risk premium accruing while you
hold, *"captured by holding, not by trading."*

### The reliable negative screen

⚠ **Turnover above ~50%/month.** One stored column, no backtest needed, and it
disqualifies faster than anything else here. Saving money by not trading is a
real return and it is the most certain one in this file.

### What this means practically

**We are unlikely to get rich predicting returns, and we can build something
sound out of predicting risk.** Size by volatility, exit by volatility, refuse
high turnover, capture the premium, and add the one or two return signals that
have a mechanism and a low trade count. ⚠ That is a smaller and duller ambition
than a winning chart pattern, and it is the version supported by evidence.

---

## 1. ⚠⚠ The four filters, and the historical three-control snapshot

Each filter is a published result. The scores are **our own stored numbers**,
recomputed — never quoted from here without re-running.

| filter | source | s1 time-series mom | s2 cross-sectional mom | s3 mean-reversion |
| --- | --- | --- | --- | --- |
| **turnover < 50%/month** | Novy-Marx & Velikov | ✗ **600%/mo (12× over)** | ✓ **35%/mo INSIDE** | ✗ 333%/mo (6.7× over) |
| **family survives costs** | Frazzini/Israel/Moskowitz | ✓ momentum most scalable | ✓ momentum most scalable | ✗ **reversal is the MOST constrained** |
| **responds to vol scaling** | Cederburg et al. | ✓ momentum is the exception that works | ✓ same | ✗ not in the working subset |
| **construction not microcap-inflated** | Hou/Xue/Zhang | ⚠ indirect (equal-weight pooling) | ⚠ **direct hit** (cross-sectional sort, `MIN_CLOSE=1.0`) | ⚠ indirect |

**That stored snapshot had S-2 as the only one of the three above buy-and-hold.**
S-1..S-4 are now permanent harness controls and current evidence must be read
from their immutable recent-window identities, never copied from this prose.

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

> **Check turnover BEFORE anything else.** It is one column, already stored, and
> it screens faster than any backtest.

⚠ **Do not read it as an absolute** (Codex audit). Novy-Marx & Velikov say *most*
anomalies below 50%/month generate net spreads and *few* above do — **not that
every high-turnover strategy is dead.** It is a strong prior for a daily-bar,
retail-cost operation, and it would be the wrong filter for a genuine intraday
book where the whole point is high turnover at tiny per-trade cost.

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

⚠⚠ **AND AN AUTOCORRELATION SIGN IS NOT A TRADABLE SIGNAL** (Codex audit). A
negative coefficient says the series mean-reverts on average; it does not say the
reversion is larger than the spread, reachable at a fill you could get, or
present when you need it. §2.11a is the immediate proof — the same negative
autocovariance is exactly what a bid-ask bounce produces. **Treat every cell in
this table as a hypothesis about a mechanism, never as a strategy.**

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
> applies to any future candidates in the two momentum families. It is a *two-condition
> state*, not a threshold, and it fires in under 8% of months — so it costs
> almost nothing in normal times.

⚠ Three papers now converge on the same instruction for momentum specifically:
Moreira-Muir (scale by inverse variance), Cederburg et al. (vol management works
*for momentum*), Daniel-Moskowitz (dynamic scaling doubles Sharpe). #2482's
separate immutable SPY price-return comparator reaches **2026-07-08**, so this
state is now measurable on recent windows. The candidate identity must name
that snapshot; missing sessions remain fail-closed and are never carried
forward.

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

Three uses, all available to us on the 75,972,649 bars / 30,591 series corpus with nothing new
ingested (`select count(*), (select count(*) from research_price_series) from research_price_daily` → 75,972,649 / 30,591 (2026-08-22)):

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

## 2.10b ⚠⚠ What skilled day traders actually read — and exactly why we cannot

⚠⚠ **CORRECTION AGAINST MYSELF — I READ HALF OF ONE PAPER.** I researched day
trading by searching for *failure rates*, found Barber/Odean and Chague et al.,
and treated that as the answer. It was a loaded query and I took the answer it
was shaped to give.

**The same Barber, Lee, Liu & Odean Taiwan paper cuts BOTH ways, and I quoted
only one side:**

| what I quoted | what I missed, from the SAME study |
| --- | --- |
| fewer than **1%** are predictably profitable net of fees | **the top 500 traders ranked by PRIOR-YEAR performance earned 37.9 bps/day after fees the following year** |

⚠⚠ **That is persistence.** Past performance and trade concentration predict
future performance. **Day-trading skill is real, it is rare, and it is
identifiable ex ante** — which is the opposite of the conclusion I drew, from
the same source.

Corroborating, and also missed:

- **Locke & Mann** — discipline measures predict subsequent trader success **out
  of sample**; successful floor futures traders are *"rational and disciplined"*.
- **Boyd & Kurov** — trader profits, experience, sophistication and dual trading
  predict **survival** in energy futures.
- **Hasbrouck** — trades carry information and price impact arrives with a lag.

> **The honest summary is not "97% lose."** It is: **the distribution is brutal,
> and the right tail is real, persistent and predictable from past performance.**
> Those are compatible, and only the second one is useful.

**They are reading order flow, and it is one of the best-established results in
market microstructure.**

**Cont, Kukanov & Stoikov, "The Price Impact of Order Book Events", *Journal of
Financial Econometrics* 12(1) (2014), 47-88.** NYSE TAQ, fifty US stocks:

> Over short time intervals, **price changes are mainly driven by the order flow
> imbalance (OFI)** — the imbalance between supply and demand at the best bid
> and ask. The relation is **LINEAR**, with a slope **inversely proportional to
> market depth**, and is **robust to intraday seasonality and stable across time
> scales and across stocks.**

⚠ Read that again against the folklore. What a tape reader describes as *"heavy
buying hitting the ask"* is a measured, linear, stable relationship. **The
intuition is real and it has a formula.** It also implies the square-root impact
law (§2.11b), so it is the same physics from the other end.

### ⚠⚠ And the precise reason we cannot compute it

**OFI needs the SIZES at the best bid and ask, and how they change.** Not the
prices — the quantities resting there.

Measured on our own feed (`app/services/etoro_websocket.py`, 2026-08-09):

```text
Trading.Instrument.Rate pushes:   bid, ask, last          (prices only)
bid size / ask size:              ABSENT
depth beyond L1:                  ABSENT
volume at price / trade side:     ABSENT
```

⚠ Every occurrence of "size" in that module refers to **WebSocket frame bytes**,
not order size. Verified by grep, not assumed.

> **So the honest gap is NOT "we have no intraday data" — a claim I made
> repeatedly and which is wrong. We have a live authenticated tick feed to every
> instrument. What we lack is DEPTH.** That is a far sharper statement and it
> changes what a data purchase would have to buy.

| we CAN build from the live feed | we CANNOT, at any effort |
| --- | --- |
| tick history, true intraday candles | **OFI** — needs L1 sizes |
| observed bid-ask spread over time ⚠ | footprint charts / volume-at-price |
| quote-update frequency (activity proxy) | cumulative delta, absorption, exhaustion |
| realised intraday volatility | trade-side classification |

⚠ **The spread row is worth noticing on its own.** `quotes` carries `bid`, `ask`
and `spread_pct` — **observed** spreads. Our cost model uses *calibrated bands*
from samples of 76-244 series, and the era mismatch between those bands and a
1962-2026 corpus is what made the Roll test inconclusive (§2.11a). **Recording
observed spreads over time replaces a calibration with a measurement.**

### What this means for strategy selection

1. **The intraday order-flow game is closed to us on this feed**, and not for
   want of skill or speed — for want of a field in the payload. ⚠ Stop treating
   it as an aspiration; treat it as a data-purchase decision with a known
   requirement (L1 depth minimum, ideally full book).
2. **The tick feed is still worth persisting.** `price_intraday` exists and holds
   **0 rows**. Subscribing a chosen universe and aggregating into candles builds
   an intraday corpus from today forward at zero data cost — and in months it
   supports the intraday work we currently cannot even backtest.
3. ⚠ **Do not conclude "day trading is impossible".** Conclude that **the
   specific mechanism the profitable minority uses requires data we do not
   receive**, and that our own asset — SEC filings at depth — is a different
   game with a different mechanism (§3.1).

## 2.10c Trailing stops — the evidence is SPLIT, and the split is usable

Asked whether a stop should ratchet up as price clears resistance. The evidence
divides cleanly and the dividing line tells us where to apply it.

**FOR — and specifically for momentum:**

- **Han, Zhou & Zhu (2014)** — stop-loss enhanced momentum strategies show a
  **67% reduction in maximum drawdown and a 94% improvement in Sharpe ratio.**
- ETFs 2001-2021 — thresholds of **1.0 to 1.5 standard deviations** give
  significantly higher excess returns, positive **even after transaction costs**.
- Stop-loss rules raise returns on stocks with **lottery features** (sporadic big
  gains, frequent small losses) — cut the frequent small losses, keep the tail.

**AGAINST — for broad holding:**

- US stocks **1926-2016**: trailing-stop portfolios show lower total risk but
  ⚠ **lower returns AND lower Sharpe** than the benchmark. The stop cuts the
  compounding path.

> **The rule that falls out: stops belong on MOMENTUM and lottery-shaped
> positions, not on a broad hold.** Applied indiscriminately they are a tax on
> compounding; applied to momentum they are the medicine for its known disease.

⚠⚠ **And that disease is one we already measured.** Daniel & Moskowitz (§2.8b)
show momentum's tail risk is concentrated in forecastable panic states — 28 of
357 months on our own SPY series, clustered on 2001-03 and 2008-10. **Han/Zhou/Zhu
and Daniel/Moskowitz are two treatments for the same illness**: scale down on the
risk forecast, and stop out when it happens anyway.

### ⚠⚠ Ratcheting on RESISTANCE specifically — and why the obvious design is wrong

**I found no evidence for ratcheting a stop on structural levels.** The evidenced
form is **volatility-based** (σ or ATR multiples, ~1.0-1.5σ). Structure-based
ratcheting is practitioner convention, untested in the literature this sweep
reached.

⚠⚠ **Worse than untested — Osler (§2.12) gives a positive reason to AVOID the
obvious placement.** Her order-book data shows **stop-loss orders cluster just
beyond round numbers and support levels**, and that clustering is *what produces
the rapid move once a level breaks*.

> **So putting your stop just under the obvious support puts it inside the
> cluster that gets run.** The level is where the liquidity hunt happens. If
> structure informs the stop at all, it should be an OFFSET from the level — far
> enough back to sit outside the cluster — and the offset should be measured in
> ATR, which is the one quantity we can forecast.

**Design that follows from the evidence:**

| element | rule | source |
| --- | --- | --- |
| initial stop | `entry - m * ATR14`, m ~ 1.0-1.5σ-equivalent | ETF study threshold range |
| trail | ratchet UP only, never down, on a volatility distance | Han/Zhou/Zhu |
| structure | use levels to *offset* the stop away from the cluster, not to place it at one | ⚠ Osler, inverted |
| when to apply | momentum and lottery-shaped names only | the 1926-2016 result |
| when NOT to | a broad long-term hold | same |

⚠ Still to be measured on our data before shipping: **the m that is right for our
universe**, and whether the ratchet earns its turnover after our cost model. The
literature gives a range, not our number.

## 2.11a ⚠⚠ Is our one surviving finding just the bid-ask bounce? — INCONCLUSIVE

After clustering, short-horizon reversal is the **only** part of the term
structure our own data supports (§2.8). **Roll (1984)** —
*"A Simple Implicit Measure of the Effective Bid-Ask Spread in an Efficient
Market"*, **JF 39(4)** — says that may be an artefact with no economic content:
with a spread, trades alternate between bid and ask, so observed price changes
carry **spurious negative serial covariance**, which is literally what a
short-term reversal pattern looks like. And the spread is recoverable from it:

```text
effective spread = 2 * sqrt( -cov(dP_t, dP_t-1) )
```

**The test** (`scripts/verify_2437_roll_bounce.py`): compare the spread IMPLIED
by our measured autocovariance against the spread `cost_model.BANDS`
independently believes those names have. Close ⇒ bounce. Much larger ⇒ economic
reversal on top of the bounce.

```text
band        implied RT   our RT   ratio
<$5             4.437%   1.450%   3.06x
$5-20           2.162%   0.571%   3.79x
$20-100         1.537%   0.509%   3.02x
>=$100          3.908%   0.322%  12.14x
```

⚠⚠ **DO NOT READ THIS AS "THE REVERSAL IS REAL." The test has two confounds and
both push in the flattering direction.**

**Confound 1 — my own selection bias, and it is the worse one.** Roll is
undefined for a positive covariance, and **26-30% of series had `cov >= 0`**
(229/870, 720/2494, 599/2169, 126/427). The script averaged `sqrt(-cov)` over the
**negative subset only**. ⚠ If the true covariance were zero with noise around
it, keeping only the negative draws and averaging their square roots produces a
**positive implied spread out of pure noise**. The discard rate is the tell, and
the fix is to average the covariance across ALL series first and take the root
once — not to root each and average.

**Confound 2 — era mismatch.** `cost_model.BANDS` is calibrated on **modern**
eToro spreads (p75, samples of 76-244). The research corpus runs 1962-2026, and
**12,281,013 of 75,972,649 bars — 16.2% — predate decimalisation** (2001-04-09),
⚠ re-measured 2026-08-22 after the corpus tripled;
`select count(*) filter (where bar_date < date '2001-04-09'), count(*) from research_price_daily`.
The share MOVED (was 20.3% on 25.9M bars), so this is not a denominator swap —
when US equities traded in eighths and sixteenths. A 1/8 tick on a \$10 stock is
**1.25%**, an order of magnitude above a modern spread. ⚠ So a large part of the
"excess" may simply be that historical spreads were genuinely far wider than the
band table says. **A 3x ratio is entirely consistent with comparing a
1962-2026 autocovariance against a 2020s spread.**

⚠ The `>=$100` band at **12.14x** is the row that should provoke most suspicion,
not most excitement: expensive names have the *tightest* relative spreads, so
that is the cell where the ratio should be smallest. It being the largest
suggests the estimator is measuring something other than the spread there.

> **Verdict: NOT SETTLED.** The honest statement is that short-horizon reversal
> is *not obviously* pure bounce, and that our test cannot currently distinguish
> "real reversal" from "wider historical spreads plus a biased estimator".
>
> **Two fixes, both cheap, before this claim is used for anything:** average the
> covariance across all series before rooting (removes confound 1), and split the
> corpus at **2001-04-09** (removes confound 2). If the post-decimalisation ratio
> is still comfortably above 1, the finding stands.

⚠ Recorded as inconclusive deliberately. The result came back in the direction
that would justify the one strategy family our data still supports, and that is
precisely when a test deserves the hardest look.

## 2.11b Our cost model is right for a small account and silent about capacity

`cost_model.BANDS` charges a **flat per-price-band half-spread** — `<$5` at
1.450% round trip, `$5-20` at 0.571%, and so on. It is size-independent.

⚠⚠ **Real market impact is not.** The empirical regularity is the **square-root
law**: impact on a metaorder of size `Q` grows roughly as `sqrt(Q)` — strongly
**concave**, so the marginal cost of the next share falls as the order grows.
Almgren & Chriss decompose it into a **temporary** component (the cost of
demanding liquidity, which reverts once you stop) and a **permanent** one (the
lasting shift reflecting your trade's information). Later work argues the
dependence is closer to logarithmic than square-root; the direction is not in
dispute.

**What that means for us, honestly, in both directions:**

- ✅ **At small size a flat spread is a defensible approximation.** When you are
  trading a few thousand pounds, you cross the spread and move nothing. Impact is
  negligible and the spread genuinely is the cost. The model is fit for the
  account it currently describes.
- ⚠⚠ **It has NOTHING to say about capacity, and capacity is the question that
  matters the moment a strategy works.** A flat cost model reports the same cost
  at £1,000 and £10,000,000. It therefore **cannot tell us the size at which a
  strategy stops working** — and it will never warn us, because the number does
  not move.

> **So the model can answer "does this edge exist at our current size" and cannot
> answer "how much money can it hold". Those are different questions and only the
> first is currently askable.**

⚠ This is exactly the axis Frazzini/Israel/Moskowitz measure (*"capacity more
than an order of magnitude larger than previous studies suggest"*, value and
momentum most scalable) and Novy-Marx & Velikov measure (*"strategies based on
size, value and profitability have the greatest capacity to support new
capital"*). **Both papers rank strategies on a dimension our cost model cannot
represent at all.**

⚠ And note the interaction with §2.2: a flat percentage under-penalises exactly
the illiquid microcaps where real impact is worst — the same names Hou/Xue/Zhang
say are inflating our results. **Two independent reasons the sub-\$5 band is
flattered, and a size-aware or Amihud-scaled cost model corrects both at once.**

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
| 7 | **stock stronger than the market** | recent price-relative strength is computable through 2026-07-08 using immutable snapshot `etoro-comparators-2026-07-08-v1`; total-return-relative claims still refuse because recent comparator `adj_close` is NULL |
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
(§2.11) and is computable on our 75.97M-bar corpus with no new data. ⚠ Note what this
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

## 2.13 ⚠⚠ Opening Range Breakout — a prominent "day trading works" claim and a broker-cost falsification

The operator asked whether anything works on **intraday candles alone** — no
depth, no order book. ORB is a prominent published claim and deserves a fair
falsification rather than a dismissal. It is **not** an eBull capital candidate:
the experiment measures whether the published effect survives this broker and
whether its Stocks-in-Play filter is useful to a separately specified strategy.

**Zarattini, Barbon & Aziz, "A Profitable Day Trading Strategy For The U.S.
Equity Market"** (SSRN 4729284). **7,000+ US stocks, 2016-2023**, 5-minute
opening-range breakout. Headline: **1,484% versus 169% for QQQ.** Restricting to
*"Stocks in Play"* improves it further, *"even after considering transaction
costs"*.

⚠ It needs **only candles**. No depth, no sizes, no trade side. **It is precisely
the shape the operator asked about, and it is testable on data we can record
ourselves from today forward.**

### The conflicts, stated plainly

- **Andrew Aziz** founded Peak Capital Trading and wrote *"How to Day Trade for a
  Living"* — he **sells day-trading education**.
- **Carlo Zarattini** runs Concretum Research, a firm focused on intraday US
  markets; several of the hosting pages are the authors' own site.
- ⚠ Andrea Barbon is a genuine academic (Swiss Finance Institute / St Gallen),
  which is the counterweight. **This is not vendor content — but it is not
  disinterested either.**
- No independent replication found by this sweep.

### ⚠⚠ The number that decides it for us

Their assumptions: \$25,000 capital, **maximum 4x leverage**, commission
**\$0.0005 per share**, size calibrated so a stop costs 1% of capital.

**Our posture forbids leverage outright.** And the commission assumption is a US
direct-access rate, where ours is a *spread*:

```text
             their round trip   ours     ratio
$5   stock        0.0200%       1.450%     72x
$20  stock        0.0050%       0.571%    114x
$100 stock        0.0010%       0.509%    509x
```

> ⚠⚠ **The strategy was validated at a cost between 72x and 509x lower than
> ours.** A day strategy turns the pot over roughly once per day; at ~0.5% round
> trip that is ~125% of capital paid in spread per year. **No intraday edge of
> this size survives that.**

### What that actually means — and it is not "give up"

1. **ORB is not disproven. It is priced out at OUR execution.** The finding is
   about our broker, not about the strategy.
2. ⚠ **This is the single strongest argument for caring about execution cost**
   rather than signal quality. A 500x cost disadvantage cannot be out-thought.
3. **The falsifiable question it leaves is narrow:** does the published ORB
   survive at *our* costs on *our* recorded candles, restricted to the widest,
   most liquid, tightest-spread names? The honest prior is no. ⚠ Pre-register
   the rejection test because a famous strategy invites a favourable reading.
   Even a pass does not place ORB on the strategy menu; it only licenses a new,
   separately specified candidate hypothesis if the selection filter is useful.
4. **"Stocks in Play" is doing real work** in their result — a relative-volume
   and gap filter. Any replication must include the selection step, not just the
   breakout rule.

## 3. Family viability, with the evidence attached

| family | evidence | our data | verdict |
| --- | --- | --- | --- |
| **insider purchases** | ⚠⚠ **the best-evidenced family we can actually build.** Lakonishok & Lee (2001): heavy insider buying beats the market ~6%/12mo. Jeng, Metrick & Zeckhauser (2003): ~11.2%/yr abnormal on purchases, **sales show no effect**. Cohen, Malloy & Pomorski: **routine trades have ZERO predictive power; opportunistic trades pay 82 bps/month** (~10%/yr) — ~4× the undifferentiated signal | Form 4 tables held | **build** — ⚠ purchases only, and the routine/opportunistic split is the whole edge |
| cross-sectional momentum | family survives costs in published work and responds to vol scaling | S-2 is a permanent harness control | **control only** — any investable formulation needs a new preregistered candidate identity and recent after-cost evidence |
| time-series momentum | family evidence exists, but the S-1 implementation turns over 12× above the viability bar | S-1 is a permanent harness control | **control only** — do not tune or promote S-1 |
| short-term reversal | ⚠ most cost-constrained family; S-3 is 6.7× above the turnover bar | S-3 is a permanent daily RSI harness control | **control only** — do not transfer its evidence to a different reversal definition |
| intraday factor-residual reversal | published long-short construction uses 30-minute midpoints and factor residuals | exact inputs are not yet reproducible | separately preregistered, data-gated replication (#2484); it does not inherit S-3 evidence |
| volatility-compression breakout | ATR supplies risk scale, not expectancy | S-4 now has causal brackets and forward resolution | **control only** — resolved forward outcomes are pipeline proof, not performance evidence |
| PEAD | *"largely disappeared in many segments"*, persists only where limits to arbitrage bind | event + XBRL, ⚠ **no analyst estimates** | conditional at best |
| 13F flow / crowding | ⚠ mixed. Cloning ≈ market returns; the 45-day lag is the design problem, and institutions *deliberately* delay to deter copycats | 7M rows | ⚠ crowding/unwind angle only, not cloning |
| filing text | — | ⚠ **NOT held** — `filing_documents` is a manifest of URLs, no bodies | needs a fetch pipeline first |
| short volume | — | RegSHO daily flow; ⚠ not short interest, not borrow | proxy only |

---

## 3.0 ⚠⚠ MEASURED ON OUR DATA: insider purchases, and the one thing blocking them

`scripts/verify_2437_insider_forward_returns.py`, 47,369 purchases joined to
prices, entry at the **next** bar after the transaction, against a **matched
same-series control**, year-clustered:

```text
 horizon    events   event ret    control     EXCESS       t  years
     1mo    18,998       3.72%      1.60%      2.12%    0.98      6
     3mo    18,998      12.60%      4.19%      8.41%    1.22      6
     6mo    18,998      18.40%      6.34%     12.05%    1.06      6
    12mo    18,984      28.37%     21.98%      6.39%    0.50      6
```

**Every horizon is the right sign, and the magnitudes are large** — +12.05%
excess over a matched control at six months. ⚠ **And not one reaches |t| = 2.**

### ⚠⚠ The reason is the corpus, not the signal

```text
insider PURCHASES by year, joinable to a price series
  2004-2022      < 250 per year, most years in single digits
  2023         8,456
  2024        15,275
  2025        16,163
  2026         6,997
```

**99% of our insider history is 2023 onward.** The year-clustered test therefore
has **~4 real years**. At n=4 a t-statistic of 1.0 means nothing in either
direction — **we cannot establish the effect and we cannot reject it.**

### The fix is a backfill, not research

`data-sources/sec-edgar.md` §Form 3/4/5: **"XML mandate since 2003-06-30"**,
*"decades of XML coverage"*, and the `<ownershipDocument>` schema *"has been
stable"*.

> ⚠⚠ **~20 years of Form 4 data is available, in a stable schema, with a parser we
> already own — and we are ingesting the last three.** Extending the corpus to
> 2003 takes the sample from ~4 usable years to ~22, which shrinks the standard
> error by roughly `sqrt(22/4) = 2.3x`. **A t of 1.06 becomes ~2.5 if the effect
> is real** — the difference between "cannot tell" and "established".

**This is the highest-value bounded task identified in the entire research pass.**
It is not a bet on a hypothesis; it is removing the reason the best-evidenced
signal we have is currently unmeasurable.

⚠ Read the table as an **upper bound** regardless. It keys on the transaction
date rather than the filing date we could actually have seen; it applies no
routine/opportunistic split, so the informative subset is diluted; it charges **no
costs**; and the corpus is survivor-heavy, which flatters any long signal.

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

## 3.2 ⚠⚠ THE R5 KILL TABLE — 32 candidates already examined and rejected (#2832, 2026-08-22)

**Read this before proposing any strategy family.** After #2827 measured all ten TA
strategies dead at ZERO cost, a 19-agent sweep asked the full-width question — *what can
reliably make money programmatically for this account* — and examined **38 sub-strategies.
Six survived as preregistered spikes; 32 were killed with reasons.** Those reasons are
recorded here so no future session pays to re-derive them.

Full per-verdict record: #2832 and its workflow journal `wf_f24998c8-93a`.

| family | verdict | why |
| --- | --- | --- |
| **Flip the losers** (invert s2/s10) | KILLED | four independent reasons, any one sufficient — see below |
| Intraday / overnight: overnight split, ORB, first-half-hour, gap plays, weekend | KILLED | die at our spreads. ORB was validated at **72–509× lower costs than ours** (§2.13) |
| Crypto / FX systematic: momentum, carry, seasonality | KILLED | ~1%/side crypto spreads; weak or decayed OOS evidence |
| Pairs / statistical arbitrage | KILLED | decayed post-2002; costs; borrow |
| Options income (covered calls, put-writing) | KILLED | **no options on eToro UK** — 0 instruments, no API endpoints (VERIFIED-PORTAL). Packaged ETFs (QYLD, JEPI) demonstrably do not preserve the premium |
| Index adds, spin-offs, buybacks, tenders, lockup expiries | KILLED | decayed post-publication and/or unexecutable at our order types |
| PEAD, generic | KILLED as a family | decay. ⚠ **#2493's specific measured +2.676% long arm stands as its own evidence path** — reconciliation posted there |
| Small/micro-cap "retail capacity" thesis | KILLED | the anomalies live in <\$5 names, where our own 1.45% cost band and the delisting tail eat them |
| CEF discount capture | KILLED | **0 of 12 canonical CEF tickers in our universe** |
| Convertible arbitrage | KILLED | no convertible instruments on the venue |
| FINRA RegSHO daily | KILLED | it is short **VOLUME**, not short interest or borrow — it cannot price the published SI literature |
| Spread-betting tax wrapper | KILLED | not offered on this venue |

### Why "flip the losers" is dead, in full

It is the most natural question to ask about a measured-negative strategy, so it gets the
long answer once:

1. **Inverting after observing the hold-out converts the hold-out into training data.** The
   sign is a parameter like any other; choosing it on the evidence spends the confirmatory
   shot.
2. **Per-trade signal-to-noise is symmetric under a sign flip**, so it cannot close a 5–20×
   deflation miss — the bar does not move when the sign does.
3. **The short leg pays** 1.45% round trip + ~0.02%/day CFD financing over 60-day holds, and
   a mandatory `stopLossRate` in exactly the gappiest names.
4. **The magnitudes disagree with the literature by two orders.** Documented anti-momentum
   is ~−0.4%/**month** (Cooper 2004); s2/s10 measured −3.8 to −6.0%/**trade**. That gap is a
   regime artifact of a signal we ourselves measured as broken (#2797).

**You cannot harvest the inverse of a bug.**

### Deferred with a re-arm condition — NOT killed

- **News / text strategies.** `news_events` is **forward-only from 2026-06-27**, and text
  strategies additionally need filing BODIES — `filing_documents` holds 9.2M URLs and **no
  bodies**. Re-arm condition: **≥24 months of accumulation** *and* a filing-body fetch
  pipeline. Until both hold, any backtest here is measuring a corpus that did not exist.
- **13F crowding / unwind.** `institutional_holdings` holds 8.46M rows over ~20 year-clusters
  — the exact cluster depth the insider family is starved of, and §3 already rates the angle
  viable-but-not-cloning. Deferred rather than killed; the deferral note is on #2832.

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

### Recent-regime and storage contract (#2437)

Do not let a 1962–2026 aggregate answer whether a strategy is viable now. Keep
that row as stress/context evidence and require the code-pinned windows in
`strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS`: primary 2022+, rolling
24/36 months, and each calendar year/YTD. Each window is part of
`ResultIdentity.version`; never accept raw dates from an operator. A strategy
must fail closed when any required recent window is absent or loses its net
expectancy sign. Pre-2000 performance cannot rescue it.

Store only the aggregate result arms. At the current four runnable controls,
the complete recent matrix is 128 rows (8 windows x 4 strategies x 2 ambiguity x
2 quarantine). Only S-4 independently resolves the best/worst ambiguity arms;
the shared, non-level S-1..S-3 measurement is intentionally written under both
arm identities so the physical denominator stays complete. Reuse the daily
corpus during compute; do not persist indicator, position or equity-curve time
series merely to render the picker. Fired signals remain durable;
non-fired/not-evaluable detail is a bounded-retention concern and must have
checked daily aggregates before deletion.

Keep historical backtest, forward observation, paper and live as separate arms.
All fired signals remain visible when unfunded. Missing broker cost semantics,
survivorship, carry, outcomes, controls or ownership produce named refusals; they
never become zero/default inputs.

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

## ⚠⚠ The short side — where our own data actually has an edge (2026-08-09, #2437)

**Shorting was permitted by operator decision on 2026-08-09** (research and paper
trading; leverage still barred until validation). It immediately re-read three
results this project had recorded as failures, and the lesson generalises:

⚠⚠ **A hard constraint does not merely block strategies — it makes findings look
like nulls.** "Buy the one-day loser" lost money monotonically and was written up as
a null three times. It was never a null: an isolated stock that drops hard *keeps
dropping*, which is the strongest and most consistent effect measured on our corpus.
Long-only, that finding had no expression beyond "do not buy", so it was filed as
absence of signal rather than presence of an inverted one.

**The surviving arm — short a >=12% one-day drop, cover after 5 bars, 20% stop**
(`scripts/verify_2437_short_stops.py`, 2020+, day-clustered, 1,364 event days):

```text
stop   gross  median  win%     t  ex-top1%   worst  net@30%/yr
none   216.3   133.9  54.4  5.59      82.5  -41254       108.9
 20%   156.8    74.1  52.3  4.83      77.6   -8749        49.4
 12%   130.4   -68.7  48.1  4.61      81.0   -8749        23.0
  5%    81.4  -500.0  32.2  3.94      46.6   -3429       -26.0
```

**Checks it passed that killed every sibling arm:**

- ⚠ **`ex-top1%` barely moves** (82.5 → 77.6 → 81.0) as the stop tightens. Deleting
  the best 1% of trades is the test for "is this a lottery on collapses". It is not.
  Every **10-day** arm failed exactly here, inverting to negative.
- **Delisting is not the driver.** Our corpus is survivorship-controlled, so a
  backtested short collects in full from names that went to zero. Dying names are
  0.4-1.0% of trades and their mean is *negative* at -12%; excluding them **raises**
  the edge (333 → 341 bps).
- **Survives borrow.** Still +49 bps net at a 30%/yr tier.

⚠⚠ **A stop does not protect a short against a gap.** 141 of 1,402 stops filled at an
OPEN above the level. Worst trade is **-87% even with the 20% stop**, and identical at
12% and 8% — no stop level catches it. Shorts gap against you exactly when the news is
good (takeover, trial result, beat). **Position sizing must survive -87% on one name
regardless of the stop**; that is a concurrency and weight constraint, not a risk note.

⚠ **Tighter is not safer.** Below a 20% stop the median goes negative and by 5% the
win rate is 32%: you are stopped out of trades that would have won — spike, cover,
then the collapse happens without you.

⚠⚠ **Cost model for an eToro short is NOT the long one.** It is a CFD, not stock.
Easy-to-borrow costs spread only; hard-to-borrow (>10%/yr) accrues daily at 21:00 GMT
and **triple at weekends**, so borrow accrues per CALENDAR day. A name that just fell
12% is the archetypal hard-to-borrow candidate, and eToro's docs name "temporary
unavailability of shares to borrow" as a live restriction.

⚠⚠ **Not yet a strategy.** This arm emerged from ~100 tested arms in one session, so
`t 4.83` is a searched-over statistic — register it in `trial_register.py` with the
full search count first. Outstanding: portfolio simulation (per-trade results say
nothing about an equity curve at ~6 concurrent firings/day), eToro borrow
availability at firing time, and a genuine out-of-sample window.

### Why the long side kept failing, stated once

⚠ Unconditional 10-day drift on the liquid universe is **44 bps against a 50 bps round
trip.** The entire long-only short-horizon game is played inside the spread. No
quantity of extra conditions repairs a cost problem — which is why the operator's
shorting decision mattered more than any signal found in the same session: the short
side has 200-440 bps of gross to work with, not 44.
