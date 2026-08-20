# data-sources/market-structure

## When to use

**MANDATORY before speccing any strategy, indicator, level or pattern.** Read it the moment
a ticket mentions a market-structure term — Fibonacci, support/resistance, VWAP, moving
average, RSI, ATR, Bollinger, swing, breakout, retest, beta, relative strength, volume
confirmation.

It exists because these terms carry two meanings: the one in a trading book, and the one our
code computes. Speccing against the first while the second ships is how an invented
formulation gets past review. (The trading-book meaning itself — each concept's published
source and honest evidence status, plus the chart-read protocol — is owned by
`.claude/skills/market-technician/SKILL.md`; this file owns the mapping to OUR code and the
measurement traps.) `.claude/CLAUDE.md` already carries the precedent — a spec cut
the volatility regime at 20th/80th percentile BandWidth; Bollinger's published rule is the
lowest/highest in **126 trading days**. Caught by Codex, not by any gate.

> **The rule this file enforces:** every market-structure concept ships either with its
> **published formulation cited**, or — where none exists — **fixed by construction with the
> constants frozen in a rule-set version hash**. Never a third option, and never a threshold
> chosen because it looked reasonable.

---

## The map — concept → published source → our implementation → the trap

Everything below is in `app/services/indicator_series.py` (`indicator-series-v1`) or
`app/services/price_structure.py` (`price-structure-v1`, #2279). Both hash their own module
source into a `RULE_SET_VERSION` that is carried into strategy identity, so **changing any
constant here invalidates every stored signal** — deliberately.

| Concept | Published source | Ours | ⚠ The trap |
| --- | --- | --- | --- |
| SMA / EMA | standard; the 50/200 "golden cross" is the conventional pair | `sma_series`, `ema_series` | none material — but the pair is a *convention*, not a result. Do not tune it and call it evidence |
| **RSI** | Wilder, *New Concepts in Technical Trading Systems* (1978), 14-period | `rsi_series` | ⚠⚠ **Wilder smoothing is recursive and causal.** #2260 claimed RSI<30 → 76.8% win; measured with causal Wilder it is **51.8% / 50.4%**, and the claim was withdrawn. Any RSI result computed with a non-causal or simple-average smoother is wrong in the flattering direction |
| **ATR** | Wilder (1978), 14-period, Wilder smoothing — *not* a simple mean | `atr_series`, `atr_window_series`, `ATR_PERIOD = 14` | same smoother trap as RSI |
| **Bollinger / BandWidth** | Bollinger, *Bollinger on Bollinger Bands*, ch. 21 | `bollinger_series`; `BANDWIDTH_WINDOW = 20`, `BANDWIDTH_LOOKBACK = 126` | ⚠⚠ **The Squeeze is BandWidth at its lowest in six months (126 trading days), NOT a percentile cut.** This is the #2279 precedent |
| MACD, Stochastic | Appel; Lane | `macd_series`, `stochastic_series` | standard periods are conventions — same warning as SMA |
| **Swing / pivot** | n-bar fractal | `detect_swings(bars, n)` | ⚠⚠ **LOOK-AHEAD.** A pivot at `i` needs `2n` neighbours, so it is only knowable `n` bars later. S-6's "last swing" was a look-ahead trap for exactly this reason. A plateau yields **no** pivot rather than two — an equal high is the absence of a new extreme, and double-counting inflates a level's touch count, which is the only thing a level asserts |
| **Support / resistance** | ⚠ **no published formulation exists** | `cluster_levels`, single-linkage, `CLUSTER_ATR_K = 0.5` | fixed **by construction** and frozen in the version hash, per the rule above. ATR-relative, not a percentage, because a fixed percentage means different things on a $3 stock and a $600 one, and different things on the same name in 2008 and 2017. **Do not invent a citation for this** |
| **Fibonacci retracement** | ratios from the Fibonacci sequence | `fib_levels`, `select_leg`, `FIB_RATIOS = (0.236, 0.382, 0.5, 0.618, 0.786)` | ⚠⚠ **0.5 is NOT a Fibonacci ratio** — it is Dow Theory's halfway retracement, conventionally included. The constant's name implies a provenance one of its five members does not have. Inherits the swing look-ahead above, since a leg needs two confirmed pivots |
| **Anchored VWAP** | standard execution benchmark | `anchored_vwap` | the **anchor is the entire signal**. An anchor chosen after seeing the outcome is the purest form of the look-ahead this repo keeps catching. Declare the anchor rule causally |
| Break and retest | no single published formulation | `find_break_and_retest`, `classify_interaction` | by construction; same freezing rule as levels |

---

## What we CANNOT compute, and precisely what it blocks

Measured on the dev corpus, 2026-08-08. Re-measure before citing — these move.

| Gap | Evidence | Blocks |
| --- | --- | --- |
| **Recent total-return benchmark** | #2482 adds 18 separate eToro price-return comparators / **18,198 bars** through `2026-07-08`, snapshot `etoro-comparators-2026-07-08-v1`. Their `adj_close` is deliberately NULL: eToro exposes price candles, not dividend-adjusted closes | recent market trend, realised vol, beta and sector-relative **price-return** features are available; a recent total-return benchmark remains unavailable |
| ~~Split-adjusted only~~ **CORRECTED — total return IS available** | `adjustment_basis = 'split_adjusted'` describes **OHLC only**. `adj_close` is split **+ dividend** adjusted. Verified full-population: latest factor `= 1.0` on **7,693/7,693**, no factor `> 1`, monotone increasing except 22 material steps in 9 series (0.12%) | nothing. Use `adj_close` for returns and `close` for price levels. Reading `adjustment_basis` as describing `adj_close` understates what is computable — a mistake this file made in its first version |
| **No retained intraday research corpus** | `price_intraday` → **0 rows**, but eToro REST serves up to 1,000 bars on demand for OneMinute..FourHours | deep walk-forward intraday validation. Short on-demand windows are available; prospective bounded collection is #2477 |
| **No volume-flow indicators** | no OBV / accumulation-distribution anywhere in `app/services` | volume confirmation as a *signal*. Raw volume IS present on all 25,818,944 rows, so these are buildable — just absent |
| Delisting linkage thin | 2 series carry a `delisting_date` | survivorship correction inside the research corpus specifically (the Form 25 register is separate) |

**The recent price-return frontier is now available (#2482).** Do not splice it
onto the frozen dividend-adjusted series: it is a separate immutable identity,
fingerprinted in `research_comparator_snapshots`. The 18-symbol overlap verifier
reports daily-return correlation 0.994657–0.999779 after accounting for State
Street's five official December-2025 2:1 splits. Use `close` for recent
price-return features; refuse any claim requiring a recent total-return series.

---

## Published ≠ profitable — size the decay before choosing a published rule

McLean & Pontiff, *Does Academic Research Destroy Stock Return Predictability?*, Journal of
Finance (2016): portfolio returns are **26% lower out-of-sample and 58% lower
post-publication**, ≈32 points of which is attributed to publication-informed trading.

This is why S-1..S-4 — deliberately chosen as canonical, heavily-replicated anomalies at
untuned parameters — are the right instruments for **validating a harness** and close to the
worst instruments for **making money**. Do not read their marginal results as "the harness
is broken" or "technical analysis does not work". Read them as the expected decay of a
famous signal.

A candidate rule that is published should carry an explicit note on how widely it is
traded. A candidate that is *not* published must instead carry the by-construction treatment
and a full-population verification, because it has no prior evidence at all.

---

## The trial budget — the constraint that governs all research here

Every hypothesis tested spends from a shared budget and **raises the bar for every strategy
we already own**. Measured with our own trial-Sharpe variance (5e-3 full-corpus run, #2240):

```text
independent trials    threshold SR_0    multiple of today
                10         0.083344            1.00x
               250         0.151499            1.82x
             1,000         0.173786            2.09x
           100,000         0.234420            2.81x
```

S-3 currently measures **0.0823** against the 0.0833 bar at ten trials. At a thousand it is
not close — and nothing about S-3 would have changed.

**Therefore: declare the hypothesis and its acceptance criterion to
`app/services/trial_register.py` BEFORE running the test, not after.** Pre-registration is
the same discipline clinical trials use, for the same reason. A pattern search that records
its trials afterwards is indistinguishable from one that reports only its winners.

⚠ The register is a documented **floor** — under-counting `M` raises the DSR, so every
stored value is an upper bound on the honest one.

---

## ⚠⚠ Four traps that killed a real finding on 2026-08-08 — check every one

A research pass measured a near-monotonic overnight mean-reversion across 18.8M bars:
decile 1 (fell 6% intraday) → **+43.9 bps** overnight, decile 10 → −17.5 bps, spread 61.4 bps
against a ~50.9 bps round trip. It looked tradable. **All of it was wrong**, in four separate
ways, and each is a reusable trap.

### 1. The shared-print trap — the one that generated the whole effect

The sort variable **ended** at `close(t)`. The outcome **started** at `close(t)`. Any error
in that single print — a bid-side fill, a stale quote, one tick on a cheap stock — enters
the sort negatively and the outcome positively. **Monotonicity across deciles is the
signature of the artefact, not evidence against it.**

> **Rule: never sort on a variable that terminates at price P and measure an outcome that
> originates at price P.** Put a tradable gap between them.

### 2. The unfillable-window trap

Signals complete at `close(t)`; §3.5 fills at `open(t+1)`. The measured +43.9 bps lived
**entirely inside `close(t) → open(t+1)`** — consumed before a fill can exist. Not a cost
problem, a **timing impossibility**.

Re-measured on what a next-open fill actually earns (`open(t+1) → close(t+1)`), same sort,
1.79M obs per decile, inflated series excluded:

```text
decile   intraday    overnight (unfillable)   FILLABLE       se
     1     -5.63%            +39.19 bps       -7.16 bps    0.39
    10     +5.92%            -13.96 bps       +2.72 bps    0.38
```

**The tradable signal is the opposite sign to the apparent one.** Buying the fallers loses.

> **Rule: measure every outcome from the first price you could actually transact at.**

### 3. Equal-weighted per-bar means are micro-cap means

> ⚠⚠ **This trap has a portfolio-level twin that cost a real number — see "The
> benchmark is a construction" below.** Same population, same mechanism, but it
> reaches you through a *rebalance* rather than through a mean.

A series contributes in proportion to its **bar count**, not its size or tradability. The
same pass concluded "the intraday session has negative expectancy" from a −0.876 bps
corpus-wide mean. By price band, clean series:

```text
band        overnight   intraday    total    ~annual
<$5             2.082     -5.060   -8.689    -21.90%
$5-20           4.414     +0.465   +3.690     +9.30%
$20-100         4.843     +3.044   +7.367    +18.56%
>=$100          7.323     +1.972   +7.887    +19.88%
```

Intraday is negative **only below $5**. The corpus-wide claim was penny-stock print noise
outvoting every tradable name.

> **Rule: stratify by price band and dollar volume before believing any corpus-wide mean.
> And prefer a per-day cross-sectional mean then a time-series mean of those** — same-day
> returns are heavily correlated, so an N of 18.8M bars is really an N of ~5,400 days.

### 4. Stratifying on a back-adjusted price level

The band cut above was itself keyed on back-adjusted `close`, which #2400 shows is
meaningless as a *level* — serial reverse-splitters inflate to `3e17`, forward-splitters
deflate a 1990s large cap under $5. Two findings from one pass silently contradicted each
other.

> **Rule: any analysis that buckets by price level must first exclude or reconstruct
> adjustment-distorted levels.** Returns are safe; levels are not.

**What survived all four:** the overnight drift itself (~4–5 bps/day in liquid names, in
every decile) — which is just the equity risk premium accruing while you hold, and is
captured by holding, not by trading. Its corollary is worth carrying into strategy design:
**every hour out of the market forfeits drift**, so `exposure_time_pct` is not "is my cash
idle" but "how much of the premium am I giving up", and it is the reason
`return_vs_buy_and_hold_pct` is the catalogue's bar.

## The actual formulas — what we compute, exactly

⚠ Transcribed from the code, not from a textbook. Every constant below is hashed
into `RULE_SET_VERSION`, so changing one invalidates every dependent signal.

### Peaks and troughs — `detect_swings(bars, n)`

A pivot high at `i` requires `high[i]` **strictly greater** than all `2n`
neighbours; a pivot low, strictly less.

```text
pivot_high(i)  <=>  high[i] > high[j]  for all j in [i-n, i+n], j != i
```

⚠⚠ **Strictly.** A plateau — two equal highs in one window — yields **no** pivot
rather than two. An equal high is the absence of a new extreme, and emitting
both inflates a level's touch count, which is the only thing a level asserts.

⚠⚠ **A pivot at `i` is knowable only at `i + n`.** It needs `n` right-hand bars.
"The last swing low" is not available in real time and this is the look-ahead
that caught S-6.

`SWING_LADDER` = `{short: 5, medium: 21, long: 63}` — roughly a week, a month, a
quarter either side. ⚠ The docstring is explicit that N *can* be fitted; the
claim is only that this ladder was not fitted here.

### Support and resistance — `cluster_levels`

**Single-linkage agglomeration on price**, tolerance ATR-relative:

```text
merge two swings into one level  <=>  |p_a - p_b|  <=  k * ATR14(at the later bar)
k = CLUSTER_ATR_K = 0.5          (half a day's true range)
```

⚠ Highs and lows cluster **separately** — a level asserts which side price
approached from, and merging the two makes that unstateable. `touches` is
emitted, never filtered on. ⚠ **No published formulation exists**; fixed by
construction, ATR-relative rather than a percentage because a fixed percentage
means different things on a \$3 stock and a \$600 one, and different things on
the same name in 2008 and 2017.

### Fibonacci — `select_leg` then `fib_levels`

**Leg selection** (deterministic, because fractals do NOT alternate — three
highs in a row with no low between is ordinary): take the **last swing** as the
leg end, then walk back to the **most recent swing of the opposite kind**.

**The arithmetic depends on direction**, and an early spec draft omitted this —
two anchors alone do not say whether you measure down from the high or up from
the low:

```text
span = high - low
up-leg   (ends on a high):   level(r) = high - r * span
down-leg (ends on a low):    level(r) = low  + r * span

FIB_RATIOS = (0.236, 0.382, 0.5, 0.618, 0.786)
```

⚠ **0.5 is not a Fibonacci ratio** — it is Dow Theory's halfway retracement,
conventionally included. The constant's name implies a provenance one of its five
members does not have.

⚠ `usable_from_index` is the **later of the two anchors' confirmations** — the
retracement is not knowable before it, whatever the arithmetic says.

### Volatility and the band width

```text
ATR      Wilder smoothing, period 14 — NOT a simple mean
Bollinger  SMA(20) +/- k*sigma;  BandWidth = (upper - lower) / middle
Squeeze  BandWidth at its LOWEST in 126 trading days (six months)
```

⚠ The Squeeze is a six-month extreme, **not a percentile cut** — the #2279
precedent, caught by Codex.

### Momentum

```text
RSI      100 - 100/(1 + RS),  RS = avg gain / avg loss, WILDER-smoothed (recursive, causal)
MACD     EMA12 - EMA26;  signal = EMA9(MACD);  histogram = MACD - signal
Stoch    (close - low_n) / (high_n - low_n) * 100
```

⚠⚠ Wilder smoothing is **recursive and causal**. #2260 claimed RSI<30 → 76.8%
win; with causal Wilder it is **51.8% / 50.4%** and the claim was withdrawn. Any
RSI result from a non-causal smoother is wrong in the flattering direction.

### Liquidity / order-flow footprint

```text
Amihud illiquidity = mean( |return| / dollar volume )
```

⚠ Daily data only, no tape. Proxies price impact per unit of order flow (the
empirical cousin of Kyle's λ). **Not implemented yet** — but computable on all
25.9M bars with nothing new ingested, and it is also the quantity behind
Wyckoff's "effort versus result".

---

## ⚠⚠ What we do NOT have: projection

**Where a reversal might LEAD has no implementation at all.** Grepped
2026-08-09: no Fibonacci extensions (1.272, 1.618, 2.618), no measured moves, no
point-and-figure counts, no swing projections. Nothing in
`price_structure.py` or `indicator_series.py` computes a price target.

That is the asymmetry to be aware of: **we can find structure, and we cannot
project from it.** Concretely missing —

| concept | the maths | why it matters |
| --- | --- | --- |
| Fib extension | `level(r) = end +/- r * span`, `r in (1.272, 1.618, 2.618)` | the conventional target after a retracement holds |
| measured move | project the prior leg's span from the breakout point | the flag/pennant target |
| P&F count | horizontal congestion width x box size x reversal | ⚠ Wyckoff's Law of Cause and Effect rests entirely on this, and it is Buying Test #1 |
| ATR-multiple stop/target | `entry +/- m * ATR14` | ⚠ **the one that blocks S-4** — `ExitLevels` exists in `outcome_resolver.py` and nothing computes one |

⚠ The ATR-multiple row is not a research question. It is a small piece of
arithmetic standing between S-4 and being runnable, and the same primitive gives
every other strategy a principled stop instead of none.

---

## Before speccing a new pattern — the checklist

1. **Name the published formulation, or state that none exists.** No third option.
2. If none exists, **fix it by construction**, say why the construction is what it is, and
   freeze the constants in the module's `RULE_SET_VERSION`.
3. **Prove it is causal.** Can it be computed with bars up to and including the decision bar
   only? Pivots, legs, anchors and "the last swing" all fail this by default.
4. **Declare the trial** before measuring.
5. **Measure on the full population**, never a sample — and report the count it *rejected*,
   not only the count it admitted (criterion 9).
6. **Check it against the gaps table** above. A spec needing beta or intraday is blocked, not
   hard.

---

## The measurement rig that survives — use this, not a fresh one

Built and validated over the 2026-08-08 pass. It killed four candidates and let one
reach a verdict. Every future pattern test should be cut this way:

1. **Fillable windows only.** Signal from `close(t)`, entry at `open(t+1)`, outcome measured
   from that entry. Anything measured across `close(t) → open(t+1)` is unreachable.
2. **Causal trailing beta against SPY.** `regr_slope(ret, mret) OVER (PARTITION BY series
   ORDER BY bar_date ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING)` — the `1 PRECEDING`
   excludes the signal bar. Alpha is `fwd − β·market_fwd`, never a raw return.
3. **Decile WITHIN each day**, then collapse to one observation per day. A cross-sectional
   sort avoids the level drift that a pooled sort inherits.
4. **Day-clustered inference**, then **non-overlapping** periods. A 20-day forward return
   sampled daily shares 19 days with its neighbour; t fell from 50.3 → 17.7 → 5.1 across
   these three corrections on the same effect. **The first two numbers were fiction.**
5. **Stratify on price band before believing any pooled result** — and exclude
   adjustment-inflated series first (#2400).

### The stratification that mattered most

A pooled effect measuring **−22.0 bps (t −1.54)** over 2021-24 was two opposite regimes
cancelling:

```text
$20-50     +40.0   t  2.66
$50-150   -121.8   t -7.12
>$150     -169.4   t -7.01
```

> **Rule: a near-zero pooled result is not evidence of no effect.** Stratify on price and
> liquidity before concluding anything, in either direction.

### Liquidity provision — the one published frame that fitted

**Nagel, "Evaporating Liquidity", *Review of Financial Studies* 25(7), 2012, 2005-2039.**
Short-term reversal returns proxy the return to **liquidity provision**, and are *"highly
predictable with the VIX"*. Reproduced here across the full sample — alpha by trailing SPY
realised-vol quartile, day-clustered, n=4,904: **56.6 / 57.0 / 79.8 / 184.7 bps**, monotone.

⚠ And it still did not save the candidate: the vol conditioning holds across the sample yet
fails to explain the 2021-24 inversion, which is a **price-segment** effect. A correct
published mechanism can be reproduced and still be the wrong axis for the decision.

⚠ **Economically dead where it is statistically alive.** The surviving cell pays +40.0 bps
gross per 20-day hold against a 50.9 bps round trip. A t-statistic is not an edge; the
spread is the hurdle.

---

## ⚠⚠ The benchmark is a CONSTRUCTION, and "buy-and-hold" is not self-defining (#2426)

`return_vs_buy_and_hold_pct` is the catalogue's own bar — criterion 7: *"a
strategy that fails to beat buy-and-hold after costs is not a strategy"* — so
the comparator's construction decides every verdict built on it. It shipped
wrong, read **33,706,844.28%**, and the reason is worth carrying.

**What happened.** The benchmark legs were right (one per evaluated instrument,
first usable bar to last). They were then run through `build_equity_curve`, the
strategy engine — on the correct argument that sharing the **cost model** and the
**fill contract** is what stops the machinery's difference being attributed to
the strategy. But the engine also carries `SIZING_RULE_ID`, which **re-imposes
equal weight on every event date**. So the "buy-and-hold" portfolio traded.

**The source rule, which exists.** Blume & Stambaugh, *"Biases in computed
returns: An application to the size effect"*, **JFE 12 (1983), 387-404**:
rebalancing trades into the bid-ask noise in each closing print, and *"returns
computed for buy-and-hold portfolios largely avoid the bias induced by closing
prices"*. They measure it **fifty times larger on small firms** — 0.056%/day on
the small-firm decile against 0.001% on the large-firm decile — and the published
size effect halves when recomputed buy-and-hold. Corroborating: Canina, Michaely,
Thaler & Womack, *"Caveat Compounder"*, **JF 53(1) (1998), 403-416**, ~0.43%/month
compounding the daily EW index, *"large enough to reverse the conclusions"*.

**Measured here**, full population, identical legs, identical cost model
(`scripts/verify_2426_benchmark.py --compositions`):

```text
                                     total return       CAGR   traded notional
rebalanced (equal_weight_concurrent) 1,204,631,084%    28.754%  137,477,862x
buy-and-hold (equal_weight_buy_and_hold)     3,223%     5.581%           34x
```

**23.2 points of annual return, manufactured by rebalancing** — on our panel
specifically, because it is 5,266 predominantly small and delisted US names.

### The rules this leaves

1. **A buy-and-hold comparator is committed once and never rebalanced.** Not a
   preference. If a proposed benchmark "rebalances on the strategy's cadence",
   that is the biased construction wearing a like-for-like justification.
2. **Reusing an engine imports its POLICY as well as its plumbing.** Cost model
   and fill contract are plumbing. A sizing rule is policy — and policy is the
   thing the comparison exists to isolate. Say which of a shared component's
   decisions you meant to inherit.
3. **The comparator belongs on the result identity hash**
   (`ResultIdentity.benchmark_rule`, `equity_curve.BENCHMARK_RULE_ID`). A bar
   that can change without the version moving is a bar that can be tuned
   invisibly — the same argument §5.4 already makes for the sizing rule.
4. **On an unbalanced panel, no published rule covers listing/delisting**, so it
   is fixed by construction and frozen: `1/N` of the starting pot at each
   instrument's first usable bar, held to its last, proceeds to cash, cash earns
   0. ⚠ CRSP's equal-weighted index does **not** govern — it redistributes to
   survivors, which is a rebalance — and its delisting-return rule needs a field
   our corpus does not carry.
5. ⚠ **SPY cannot be the primary bar.** #2398 loaded it from **1993-01-29**; the
   in-sample axis starts **1962-01-02**. Secondary comparator on hold-out only.
6. **The engine's wealth accounting is total-return since #2429.**
   `load_masked_series` carries raw OHLC and an aligned `adj_close` wealth
   series. Raw prices remain authoritative for signals, fills, spread bands and
   TP/SL; both strategy and benchmark returns use the wealth series. Existing
   price-return rows retain their v1 identity and cannot satisfy a current v2
   evidence query.

⚠ **And the diagnostic lesson**: #2426 was filed as *"per-instrument returns are
being summed"*, from `33,706,844 / 3,541 ≈ 9,519`. Numerically plausible,
mechanically impossible — nothing on the path adds return percentages, and the
measured sum was **0.0141×** the stored figure. **A plausible ratio is a
hypothesis about a mechanism. Read the code path before you accept it.**
