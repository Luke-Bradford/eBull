# data-sources/market-structure

## When to use

**MANDATORY before speccing any strategy, indicator, level or pattern.** Read it the moment
a ticket mentions a market-structure term — Fibonacci, support/resistance, VWAP, moving
average, RSI, ATR, Bollinger, swing, breakout, retest, beta, relative strength, volume
confirmation.

It exists because these terms carry two meanings: the one in a trading book, and the one our
code computes. Speccing against the first while the second ships is how an invented
formulation gets past review. `.claude/CLAUDE.md` already carries the precedent — a spec cut
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
| ~~No benchmark or sector series~~ **CLOSED 2026-08-08 (#2398)** | 16 comparators loaded, **102,027 bars**, SPY `1993-01-29 → 2024-09-27`. `vendor = 'icyDenev/Intrader'`, `scripts/ingest_2398_benchmark_series.py`. ⚠ Stored `instrument_id IS NULL` so the `resolution_evidenced` CHECK keeps them out of the validated universe **by construction** — never resolve them to an instrument | nothing. Beta, relative strength, sector rotation and regime conditioning are all computable. ⚠ Coverage stops **2024-09-27**, so anything after that date has no market leg |
| ~~Split-adjusted only~~ **CORRECTED — total return IS available** | `adjustment_basis = 'split_adjusted'` describes **OHLC only**. `adj_close` is split **+ dividend** adjusted. Verified full-population: latest factor `= 1.0` on **7,693/7,693**, no factor `> 1`, monotone increasing except 22 material steps in 9 series (0.12%) | nothing. Use `adj_close` for returns and `close` for price levels. Reading `adjustment_basis` as describing `adj_close` understates what is computable — a mistake this file made in its first version |
| **No intraday bars** | `price_intraday` → **0 rows** | true session VWAP, intraday scalping, any sub-daily entry. `anchored_vwap` over daily bars is a daily approximation, not the intraday benchmark traders mean |
| **No volume-flow indicators** | no OBV / accumulation-distribution anywhere in `app/services` | volume confirmation as a *signal*. Raw volume IS present on all 25,818,944 rows, so these are buildable — just absent |
| Delisting linkage thin | 2 series carry a `delisting_date` | survivorship correction inside the research corpus specifically (the Form 25 register is separate) |

**The benchmark gap is the cheapest high-value fix on the board.** These are the most
available series in existence, and nothing cross-asset can be specced until they land.

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
