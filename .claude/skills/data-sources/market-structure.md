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
| **No benchmark or sector series** | `SPY QQQ IWM DIA VTI XLK XLF XLE GLD TLT` → **0 rows** in `research_price_series` | **beta · relative strength · sector rotation · pairs · market-regime conditioning · "did it beat the market that day"** — the entire cross-asset half |
| **Split-adjusted only** | `adjustment_basis = 'split_adjusted'` on **7,693 of 7,693** series | total return; dividend-sensitive momentum. A 12-month lookback systematically understates high-yield names |
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
