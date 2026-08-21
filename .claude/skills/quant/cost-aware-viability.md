# Cost-aware strategy viability — the arithmetic that decides before a backtest does

## When to use

Before proposing, backtesting, defending or promoting ANY strategy, and before
reading any backtest result. Read with `strategy-evidence.md`, which covers whether
a signal family replicates at all; this file covers whether a signal that DOES
replicate can survive being traded, and which numbers are allowed to answer that.

⚠ This exists because on 2026-08-21 all ten strategies were measured on hold-out and
every one lost money per trade, and the session's first reading of that table was
**"two strategies work"** — from CAGR and annualised Sharpe, on a distribution with
kurtosis 14,702. Both numbers were real. Both were the wrong ones to read.

---

## 1. The pre-backtest gate: turnover

**Rule (Novy-Marx & Velikov 2016, "A Taxonomy of Anomalies and Their Trading Costs",
Review of Financial Studies 29(1)):** strategies with turnover above roughly
**50%/month** rarely survive their own trading costs, and the cost of an anomaly
scales with its turnover far more than with its gross alpha.

This is one stored column. It disqualifies faster than any backtest, and it must be
checked FIRST.

```
turnover_per_month = turnover_annualised / 12
```

⚠ Measured 2026-08-21: the LOWEST of our ten was s2 at **44%/month**; the rest ran
68% to **696%** (s1 turns the portfolio 83x/yr). All ten were above or at the bar
before a single backtest was run.

---

## 2. Break-even arithmetic — derived, not cited

Net alpha is gross alpha minus what trading costs:

```
net_alpha ≈ gross_alpha − turnover_annualised × round_trip_cost
```

Two forms of the same equation, and both are decision-grade:

```
max_viable_turnover = gross_alpha / round_trip_cost      # how often you may trade
min_gross_edge_per_trade = round_trip_cost               # what one trade must clear
```

⚠ **`min_gross_edge_per_trade = round_trip_cost` is the whole game.** A strategy whose
average trade does not clear one round trip is not a weak strategy, it is a losing
one, however good its equity curve looks.

Round-trip cost is NOT a guess here — `cost_model.BANDS` carries the p75 in-session
spread actually calibrated on our universe:

| price band | round-trip |
| --- | ---: |
| <$5 | 1.450% |
| $5–20 | 0.571% |
| $20–100 | 0.509% |
| ≥$100 | 0.322% |

⚠ p75 is deliberately pessimistic (75th percentile, not median). Do not "correct" it
downward to make a strategy work — that is fitting the cost model to the result.

⚠⚠ **A per-trade expectancy that sits at roughly minus-one-round-trip across SEVERAL
structurally different strategies is a cost signature, not N independent failures.**
Measured 2026-08-21: expectancies clustered at -0.83% to -1.76% across momentum,
mean-reversion, breakout, cross-sectional and range strategies. Independent signal
failures do not cluster at one magnitude. Read that pattern as "breaking even gross,
losing the spread net" and go measure gross before concluding anything about signal.

---

## 3. Which metrics decide — and which lie

**Decide on these:**

| metric | why it is trustworthy |
| --- | --- |
| `expectancy_per_trade_pct` | the average trade's P&L. Cannot be flattered by compounding or by one outlier's size relative to the horizon. |
| `profit_factor` | gross wins ÷ gross losses. **< 1.0 means the strategy loses money, full stop.** |
| `dsr_trade_sharpe` | per-trade Sharpe, the input the deflation actually consumes |
| `deflated_sharpe` | trials-adjusted; the promotion gate |

**Do NOT decide on these:**

| metric | how it lies |
| --- | --- |
| `cagr_pct` | compounding is dominated by the largest winners. A negative-expectancy strategy with a fat right tail posts a large positive CAGR. |
| `sharpe` (annualised) | assumes returns are roughly iid and symmetric. Under skew 36–102 and kurtosis 1,976–14,702 it is not measuring what its name implies. |
| `sortino` | same objection; a high Sortino next to a negative per-trade expectancy means the LOSSES are small and frequent while the wins are rare and huge — a lottery profile, not an edge. |
| `return_vs_buy_and_hold_pct` | inherits every problem above, and over a long window is dominated by the benchmark's own compounding. |

⚠⚠ **The 2026-08-21 worked example, kept because it is the exact trap:**

| | s8-range-mean-reversion | s5-support-bounce |
| --- | ---: | ---: |
| CAGR | **+45.0%** | **+28.7%** |
| annualised Sharpe | +0.85 | +0.58 |
| Sortino | 4.16 | 5.48 |
| **expectancy per trade** | **-0.96%** | **-1.12%** |
| **profit factor** | **0.83** | **0.80** |
| skew / kurtosis | 36.5 / 1,976 | 101.6 / 14,702 |
| deflated Sharpe | ~0 | **7.1e-160** |

Every number in the top half says "this works". Every number in the bottom half says
"this loses money on the average trade". **The bottom half is right.**

---

## 4. The deflation bar, and reading it correctly

`deflated_sharpe` (Bailey & López de Prado 2014, "The Deflated Sharpe Ratio",
Journal of Portfolio Management 40(5)) adjusts for how many strategies were tried.

⚠ The bar is NOT the declared trial count. Declared searches collapse to
`dsr_independent_trials` once the measured cross-trial correlation is applied. Read
`dsr_expected_max_sharpe` — that is the null's expected maximum, and it is the number
to beat.

⚠ Measured 2026-08-21: 274 declared searches → **`E[max SR] = 0.275`** with
`dsr_independent_trials = 223`, `measured_trials = 10`. That bar is ORDINARY and
reachable. Do not describe the deflation as structurally unpassable — when nothing
passes it, that is a fact about the strategies.

⚠ `dsr_independent_trials` moves with how many trials THIS run measured. Comparing a
DSR from a 3-trial run against one from a 10-trial run compares two different bars.

---

## 5. The order of operations

1. **Turnover.** Above ~50%/month → stop. Do not backtest it.
2. **Break-even.** Is the expected gross edge per trade larger than one round trip in
   the price bands the strategy will actually trade? If not → stop.
3. **Backtest, then read `expectancy_per_trade_pct` and `profit_factor` FIRST.**
   PF < 1.0 → stop, whatever the CAGR says.
4. **Gross vs net.** If net is negative and roughly one round trip below zero, measure
   gross (`sql/256`'s `gross_return_pct`) before concluding the signal is dead — the
   fix may be turnover rather than signal.
5. **Deflation.** Compare `dsr_trade_sharpe` against `dsr_expected_max_sharpe`.
6. **Synthetic control.** Only for something that has cleared 1–5. §9's control is
   1,000 equity curves per arm; paying it for a strategy that fails step 3 is waste.

---

## 6. Prior probabilities — do not skip this

`strategy-evidence.md` carries the replication literature in full. The two numbers to
keep in mind before proposing anything:

- Hou, Xue & Zhang (2020), "Replicating Anomalies", RFS 33(5): **65–82% of 452
  anomalies fail** under value weighting.
- Harvey, Liu & Zhu (2016), "…and the Cross-Section of Expected Returns", RFS 29(1):
  with a multiple-testing-adjusted bar of **t > 3.0, 9 of 313 factors survive**.

A new strategy idea drawn from the standard anomaly families starts from a low prior.
That is not pessimism, it is the base rate.
