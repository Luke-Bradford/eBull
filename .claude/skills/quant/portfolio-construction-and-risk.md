# quant/portfolio-construction-and-risk: turning signals into a book that survives

## When to use

Use this when sizing positions, combining sleeves, setting limits, stress-testing, or reading a strategy's results against SPY.

## Construction

- **Benchmark-relative thinking.** Every active sleeve is judged against SPY total return through two numbers:
  - active return (sleeve minus SPY);
  - tracking error (the volatility of the active return). Their ratio is the IR.

  A realistic skilled IR is 0.3–0.5; a top-decile manager is around 1.0 (Grinold & Kahn, *Active Portfolio Management*). Size expectations accordingly.
- **The fundamental law: IR ≈ IC × √breadth × transfer coefficient.**
  - Breadth (more independent bets) beats concentration.
  - The transfer coefficient falls with every constraint we add between the signal and the weights: long-only, minimum ticket, caps.
  - A 25-name book has an idiosyncratic tracking error of roughly 8% a year against its own universe, so a small edge is invisible for years. Prefer 50–100 names where costs allow.
- **Weighting.**
  - Equal weight is a valid baseline (DeMiguel, Garlappi & Uppal 2009).
  - Inverse-volatility weights equalise risk contributions only when correlations are equal; in general equal risk contribution needs the covariance matrix.
  - Full mean-variance optimisation needs a covariance estimate. Shrink it (Ledoit & Wolf 2004) or do not use it.
  - Pre-register the choice; never retrofit it after a backtest.
- **Neutralise unintended bets.** Rank accounting signals within industry. Cap any sector at about 2× its benchmark weight. Count NULL-sector names as their own bucket. Report beta, size and sector exposure for every book.
- **Turnover control.** A buy/hold band (enter in the top decile, hold until out of the top tercile) is the most effective cost mitigation (Novy-Marx & Velikov 2016).
- **Sleeves.** The engine pot is the sum of:
  - a market core (passive beta);
  - active sleeves (factor tilt, trend);
  - cash reserve.

  Each active sleeve gets an explicit risk budget at declaration. They never compete first-come for one pool.

## Risk limits that act, not just display

- **Entry limits** already exist: concurrency, daily realised loss, drawdown, exposure caps (`strategy_paper_executor._capacities`).
- **What a desk adds:**
  - **A drawdown de-gross ladder**, e.g. cut gross exposure by a fixed fraction at −10%, −15% and −20% of the pot. Without it, a 2008-like fall takes a fully invested book to about −39% while the "25% max drawdown" only stops new buying. This changes the 2026-08-22 settled decision, so it is the operator's call; the arithmetic is on #3612.
  - **Volatility targeting at book level.** Scale gross exposure down toward the mandate's target volatility (`target_volatility_pct` is currently display-only). Unleveraged, it can only scale down from fully invested, so specify the estimator and its lag, rebalance bands and the turnover cost. Evidence of net benefit is strongest for momentum books (Moreira & Muir 2017; Cederburg et al. 2020); forecastable volatility does not by itself improve net returns.
  - **Daily loss on marked P&L**, not realised only.
- **Stress library.** Linear shocks are a floor, not a stress test. Keep:
  - 1987 (−20% in a day);
  - 2000–02;
  - 2008 (SPY −56.5% peak to trough);
  - 2020-03;
  - 2022;
  - a historical replay of current holdings on their own paths.

  Crisis betas of small caps exceed calm betas.
- **Stops.** The operator requires broker-side SL/TP on every non-core position. For slow factor books, use wide volatility-based levels (k ≈ 4 × ATR, TP ≥ 4R). Stops truncate the drift that slow premia earn (Lo & Remorov; `trade-lifecycle.md` §1). The catastrophe levels on the core (−50% / +200%) are not a risk control.
- **Gap risk.** A stop does not protect against an opening gap. Size so a −50% single-name gap is survivable at the book level.

## Attribution

Every readout decomposes a sleeve's return into:

- SPY beta;
- the universe effect (equal-weight universe minus SPY);
- the size, sector and style exposures (FF5 + momentum regression);
- selection.

Without this, a sleeve beating or trailing SPY by a few points is uninterpretable: it may only be its size tilt.

## Whole-engine hurdle

Idle cash and the core weight set the bar for the active sleeves. Example: 50% SPY + 30% active + 20% cash in a year SPY returns 10%. The active sleeve must return about 16.7% for the engine to match SPY. Report the engine-level comparison alongside sleeve results.
