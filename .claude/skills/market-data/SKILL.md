# market-data

## Purpose

Refresh quotes, candles, and price-derived features for covered instruments.

## Inputs

- current quote data\n- historical candle data

## Outputs

- normalized market snapshots\n- rolling return metrics\n- volatility and price trend features

## Rules

- Flag stale quotes\n- Separate raw prices from derived indicators\n- Do not invent missing data

## Failure conditions

- Missing critical source data
- Stale timestamps beyond allowed threshold
- Contradictory evidence without explicit uncertainty handling

## Deliverable format

Return:
- status
- summary
- structured fields
- confidence / uncertainty note where relevant

## Risk-metrics conventions (#591, `app/services/risk_metrics.py`, `risk_v1`)

Price-derived risk stats are computed from the `price_daily.close` chain (instrument + SPY benchmark). Pinned conventions — keep identical across any future risk math:

- **Returns: SIMPLE** (`close[i]/close[i-1] - 1`) between consecutive *surviving* rows. A close is valid iff finite AND `> 0`; an invalid row **breaks the chain** (no gap-spanning synthetic return); the return is keyed to the later close's date.
- **Volatility: annualized realized** = sample std (n−1) of daily returns × `sqrt(TRADING_DAYS)`, `TRADING_DAYS = 252` (a fixed constant — never derived from observed row count). This realized vol is **NOT** the scorer's TA "volatility-regime" term (Bollinger/ATR) — distinct figure, distinct purpose.
- **CAGR: calendar-time** `(final/first)^(365/calendar_days) - 1`, NOT `(252/n_returns)` (the trading-day form silently inflates the figure when the survivor chain has gaps). Calmar's numerator uses the SAME annualizer.
- **Calmar** = `annualized_return / abs(max_drawdown)` (Calmar, NOT Sharpe — eBull has no risk-free-rate series, so an honest Sharpe is impossible; never label it Sharpe). `abs(max_dd)` near 0 → null.
- **Beta vs SPY: OLS on DATE-ALIGNED returns** — pair only dates where BOTH series have a return (intersection of date keys; NEVER positional-zip — holiday gaps differ between an instrument and SPY and a positional zip silently mis-pairs them). `beta = cov/var_m`, `r2 = corr²`; `var_m=0` → beta null; `var_i=0 or var_m=0` → r2 null. No `alpha` in v1.
- **var_5 (empirical VaR)** = type-7 linear-interpolation 5th percentile of daily returns, **signed** (a loss is negative — stored as-is, never abs'd). skew/excess-kurtosis use biased ÷n central moments (Fisher: `kurtosis − 3`); flagged low-sample below ~250 obs.
- **Float island:** skew / excess-kurtosis / percentile computed in float (numpy) then quantized to Decimal at the persistence boundary; everything else (returns/vol/drawdown/beta/CAGR/Calmar) end-to-end Decimal.
- **Windows** 1y/3y/full: standalone metrics use the instrument's valid history *within the window*; benchmark metrics (beta, excess) use the aligned-overlap window. Min-obs: vol/beta need ≥60 returns; annualized figures flag `partial_window` below ~252 returns.
- v1 is **price return, not total return** (dividend-adjusted TR is a filed follow-up). Persisted in the two-layer `instrument_risk_metrics_*` tables; served by `GET /instruments/{symbol}/risk-metrics`.
