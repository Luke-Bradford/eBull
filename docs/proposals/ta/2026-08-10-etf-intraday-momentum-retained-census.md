# ETF intraday momentum retained-data census

Status: rejected for allocation; read-only gross feasibility result for #2502.
This is not a promoted backtest result and creates no database rows.

## Evaluation identity

- Candidate: `etf-intraday-momentum-v1+0b3804ab4111`
- Active universe: `ETORO-RTH-V2`
- Run date: 2026-08-10
- Source accepted: retained `etoro/<universe_version>/nyse_rth` 30-minute bars only
- Primary instrument: SPY; QQQ and IWM are predeclared robustness checks
- Outcome: 15:30 candle close/open gross return proxy, not a fill or net return

The first census attempt selected no rows because the frozen source label said
bare `etoro`, while the harvester durably stores the universe-versioned source
above. No outcome was loaded. The source contract and candidate hash were
corrected before this run; formula, instruments, dates and thresholds did not
change. The database driver also returns OHLC columns as floats, so the
read-only loader now normalises them to `Decimal` at its boundary.

## Result

| Instrument | Complete sessions | Long fires | Cadence | Gross expectancy | Hit rate | Profit factor | 95% block-bootstrap CI | Worst | ES 5% |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SPY | 20 | 12 | 60.0% | -0.0388% | 33.3% | 0.295 | [-0.0825%, 0.0035%] | -0.1775% | -0.1775% |
| QQQ | 20 | 11 | 55.0% | -0.0765% | 36.4% | 0.338 | [-0.1500%, -0.0097%] | -0.3884% | -0.3884% |
| IWM | 69 | 41 | 59.4% | 0.0201% | 56.1% | 1.241 | [-0.0482%, 0.0914%] | -0.7199% | -0.4648% |

SPY's always-long last-half-hour comparator was -0.0414% gross expectancy
across 20 observations. The published signed SPY diagnostic was -0.0051% with
a 35.0% hit rate and confidence interval [-0.0825%, 0.1089%]. QQQ's long-only
confidence interval is wholly negative. IWM's small positive point estimate is
not distinguishable from zero and its always-long comparator was itself
positive at 0.0137%; robustness instruments cannot rescue a failed primary.

## Data census

- SPY and QQQ cover 2026-07-10 through 2026-08-10. Each has 22 candidate
  sessions, one missing prior full-session close, and one missing final candle
  (the in-progress run date).
- IWM covers 2026-04-17 through 2026-08-10. Of 79 candidate sessions, 69 have
  complete required observations; five lack the final candle, two lack the
  opening candle, and three lack the prior full-session close.
- No missing interval becomes a zero return. It is counted as a refusal.
- No non-fired long-only session becomes a trade. Published short outcomes are
  reported only in the separate diagnostic.

## Decision and remaining gates

Do not add this candidate to the strategy manifest, make it selectable, or let
it receive demo capital. SPY is below the 60-complete-session descriptive floor
and the 30-fired-observation/six-month prospective minimum. More importantly,
the presently observed primary gross result is negative before spread,
slippage, financing or missed fills. The standing refusals remain:

- `sample_immature`
- `historical_entry_exit_quotes_unavailable`
- `published_short_leg_not_executable`
- `prospective_outcome_interval_missing`

The pre-registration also asks for drawdown, random-sign inference, strength
calibration, month/volatility/volume views and execution stresses. Those must
not be improvised after this outcome was seen. Any filters, thresholds, stop,
target or alternate holding interval now require a new trial identity, a
frozen analysis plan and later untouched data. Retained prospectively observed
quotes are still required before any gross-positive future candidate can make
an after-cost claim.
