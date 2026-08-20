# Opportunistic insider-purchase measured verdict

Status: measured once and rejected for capital promotion under #2480.
Preregistration: `2026-08-10-insider-purchase-preregistration.md`.

## Frozen identity

- trial: `form4-code-p-opportunistic-purchase-v1`
- SEC archives: `2019q1_form345.zip` through `2026q1_form345.zip` (29
  contiguous quarters)
- archive manifest SHA-256:
  `0531975fae43fa401cfdf26da42d50e383e20e0e5b72c7ed8ff5bcf8b9087d9f`
- last complete portfolio month: 2026-04
- source-classified purchases: 9,317
- research-resolved classified purchases: 5,427
- deduplicated firm-month signals: 2,839
- complete primary portfolio months: 49

## Sealed result

The purchase-value-weighted long-opportunistic/short-routine primary spread
averaged **+1.192% per month**, but its block-bootstrap 95% interval was
**[-1.452%, +4.215%]**. The lower bound is not positive, so the primary effect
gate fails.

The timing-matched placebo averaged **+1.992% per month**. On the 46 months in
which both portfolios were defined, primary minus placebo was **-0.721%** with
a 95% interval of **[-5.825%, +3.930%]**. The control gate fails.

Recent evidence is worse than the older full-period mean:

| Window | Months | Mean/month | 95% interval | Win rate | Profit factor |
|---|---:|---:|---:|---:|---:|
| trailing 36 | 34 | -0.660% | [-3.581%, +2.468%] | 38.2% | 0.831 |
| trailing 24 | 22 | -0.381% | [-3.170%, +3.090%] | 36.4% | 0.892 |
| 2024 | 10 | -4.512% | [-9.178%, -0.112%] | 10.0% | 0.270 |
| 2025 | 12 | +1.334% | [-2.369%, +5.727%] | 50.0% | 1.524 |
| 2026 YTD | 4 | -3.094% | [-8.589%, +2.402%] | 25.0% | 0.312 |

Full-period maximum drawdown was **-54.84%**, expected shortfall at 5% was
**-14.29%**, and the worst month was **-14.89%**. Purchase-dollar weights
reached **100% in one firm**, exposing unacceptable concentration. The
equal-weight spread was **-1.148% per month**, so the favourable full-period
weighted point estimate is not broad-based.

## Decision

`capital_candidate = false`. No manifest row, strategy-picker entry, bracket,
forward resolver or allocation authority is created.

The measured trial stays in the global register. It may not be rescued by
searching role, purchase-size, filing-lag, weight-cap, holding-horizon, stop or
target variants on the same interval. A materially new hypothesis needs a new
preregistration and a genuinely forward cohort.

Independent promotion blockers also remain: survivor-only price history,
missing dividend total returns, historical rather than broker-proven costs,
live exact-acceptance coverage, no registered tail bound, no bracket and no
forward-shadow evidence.

## Storage impact

No application table was expanded. The 296 MB compressed SEC history was read
from a transient source cache; only this aggregate verdict and the trial
declaration are retained in git. No per-bar indicators, polling snapshots,
signals or outcomes were inserted.
