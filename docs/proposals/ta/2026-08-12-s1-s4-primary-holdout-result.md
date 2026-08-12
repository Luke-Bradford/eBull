# S1-S4 primary holdout result

Date: 2026-08-12  
Parent: #2469  
Window: 2022-01-01 through 2026-07-08  
Status: retained rejection evidence; recent-window refresh continues

## Decision

None of S1-S4 is an investable strategy. They remain permanent
`harness_validation` controls and must never receive a capital toggle. This is
not a judgement based on the earlier dashboard success percentage: it is the
first completed window under the current immutable identities, total-return
accounting and audited holdout namespace.

The sanctioned holdout reader recorded one access per strategy under
`codex-#2469`, with purpose `inspect completed primary-2022-plus control
evidence and report all failures before candidate design`. The audited strategy
versions were:

- S1 `strategy-registry-v1+67dbf07c9d72`;
- S2 `strategy-registry-v1+83967fcb1fca`;
- S3 `strategy-registry-v1+d58989368716`;
- S4 `strategy-registry-v1+91aadde63f07`.

## Representative adverse result

The table quotes the masked, worst-case ambiguity arm. S2's ambiguity labels
are numerically immaterial in this window; S4's adverse arm is deliberately
shown. Percentages are percentage points.

| control | trades | net expectancy / trade | 95% clustered interval | profit factor | Sharpe | max drawdown | total return | return vs buy-and-hold | exposure | annual turnover |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| S1 time-series momentum | 583,848 | -1.400% | [-1.557%, -1.239%] | 0.403 | -5.972 | -99.32% | -99.33% | -131.54% | 100.00% | 91.75x |
| S2 cross-sectional momentum | 6,005 | +0.556% | [-1.689%, +2.785%] | 1.048 | +0.362 | -31.22% | +28.68% | -43.68% | 100.00% | 3.88x |
| S3 mean reversion in trend | 4,142 | -0.317% | [-1.082%, +0.419%] | 0.909 | -1.991 | -91.31% | -91.31% | -147.23% | 96.38% | 46.93x |
| S4 volatility-compression breakout | 56,824 | -0.861% | [-1.650%, -0.126%] | 0.851 | -1.982 | -85.59% | -84.51% | -111.87% | 100.00% | 33.95x |

All rows use a survivor-only historical universe and retain
`carry_unmodelled = true`. Those defects can only bar promotion; they do not
turn the negative controls into hidden candidates. All four also lack the
required random-entry synthetic-control result and immutable portfolio
promotion evidence.

## What separates the least-bad result

S2 is not a successful strategy. Its positive sample mean is small relative to
uncertainty, its Deflated Sharpe is effectively zero after the declared 122
trials, its 31% drawdown is material, and buying and holding the same comparator
returned 72.36% against S2's 28.68%.

Its main difference is lower turnover, not a higher count of confirming
indicators. S1 and S4 trade so frequently that adverse execution economics
overwhelm them; S3 combines a slightly negative trade distribution with nearly
47 portfolio turns per year. All four stay almost continuously exposed, so
they are not the sparse, high-conviction decision engine the product requires.

The arm comparisons also reject data-cleaning ambiguity as an explanation:

- S1 changes from -1.400% to -1.398% expectancy when quarantined series are
  admitted;
- S2 is unchanged at +0.556%;
- S3 improves only from -0.317% to -0.300%;
- S4 remains negative across every arm, approximately -0.797% to -0.861%.

The difference between viable and non-viable is therefore not “one more star.”
It is an independent economic mechanism that survives causal timing, adverse
costs, a sparse decision boundary, clustered uncertainty, null challengers,
finite-capital construction and later prospective execution.

## Product and research consequence

The Strategies page may expose these controls only inside collapsed research
evidence. They are not selectable strategies, should not contribute to the
operator's portfolio success rate and cannot be enabled through legacy
allocation state.

The remaining seven recent-regime windows continue under run 91364. They may
show when a control fails less badly or detect a harness regression; they cannot
promote a control whose purpose is permanently `harness_validation`. New alpha
hypotheses receive separate preregistered identities, starting with the merged
Schedule 13D public-catalyst falsification in #2584.
