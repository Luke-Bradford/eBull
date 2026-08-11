# Strategy sizing-rule attribution result (#2430)

Status: full-population research complete on 2026-08-12; no sizing rule
promoted.

## Decision

`equal_weight_concurrent_v1` damages high-turnover strategies because every
entry or exit can resize the entire tradeable book. It is not, however, the
reason the current broad strategy controls lack alpha.

Two causal research arms were measured without changing signals, fills, exits,
costs, quarantine, total-return prices or the matched buy-and-hold benchmark:

1. `entry_weight_drift_v1` sets a causal entry-time allocation and never sells
   an existing position to fund or equalise another one.
2. `calendar_month_end_equal_weight_v1` permits new entries from available cash
   and restores equal weight only on an observed panel month boundary.

Neither arm is a production strategy identity, persistence option, API option
or UI picker. Production remains unchanged. A later sizing v2 requires its own
identity and validation; historical v1 evidence must remain attributable to
v1.

## Full-population evidence

Both comparisons used the pinned `primary-2022-plus` window, 2022-01-01 through
2026-07-08, the `masked` quarantine arm, split/dividend-adjusted wealth, and all
5,266 corpus series (5,264 evaluable per strategy). Position counts, realised
trade counts and matched benchmark returns were identical across each pair.

### Entry-weight drift

| Strategy | Production return | Drift return | Drift vs buy/hold | Drift Sharpe | Drift max DD | Production / drift turnover | Short-funded entries | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| S-1 time-series momentum | -82.94% | -75.28% | -108.58 pp | -1.54 | -77.63% | 86.76 / 71.35 | 92,009 | fails; severe capital starvation |
| S-2 cross-sectional momentum | +47.83% | +92.78% | +19.09 pp | +0.66 | -31.58% | 3.88 / 2.55 | 1,276 | diagnostic improvement, not deployable |
| S-3 mean reversion in trend | -47.43% | -33.00% | -90.25 pp | -0.28 | -45.68% | 46.02 / 20.78 | 1,633 | fails |
| S-4 breakout, best case | -49.14% | -15.97% | -44.39 pp | -0.15 | -36.81% | 33.03 / 19.38 | 12,972 | fails even optimistically |
| S-4 breakout, worst case | -51.93% | -20.75% | -49.17 pp | -0.23 | -39.54% | 33.07 / 19.38 | 12,972 | fails |

The S-2 headline cannot be accepted as alpha. The arm could not fund 1,276 of
6,463 entries, so it changed effective participation by refusing later signals
when cash was already committed. Its Sharpe remained weak and drawdown worsened
slightly. This arm isolates the cost of continuous maintenance; it is not a
viable capital policy.

### Calendar-month-end equal weight

| Strategy | Production return | Monthly return | Monthly vs buy/hold | Monthly Sharpe | Monthly max DD | Production / monthly turnover | Short-funded entries | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| S-1 time-series momentum | -82.94% | -76.86% | -110.16 pp | -1.64 | -78.51% | 86.76 / 76.63 | 54,895 | fails |
| S-2 cross-sectional momentum | +47.83% | +44.91% | -28.78 pp | +0.46 | -30.28% | 3.88 / 4.04 | 123 | fails passive and risk hurdles |
| S-3 mean reversion in trend | -47.43% | -39.90% | -97.15 pp | -0.36 | -47.67% | 46.02 / 24.60 | 1,655 | fails |
| S-4 breakout, best case | -49.14% | -23.13% | -51.55 pp | -0.25 | -43.25% | 33.03 / 22.34 | 14,134 | fails even optimistically |
| S-4 breakout, worst case | -51.93% | -27.42% | -55.84 pp | -0.33 | -45.64% | 33.07 / 22.34 | 14,137 | fails |

Monthly maintenance sharply reduced S-3/S-4 turnover and rebalance cost, yet
did not make either strategy profitable. It reduced S-1's loss only modestly
and left extreme turnover. S-2 funded more signals than production but returned
2.92 percentage points less, remained 28.78 points behind its matched passive
book and had Sharpe 0.46. No arm passes a capital gate.

## Boundary correction

The first monthly run was deliberately stopped after review found that the
truncated final evaluation date (2026-07-08) was being labelled a month-end.
That would create a synthetic rebalance with no forward holding period. The
rule now schedules a rebalance only when the next observed panel date belongs
to a different month. A regression test freezes this causal boundary before
the full result above was produced.

## What this settles

- Event-driven equalisation is an unsuitable default for broad, high-turnover
  books and must be reconsidered before any future strategy is promoted.
- Removing that churn does not rescue the four current controls. S-1, S-3 and
  S-4 fail at the signal/exit level; S-2 remains below its passive hurdle with
  weak risk-adjusted performance.
- A return improvement caused by funding fewer signals is not strategy alpha.
- These four entries remain harness controls, not choices to which a user
  should allocate capital.
- The next alpha work should remain on pre-registered conditional mechanisms,
  regime/cohort discrimination and prospective calibration. It should not tune
  these four broad controls or their sizing until one happens to look good.

## Reproduction and storage

The read-only verifier is:

```bash
PYTHONPATH=. uv run python scripts/verify_2430_sizing_rule_ab.py \
  --window primary-2022-plus
```

Pass `--rules equal_weight_concurrent_v1 calendar_month_end_equal_weight_v1`
to reproduce the exact monthly comparison above. The verifier refuses unknown,
duplicate or non-production-first arms and fails on any change in result keys,
position count, trade count or matched benchmark.

No result row, event row or bar row is written. This work adds no table, index,
column or retained market-data payload, so database size and runtime API queries
are unaffected.
