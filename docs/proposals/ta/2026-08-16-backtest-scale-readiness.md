# Backtest scale readiness — #2772

## Decision

A full 1,000-member synthetic-control invocation is not allowed to fan out on
faith. It first computes exact member indices `0..2` through the production
engine, emits timing only, and projects the remainder at the configured worker
count with a 1.5x safety factor. The pilot members remain members of the final
cohort; their seeds and outcomes are neither discarded nor redrawn.

The launch is refused when either bound would be crossed:

- one projected cohort: 15 minutes;
- cumulative projected controls in one backtest invocation: four hours.

These are operational resource bounds, not estimator parameters. Refusal
changes no strategy or cohort identity and publishes no performance value.

## Compute representation

PostgreSQL remains the authority for admitted bars, quarantine decisions and
result storage. During an arm pass, `CohortCollector` converts the already-read
eligible bars into immutable NumPy arrays. Production cohort members now store
one source index per leg and reference those arrays rather than copying each
leg's complete mark history. The old flat-mark `LegBook` path remains the slow
reference implementation used by the metric-axis legacy arm and differential
tests.

The optimized and reference layouts share the same equity-curve walker. Tests
require identical member entries, exits, net prices, trade returns, date lists,
equity, invested capital, open counts, traded notional and all curve counters.
Member seeds remain the exact `member_seed(index)` mapping.

## Fixed scale curve

Run:

```bash
uv run python -m scripts.benchmark_2772_synthetic_control_scale \
  --output /tmp/ebull-2772-scale.json
```

The digest-bound constructed matrix is fixed at:

| case | placement series | trades/member | members |
| --- | ---: | ---: | ---: |
| wiring | 8 | 128 | 1 |
| small | 32 | 2,048 | 2 |
| medium | 64 | 16,384 | 3 |
| scale | 128 | 131,072 | 3 |

Each case runs reference then shared-mark storage over identical member indices.
The report contains fixture digest, query count (zero for this pure compute
stage), decoded bars, placement and trade counts, wall time, CPU time, throughput,
peak process RSS and reference equivalence. It deliberately contains no return,
Sharpe, drawdown, profitability or strategy verdict.

Initial local baseline on 2026-08-16 (Apple Silicon development host):

| case | reference wall | shared-mark wall | exact equivalent |
| --- | ---: | ---: | --- |
| wiring | 0.0215s | 0.0008s | yes |
| small | 0.0119s | 0.0092s | yes |
| medium | 0.1315s | 0.1018s | yes |
| scale | 1.0817s | 0.8010s | yes |

The timings establish direction and wiring, not a full-corpus forecast. The
production pilot measures the actual collector because trade count, holding
spans and event concurrency dominate and are intentionally absent from the
constructed fixture's market interpretation.

## Remaining performance boundary

This change removes the largest per-member mark-copy amplification. It does not
yet eliminate the strategy-by-strategy `research_price_daily` fetch and Decimal
object reconstruction. A digest-bound Arrow/Parquet or equivalent corpus
snapshot may remove that repeated read, but it must first be differentially
proved against `load_arms`; switching numeric representation without that proof
could change signal thresholds. Until then, the production pilot and cumulative
budget are the fail-closed boundary: the full run may proceed only if the actual
work fits, otherwise the snapshot/read-layout work remains required.

## Workflow and cadence

1. deterministic correctness fixtures;
2. fixed wiring case;
3. all four scale cases;
4. production three-member launch pilot and budget decision;
5. complete the frozen 1,000-member cohort only after admission;
6. structural audit before outcome access;
7. holdout and prospective paper observation only under their registered gates.

A failed correctness or scale gate stops the run. A trigger or strategy change
after reading an outcome is a new strategy version and declared trial. Full
historical reruns are event-driven by a frozen strategy, corpus, cost, rule or
window change; routine monitoring uses incremental forward/shadow/paper ledgers.
