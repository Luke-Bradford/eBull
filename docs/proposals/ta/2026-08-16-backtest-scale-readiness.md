# Backtest scale readiness — #2772

## Decision

A full 1,000-member synthetic-control invocation is not allowed to fan out on
faith. It first computes exact member indices `0..2` through the production
engine, emits timing only, and projects the remainder at the configured worker
count with a 1.5x safety factor. The pilot members remain members of the final
cohort; their seeds and outcomes are neither discarded nor redrawn.

The launch is refused when either bound would be crossed:

- one projected cohort: 20 minutes;
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

The optimized shared-mark layout uses a Numba-compiled event/mark walker; the
flat-mark layout remains the independent Python reference. The kernel keeps
stable leg ordering, sequential floating-point operations and the exact
exits-before-entries event order; it does not enable `fastmath`. Tests require
identical member entries, exits, net prices, trade returns, date lists, equity,
invested capital, open counts, traded notional and all curve counters. Member
seeds remain the exact `member_seed(index)` mapping.

The immutable collector arrays are packed into four named shared-memory blocks
and attached read-only by spawned workers. The scale gate counts those blocks
once, then adds a conservative private-process allowance for every child. This
avoids repeated pickling and avoids counting the same shared pages once per
worker when projecting unique memory.

Compact storage retains the reference builder's fail-closed span boundary.
Construction rejects a leg that closes before it opens, starts before its mark
source, ends after that source, or supplies a non-columnar source. This check is
required because NumPy accepts a negative index by wrapping to the opposite end
of the array; relying on the later walker to fail could therefore produce a
finite but incorrect curve. Adversarial tests cover both bounds and an interior
missing mark, and the equity/statistics mutation harness proves removal of each
new guard is observed.

## Durable match-quality gate

A synthetic-control outcome is not promotion evidence unless the stored row
also says what population it actually matched. Each new control therefore
carries a versioned match-quality block containing its placement-space id,
matchable trade count, cohort mean trade count, reasoned exclusion census,
no-slack series count, placed-series count, and the strategy/cohort exposure and
annualised turnover measurements.

`synthetic-control-exact-match-v1` admits exact equality only. No published
project source defines a favourable tolerance, so adding one later requires a
new policy id and review rather than changing old verdicts silently. Promotion
fails closed, with separate reasons, when:

- a legacy control has no durable match evidence;
- the policy id is unknown;
- any realised trade was excluded, any series had no placement slack, or the
  cohort did not preserve trade count;
- exposure differs; or
- annualised turnover differs.

The thirteen-field block is all-or-nothing in PostgreSQL. Its derived verdict
must agree with its inputs; the application independently re-derives the reason
census and verdict when reading. Strategy-side exposure and turnover are bound
to the result's metric set, just as the existing control binds Sharpe and total
return. Legacy rows remain readable but cannot promote. Non-finite measurements
are rejected before storage and the in-memory exclusion census is immutable
after validation.

This gate is intentionally diagnostic as well as restrictive. A pilot can now
show whether the blocker is population construction, exposure, or turnover
before any 1,000-member fan-out. A non-exact pilot stops; it is evidence to fix
the cohort construction or explicitly specify a reviewed new policy, not a
reason to spend the full run.

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

Current local baseline on 2026-08-17 (Apple Silicon development host, after the
first-call compiler warm-up):

| case | reference wall | shared-mark wall | exact equivalent |
| --- | ---: | ---: | --- |
| wiring | 0.0058s | 0.8292s | yes (includes compilation) |
| small | 0.0105s | 0.0024s | yes |
| medium | 0.1101s | 0.0171s | yes |
| scale | 0.9452s | 0.1299s | yes |

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
could change signal thresholds. The full S-1 preparation currently takes roughly
14–16 minutes and is therefore the next reusable-corpus optimization, but it is
no longer a blocker to the first bounded run. The production pilot and
cumulative budget remain the fail-closed boundary.

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

## Bounded PostgreSQL and worker canaries

The next launch boundary is split deliberately. `run_worker_canary` executes
member indices `0..7` in newly spawned 1-, 2-, 4- and 8-worker pools, requires every
requested process to initialise the real collector, measures startup/transfer,
member wall time and conservative parent-plus-child peak RSS, proves the member
outcomes are exactly invariant internally, and returns no outcomes. It has no
full-cohort argument or continuation path and always stops after the fixed work.

The database half is independently reproducible:

```bash
uv run python -m scripts.verify_2772_research_read_canary --plan
uv run python -m scripts.verify_2772_research_read_canary \
  --expected-selection-digest DIGEST \
  --output /tmp/ebull-2772-read-canary.json
```

The next step uses the genuine evaluator and collector but is still bounded:

```bash
uv run python -m scripts.verify_2772_production_worker_canary \
  --series-limit 32 \
  --output /tmp/ebull-2772-worker-canary.json
```

It runs S-1/masked over at most 100 survivor-only series, intercepts the exact
production `run_cohort` boundary, executes the fixed worker canary and raises a
private completion signal. The process therefore cannot enter the 1,000-member
cohort or the result writer even when the canary succeeds.

Observed production-collector curve before shared memory and compilation on
2026-08-16:

| admitted cap | placement series | trades/member | workers | member wall | throughput | aggregate peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 22 | 18,387 | 1 | 0.396s | 10.09 members/s | 295,714,816 bytes |
| 32 | 22 | 18,387 | 4 | 0.126s | 31.73 members/s | 538,640,384 bytes |
| 100 | 71 | 48,288 | 1 | 0.976s | 4.10 members/s | 339,148,800 bytes |
| 100 | 71 | 48,288 | 4 | 0.301s | 13.31 members/s | 619,560,960 bytes |

Every trial used member indices `0..3`, observed every requested process and
was exactly equivalent. The 100-series command completed in 11.2 seconds and
wrote nothing.

The full-production launch pilot has now exercised S-1/masked over the complete
survivorship-free corpus. It performs no worker fan-out and returns no member
outcomes; the command intercepts the production `run_cohort` boundary and stops
after exact members `0..2`.

| engine | placement series | trades/member | pilot/member | projected cohort (1.5x) | projected unique memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python reference | 10,492 | 4,227,215 | 28.398s | 5,393.858s (89.9m) | 4,647,837,480 bytes |
| compiled shared-mark | 10,492 | 4,227,215 | 5.183s | 984.505s (16.4m) | 4,393,377,576 bytes |

The compiled result is an 81.7% reduction in projected cohort time. It admits
under the calibrated 20-minute cohort and 8 GiB memory ceilings while preserving
the 1.5x projection safety factor and four-hour cumulative bound. The first
production invocation remains S-1 only; it is an operational proof before the
full registered strategy set, not evidence for changing a signal.

The plan query reads series metadata only, selects five evenly spaced bar-count
strata and hashes their IDs, counts and date bounds. The measured invocation is
read-only, refuses a changed digest or more than 100,000 declared bars before a
bar query, calls `load_arms` once per selected series, and stops. It reports
query/bar/resource/shape evidence only.

Local baseline on 2026-08-16 against selection digest
`8ba9077e48f55b2b2129135d12454528ffc88faee3ca7d2797d87f7fcccfabff`:

| corpus series | eligible / fail-closed | selected declared/decoded bars | queries | wall | CPU | process lifetime peak RSS | arm shape |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 30,591 | 30,572 / 19 | 21,470 / 21,470 | 6 | 0.121s | 0.053s | 80,756,736 bytes | identical |

The 19 exclusions have no coverage row for the current quarantine rule set.
They are not silently treated as clean; they remain a visible coverage-refresh
gap and decode zero bars until evaluated.

This bounded result says the sampled PostgreSQL fetch and Decimal/object decode
are not themselves a multi-hour operation. It does **not** extrapolate five
series to the full strategy matrix or authorize the 1,000-member cohort. The
remaining measured gate is the fixed worker canary over a real production
collector; only its 1/2/4-worker time and aggregate-memory curve can decide
whether the production pilot is safe.

## Parked acceptance gap — CLOSED 2026-08-19

The parked task was `scripts/probe_2240_statistics.py` outliving the
compiled/reference split. Measured before the fix, over the twelve equity-curve
probes: five printed `*** BAD ANCHOR ***` because the split had moved their
anchor text, and two printed `*** NOT CAUGHT ***` because they mutated the
compiled kernel while their selectors built a `LegBook` and therefore executed
only the Python reference. Seven of twelve curve probes were proving nothing
while the PR body reported "31/31 caught".

`build_equity_curve` dispatches on the storage type, so one rule now lives in up
to three copies: the Numba kernel, the reference walk's `all_realised` fast half,
and the half that can carry a frozen leg. Each copy now carries its own probe and
a selector whose fixture provably enters that copy. Two tests were added for the
copies that had none.

`TestCompiledSharedWalkEquivalence` builds one adversarial book in both storages
and asserts byte-exact equality of every curve field, which is legitimate only
because the kernel keeps sequential operation order and does not enable
`fastmath`. It is the only test that executes the kernel's arithmetic, and its
fixture makes each duplicated rule load-bearing: a same-date exit-then-entry, a
`bars_held = 0` leg, two legs opening on one date so the basket denominator is
not one, a halted bar inside a live hold on each mark source, and a cash-capped
two-sided rebalance. `TestUnrealisedWalkOrdering` carries an unrealised leg,
which is the only way to reach the general half, and discriminates on
`open_count` because it is integer-exact.

The harness is now forty probes, every one `CAUGHT`, with the restored suite
green. `@njit(cache=True)` does not defeat it: numba re-keys its on-disk cache on
the source file, so a rewritten file recompiles rather than serving the
pre-mutation kernel — proved by the compiled probes reporting `CAUGHT` at all.

The lesson is recorded in `docs/review-prevention-log.md` and in the harness's own
module docstring: when a function grows a second implementation of a rule, a probe
over that rule needs one probe per copy, each with a selector whose fixture
provably enters it.

## Launch sequence from here

#2773 leaves draft, merges into its stacked base `fix/2697-metric-axis-integrity`,
and only then is the jobs daemon restarted from the merged checkout. The merge
order is that way round deliberately: #2757's acceptance step 3 is a
full-population old/new A/B, and that is only affordable with this branch's
compiled path and scale gates in place.

There is currently no active `strategy_backtest_run`; requests 396 and 408 are
terminally rejected and S-1 has no stored synthetic-control result. Measured on
the dev DB on 2026-08-19, neither rejection is a fail-closed metric-axis refusal:
run 98349 (request 396) and run 99585 (request 408) both ended `failure` /
`internal_error` with `orphaned: reaped at boot (owning worker thread died
without a terminal status)`, and 99585 finished on 2026-08-16 11:39 UTC. Request
408's own row records `operator terminate: oversized backtest stopped before
jobs-daemon restart` — the oversized run this branch exists to bound. Nothing is
still pending on either.

The first post-merge invocation should therefore be S-1 only with
`synthetic_control=true` and the current trial-register version. Watch
`job_runs.progress_json` and do not expose outcome fields before its structural
audit completes.
