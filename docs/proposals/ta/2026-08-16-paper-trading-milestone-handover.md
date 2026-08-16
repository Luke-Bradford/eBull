# TA paper-trading milestone handover — 2026-08-16

## Objective and current truth

The target is an operator-ready, demo-only strategy lane: evidence-qualified
strategies can be reviewed, promoted, assigned bounded paper capital, executed
through broker preflight, and audited through signal, order, position, close,
fees, and P&L. Real-money activation remains fail-closed.

The plumbing is implemented and pushed, but the milestone is **not complete**.
There is no evidence-qualified capital candidate yet. Every current catalogue
entry remains `harness_validation`, and no profitability or promotion-ready
claim is valid until the ordered research gates below complete.

## Branches and pull requests

| Purpose | Branch / exact head | PR | State |
| --- | --- | --- | --- |
| Causal metric-axis correction | `fix/2697-metric-axis-integrity` / `7b87e567d90707fe41d8ae3ee0577c887f1e3f56` | #2757, base `main` | Pushed, clean, draft; hosted CI green; hosted review skipped because draft |
| Backtest scale gate and compact synthetic controls | `fix/2772-backtest-scale-gate` / `f358fc9d78688557481c63df9e25032f4c1ad8b1` | #2773, base `fix/2697-metric-axis-integrity` | Pushed, clean, stacked draft; exact-head local pre-push gate green; main-only hosted workflows intentionally do not run on this base |
| Recovered MT-1 and operator paper lane | `integration/ta-demo-mt1-operator`; implementation baseline `ca1ca82bd7bc00e7fc1d1551ebe42db708884000`, with this handover added at the branch tip | #2771, base `fix/2697-metric-axis-integrity` | Pushed, clean, stacked draft; main-only hosted workflows intentionally do not run on this base |

`/Users/lukebradford/Dev/eBull` remains the exact detached legacy execution
tree. The oversized worker has been terminated; do not restart its request or
use that tree for development.

## Terminated exact legacy run

- former PID: `81457` (confirmed absent after exact-PID terminate)
- command: `/Users/lukebradford/Dev/eBull/.venv/bin/python3 -m app.jobs`
- detached head: `ef206555d05acf44750a79c35e02513f04c4885d`
- job run: `99585`
- linked request: `408`
- exact payload:

  ```json
  {"control": {}, "params": {"synthetic_control": true, "trial_register_version": "trial-register-2026-08-15-r6"}}
  ```

The operator approved termination on 2026-08-16 after a bounded operational
audit projected an order-of-weeks completion time and found no usable progress
telemetry. The durable terminate request was recorded, request `408` was
rejected before restart, and only the jobs daemon was restarted. Run `99585`
was reaped to `failure`; its automatic retry was cleared (`next_retry_at` is
null), no later `strategy_backtest_run` was created, and the old worker ignored
`SIGTERM` before the two exact orphan PIDs were killed. The stop row is closed
with `observed_at` null, preserving the external-termination audit sentinel.

The structural auditor was then run from the metric-axis worktree:

```bash
cd /Users/lukebradford/Dev/.ebull-ui
uv run python -m scripts.audit_2697_legacy_metric_axis --run-id 99585
```

It exited `2` at the terminal-failure guard and did not read result rows. The
legacy structural audit and the full old/new A/B therefore remain unfulfilled.
Do not relaunch either full-population run until #2772's observability,
multi-size scale benchmark, launch-budget refusal, and optimized/reference
differential gates pass. The legacy log exposure already recorded on #2697
also means run `99585` could not have supported blind interpretation.

## #2772 scale-readiness slice

Draft PR #2773 now implements the code-level relaunch guardrails:

- outcome-free global progress records include control and member ordinals,
  elapsed time, processing rate, and ETA;
- an exact three-member production pilot is retained in each final cohort and
  must pass declared per-cohort and cumulative projected-wall budgets before
  parallel fanout;
- production synthetic members share immutable mark series instead of copying
  every leg's complete mark history;
- compact construction fails closed on reversed legs, negative source offsets,
  truncated mark spans, and non-columnar sources before a curve is evaluated;
- the compact and slow reference engines have an exact differential over
  entries, exits, prices, returns, dates, curve arrays, and counters;
- a fixed, digest-bound, outcome-free benchmark records wall time, CPU,
  throughput, peak RSS, decoded bars, placement size, and reference
  equivalence without querying the database or exposing strategy outcomes;
- operational backtest logs have a static guard against outcome terms.

The fixed local benchmark passed exact equivalence at all four declared sizes;
its largest case measured 131,072 trades per member and three members. This is
a compute-layout baseline, not a full-corpus forecast or strategy result.

The audit after the first slice found four gates that must precede the actual
production pilot: durable fail-closed match-quality evidence; a production
memory refusal; measurement of spawned-worker startup, input copying, worker
RSS, and observed parallel scaling; and an outcome-blind differential over
stratified real-corpus shapes. Repeated strategy-by-strategy PostgreSQL reads
and Decimal/object reconstruction also remain outside the current projection.
A digest-bound reusable corpus representation with `load_arms` equivalence is
therefore required before the pilot, not deferred until after a refusal. No
full-population run has been launched.

## Ordered #2757 completion

1. Persist the synthetic cohort's unmatchable/no-slack census and exposure/
   turnover match residual, and make unacceptable or absent structural evidence
   refuse promotion rather than disappear from the durable result.
2. Add a production memory budget and a parallel canary that measures process
   startup, collector transfer, worker RSS, and observed scaling.
3. Build a digest-bound reusable corpus representation, prove `load_arms`
   equivalence on deterministic and stratified real-corpus samples, and include
   its read/decode cost in the launch projection.
4. Review and merge stacked #2773 onto #2757, preserving exact-head local and
   hosted gates when its base becomes eligible, then run only the exact
   three-member production pilot. Stop at the first correctness or time/memory
   refusal; do not expose outcome values. Any outcome-informed adjustment is a
   new declared trial/version.
5. Launch a new exact legacy structural run only if the measured projection is
   inside budget, then pass its structural audit without reading performance.
6. Run the full-population old/new A/B with output redirected, for example:

   ```bash
   uv run python -m scripts.verify_2697_metric_axis_ab \
     > /tmp/ebull-2697-metric-axis-ab-7b87e567.jsonl
   ```

7. Before reading that output, run its structural auditor:

   ```bash
   uv run python -m scripts.audit_2697_metric_axis_ab \
     /tmp/ebull-2697-metric-axis-ab-7b87e567.jsonl
   ```

8. Require the declared current and legacy populations, sealed in-sample
   corpus/regime window, and final exact-head/clean-tree recheck.
9. Only after the structural audit passes may the A/B values be interpreted.
10. Update #2697 and #2757 with exact evidence, mark #2757 ready, wait for the
   hosted implementation review, address every substantive response, rerun the
   final-SHA gates, and merge only when review and evidence are clean.

## Ordered programme work after #2757

1. Rebase `integration/ta-demo-mt1-operator` onto accepted `main`; update #2771
   from its temporary stacked base.
2. Rerun full pre-push/static/backend/frontend/real-Postgres gates on the final
   SHA and obtain substantive hosted review.
3. Run the audited MT-1/S-8 declaration-supersession step; do not reuse stale
   declarations after the metric-axis contract changed.
4. Run the corrected preregistered MT-1 in-sample trial through the two-phase
   structural-commit/outcome path and its structural, invocation, and derivation
   auditors.
5. Apply the complete conjunctive decision rule. A failed or incomplete result
   remains research evidence; do not search around it or tune after seeing it.
6. Only if warranted, freeze and review a distinct `capital_candidate` version,
   then advance through registered holdout and prospective forward observation.
7. Configure explicit disabled-first strategy limits and the shared paper risk
   mandate, then exercise bounded demo execution and audit the actual generated
   lifecycle end to end.

## Operator-path implementation already recovered

The stacked integration provides:

- outcome-free MT-1 structural preparation followed by atomic trial-result storage;
- complete pinned-evidence replay at promotion, operator readiness, and before broker access;
- authenticated promotion and explicit first paper setup, created disabled;
- an independent strategy paper master switch that does not enable the legacy auto-order lane;
- shared and per-strategy capital, sizing, freshness, cost, positive-net-expectancy,
  exposure, drawdown, daily-loss, concurrency, long-only, and no-leverage checks;
- one reserved paper lifecycle worker slot without increasing the PostgreSQL connection budget;
- demo-only order submission, exact strategy-owned position tracking, conservative
  ambiguous/reconciliation-required states, close history, fees, and P&L;
- `/strategies` navigation and UI for evidence, controls, generated activity,
  owned positions, reconciliation, and lifecycle audit;
- no live activation writer: generic `live_enabled` promotion is refused and the
  live endpoint records an assessment only.

## Verification already completed

- All three branch heads are clean and pushed.
- #2757 exact-head local pre-push gate and hosted CI are green.
- #2773 exact-head local pre-push gate is green; its fixed scale benchmark
  passed exact slow/compact equivalence, and the synthetic/statistics/equity/
  metric-axis mutation probes caught 7/7, 31/31, and 7/7 mutations respectively.
- #2757 result-model mutation probe caught 28/28 mutations; its dedicated
  metric-axis probe caught 7/7 structural reverts; 291 global probe anchors pass.
- Integration whole-repository Ruff, formatting, and Pyright pass.
- Integration focused MT-1/promotion/allocation/executor/runtime/API suites pass.
- All 137 frontend files / 1,537 tests, frontend typecheck, and production build pass.
- The broad backend run had nine parallel global-state/connection/source-lock
  failures; every one passed on a clean serial rerun.
- Real-Postgres lifecycle acceptance covers ranking, funding, broker-priced
  preflight, exactly one demo submission, reconciliation, exact ownership,
  lifecycle reads, replay idempotence, and exclusion of a same-instrument manual
  position. This proves plumbing, not profitability.

## Ticket ownership

| Ticket | Remaining disposition |
| --- | --- |
| #2697 | Close only through reviewed and merged #2757 after legacy audit and exact-head A/B |
| #2772 | Draft #2773 implements observability, compact controls, time budgets, exact fixture differential proof, and fail-closed mark spans; durable match evidence, production memory/parallel measurement, reusable corpus reads, and stratified differential proof remain before review/merge or pilot |
| #2766 | Implemented in #2771; close only after final-base review verifies first paper enable and independent master switch |
| #2767 | Implemented in #2771; close only after final-base worker-capacity verification |
| #2768 | Implemented in #2771; close only after final-base real-Postgres lifecycle acceptance |
| #2769 | Paved evaluator implemented in #2771; outcome/evidence gate remains outstanding |
| #2770 | Operator promotion path implemented in #2771; actual evidence-qualified candidate and demo exercise remain outstanding |

## Explicit non-claims

- No current strategy is known to be profitable.
- Backtest or controlled-trial output alone does not authorize capital.
- The operator UI and paper executor prove reachability and safety plumbing, not edge.
- Live/real-money strategy activation is unavailable and must remain so until a
  separately validated broker contract and measured live-promotion gate exist.
