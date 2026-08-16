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
| Recovered MT-1 and operator paper lane | `integration/ta-demo-mt1-operator`; implementation baseline `ca1ca82bd7bc00e7fc1d1551ebe42db708884000`, with this handover added at the branch tip | #2771, base `fix/2697-metric-axis-integrity` | Pushed, clean, stacked draft; main-only hosted workflows intentionally do not run on this base |

Do not edit or reload `/Users/lukebradford/Dev/eBull` while the legacy worker
below is active. It is an exact detached execution tree, not the development
worktree.

## In-flight exact legacy run

- PID: `81457`
- command: `/Users/lukebradford/Dev/eBull/.venv/bin/python3 -m app.jobs`
- detached head: `ef206555d05acf44750a79c35e02513f04c4885d`
- job run: `99585`
- linked request: `408`
- exact payload:

  ```json
  {"control": {}, "params": {"synthetic_control": true, "trial_register_version": "trial-register-2026-08-15-r6"}}
  ```

This is a long single-process legacy computation. Repeated narration adds no
evidence. On resume, check the PID once. If it is still active and compute-bound,
leave it alone and wait outside the coding loop. Do not query result, progress,
metric, trade, comparator, bootstrap, deflation, or synthetic-control values.

After process exit, the **first result-related command** must be run from the
metric-axis worktree:

```bash
cd /Users/lukebradford/Dev/.ebull-ui
uv run python -m scripts.audit_2697_legacy_metric_axis --run-id 99585
```

The auditor must prove all of the following before any performance value is
read: exact run/request/payload provenance, atomic success, the complete declared
40-row identity population, no invented metric-axis fields, and current
promotion refusal `metric_axis_unproven`. Any failure stops the sequence.

## Ordered #2757 completion

1. Confirm `.ebull-ui` is clean and still exactly at `7b87e567d90707fe41d8ae3ee0577c887f1e3f56`.
2. Pass the legacy structural audit above without reading performance.
3. Run the full-population old/new A/B with output redirected, for example:

   ```bash
   uv run python -m scripts.verify_2697_metric_axis_ab \
     > /tmp/ebull-2697-metric-axis-ab-7b87e567.jsonl
   ```

4. Before reading that output, run its structural auditor:

   ```bash
   uv run python -m scripts.audit_2697_metric_axis_ab \
     /tmp/ebull-2697-metric-axis-ab-7b87e567.jsonl
   ```

5. Require the declared current and legacy populations, sealed in-sample
   corpus/regime window, and final exact-head/clean-tree recheck.
6. Only after the structural audit passes may the A/B values be interpreted.
7. Update #2697 and #2757 with exact evidence, mark #2757 ready, wait for the
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

- Both branch heads are clean and pushed.
- #2757 exact-head local pre-push gate and hosted CI are green.
- #2757 result-model mutation probe caught 28/28 mutations; its dedicated
  metric-axis probe caught 7/7 structural reverts; 289 global probe anchors pass.
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
