# Resolving a lost close acceptance from the trade ledger (#2979)

Status: proposal, revision 2. Targets `app/services/strategy_position_manager.py::_resume_operation`
and one typed refusal in `app/services/strategy_engine_capital.py`.

⚠ Revision 1 proposed calling `broker.get_trade_history` from the recovery path and
releasing on "a history row exists ∧ the position is absent from the portfolio". Codex
checkpoint 1 killed it on two confirmed defects, both re-verified against source here:

- `_load_realised_delta` (`strategy_engine_capital.py:101-108`) raises
  `"exact-owned realised P&L is incomplete"` for any ownership row that is `released`
  with **zero** `trade_events` close rows. Releasing without a ledgered close would have
  moved the raise from `resolve_engine_capital_usage` to `_load_realised_delta` — the
  same wedge, one function later, and acceptance 1 unmet.
- `get_portfolio` (`etoro_broker.py:934-968`) coerces a missing container with
  `portfolio.get("positions") or []` and `continue`s past any row that is not a dict or
  lacks `instrumentID`. A live position can therefore be **silently omitted**, so
  "absent from the portfolio" is not evidence of closure. That is precisely the
  false-negative shape that killed #2961's terminalisation surface.

Revision 2 replaces the absence argument with the repo's own quantitative
partial-close model and drops the broker call entirely.

## The defect, restated

A crash between `close_demo_strategy_position` reaching the broker and
`strategy_position_operations.broker_order_ref` reaching us leaves the operation at
`intent_persisted`. `_resume_operation` hard-codes `landed = False` for a close
(`:520`), so scenario 7a (the broker never saw it) and 7b (the broker executed it)
terminalise to the **identical** row: `reconcile_required` /
`crash_before_submission_identity`, with `strategy_position_ownership` left `active`.

7a is harmless. 7b names a position the account no longer carries, so
`resolve_engine_capital_usage` refuses the join (`strategy_engine_capital.py:329`) and
`execute_core_rebalance` wraps it into `StrategyCoreExecutionError`
(`strategy_core_executor.py:527`) — the allocator does not refuse politely, it raises, on
this and every later cycle.

Demonstrated across a real process boundary in
`tests/test_2949_core_close_recovery_db.py::test_scenario_7b_lost_close_acceptance_wedges_the_core_capital_reader`.

## Source rule — our own ledger invariants, not a new broker claim

The evidence this design releases on is already ingested, on a schedule, by
`app/services/trade_events.py` (job `daily_portfolio_sync`, `:453`) from
`GET /api/v1/trading/info/trade/demo/history`. **The recovery path makes no broker call
and asserts no new broker capability.** The governing rules are the ledger's own, all
read from source:

1. **One close row per slice.** `uq_trade_events_close` is unique on
   `(position_id, executed_at) WHERE event_kind='close'` (`sql/194_trade_events.sql:60`);
   `BrokerClosedTrade`'s docstring records that partial closes reduce the same
   `positionId` across several rows (#1593 spec §4). **A close row therefore proves a
   slice closed, never that the position is closed.**
2. **Σ(close units) ≤ open units is the partial-close model**, enforced as an anomaly
   counter by `_check_close_sum_invariant` (`trade_events.py:416`, spec §4/§22.1). Its
   equality case is the completeness witness this design needs.
3. **Only a portfolio-sourced open can witness completeness.** `events_from_history`
   synthesizes an open whose `units` is **Σ of the very slices being counted**
   (`trade_events.py:193`), so for a history-only position the witness is circular and
   trivially true after a single partial close. `merge_events` prefers the
   portfolio-sourced open, which carries `initialUnits` (`:122`, `:241`). The witness is
   therefore admissible only when the stored open has `source='etoro_sync'`.
4. **`_load_realised_delta`'s precondition.** It raises when any owned position has a
   close row with NULL `realized_pnl_usd`, and when a `released` ownership has zero close
   rows (`strategy_engine_capital.py:93-108`). A release that does not satisfy both trades
   one raise for another.

⚠ Live-portal re-check, 2026-09-13,
`https://api-portal.etoro.com/api-reference/trading--demo/list-trading-history.md`:
`minDate` filters on the **close** timestamp, and rows carry `positionId`,
`closeTimestamp`, `orderId`, `units`, `netProfit`. Recorded because the ingest this design
depends on rests on it — not because the recovery path calls it.

## Why this is not #2961's killed design

#2961 was refused because its safety rested on a conjunction of two **absences**, each
with structural false negatives, and because terminalising there released capital that
could fund a duplicate *entry*. Here:

- the load-bearing conjunct is **quantitative and positive** — Σ(closed units) ≥ the
  portfolio-reported opened units. Nothing about a degraded read can manufacture closed
  units; the #2961 failure was a *missing* observation being read as proof;
- portfolio absence is kept only as a **cross-check**, never as the evidence, precisely
  because `get_portfolio`'s parser is lenient (verified above);
- releasing is the correct accounting, not a bypass: the position is proven gone, so the
  capital genuinely returned. A released core ownership funds a new buy only after the
  old one is witnessed closed.

## The decision table

Evaluated in `_resume_operation`'s `status == 'intent_persisted'` branch when
`operation_type == 'close'` — the branch that today evaluates to `landed = False`
unconditionally. `position` is the already-computed `_exact_broker_position` result; no
new broker I/O is added.

Let, for `P = owned.broker_position_id` and `T = operation["created_at"]`:

- `attributable` — ∃ close row for P with `executed_at >= T`. No clock-skew tolerance is
  granted: broker and engine clocks are independent, so a genuine close stamped early
  degrades to today's outcome rather than to a release. Inventing a tolerance window would
  be an unsourced constant.
- `witnessed` — ∃ open row for P with `source='etoro_sync'`, and Σ(close units for P) ≥
  that open's `units`.
- `priced` — no close row for P has NULL `realized_pnl_usd`.

| `position` | `attributable` | `witnessed ∧ priced` | outcome |
| --- | --- | --- | --- |
| present | no | — | terminal `reconcile_required` / `crash_before_submission_identity` — **7a, byte-for-byte today's behaviour** |
| present | yes | — | terminal `reconcile_required` / `close_landed_on_open_position` — a slice closed while the position lives: a genuine divergence for #2602, not a recovery |
| absent | no | — | pending `close_evidence_not_ingested` — the ledger has not caught up; retry |
| absent | yes | no | pending `close_completeness_unwitnessed` — retry |
| absent | yes | yes | **release** → applied `exact_position_closed_by_ledger` |

Release writes exactly what the existing `submitted`-branch resolution writes at
`:567-589`, which is the same decision reached by a different identity route:
`orders SET status='filled'` on the linked exit order, then
`_finish_close(reason=operation["trigger_code"])`, which releases the ownership and moves
the trade to `closed`.

⚠ **`orders.broker_order_ref` is deliberately NOT backfilled.** The ledger's `order_id`
is not sourced as the close order's id — `docs/proposals/etl/2026-06-13-etoro-trade-ledger.md:98`
records that it may identify the *opening* order — and the parser preserves `orderId=0` as
a sentinel. Writing an unsourced identity into the exit order would be worse than leaving
it NULL.

**Pending is durable, not silent.** Each pending outcome writes `last_error_code` and
`updated_at` on the operation row while leaving `status='intent_persisted'`, so the reason
the last attempt did not resolve is readable from the database rather than only from a
`PositionManagerResult` the paper cycle discards.

## The allocator must refuse, not raise, while the ledger lags

Between the broker executing the close and `daily_portfolio_sync` ingesting it, ownership
is still `active` on an absent position, so `resolve_engine_capital_usage` raises and
`execute_core_rebalance` converts it into `StrategyCoreExecutionError`. The ticket's
acceptance asks for a verdict, so:

- `strategy_engine_capital.py` gains `CoreOwnershipUnwitnessedError(EngineCapitalObservationError)`,
  raised at the single "active core position N is absent from broker snapshot" site
  (`:329`). Being a subclass, every existing `except EngineCapitalObservationError`
  caller — the mandate writer, `core_rebalance_observation` — is unchanged.
- `execute_core_rebalance` catches it ahead of its broad handler and returns
  `_result("refused", "core_ownership_absent_from_snapshot")`.

This is **not** a weakened gate: `_result("refused", …)` returns before any broker
mutation, exactly as the existing `sandbox_exceeded` refusal does. It converts an
unhandled exception with no reason code into a recorded refusal with one.

## Verified chain after a release

Checked against source rather than assumed, because a release that satisfies one reader
and breaks the next is the defect this revision exists to avoid:

- `_finish_close` sets `strategy_trades.status='closed'`; `_TERMINAL_TRADES` is
  `{closed, failed}` (`:27`), and the entry's reconciliation state is `resolved`, which is
  in `_TERMINAL_RECONCILIATION` `{resolved, rejected}` (`:28`), with no active ownership —
  so `load_engine_capital_authority` takes the `continue` at `:277` and raises nothing.
- The position leaves `core_active_position_ids`, so `resolve_engine_capital_usage` never
  looks for it in the snapshot.
- `_load_realised_delta` sees `status='released'` with `close_count > 0` and
  `missing_pnl = 0`, satisfying both of its raise conditions.

## Scope — what this does NOT do

- **No `strategy_order_reconciliation_state` row for EXIT orders.** The issue's fact 1
  stands; exit recovery runs through `_resume_operation`, reached every paper cycle from
  `strategy_paper_runtime.py:493`.
- **No change to `owned_position_missing`.** The generic "position absent, no outstanding
  close intent" path still returns `reconcile_required` without releasing. Candidate 2 of
  the issue is explicitly NOT taken: absence is a cross-check here, never the evidence.
- **The accepted-but-pending close is still unresolved, and this is the residual.** A
  close the broker accepted but has not filled leaves the position present and no close
  row, which is indistinguishable from 7a and terminalises as 7a — today's behaviour,
  unchanged. Separating those two needs identity by `referenceId`, which is the coverage
  question blocking #2961/#2965/#2942 half 2 and is not settled here.
- **`released_at` is recovery time, not close time**, so a close recovered days later
  leaves intervening `strategy_wealth` snapshots expecting a holding that had gone. The
  ledger's `executed_at` is available and could back-date it, but the submitted-branch
  release has the same property and the two should change together under #2602, which owns
  attribution. Recorded as a known limitation, not fixed here.
- **No migration, no backfill.** `release_reason` is free-text `TEXT` (`sql/281`,
  `sql/284`); every status written already exists in the `strategy_position_operations`
  CHECK (`sql/289:68`). Measured on the dev DB: `strategy_position_operations` and
  `strategy_position_ownership` are both empty with 0 strategy-origin EXIT orders — the
  wedge is latent in a wired path.

## Migration locking assumption (review NITPICK on PR #2981)

`377` rebuilds `idx_strategy_position_one_unresolved_operation` with a plain
`DROP INDEX` / `CREATE UNIQUE INDEX`, which takes an `ACCESS EXCLUSIVE` lock on
`strategy_position_operations` for the duration. **That is safe here only because the
table is empty** — measured on the dev DB before the change, 0 rows — so the rebuild is
effectively instant.

Stated because it does not generalise: a future rebuild of this index against a populated
table should use `CREATE UNIQUE INDEX CONCURRENTLY`, which this runner supports via the
`-- runner: autocommit` directive on line 1 of a migration
(`app/db/migrations.py:45`). `CONCURRENTLY` cannot run inside a transaction block, so the
directive is not optional for it.

⚠ The note lives here rather than in the migration's own comment because `377` and `378`
are already applied and `schema_migrations.content_sha256` pins their bytes
(`app/db/migrations.py:105`, `:149-170`) — editing either file would fail the ledger check
at the next boot rather than document anything.

## Acceptance

1. `test_scenario_7b_lost_close_acceptance_wedges_the_core_capital_reader` inverted:
   after the real `daily_portfolio_sync` ingest runs against the fake broker's history,
   the resume releases ownership, the trade reaches `closed`, and `execute_core_rebalance`
   returns a verdict instead of raising.
2. `test_scenario_7a_close_intent_without_submission_costs_the_request_not_the_position`
   passes unchanged — ownership `active`, `crash_before_submission_identity`, and its
   existing end-state assertion that a deliberate re-request leaves `close_calls == 1`.
3. In 7b the broker sees no second close: `close_calls` stays 1 across the resume, the
   release and a subsequent allocator cycle.
4. Before the ingest has run, 7b's resume returns pending `close_evidence_not_ingested`,
   releases nothing, and `execute_core_rebalance` returns
   `refused` / `core_ownership_absent_from_snapshot` rather than raising.
5. Pure-logic coverage of the discriminator's decision table, including: a post-intent
   partial close with the position still present (no release); a history-sourced open as
   the only open row (no release — the circular witness is rejected); a close row with
   NULL `realized_pnl_usd` (no release); a close row predating the intent (no release).

The `FileBackedFakeBroker` gains `get_trade_history`, derived from the closes it has
already recorded, so the 7b ledger is produced by the **real** ingest service reading the
**real** broker interface rather than by staging `trade_events` rows in the test.
