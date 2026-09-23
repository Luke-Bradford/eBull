# Releasing ownership of a broker-closed position — third design (whole one-shot close only)

Status: spec r3 · 2026-09-23 · Refs #2965, #3284, #2603, #2437. Supersedes the key in
`2026-09-22-broker-closed-ownership-release.md` (REFUSED twice); that doc's "Why now", site
and locks are inherited and not restated. Revision history at the end.

## Scope: exactly the case #3284 makes routine

A mandated SL or TP firing closes the WHOLE position in one execution. That is the case that
wedges the engine today (`strategy_position_manager.py:1264-1274` leaves ownership `active`;
`strategy_engine_capital.py:412-418` then raises `engine_capital_ownership_unwitnessed` on
every resolution). This design releases ownership ONLY for that shape — one close execution
covering the position's whole initial size, with no partial ever. **Every partially-altered
position stays wedged for a human**, by design: r1/r2 tried to prove a multi-slice close
whole by arithmetic and Codex found 16 release-safety gaps in it, all rooted in unobserved
booking shapes. The narrow key needs none of that.

## Evidence the key rests on

1. **Attended partial close, 2026-09-23** (fixture
   `tests/fixtures/etoro/attended_2026-09-23-01_partial_close.jsonl`, write-up #2965 00:19Z):
   the snapshot's `initialUnits` did not change across a partial close; the partial slice's
   history row got a NEW `positionId` but kept the opening `orderId` (both rows
   `orderId = 383320185`). ⇒ any partial slice leaves a close row under the SAME `orderId`
   and a different position id — which is what condition 4 refuses on. History was read only
   after the final close; the key does not depend on the in-between state (see cond. 3–4).
2. **The open witness is a live-snapshot record, not history-derived.**
   `open_events_from_positions` (`app/services/trade_events.py:143-190`) writes one `open` row
   per live position with `source='etoro_sync'`, `units = initialUnits`, `raw_payload` = the
   snapshot object; first observation wins (`uq_trade_events_open`, `sql/194_trade_events.sql`).
   Dev DB, 2026-09-23: all 8 `etoro_sync` open rows carry raw `orderID` and raw
   `initialUnits` equal to `units` (`select position_id, units, raw_payload->>'initialUnits',
   raw_payload->>'orderID' is not null from trade_events where source='etoro_sync'`); the one
   active engine ownership (`3601264304`) has one, `isPartiallyAltered = false`.
3. `broker_positions_closed` is NOT usable: 0 rows ever in dev
   (`select count(*) from broker_positions_closed` → 0), despite 6 stored close rows.

## Key

Inside the `position is None` branch, reached only after `_resume_operation` returned `None`,
same locks. One clock: `observed_at`. The predicate is a pure function over fetched rows;
any missing, malformed, non-finite or mistyped field → refuse, never an exception. Integer
ids are accepted as `int` or a decimal-integer string (`orders.broker_order_ref` is text).
Each refusal logs a reason code + the witness ids, so the human resolving a wedge sees why.

1. **Absent** from the branch's fresh `get_portfolio()` (existing).
2. **Entry reference**: the trade has exactly one `strategy_trade_orders` row with the entry
   purpose, and its `orders.broker_order_ref` is a positive integer E.
3. **Open witness O**: exactly one `open` row for P; `source = 'etoro_sync'`;
   `raw.positionID = P`; `raw.orderID = E`; `raw.instrumentID = O.etoro_instrument_id`;
   raw `initialUnits` and raw `units` finite, > 0 and both equal to `O.units` (first sighting
   was of the whole, unaltered position); `raw.isPartiallyAltered = false`;
   `instrument_id` = the trade's; `raw.isBuy = true`; `raw.leverage = 1`;
   `raw.initialAmountInDollars` finite > 0.
4. **Close witness R**: exactly one `close` row for P (no time filter), AND no close row for
   ANY other position id with `order_id = E` OR `raw.orderId = E` (a partial slice ⇒ refuse).
   R: `source = 'etoro_history'`; `order_id = E` and `raw.orderId = E`; `raw.positionId = P`;
   `raw.instrumentId = R.etoro_instrument_id = O.etoro_instrument_id`; `instrument_id` = the
   trade's; `raw.openTimestamp` (parsed) = `O.executed_at` (same opening); `side = 'sell'`;
   `raw.isBuy = true`; `raw.leverage = 1`; **`R.units = O.units` exactly** (both the broker's
   6-dp figure) and `raw.units = R.units`; `raw.investment = raw.initialInvestment =
   O.raw.initialAmountInDollars`, all finite > 0; `O.executed_at <= R.executed_at <= observed_at`; `realized_pnl_usd` non-NULL
   and finite.
5. **Quiet**: no `strategy_position_operations` row for this ownership in
   `intent_persisted | submitting | submitted`, and no `reconcile_required` row with
   `operation_type = 'close'`. A `reconcile_required` row of type `fixed_exit_repair` or
   `stop_ratchet` does not block (the CHECK constraint makes those three types exhaustive):
   an edit changes no exposure, and R proves the position closed whole. The entry order's
   `strategy_order_reconciliation_state.state = 'resolved'`.

Why a transient snapshot omission cannot release: conditions 3–4 need a broker HISTORY row
under P whose units equal the whole initial size, with no sibling slice. A live position has
no such row. A partial booked under P would carry fewer units than `initialUnits` (fails
`R.units = O.units`); a partial booked under a new id fails the no-sibling check.

Any failure → existing behaviour, re-checked next cycle. Refusals on immutable stored
evidence (first observation wins) are permanent: same fail-closed wedge as today.

## Writes (one transaction, inherited lock/row order)

- ownership: `UPDATE … SET status='released', release_reason='broker_closed_externally',
  released_at = R.executed_at WHERE ownership_id = %s AND status = 'active'`. Rowcount ≠ 1 →
  roll back and return today's `reconcile_required / owned_position_missing` result.
- trade: `FOR UPDATE`, then `closed` iff no active ownership remains; otherwise untouched.
- result: `PositionManagerResult(..., "applied", "broker_closed_externally")`.

## Stated limits

- Partially altered positions (any sibling slice) stay wedged for a human. The realised P&L of
  their slices is also uncounted by `_load_realised_delta` (joins by `position_id`) — that is
  #2603 U3's, pre-existing, and this key never releases such a position.
- `_finish_close` (engine-initiated close) releases without this proof and writes the trade
  `open` when other ownership remains (`strategy_position_manager.py:438-456`): pre-existing, unchanged.
- A position opened and closed between syncs has no `etoro_sync` open row → refuse. Engine
  positions under SL/TP live across many syncs.
- `trade_events` is single-account by design (`sql/194_trade_events.sql` header); E binds R
  to the order this strategy placed.

## Tests

Pure predicate, table-driven from the fixture shape: whole close → release at `R.executed_at`;
sibling slice under E → refuse; R.units < O.units → refuse; history-sourced O → refuse;
O raw initialUnits ≠ O.units / missing → refuse; E mismatch or missing or two entry links →
refuse; two close rows under P → refuse; future R → refuse; R before O → refuse;
NULL/non-finite P&L → refuse; investment ≠ initialInvestment → refuse; isBuy false /
leverage 2 → refuse; malformed JSON types → refuse.
One DB test for the SQL + writes: release, trade `closed`, retry no-op; in-flight op →
unchanged; `reconcile_required` close op → refuse; `reconcile_required` `stop_ratchet` →
release.

## Revision history

- r3 round (32 findings, 13 release-safety): adopted raw-identity corroboration (positionID,
  instrumentID, orderId sibling lookup, `source='etoro_history'`, openTimestamp = O's open,
  `isPartiallyAltered=false`, investment = O's initial dollars), refusal diagnostics, int
  parsing. Rebutted: precision (units are the broker's own figure; a residual below its
  precision is not holdable), corrections (history rows are post-close records; first-wins
  storage refuses on disagreement at ingest, logged), `initialUnits` rebase/increase (an eToro
  buy opens a new position; the one observed alteration left it unchanged). Stated: SL/TP
  whole-close is by construction of a stop, not observed; account namespace is single-demo
  (`broker_credentials` holds demo rows only) and a collision needs P, E and the open
  timestamp all reused; ingest race — a sibling landing after release is a post-close slice
  of an already-whole close, which the no-sibling rule makes impossible under the observed
  booking.

- r1 (family arithmetic, Σ investment over rows sharing `orderId`): 38 Codex findings.
- r2 (r1 + two-omission witness + entry binding): 29 findings, 16 release-safety — the family
  model and the status-based omission witness. Per `.claude/CLAUDE.md` step 3c the MODEL was
  replaced, not re-keyed: r3 accepts only the one-shot whole close and refuses every partial.
