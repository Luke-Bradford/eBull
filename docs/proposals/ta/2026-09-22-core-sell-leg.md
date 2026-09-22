# Core sell leg — partial close of one owned core position (#2603)

Status: **PARKED at Codex ckpt-1 round 2 (2026-09-22). Not built.** Queue: #2437 Tier A
item 2. Revision 2 answers round 1's 33 findings, cited below as `C1-n`. Round 2's verdict
is at the end, and a revision 3 must start there.

## What is missing

`evaluate_core_rebalance` returns `sell_core` when the core weight is strictly above the
band's upper edge (`strategy_core_allocator.py:311-339`). Nothing can execute that sell:

- `assess_core_broker_preflight` refuses it before any broker call with
  `core_close_side_cost_quote_unavailable` (`strategy_core_broker_preflight.py:317-320`).
- `core_order_shape_for` has no `sell_core` entry (`strategy_core_executor.py:57-59`).
- The only close mutation, `close_demo_strategy_position`, sends `UnitsToDeduct: None`
  and so closes the whole position (`etoro_broker.py:826`).

The preflight docstring (`:196-205`) says to build this as one slice (preflight quote plus
submission) and not to lift the refusal alone.

## Premise checks (dev DB and source, 2026-09-22)

- **One caller.** `execute_core_rebalance` is called only from the attended endpoint
  (`app/api/strategies.py:4057`). Nothing unattended reaches the sell path.
- **One owned core position.** `strategy_position_ownership` joined to core trades has
  1 `active` row (position `3601264304`). The realistic sell is therefore a trim of a
  single position. A whole-position-only design would take the sleeve to 0% core, below
  the lower edge, and the next cycle would buy it back. That rules out avoiding a partial
  close.
- **Ownership carries no units.** Its columns are `ownership_id, strategy_trade_id,
  broker_position_id, status, claimed_at, released_at, release_reason`. Capital usage
  values the owned ids from the live snapshot (`resolve_engine_capital_usage`). So if a
  partial close keeps the position id, no ownership write is needed.
- **Capital population.** The core row query counts only `purpose='entry'` links
  (`strategy_engine_capital.py:288-297`), so an `exit` link on the entry trade does not
  trip `entry_count != 1`. `core_active_recorded_committed` keeps the entry's full
  `requested_amount` after a trim. That overstates committed capital, which is the
  conservative direction (`strategy_control_plane.py:384`). Accepted and stated, not fixed
  here.
- **Realised P&L** comes from `trade_events` `close` rows keyed by `position_id`
  (`_load_realised_delta`, `:112-160`). Whether a partial close writes one is the #2965
  unknown. If it does, the rule counts it. If it does not, realised P&L under-reports until
  the position is fully closed. The attended acceptance observes which.

## Source rule (eToro, `tests/fixtures/etoro/openapi_v1.375.0.json` + `etoro-api.md`)

- Market close by position: *"If `UnitsToDeduct` is provided, only the specified portion
  will be closed … A 200 response means the close order was submitted, not that the
  position was closed. Confirm the outcome with GET …/info/demo/close-orders/{orderId},
  using the orderID returned under orderForClose."*
- `OrderForClose.unitsToDeduct`: *"The number of units closed in this order."*
- `OrderForCloseInfoResponse.positions[]`: `positionID` (required); `units`: *"Number of
  position units closed, when available"*; `rate`; `amount`.
- `InstrumentEligibility.allowPartialClosePosition` is already enforced
  (`core_partial_close_unproved`, `strategy_core_submission_gate.py:462-470`).
- What-if close arm: needs `positionIds`, measured with an amount ticket. An open-arm quote
  does not bound close cost (`etoro-api.md` close-arm section, #2712).
- Close-side minimum: none documented. Every spec minimum is open-side
  (`strategy_core_broker_preflight.py:186-194`), so `broker_minimum=None` on a sell and the
  floor is the mandate's `min_rebalance_amount`.

**Not documented, so not guessed:** (U1) whether a partial close keeps the `positionID`;
(U2) what unit increment `UnitsToDeduct` accepts. The design fails closed on both, and the
attended acceptance observes both.

## Design

### 1. Selection and units — inside the broker preflight, from ONE snapshot (C1-20, C1-21)

All of these come from the single `get_account_risk_snapshot()` the sell arm already takes:
the selected position's `units`, the position's own marked value, and `units_before`. The
verdict carries `position_id`, `units_before` and `units_to_deduct`. The DB-preflight bid is
not used, so the dataflow is not circular.

- **Selection:** the owned core position (from `capital_authority.core_active_position_ids`)
  with the largest snapshot `units`. Ties go to the lowest id. Choosing a lot does not
  change tax, because same-class shares pool into one s104 holding (`tax-ledger` skill).
- **Conversion uses the position's own mark, not the bid (C1-16).** The sizer's sell amount
  `S` is marked value removed (`strategy_core_sizing.py:365-372`). The units that remove `S`
  of marked value are `S × units / position_market_value`, where both are the selected
  position's figures in the same snapshot. This keeps the submitted quantity inside the
  model the sizer checked.
- **Increment (C1-14):** U2 is undocumented. Construction: round down to 6 decimal places,
  the scale of our stored units (the migration fixes `units_to_deduct NUMERIC(18,6)`,
  matching `strategy_core_sizing`'s amount shape). This is our storage scale, not a claim
  about eToro. If the broker needs a coarser increment, it either rejects the order (a
  clean `rejected` outcome) or rounds it, and a rounded fill shows up as a units mismatch
  in §4 → `reconcile_required`. Both fail closed.
- **Re-check after rounding (C1-15, C1-18):** recompute the realised ticket
  `A' = units_to_deduct × position_market_value / units`. Refuse
  `below_min_rebalance_amount` if `A' < min_rebalance_amount`. Re-run the sizer's band
  endpoint check (`resolve_core_trade_size`'s `at_zero`/`at_bound` bracket) on `A'` with the
  quoted rate, and refuse `cost_breaches_far_edge` if it fails. `A' ≤ S`, so the ticket
  extrapolation ratio against the quote only shrinks.
- **Bounds (C1-13, C1-28):** `0 < units_to_deduct < units_before`, both finite, else refuse
  `core_sell_spans_positions`. Above `max_units_per_order`, refuse
  `core_sell_exceeds_max_units`. There is no broker-side "expected units" condition, so an
  external reduction between the snapshot and execution is not prevented. If it happens,
  the order may close more than intended. That outcome is detected in §4, because the id
  is absent or the units mismatch, and it goes to `reconcile_required`. It is stated as a
  residual risk, and the sell path is attended-only.

### 2. Close-side cost quote (C1-17, C1-19)

Same shape as the buy arm: fresh snapshot, age check, `observe_core_sleeve`, and a fresh
`evaluate_core_rebalance` that must return `sell_core` with the same amount (else
`core_sleeve_moved_since_decision`). Then
`get_what_if_costs(BrokerWhatIfOrder(action="close", transaction="sell",
position_ids=(pid,), amount=fresh.amount, …))` → `decode_quoted_trade_cost` →
`resolve_core_trade_size` (sell arm) → §1's units and re-check.

The quote uses an amount ticket because that is the only close-arm form measured
(#2712). Using its rate for the units order is the same model the buy arm already relies
on, since a what-if rate is applied to an executed ticket. Nothing here bounds slippage or
market movement, which the sizer's contract already excludes (`strategy_core_sizing.py:
325-328`). This is inherited, not new.

### 3. Refusals that must not block a reduction (C1-33)

A sell reduces exposure. `sandbox_exceeded` (broker preflight and executor) and the
executor's drawdown refusal therefore do **not** refuse a `sell_core`. The drawdown
**observation** is still recorded. This matches `manage_owned_position`'s stated rule that
kill switches block new risk, not de-risking (`strategy_position_manager.py:1245-1249`).
All other refusals apply.

### 4. Authority, locking and submission — a position-manager operation (C1-8, C1-9, C1-10)

The sell is submitted by a new `strategy_position_manager.submit_core_reduce(...)`, not by
the executor's buy tail. That function:

- Takes `_paper_allocator_lock` then `_position_lock(pid)`, the same pair
  `manage_owned_position` takes. The executor already holds `core_submission_lock`. The
  implementation must show that no path takes `core_submission_lock` while holding either
  of the other two, so the order is total (core → allocator → position). If that cannot be
  shown, the sell is refused. It is not built around.
- Re-proves under those locks, in one transaction, before inserting anything: ownership is
  `active`; there is **no operation on any core-owned position** with status in
  (`intent_persisted`, `submitting`, `submitted`); and there is **no `reduce` operation in
  `reconcile_required`** on any core-owned position. The check is sleeve-wide, not
  per-position, so a pending reduction elsewhere or an unresolved one blocks sizing
  (C1-4, C1-11, C1-12). Refusal: `core_sell_operation_outstanding`.
- The authority transaction inserts an `orders` row (`action='EXIT'`, `order_type='MARKET'`,
  `requested_amount = A'`, `status='submitted'`), and links it to the **owning entry
  trade** with `purpose='exit'` (the existing close convention,
  `strategy_position_manager.py:942-949`). It also inserts a `strategy_position_operations`
  row: `operation_type='reduce'`, `trigger_code='core_rebalance'`, `status=
  'intent_persisted'`, with `units_before`, `units_to_deduct`,
  `core_rebalance_intent_id` and `core_eligibility_proof_id` (C1-31). It writes **no**
  `strategy_order_reconciliation_state` row, because `reconcile_backlog` must never see
  this order (C1-9). It writes **no** `strategy_core_entry_exit_levels` row, because a
  reduce opens nothing and the position's SL/TP stand.
- `mark_close_submitting` runs in its own commit. Then comes the new provider verb
  `reduce_demo_strategy_position(position_id, instrument_id, units_to_deduct, request_id,
  persist_response)`: the whole-close adapter with `UnitsToDeduct` set. It uses the same
  `refuse_broker_mutation_if_unattended`, demo-only check and quota lane (same endpoint, so
  the same `etoro_quota_lanes.CALL_SITES` lane). The adapter refuses a non-finite,
  non-positive or `None` quantity before any I/O, and serialises it as a JSON number from
  the `Decimal` string. It never omits the key and never sends `null` (C1-28).
- Submission outcomes mirror `_submit_close` (`:977-1000`, C1-27):
  `BrokerPositionMutationUncertain` → operation `reconcile_required`
  (`broker_reduce_uncertain`), trade status unchanged. Definite rejection → operation
  `rejected`, order `rejected`, trade stays `open`. The executor's buy error path is
  **not** reused, because it would mark the owning trade `failed` (C1-8). Ack → store
  `orderForClose.orderID` as `broker_order_ref`, and set operation `submitted`. If the
  ack's `unitsToDeduct` is present and not numerically equal to the request →
  `reconcile_required` (`reduce_ack_units_mismatch`).
- The executor records the intent as today, then calls `submit_core_reduce` in place of
  the buy tail. `_CORE_ORDER_SHAPE` stays buy-only. The sell never passes through
  `core_order_shape_for`, and that function's backstop still refuses any action sent to the
  buy tail by mistake.

### 5. Resolution — its own resume branch, dispatched FIRST (C1-3, C1-6, C1-7)

In `_resume_operation`, a `reduce` operation is dispatched **before** the pre-marker
special case, the edit logic and the close branch.

- `intent_persisted` (pre-marker): the operation provably never entered submission →
  `rejected` (`reduce_abandoned_before_submission`). Units are not inspected (C1-3).
- `submitting` with no `broker_order_ref` (a crash after the marker, before the ack):
  → `reconcile_required` (`reduce_crash_before_identity`). Unchanged units do **not**
  prove the broker never saw it, because the order may be pending or the snapshot may lag,
  so it is never `rejected` (C1-1, C1-2).
- `submitted`: `get_close_order(orderId)`.
  - `pending` → stays `submitted` (pending), resumed next cycle.
  - Broker status rejected/cancelled and `positions` empty → `rejected`. No exposure
    changed.
  - **Landed** iff `status == "filled"`, `position_ids == (owned id,)`, reference and
    instrument match (the close branch's rules, `:648-669`), **and** the affected
    position's `units` is present and numerically equal to `units_to_deduct`. The witness
    is the broker's record **of this order**. No snapshot arithmetic is used (C1-22,
    C1-23). Then U1: the snapshot must still carry the owned id → operation `applied`,
    order `filled`, ownership untouched, trade `open`.
  - Filled but the id is absent from the snapshot → `reconcile_required`
    (`reduce_position_id_not_retained`). Filled with `units` absent or unequal →
    `reconcile_required` (`reduce_units_unwitnessed`). Any other combination →
    `reconcile_required` (`reduce_outcome_unclassified`) (C1-26).
- **`reconcile_required` on a reduce is sticky.** It blocks every later core sell (§4) until
  an operator resolves it. This is deliberate. The manager stops resuming it (C1-5),
  because a later automatic reading could not tell a late fill from an external action.
  The operator-facing reason codes above are the refusal surface. Clearing it is manual
  and out of scope, the same as the close path today.
- `_finish_close` gains a guard: it raises unless the operation row is `operation_type =
  'close'` (C1-7).

### 6. Migration (C1-29, C1-30)

One file:

- widen `operation_type` CHECK to add `'reduce'`, and `trigger_code` CHECK to add
  `'core_rebalance'`;
- the per-type `order_id` clause: `reduce` requires `order_id IS NOT NULL`, like `close`;
- add nullable `units_before NUMERIC(18,6)`, `units_to_deduct NUMERIC(18,6)`,
  `core_rebalance_intent_id` FK, `core_eligibility_proof_id` FK;
- conditional CHECK: `(operation_type = 'reduce') = (units_before IS NOT NULL AND
  units_to_deduct IS NOT NULL AND core_rebalance_intent_id IS NOT NULL AND
  core_eligibility_proof_id IS NOT NULL)`, and `units_to_deduct > 0 AND units_to_deduct <
  units_before` when present. Existing rows are all non-reduce with NULLs, so they satisfy
  the CHECK and need no backfill;
- the existing status, timestamp and one-unresolved-per-ownership unique index are reused
  unchanged. The implementation re-reads `sql/289` and every later migration that alters
  them (`377`, `378`, `392`, `406`, `408`) and states the resulting constraint set in the
  migration header.

### 7. What stays refused

Multi-position sells, whole closes through the allocator, the real environment (the
executor is demo-only, `:642`), units above `max_units_per_order`, and any sell while §4's
sleeve-wide outstanding check is true.

### 8. U1 residual (C1-24, C1-25)

If eToro assigns a successor id on partial close, §5 marks the reduce `reconcile_required`.
Capital resolution then raises `engine_capital_ownership_unwitnessed` for the vanished id,
which is the existing, visible fail-closed wedge. The successor position is **not** claimed
or managed by the engine until an operator resolves it. It is a demo, attended-only path,
and the successor carries whatever SL/TP eToro copies over. The acceptance below
therefore checks SL/TP on the post-trim position explicitly. One observed transition does
not prove retention forever, so every reduce re-checks U1 (§5). No run relies on an
earlier observation.

## Attended acceptance (loop-ineligible)

One demo session. Tighten the core mandate's band so SPY.RTH sits above `upper`, then
`POST /core-sleeve/rebalance`. Record: the ack body, the close-order detail, and the
snapshot `positionID` / `units` / `stopLossRate` / `takeProfitRate` before and after.
Also record any `trade_events` row written for the position.

- **Pass** = operation `applied`, same `positionID` present with reduced units, SL/TP
  unchanged, ownership `active`.
- **Safe fail** = operation `reconcile_required` with one of §5's codes, and no second sell
  admitted.

This also answers #2965's open question, since it observes the fields after a partial
close.

## Delivery

One PR: pure units/selection/re-check with table tests; the preflight sell arm; the
migration; the provider verb with guard and lane wiring; `submit_core_reduce` and the
resume branch; DB tests for the authority transaction, landed, id-not-retained,
units-unwitnessed, pre-marker abandon, crash-before-identity, and the sleeve-wide
outstanding refusal. The acceptance is recorded as pending on #2603.

## Verdict — Codex ckpt-1 round 2 (2026-09-22): PARKED on U3

Round 2 raised 29 findings. They split into two groups.

### Blocking: U3, the realised-P&L event semantics after a partial close (r2-21, 22, 23)

`_load_realised_delta` sums `trade_events` `close` rows per owned `position_id`
(`strategy_engine_capital.py:112-160`). The capital authority, and so the #2844 sandbox
bound, depends on that sum. What a partial close writes there is **undocumented and
unobserved on dev**:

- **No row written:** a realised **loss** is omitted, so capital is **overstated** and
  the sandbox admits exposure above the pot. This is the unsafe direction. The claim in
  "Premise checks" that it only "under-reports" is wrong.
- **Row written with NULL P&L:** `engine_capital_population_incomplete` fires on every
  later cycle. That is a persistent wedge.
- **Rows written cumulatively** (the partial row and then the final row both carry the
  running P&L): the sum double-counts.

Only an observation can tell these apart. This is the same unknown that refused #2965
twice ("what the broker reports after a partial close"), and the #2965 doc says not to try
a third key against it. Designing the sell leg around a guess here would put the
operator's only safety net on that guess. **No revision 3 until U3 is observed.**

### Mechanical: resolve in revision 3 (no observation needed)

- **Rounding (r2-1, 2, 4, 5):** round units UP to the increment, as the sizer does
  (`strategy_core_sizing.py:375`), not down. Rounding down can leave the weight above
  `upper`. Re-validate the extrapolation ratio on `A'` explicitly. Persist `units_before` at
  the snapshot's full precision, not NUMERIC(18,6). Refuse a missing, zero or non-finite
  mark or units before any division.
- **Increment (r2-3):** a broker-rounded fill has already changed exposure before §5 sees
  it, so 6 dp is not "fail closed". U2 joins the observation list.
- **Locking and transactions (r2-6, 7, 8):** take the manager's locks BEFORE sizing, or
  re-take the snapshot under them. Re-check snapshot age after acquiring them. Name the
  transaction boundary: the executor's authority transaction must commit before
  `submit_core_reduce` opens its own, and authority is re-proved under the new locks.
- **Authority (r2-9):** keep the manager's `allow_close_position` check alongside
  `allow_partial_close_position`.
- **Quarantine (r2-10, 11, 12, 13):** the outstanding check must also block on a `close` in
  `reconcile_required`. An unresolved reduce must quarantine the POSITION from every
  manager mutation, and block core BUYS as well as sells. Specify what happens to an
  emergency/operator close while a reduce is pending. Today it is blocked indefinitely
  (`strategy_position_manager.py:1261-1263`).
- **Resolution (r2-14 to 20):** fetch the close-order detail BEFORE the snapshot, so a
  pre-fill snapshot cannot certify retention. On `applied`, require the residual units and
  unchanged SL/TP. Validate reference and instrument before ANY terminal classification. A
  cancelled status with empty `positions` does not prove zero execution, so it goes to
  `reconcile_required`. On a lookup transport failure, stay pending. Reject the `orders`
  row atomically with the operation. The external-trim case (r2-14) is undetectable
  without a broker-side expected-units condition, so it is a stated residual risk.
- **De-risk refusals (r2-24, 25):** define an over-limit sleeve (negative headroom →
  `assigned_cash_available = 0`, sell still classified). Decide whether a disabled mandate
  or pot blocks a reduction. The manager rule says de-risking is never blocked.
- **Migration (r2-26, 27):** for non-reduce rows, require all four reduce columns NULL, not
  just "not all populated". Write out the effective status/timestamp/unique-index set from
  `289` + `377`/`378`/`392`/`406`/`408` in the spec, not a promise to read them.
- **Acceptance (r2-28, 29):** "safe fail" also covers §4's codes and a clean `rejected`.
  C1-32 (partial realised P&L) is now U3 above.

### Wake condition (per-ticket, producible, split)

**Wake:** an operator-attended demo session partially closes a NON-engine position via
`UnitsToDeduct`. The operator can do this on demand. #2965 already asks for exactly this.
The session posts on #2603 or #2965:

- (U1) the snapshot `positionID` and `units` before and after;
- (U2) the requested versus executed units (the close-order detail's `positions[].units`),
  with a requested quantity carrying more than 2 decimal places;
- (U3) every `trade_events` row for that position after the partial close, and again
  after the remainder is closed, with `realized_pnl_usd` on each.

**Split:** nothing in the sell leg is safely buildable ahead of U3. The provider verb alone
has no caller, and the operator's observation does not need it (a manual partial close in
the eToro UI is enough). #2603's other halves are unaffected by this park.
