# Core sell leg — partial close of one owned core position (#2603)

Status: **SUPERSEDED (2026-09-23) by revision 5,
`docs/proposals/ta/2026-09-23-core-sell-leg-close-rebuy.md`. The partial-close model was
replaced after Codex ckpt-1 round 4 raised 38 findings, 20 of them marked SAFETY. The
reasons are recorded there.** This file is kept as the record of revisions 1–4 and of the
U1–U3 observations.

Round 4 was run on revision 4 (below). Queue: #2437
Tier A item 2. Round 2 parked the spec on U3, the realised P&L a partial close leaves
behind. The attended session of 2026-09-23 has since observed U1, U2 and U3. Revision 3
applied those observations. Revision 4 answers Codex round 3's 46 findings.

Findings are cited as follows:

- `C1-n` for round 1;
- `r2-n` for round 2;
- `#n` for round 3.

## What is missing

`evaluate_core_rebalance` returns `sell_core` when the core weight is strictly above the
band's upper edge. Four things stop that sell from executing:

- `assess_core_broker_preflight` refuses it with `core_close_side_cost_quote_unavailable`
  before any broker call (`strategy_core_broker_preflight.py:320`).
- `_CORE_ORDER_SHAPE` has no `sell_core` entry (`strategy_core_executor.py:59`).
- The strategy close verb, `close_demo_strategy_position`, sends `UnitsToDeduct: None`
  (`etoro_broker.py:826`).
- **New in r3 (found here, not in r1/r2):** `resolve_engine_capital_usage` refuses every
  partially altered core position with `engine_capital_ownership_mismatched`
  (`strategy_engine_capital.py:428`). A successful trim therefore wedges the capital
  reader on every later cycle. The sell leg has to lift this refusal for a trim it
  witnessed itself, and only for that (§6).

## Observations (attended demo, 2026-09-23; evidence posted on #2965)

The evidence file is `tests/fixtures/etoro/attended_2026-09-23-01_partial_close.jsonl`,
lines 4–10 and 49. The position was a non-engine $50 AAPL position, id `3602886855`,
opening order `383320185`, `units` 0.147071. The session partially closed it with
`UnitsToDeduct: 0.073536` and then closed the remainder.

- **U1: the position id is retained.** After the partial close the snapshot carries the
  same `positionID`. The changes are `units` 0.147071 → 0.073535, `amount` 50 → 25 and
  `isPartiallyAltered` false → true. `initialUnits`, `orderID`, `openDateTime`,
  `stopLossRate` and `takeProfitRate` are unchanged.
- **U2: 6 dp was accepted and executed exactly.** The ack echoes `unitsToDeduct: 0.073536`
  under a new close `orderID` (383287588). The slice's history row has `units` 0.073536,
  and the residual is exactly 0.147071 − 0.073536. The session did **not** fetch the
  close-order detail, so `positions[].units` for a partial close is still unobserved. §5
  therefore does not depend on it.
- **U3: a partial close writes a close row under a NEW position id.**
  `events_from_history` groups rows by `position_id` (`trade_events.py:209`). The slice
  therefore lands as its own `open` plus `close` pair under `3602887089`. That id never
  appeared in the portfolio. The row has `order_id` 383320185 (the opening order) and
  `parent_position_id` 0. The history row existed on its own at 00:07:56, before the
  remainder's row appeared. Its fields are slice-local: `investment` 25, `units` equal to
  the slice, and `initialInvestment` 50 as the only whole-position field. It carries a
  non-NULL `realized_pnl_usd`.

  Reproduce with: `SELECT position_id, event_kind, units, realized_pnl_usd, order_id FROM
  trade_events WHERE order_id = 383320185` → 4 rows (2 open, 2 close). Close P&L is 0.0000
  on `3602887089` and −0.0100 on `3602886855`.

What U3 settles, against round 2's three cases:

- **"No row written"** is false. A row is written, but `_load_realised_delta` joins close
  rows `ON event.position_id = owned.broker_position_id` (`strategy_engine_capital.py:
  136-137`), so it **never counts the slice**. The unsafe direction (a loss omitted, so
  capital is overstated) is real, and it comes from the join, not from absence.
- **"NULL P&L"** is not observed on the slice row.
- **"Cumulative"** is **not discriminated** by this observation. Per-slice P&L is
  (339.87 − 339.94) × 0.073535 = −0.00515. Cumulative P&L is −0.00883. Both round to
  −0.01. The documented reading is per-row: the history item's `netProfit` is *"The net
  profit of the trade"* (`openapi_v1.375.0.json`, `/api/v1/trading/info/trade/demo/history`),
  and every other field on the row is slice-local. §6d's `min(reported, reconstructed)`
  and its deviation guard bound the error if that reading is wrong, so the design does
  not rest on it.

Full-population note (#41): exactly one `order_id` in `trade_events` spans more than one
`position_id` across open **and** close rows:

```sql
SELECT count(*)
FROM (
  SELECT order_id FROM trade_events
  WHERE order_id IS NOT NULL
  GROUP BY order_id
  HAVING count(DISTINCT position_id) > 1
) g
```

It returns 1, and that one is this order. So no other partial close exists in the ledger
to measure against. Every contract in U1-U3 is a single observation (#42), which is why §7
re-witnesses each one on every operation and fails closed when it does not hold.

## Premise checks (carried from r2, re-verified 2026-09-23)

- **One caller.** `execute_core_rebalance` is called only from the attended endpoint
  (`app/api/strategies.py`, `POST /core-sleeve/rebalance`). Nothing unattended reaches the
  sell path.
- **One owned core position** (`3601264304`). A whole-close-only design would take the
  sleeve to 0% core and the next cycle would buy it back, so the sell must be a partial
  close.
- **Committed capital follows the broker.** `core_committed` is the snapshot row's
  `amount` (`strategy_engine_capital.py:432`). After a trim it falls to the residual
  invested amount. So the realised P&L of the slice must be counted (§6), or a loss is
  lost.
- **Over-limit sleeve (r2-24).** `headroom_from_bound` clamps `remaining` at 0
  (`strategy_capital_sandbox.py`). An over-limit sleeve therefore observes
  `assigned_cash_available = 0`, and weight 100% classifies as `sell_core`. No change
  needed.

## Source rule

eToro, from `openapi_v1.375.0.json` and `etoro-api.md`:

- Market close by position: *"If `UnitsToDeduct` is provided, only the specified portion
  will be closed … A 200 response means the close order was submitted, not that the
  position was closed. Confirm the outcome with GET …/info/demo/close-orders/{orderId}."*
- `OrderForCloseInfoResponse.positions[]`: `positionID` (required); `units` is *"Number of
  position units closed, when available"*.
- `InstrumentEligibility.allowPartialClosePosition` is already enforced
  (`core_partial_close_unproved`, `strategy_core_submission_gate.py:462-470`).
- What-if close arm: requires `positionIds`, and is measured with an amount ticket
  (#2712).
- Close-side minimum: none is documented, so `broker_minimum=None` on a sell and the floor
  is the mandate's `min_rebalance_amount`.
- Trade history `netProfit`: *"The net profit of the trade"*; `units`: *"The number of
  units traded"*.
- Increment: not documented. 6 dp is the only observed value (U2). §1 fixes the scale by
  construction at 6 dp, the scale the broker echoed and executed.

## Design

### 1. Selection, units and rounding

All inputs come from ONE `get_account_risk_snapshot()`, taken under the locks (§4).

- **Selection:** the owned core position (`capital_authority.core_active_position_ids`)
  with the largest snapshot `units`. Ties go to the lowest id. Today exactly one exists.
- **Refuse before dividing (r2-5):** `core_sell_position_unmarkable` if `units` or
  `market_value` is missing, non-finite or ≤ 0.
- **Conversion (C1-16):** `raw_units = S × units / market_value`. `S` is the sizer's sell
  amount, and both other figures come from the selected position in the same snapshot.
- **Round UP to 6 dp (r2-1, r2-2).** The sizer quantises amounts up
  (`strategy_core_sizing.py:375`), and rounding units down could leave the weight above
  `upper`. The 6 dp scale is by construction: it is the only scale observed accepted and
  executed exactly (U2), and every later operation re-witnesses it (§7, conditions 4-5).
- **Re-check on the rounded ticket (r2-4):** `A' = units_to_deduct × market_value /
  units`. Because `A' ≥ S`, nothing checked on `S` is inherited. These are re-run on `A'`:
  - `A' ≥ min_rebalance_amount`, else refuse `below_min_rebalance_amount`;
  - the extrapolation ratio against the quote ticket, else refuse
    `cost_quote_ticket_mismatch`;
  - the sizer's `at_zero` / `at_bound` bracket, else refuse `cost_breaches_far_edge`.
- **Bounds:**
  - `0 < units_to_deduct < units_before`, else refuse `core_sell_spans_positions`;
  - above `max_units_per_order`, refuse `core_sell_exceeds_max_units`.
- **Entry reference (#12):** the owning trade must have exactly one `purpose='entry'`
  order with a positive integer `broker_order_ref`, read as `load_whole_close_evidence`
  reads it. Otherwise refuse `core_sell_entry_ref_ambiguous`. §7 binds the slice through
  this reference.

**Capped-pot sizing (#20, #21): stated, not changed.** In `fixed` (capped) mode, which is
today's pot (`strategy_paper_pool_events`: `fixed`, 500), the sleeve's cash term is the
pot headroom. Headroom excludes realised gains and absorbs any over-limit deficit first.
So a sell of `S` shrinks the sleeve by the skimmed gain, or by the deficit absorbed,
where the sizer assumes the sleeve is conserved (net of cost).

Both effects push the post-trade core weight **above** the sizer's prediction, never
below. The sell under-shoots `upper` and does not overshoot `lower`. The next attended
cycle sells again, on fresh observation, and the sequence converges from above. No
buy-back churn is possible, because weight stays above `upper`, which is above `lower`.

Codex's example: basis 100, mark 200, sell 40 → 88.9% where 80% was predicted. That is a
second attended sell, not a safety issue. A sizer term for the skim is deferred until a
drift is observed that the loop cannot close in two cycles.

### 2. Close-side cost quote

This is the buy arm's shape, run under the locks:

1. a fresh snapshot and the age check;
2. `observe_core_sleeve`;
3. a fresh `evaluate_core_rebalance`, which must return `sell_core` with the same amount
   as the recorded intent, else refuse `core_sleeve_moved_since_decision`;
4. `get_what_if_costs(BrokerWhatIfOrder(action="close", transaction="sell",
   position_ids=(pid,), amount=fresh.amount, …))`;
5. `decode_quoted_trade_cost`;
6. `resolve_core_trade_size` (sell arm);
7. §1.

Slippage and market movement stay excluded, as the sizer's contract already states.

### 3. Which refusals apply to a sell (C1-33, r2-25)

- `sandbox_exceeded` (both the broker preflight and the executor) and the executor's
  drawdown refusal do **not** refuse a `sell_core`. A sell reduces exposure. The drawdown
  **observation** is still recorded.
- **A disabled mandate or pot still refuses.** The sell is an allocator action, not
  emergency de-risking. De-risking has its own path: the manager close with
  `emergency_risk` / `operator_close`.
- All other refusals apply unchanged.

### 4. Locks, lifetime and re-proof (r2-6, 7, 8; #22-26)

**Order.** `core_submission_lock` → `_paper_allocator_lock` → `_position_lock(pid)`.

Acquisition sites, re-grepped for this revision:

- `PAPER_ALLOCATOR_ADVISORY_LOCK` is taken in:
  - `strategy_position_manager._paper_allocator_lock`, which is used by
    `manage_owned_position` and, after this change, by `submit_core_reduce`;
  - `app/api/config.py:258`;
  - `app/api/strategies.py:3600`;
  - `app/workers/scheduler.py:6763`.
- `CORE_SUBMISSION_ADVISORY_LOCK` is taken in:
  - `core_submission_lock` (the executor);
  - `app/api/broker_credentials.py:924`.

The implementation PR lists what each allocator-lock site calls while holding it, and
shows none reaches `core_submission_lock`. It adds a test asserting that
`strategy_position_manager` and the three xact sites do not import `core_submission_lock`
or `CORE_SUBMISSION_ADVISORY_LOCK`. That test is drift detection only; the listed audit is
the proof. If the audit fails, the sell is refused, not built around.

**Selection before locking (#22).** The executor selects the candidate `pid` from its
pre-lock snapshot (the one its DB preflight already used). Under the position lock,
`submit_core_reduce` takes a new snapshot and re-runs selection. A different `pid` →
refuse `core_sleeve_moved_since_decision`.

**Re-proof (#23).** Under the locks, `submit_core_reduce` re-runs the executor's **whole**
DB preflight: mandate enabled, pot enabled, capital authority, ownership `active`, the
quarantine (§5), and eligibility (`allow_close_position` AND
`allow_partial_close_position`, r2-9). It then runs §2 and §1 before any insert. It never
re-checks a subset.

**Lifetime (#24).** Both locks are held from re-proof through:

1. the intent insert;
2. `mark_close_submitting`;
3. the broker call;
4. outcome persistence.

This matches `manage_owned_position` → `_submit_close`. Recovery (§7) runs under the same
pair, so it cannot abandon an intent whose submitter is still inside the call.

**Transaction boundaries.**

1. The executor commits its intent row under `core_submission_lock`, and keeps holding
   that lock.
2. `submit_core_reduce` opens its own transactions, under the lock order above.

**Marker (#25).** `mark_close_submitting` checks the UPDATE's rowcount and raises unless
it is 1. A no-op transition then prevents broker I/O. The same fix is applied to the
existing close path, where the defect also lives (`strategy_position_manager.py:994-999`).

### 5. Authority, quarantine and submission — `strategy_position_manager.submit_core_reduce`

**Quarantine predicate** (sleeve-wide, r2-10, 11, 12). It is true when any core-owned
position has either:

- an operation in (`intent_persisted`, `submitting`, `submitted`); or
- a `reduce` or `close` operation in `reconcile_required`.

Where it applies:

- It refuses a core **sell** (`core_sell_operation_outstanding`) and a core **buy**
  (`core_operation_outstanding`).
- Other allocators on the shared pot are covered by capital resolution (#19). From the
  moment a reduce fills until it is `applied`, the owned position is partially altered
  with no witnessed conservation (§6b). `resolve_engine_capital_usage` therefore raises,
  and every allocation path on the pot is refused. Before the fill, exposure is
  unchanged, and the pending order can only reduce it.

**A reduce never writes `strategy_trades.status` (#29-31).** Every reduce outcome touches
only the operation row and its `orders` row. The trade stays whatever it was, so no
unrelated state is erased and the close path's trade-status guards are unchanged.

**Emergency / operator close (r2-13, #27, #28).**

- **While a reduce is in flight:** `manage_owned_position` resumes the in-flight
  operation first and returns (the `_resume_operation` early return in `manage_owned_position`). An emergency close therefore waits for the
  reduce to resolve. That wait is bounded by the broker's order lifetime and is the same
  wait any in-flight close or edit imposes today. It is stated, not changed.
- **After a reduce is `reconcile_required`:** the reduce is not in-flight, so the
  emergency close proceeds. The manager's repair, ratchet and timeout arms return
  `reduce_unresolved` without writing, but an explicit `close_reason` is admitted.
- **After an emergency close of a partially altered position:** #3312's whole-close
  release refuses, because an unowned slice row exists under the entry order. The trade
  goes to `reconcile_required` through the existing branch. Capital raises
  `engine_capital_ownership_unwitnessed` for the vanished id until a human resolves it.
  Fail closed, visible, no new code.

**Inserts, in one transaction after re-proof:**

- an `orders` row: `action='EXIT'`, `order_type='MARKET'`, `requested_amount = A'`,
  `status='submitted'`, linked to the owning entry trade with `purpose='exit'`;
- the operation row: `operation_type='reduce'`, `trigger_code='core_rebalance'`,
  `status='intent_persisted'`, with `units_before`, `units_to_deduct`,
  `core_rebalance_intent_id` and `core_eligibility_proof_id`.

The authority transaction verifies that the proof belongs to the intent, as the buy arm
does (#39). It writes no `strategy_order_reconciliation_state` row and no
`strategy_core_entry_exit_levels` row.

**Submission:**

- `mark_close_submitting`, with the rowcount check.
- New provider verb `reduce_demo_strategy_position(position_id, instrument_id,
  units_to_deduct, request_id, persist_response)`. It is `close_demo_strategy_position`
  with `UnitsToDeduct` set: same endpoint, same quota lane, same
  `refuse_broker_mutation_if_unattended`, same demo-only check. It refuses a `None`,
  non-finite or non-positive quantity before I/O, and sends a JSON number from the 6-dp
  `Decimal`, never `null`.
- Outcomes, mirroring `_submit_close` minus any trade-status write:
  - uncertain → `reconcile_required` (`broker_reduce_uncertain`);
  - definite rejection → operation and `orders` row both `rejected`, in one transaction;
  - ack → store `broker_order_ref` and set status `submitted`. An ack `unitsToDeduct` that
    is present but unequal → `reconcile_required` (`reduce_ack_units_mismatch`).

### 6. Capital: witnessed-trim admission and per-operation slice P&L

**6a. Snapshot fields (#43).** Parse `initialUnits` and `initialAmountInDollars` onto the
risk-snapshot direct row. Both are documented as *"This value does not change in case the
position was partially closed"* (`openapi_v1.375.0.json`, portfolio position schema), and
both were observed unchanged (U1).

**6b. Admission of a reduced core row** (`resolve_engine_capital_usage`, #16, #17).
These checks apply whenever the ownership has ≥ 1 `applied` reduce, **whatever
`is_partially_altered` says**:

- no reduce on the ownership is in a non-terminal status or in `reconcile_required`;
- `initial_units − units` equals Σ `units_to_deduct` over its applied reduces, as an exact
  Decimal comparison at full precision (#37);
- every applied reduce has a bound slice row (6c).

A core row that is partially altered with no applied reduce keeps today's
`engine_capital_ownership_mismatched`. So does any row failing the checks above.
Examples: an operator's manual trim, a split, or broker-side rounding.

For an admitted reduced row, committed capital is
`max(row.amount, initial_amount_in_dollars × units / initial_units)`. This is the larger
of the broker's residual amount and the proportional basis, so a broker `amount` that
drops faster than units cannot free headroom (#17).

**6c. Per-operation slice binding** (#8-#15, replacing r3's aggregate rule). A new column
`strategy_position_operations.slice_position_id BIGINT`, with a partial UNIQUE index,
binds each reduce to exactly one broker slice. The binding is written by §7 when it
marks the reduce `applied`, and only then.

A **candidate** is a `trade_events` row that satisfies every one of these:

- `event_kind='close'` and `source='etoro_history'`;
- `order_id` = the ownership's single entry `broker_order_ref`;
- `position_id` is no `strategy_position_ownership.broker_position_id` (any trade, any
  status);
- `position_id` is bound to no other operation;
- `units` equals `units_to_deduct` exactly;
- `executed_at ≥ operation.created_at − 60 s`.

The 60 s allowance is by construction. `created_at` is DB time written before the broker
call, so a genuine fill is later on a synced clock, and 60 s absorbs clock skew between
our host and the broker. No published rule exists.

Exactly one candidate → bind. Zero → the reduce stays `submitted` (history not yet
ingested). More than one → `reconcile_required` (`reduce_slice_ambiguous`). This ends
#8/#9: two ownerships under one entry order cannot claim the same slice, because the
binding is unique and ambiguity fails closed. It also ends the aggregate-compensation
problem (#10, #11). Nothing depends on `submitted_at` (#13, #14).

**6d. Realised P&L** (`_load_realised_delta`). There is one query in the same
transaction as the authority's other reads (#18). It sums:

- today's per-position close rows, unchanged;
- for each **bound** slice of an applied reduce on an eligible ownership, that slice's
  close row.

For every slice row, and for the final close row of a position with ≥ 1 applied reduce:

- **Required fields (#7):** `realized_pnl_usd`, `units`, `price`, and `openRate` in the
  raw payload must be present and finite, and `fees_usd` must equal 0. A missing field or
  non-zero fees → `engine_capital_population_incomplete`. Fees are 0 on every observed
  row. How fees combine with `netProfit` is undocumented (#3), so a fee'd row refuses
  rather than guessing.
- **Instrument currency (#6):** `instruments.currency` for the core instrument must be
  `USD`, else `engine_capital_snapshot_unusable`.
- **Contribution:** `min(realized_pnl_usd, (price − openRate) × units)`. This takes the
  lower of the broker's figure and its own-rate reconstruction, so neither a rounding
  difference nor a cumulative reading can raise capital above the reconstruction (#1,
  #2, #4).
- **Deviation guard:** `|realized_pnl_usd − (price − openRate) × units| > 0.01` →
  `engine_capital_population_incomplete`. The 0.01 figure is by construction, as the
  observed 2-dp resolution of `netProfit`, and it is frozen as a module constant. The
  guard makes a cumulative reading visible once it matters rather than silent.
- The x1, unleveraged, long-only premise that the reconstruction needs (#5) is already
  enforced for core rows (`is_buy` required; the executor opens at leverage 1). The
  implementation also requires `leverage = 1` on each counted row.

**6e.** A released, reduced ownership never reaches this sum automatically. #3312 refuses
to release a partially altered position, so final-close completeness (#15) is a human
resolution.

### 7. Resolution — a `reduce` branch at the TOP of `_resume_operation` (#32-36)

The branch is dispatched **before** `_exact_broker_position` is called
(`strategy_position_manager.py:620`), and so before every existing branch.

- **`intent_persisted`** → `rejected` (`reduce_abandoned_before_submission`), with the
  `orders` row `rejected` in the same transaction.
- **`submitting` without `broker_order_ref`** → `reconcile_required`
  (`reduce_crash_before_identity`).
- **`submitting` with `broker_order_ref`** (#33): unreachable, because the reference and
  `submitted` are written in one UPDATE. If it is seen anyway, it is handled as
  `submitted`.
- **`submitted`:**
  1. Validate the stored reference and instrument.
  2. Fetch `get_close_order(orderId)`, **then** take the snapshot.
  3. Validate that the returned detail's order id and instrument equal ours (#34). A
     mismatch → `reconcile_required` (`reduce_identity_mismatch`).
  4. Classify the outcome:
     - Lookup transport failure → stays `submitted`.
     - `pending` → stays `submitted`.
     - Broker `rejected`/`cancelled` **and** snapshot `units == units_before` → `rejected`,
       with the `orders` row. Any other cancelled shape → `reconcile_required`
       (`reduce_cancel_unproved`).
     - `filled`, and `positions` names exactly the owned id. Then look at the snapshot:
       - the id is absent → `reduce_position_id_not_retained`;
       - `units == units_before` (snapshot lagging the fill, #35) → stays `submitted`, and
         the next cycle re-reads it;
       - `units == units_before − units_to_deduct` exactly **and**
         `initial_units − units` equals Σ applied plus this one **and** detail
         `positions[].units`, when present, equals `units_to_deduct` → go to slice binding
         (6c). One candidate → `applied` + bind, with the order `filled`. Zero → stays
         `submitted`. More than one → `reduce_slice_ambiguous`;
       - anything else → `reduce_outcome_unclassified`.
- **SL/TP (#36, r2-15)** are not a landing condition. The manager's fixed-exit repair arm
  already compares the broker's SL/TP to the persisted exit levels on every cycle and
  repairs any gap. That arm stays the single owner of SL/TP.
- **`reconcile_required` on a reduce is sticky.** It quarantines the sleeve (§5). Clearing
  it is manual and out of scope.
- `_finish_close` raises unless `operation_type = 'close'`.

**Residual risks (stated):**

- An external trim between the snapshot and the fill fails the residual check →
  `reconcile_required`.
- A broker-side rounding of the quantity is caught by the ack echo and the residual check.
- A reduce whose slice row never appears stays `submitted` and quarantined. That is
  visible on the operation and blocks further core sells and buys.

### 8. Migration `sql/411_core_reduce_operation.sql`

- **`operation_type` CHECK** → add `'reduce'`. The sql/289 inline CHECK is dropped by the
  catalog name read from `pg_constraint`, then re-added as
  `strategy_position_operations_operation_type_vocabulary`.
- **`strategy_position_operations_trigger_code_check`** (sql/292) → add
  `'core_rebalance'`.
- **Per-type shape CHECK** (sql/289, unnamed) → re-added as
  `strategy_position_operations_type_shape`, with
  `(operation_type='reduce' AND order_id IS NOT NULL AND desired_stop_rate IS NULL AND
  desired_take_profit_rate IS NULL)` added.
- **New nullable columns:**
  - `units_before NUMERIC`;
  - `units_to_deduct NUMERIC(18,6)`;
  - `core_rebalance_intent_id` FK;
  - `core_eligibility_proof_id` FK;
  - `slice_position_id BIGINT`.
- **`strategy_position_operations_reduce_shape`:**
  - reduce ⇒ the four intent columns are NOT NULL; `units_before` is finite
    (`units_before < 'Infinity'` and `units_before <> 'NaN'`, since PostgreSQL `NUMERIC`
    admits both, #38); and `0 < units_to_deduct < units_before`;
  - non-reduce ⇒ all five new columns are NULL (r2-26);
  - `slice_position_id IS NOT NULL` ⇒ `status = 'applied'`.
- **New unique indexes:** `slice_position_id` WHERE NOT NULL, and
  `core_rebalance_intent_id` WHERE `operation_type='reduce'` (one reduce per intent, #39).
- **Carried unchanged** (the effective set after sql/289, 292, 377, 378 and 406; r2-27):
  - `strategy_position_operations_status_vocabulary` (sql/377);
  - `strategy_position_operations_resolution_shape` (sql/377);
  - `idx_strategy_position_one_unresolved_operation` UNIQUE `(ownership_id)` WHERE status
    IN (`intent_persisted`,`submitting`,`submitted`) (sql/377). It covers `reduce`
    as-is.
  - `idx_strategy_position_operation_material_identity`, which is limited to
    `fixed_exit_repair`/`stop_ratchet` and `status <> 'applied'` (sql/406).
  - sql/378 is a data-only backfill. sql/392 and 408 alter other tables.

### 9. What stays refused

- multi-position sells and whole closes through the allocator;
- the real environment;
- units above `max_units_per_order`;
- an ambiguous entry reference;
- any core sell or buy while the §5 quarantine is true;
- a reduced core row the engine did not fully witness (§6b).

## Attended acceptance (loop-ineligible)

One demo session. Tighten the core band so SPY.RTH sits above `upper`, then call
`POST /core-sleeve/rebalance`. Record:

- the ack;
- the close-order detail (`positions[].units`, unobserved for a partial);
- the snapshot `positionID` / `units` / `initialUnits` / `initialAmountInDollars` / SL /
  TP, before and after;
- every `trade_events` row under the opening order after the next sync;
- the bound `slice_position_id`;
- the next capital resolution.

Outcomes:

- **Pass** = operation `applied` with one bound slice; same `positionID` with the residual
  units; ownership `active`; the next capital resolution succeeds and includes the
  slice's contribution (6d).
- **Safe fail** = any §1/§4/§5 refusal, a clean `rejected`, or `reconcile_required` with a
  §5/§7 code. In every case the same intent is never submitted twice (#45).
- **Cumulative vs per-slice `netProfit`** cannot be discriminated by one trim (#44). It
  is discriminated the first time a reduced position is closed whole with slice P&L above
  1 cent. Until then, 6d's `min(·)` and deviation guard bound it.

## Delivery

One PR, containing:

- pure units/selection/rounding/re-check and the pure 6c/6d rules, with table tests. These
  include a cumulative-reading fixture that the deviation guard must refuse and a
  two-ownership-one-order fixture that must go ambiguous;
- the preflight sell arm and the executor `sell_core` branch calling
  `submit_core_reduce`, with intent terminal states mirroring the buy arm (#40);
- the migration;
- the snapshot field parse;
- the provider verb with guard and lane wiring;
- the marker rowcount fix;
- the quarantine;
- the `reduce` resume branch and binding;
- DB tests, one per new SQL mechanism: the authority transaction; `_load_realised_delta`
  with a bound slice (the 2026-09-23 rows); the binding uniqueness; the quarantine.

Acceptance is recorded as pending on #2603.

## History

- **r1:** 33 findings (`C1-n`).
- **r2:** 29 findings (`r2-n`), parked on U3.
- **r3:** U1/U2/U3 observed; the capital-reader lift added. Codex r3 raised 46 findings
  (`#n` above). The main one was aggregate slice matching, which could double-count
  across ownerships sharing an entry order (sql/282).
- **r4:** per-operation unique binding replaces aggregate matching; `min(reported,
  reconstructed)`; full lock lifetime and re-proof; capped-pot under-shoot stated;
  reduces never write trade status.
