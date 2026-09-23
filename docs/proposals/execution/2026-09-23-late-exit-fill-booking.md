# Book a confirmed late EXIT fill on a recommendation order (#3007 part 2)

Status: proposal (spec), rev 3, 2026-09-23. Refs #3007, #2942 (slice C), #2965.

## Problem

A live recommendation EXIT is submitted through `close_position`, and the close
is sent whole (`UnitsToDeduct=null`, per the `sql/409` column comment). The
broker's acknowledgement carries no price, no units and a numeric `statusID`,
so `execute_order` leaves the order `pending` (#3007 half 1). The poller
(`reconcile_pending_recommendation_orders`) then resolves the close through
`GET /api/v1/trading/info/{env}/close-orders/{orderId}` (half 2).

When that lookup says the close filled, `_record_unbooked_fill` parks the row
as `filled_unbooked` and books nothing (`app/services/order_client.py:3211`).
The result is permanent: the order stays `pending`, the recommendation stays
`execution_pending`, there is no `fills` row, and nothing is added to
`positions.realized_pnl`.

Part 1 (`sql/409`, `b14db5a1`) persists which lot the close addressed, so no
lot has to be guessed at poll time.

## Finding that shapes the design: `portfolio_sync` already books half of it

A late fill is booked up to one poll interval (hourly) after the broker
executed it. By then `portfolio_sync` has usually applied the close from broker
state (`app/services/portfolio_sync.py`):

| ledger effect | synchronous EXIT (`execute_order`) | `portfolio_sync` after a broker-side close |
| --- | --- | --- |
| `positions.current_units` | `_update_position_exit` subtracts | set to broker units (`:741`) / zeroed (`:877`) |
| `positions.cost_basis` | proportional s104 withdrawal | proportional withdrawal on the NET units change (`:736`) / zeroed (`:878`) |
| `positions.realized_pnl` | `(price − avg_cost) × units` | **not accrued, and cannot be** (`:867`) |
| `positions.unrealized_pnl` | untouched | refreshed from broker (`:742`, `:879`) |
| `cash_ledger` | `order_sell` row | `broker_sync` delta row (`:896-909`) |
| `broker_positions` mirror | `_deduct_closed_exit_lot` | replaced wholesale |
| `fills` row / order + recommendation terminal state | written | never |

A late booking that re-ran the synchronous sequence after a sync would
double-count units, cost and cash. Whether a sync has already run is not
reliably observable: the mirror is best-effort (`order_client.py:1092-1098`).

**Decision:** a late booking writes only what `portfolio_sync` cannot:

- the `fills` row;
- the `realized_pnl` accrual;
- the terminal order and recommendation states.

Units, cost basis, unrealized P&L, cash and the mirror stay with the sync. The
two writers update DISJOINT accounting columns of the same `positions` row.
They take the same row lock but never write the same value.

⚠ **Scope of the claim, and what IS new.** The sync's own limitations with a
broker-side close are pre-existing and apply identically today, while the same
close sits unbooked. Those are: the empty-portfolio refusal (`:831`), the mirror
refusal (`:675`), cost withdrawal on NET units change (a close netted against
an acquisition inside one snapshot withdraws too little: rev 2 finding 7), a
stale snapshot, the cash-sum race, and the tolerance band. This change is not
their fix. Two effects are **new** and are accepted, stated here:

1. **Realized and unrealized P&L briefly overlap.** Between the booking and the
   next successful sync, `realized_pnl` includes the disposal while
   `unrealized_pnl` still carries the disposed units. The next successful sync
   clears it. If syncs keep refusing (`:675`, `:831`), the overlap lasts as
   long as the refusal, and that refusal already pages as a failed job.
2. **Realized P&L is struck at the submission-time average cost** (below). The
   sync later withdraws cost from the pool proportionally. The two agree to
   rounding when nothing else moved the pool, and they differ when the sync
   nets. That difference is the sync's netting defect showing through, not a
   new one.

## Source rule, and why the booking ships DISABLED

The committed contract (`tests/fixtures/etoro/openapi_v1.375.0.json`) defines
`OrderForCloseInfoResponse.positions[]` as `OrderForClosePositionInfoResponse`,
with fields `positionID`, `occurred`, `rate`, `units`, `conversionRate` and
`amount`. Every field except `positionID` is documented as "when available".
The route was observed live exactly once (#3007, 2026-09-22 14:45Z:
`statusID=3`, `positions=[3602456774]`), and the per-position fields were not
recorded.

So three facts the booking needs are **unmeasured**:

- (a) whether `rate` and `units` are present;
- (b) whether `rate` is the executed close rate and `units` the executed units,
  rather than the position's own data echoed back;
- (c) whether `rate` is in the unit that `positions.avg_cost` and the
  synchronous path's `executionPrice` use (the contract carries a separate
  `conversionRate`).

The existing shape heuristic in `get_close_order` ("no `errorCode` plus a
non-empty `positions[]` means filled", `etoro_broker.py:928-934`) is safe
today only because its consequence is a park. It must not start moving money
until (a), (b) and (c) are observed.

**Rule, by construction:** `CLOSE_ORDER_FILL_FIELDS_VERIFIED: Final = False`
in `order_client.py`, following the `PROVIDER_REWRITE_TIMING_VERIFIED`
precedent (`app/services/bar_capture_certificate.py:287`). While it is False,
the booking is evaluated and its complete decision is recorded in the audit,
but the row takes today's `_record_unbooked_fill` park. Flipping it is a
one-line PR that must cite the attended observation of (a), (b) and (c): one
recommendation-origin EXIT closed, its close-order lookup captured verbatim,
and the captured `rate` / `units` checked against the broker's trade-history
row for the same position (`closeRate` / `units`,
`tests/fixtures/etoro/attended_2026-09-23-01_partial_close.jsonl`, which has
that shape).

⚠ This is what the "buildable now" split (#3296) actually licenses here. The
decision logic, schema and tests land now. The money-moving write goes live
only on the evidence.

**Fees.** This response has no fee field. `fills.fees = 0`, the same as the
synchronous close path (`_normalise_close_order_response` reports none), and
the audit records `fees_source: "not_reported"`. Any real fee reaches cash
through the sync's delta.

## Schema: capture the pool's average cost at submission (`sql/413`)

`realized_pnl` needs the `avg_cost` the lot was disposed against. Reading it
at booking time, up to an hour later, is wrong whenever the pool moved in the
meantime: a BUY/ADD fill, an external re-open that the sync imports with a new
`avg_cost` (`:764-807`), or an unbooked acquisition.

- **New column:** `orders.recommendation_exit_avg_cost NUMERIC(18,6)` (the
  `positions.avg_cost` grain). It is read in the claim transaction and written
  by the same claim INSERT that records the lot.
- **Constraint:** a new, separate CHECK. The column is either NULL, or it is
  on an EXIT row that carries a lot and is `> 0 AND <> 'NaN'`. The existing
  `orders_recommendation_exit_lot_check` is left as it is, so post-409 /
  pre-413 lot rows stay valid with a NULL cost, and the reader refuses them
  (discriminator 1).
- **Rollout order:** a worker running the pre-413 code simply leaves the
  column NULL, which is legal.
- **Claim-time refusal:** a live EXIT whose `positions.avg_cost` is NULL, zero,
  negative or NaN is refused before the claim, with a refusal audit
  `exit_avg_cost_unusable`, and is never claimed without a cost. Census (dev,
  2026-09-23): `select count(*) from positions where current_units > 0 and
  (avg_cost is null or avg_cost <= 0 or avg_cost = 'NaN')` = **0** of 6 open
  positions.
- **Residual, stated:** the cost is captured when the claim commits, and the
  close executes after the broker call returns, seconds later. A pool change
  inside that window from a DIFFERENT recommendation's BUY is refused by
  discriminator 8. An external acquisition inside the window is not visible to
  us at all, and the same is true of the synchronous path.

## Booking discriminator

Evaluated on the lookup result and on the order row re-read under the lock.
Every refusal **parks** (today's `_record_unbooked_fill`, claim held): a
close the broker reports filled that we cannot book is a human's, whichever
field was missing. A later answer is never allowed to release it. That makes
the execution evidence sticky (rev 2 finding 1), and it bounds the polling.

1. The context is `recorded`, with a non-None `exit_lot` and a submission
   `avg_cost`.
2. `close_detail.status == "filled"`. A rejected response carrying executions
   is demoted by `_apply_pending_order_verdict` and never books.
3. `positions[]` has exactly one entry, and its `positionID` equals
   `exit_lot.position_id`.
4. `rate` is a finite, non-bool number, `> 0`, and fits `fills.price`'s
   declared precision.
5. `units` is a finite, non-bool number. Quantised with `_BROKER_UNITS_STEP` /
   `ROUND_HALF_UP`, it is `> 0`, equals `exit_lot.units`, and fits
   `fills.units`.
6. No `fills` row exists for this order, **and** no `fills` row exists on any
   other EXIT order in the same `broker_environment` whose `broker_order_ref`
   is numerically equal to this one. The comparison is numeric, as in
   `_poll_one_pending_order`'s identity guard: `"00123"` = `"123"`.
7. A `positions` row exists for the instrument.
8. No BUY/ADD `fills` row exists for the instrument with
   `filled_at >= orders.created_at`.
9. `CLOSE_ORDER_FILL_FIELDS_VERIFIED` is True. The audit records all of 1-8
   either way.

**Serialisation.** Discriminators 6-8 run inside the write transaction, after
`SELECT … FROM positions WHERE instrument_id = … FOR UPDATE`.

- Every EXIT booking and every BUY/ADD fill for the instrument writes that
  row, so two late bookings naming the same close, or a concurrent BUY, are
  serialised on it. That closes the cross-recommendation race that the
  per-recommendation lock cannot close (rev 2 findings 2 and 5).
- Lock order is `positions` first, as in `_update_position_exit`, followed by
  `orders`.

## Writes: one explicit outer transaction

The flow opens the transaction itself, and the context and lookup reads happen
before it commits cleanly (rev 2 finding 20). The writes run under the
recommendation's submission lock:

1. `positions … FOR UPDATE`, then discriminators 6, 7 and 8.
2. **Order CAS**, `rowcount == 1`. It matches:
   - `status='pending'`, `action='EXIT'`, `recommendation_poll_parked_reason
     IS NULL`;
   - the same `broker_order_ref`, `broker_environment`, `instrument_id` and
     `recommendation_id`;
   - the same `recommendation_exit_position_id` and `recommendation_exit_units`
     that were loaded.

   It sets `status='filled', raw_payload_json=…,
   recommendation_last_polled_at=…`. On a miss, the transaction rolls back
   and the row is re-read. If it is still `pending` and unparked, it parks with
   the audit `late_fill_cas_conflict`. Otherwise the verdict is
   `no_longer_pending` (rev 2 finding 21). `filled` is outside
   `idx_orders_recommendation_open_attempt`'s predicate (`sql/375:70-73`), so
   this CAS releases the claim, correctly, because the close is now recorded.
3. **`fills`** via `_persist_fill`: `price = rate`, `units = quantised units`,
   `fees = 0`. `filled_at` is `occurred` when it is an ISO-8601 string with an
   offset (parsed by `datetime.fromisoformat`) that falls inside
   `[orders.created_at, clock_timestamp()]`; otherwise it is
   `clock_timestamp()`. `filled_at_source` goes into the audit. The timestamp
   never gates the booking.
4. **`positions`:** `realized_pnl = COALESCE(realized_pnl, 0) + (rate −
   submission_avg_cost) × units` and `updated_at`, and nothing else. This is
   the synchronous path's formula (`order_client.py:1029-1030`).
5. **Recommendation CAS:** `execution_pending → executed`. A miss raises and
   the whole transaction rolls back (the half-release rule,
   `_terminalise_rejected_order`).
6. **`_write_execution_audit`:** PASS, with the lookup payload, the
   discriminator record, `filled_at_source`, `fees_source`, `conversionRate` /
   `amount`, and the lot-level completion (`exit_scope: "lot"`, lot units,
   units closed, `position_fully_closed: null`, because the units belong to
   the sync).
7. **`enqueue_post_trade_sync`**, best-effort as on the synchronous path.

**Not written:** `cash_ledger`, `current_units`, `cost_basis`,
`unrealized_pnl`, `broker_positions`, `record_estimated_cost`. **Attribution is
not triggered.** Full closure is not knowable at booking time, and no
attribution sweep exists (`compute_attribution`'s only caller is
`order_client.py:1184`). A synchronous final-lot close that runs before an
earlier late fill is booked will likewise attribute over incomplete fills.
That is a pre-existing ordering gap, and it is noted on the PR.

New verdict: `filled_booked`, terminal (a `filled` row leaves
`_POLLABLE_ORDER_PREDICATE` by status). With the flag False it is unreachable,
and the verdict stays `filled_not_booked`.

## Also fixed in passing (the same lock re-read)

`_poll_one_pending_order`'s re-read under the lock gains
`recommendation_poll_parked_reason`. A row parked between batch selection and
the poll returns `no_longer_pending` before any broker call, so a later
rejection cannot terminalise a row whose fill was already recorded (rev 2
finding 18, pre-existing).

## Provider change

`BrokerCloseOrderDetail` gains `positions: tuple[CloseOrderPositionFill, ...]`,
each with `position_id`, `rate`, `units` and `occurred`.

- **`positionID` is strict.** It accepts a non-bool `int`, or a string of ASCII
  digits, and the value must be `> 0`. Anything else raises
  `BrokerPositionMutationUncertain`. This tightens today's `int()` coercion,
  which truncates floats and accepts bools (rev 2 finding 16). `position_ids`
  is derived from it, so `strategy_position_manager` sees the same set.
- **`rate` / `units`** are lenient: a Decimal from a non-bool `int`, a `float`
  or a numeric string, and `None` for anything else.
- **`occurred`** is lenient: the raw string, or `None`.

## Out of scope

- BUY/ADD late fills (#2942 slice C proper).
- Partial closes (#2965).
- An attribution sweep.
- Every sync limitation listed above.
- **Existing parked rows.** Census (dev, 2026-09-23): `select count(*) from
  orders where recommendation_id is not null and recommendation_poll_parked_reason
  = 'filled_unbooked'` = **0**, and there are 0 recommendation-origin orders of
  any status.

## Acceptance

- **Pure:** a discriminator table covering each refusal and the book decision,
  plus the flag.
- **DB (flag patched True in the test):**
  - booking leaves `current_units`, `cost_basis`, `unrealized_pnl` and the
    `cash_ledger` sum unchanged;
  - `realized_pnl` uses the submission cost after `positions.avg_cost` has
    moved;
  - an order-CAS miss writes nothing and parks;
  - a recommendation-CAS miss rolls back the fill and the P&L;
  - a parked row is not booked;
  - a second EXIT order whose numerically-equal ref already has a fill is
    refused.
- **DB (flag False):** the row parks exactly as today, and the audit carries
  the would-book decision.
- **Flag flip (separate PR, attended):** the (a)/(b)/(c) observation described
  above.
