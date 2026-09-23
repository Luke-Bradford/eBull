# Book a confirmed late EXIT fill on a recommendation order (#3007 part 2)

Status: proposal (spec), rev 2, 2026-09-23. Refs #3007, #2942 (slice C), #2965.

## Problem

A live recommendation EXIT is submitted through `close_position`, and the close
is sent whole (`UnitsToDeduct=null`, `sql/409` column comment). The broker's
acknowledgement carries no price, no units and a numeric `statusID`, so
`execute_order` leaves the order `pending` (#3007 half 1). The poller
(`reconcile_pending_recommendation_orders`) then resolves the close through
`GET /api/v1/trading/info/{env}/close-orders/{orderId}` (half 2).

When that lookup says the close filled, `_record_unbooked_fill` parks the row
as `filled_unbooked`. It writes an audit row, books nothing and keeps the claim
held (`app/services/order_client.py:3211`). From then on the order is `pending`
for ever, the recommendation is `execution_pending` for ever, there is no
`fills` row, and `positions.realized_pnl` never records the disposal.

Part 1 (`sql/409`, `b14db5a1`) persists which lot the close addressed. Booking
therefore no longer needs a lot guess at poll time.

## Finding that shapes the design: `portfolio_sync` already books half of it

A late fill is booked up to one poll interval (hourly) after the broker
executed it. By then `portfolio_sync` has usually applied the close from broker
state (`app/services/portfolio_sync.py`):

| ledger effect | synchronous EXIT (`execute_order`) | `portfolio_sync` after a broker-side close |
| --- | --- | --- |
| `positions.current_units` | `_update_position_exit` subtracts | set to broker units (`:741`), or zeroed when the instrument is gone (`:877`) |
| `positions.cost_basis` | proportional s104 withdrawal | same proportional withdrawal (`:736`), or zeroed (`:878`) |
| `positions.realized_pnl` | `(price − avg_cost) × units` | **not accrued, and cannot be** (`:867`: no close price) |
| `cash_ledger` | `order_sell` row | `broker_sync` row for the broker-vs-local delta (`:896-909`) |
| `broker_positions` mirror | `_deduct_closed_exit_lot` | replaced wholesale |
| `fills` row / order + recommendation terminal state | written | never |

Suppose a late booking re-ran the synchronous sequence after a sync had already
applied the close. It would double-count: `current_units` would drop by the lot
a second time and could go negative, the cost pool would be withdrawn twice,
and cash would be credited twice. Whether the sync has already run is a race
the poller cannot observe reliably. `broker_positions` presence is not a sync
marker, because the mirror is best-effort (`order_client.py:1092-1098`).

**Decision: a late booking writes only what `portfolio_sync` cannot write —
the `fills` row, the `realized_pnl` accrual, and the terminal order and
recommendation states. Units, cost basis, cash and the mirror stay with the
sync.**

⚠ **Scope of the claim.** This spec does not claim the sync handles a
broker-side close correctly in every case. Codex ckpt-1 rev 1 listed several
cases where it does not:

- the empty-portfolio refusal (`:831`);
- the mirror refusal (`:675`);
- netted snapshots;
- a stale snapshot;
- the cash-sum race;
- the tolerance band.

**Every one of them applies today, unchanged, to the same close while it sits
unbooked.** Before and after this change, the sync is the only writer of units,
cost and cash for a late close. This change adds records beside the sync's
writes and never contends with them. Those sync limitations are not this
ticket's, and nothing here makes them worse.

## Source rule: what the broker tells us about the fill

The committed contract (`tests/fixtures/etoro/openapi_v1.375.0.json`) defines
`OrderForCloseInfoResponse.positions[]` as `OrderForClosePositionInfoResponse`,
with fields `positionID`, `occurred`, `rate`, `units`, `conversionRate` and
`amount`. Every field except `positionID` is documented as "when available".

The route was observed live once, on 2026-09-22 (#3007, 14:45Z): `statusID=3`,
`positions=[3602456774]`, 404 first and filled seconds later. The per-position
fields were not recorded, so **whether they are present on the live route is
unmeasured**.

`statusID` on this route is an opaque integer with no enum (#3007 half 1). So
"filled" continues to be read from the response shape, as
`get_close_order` already does (`etoro_broker.py:928-934`): no `errorCode`
plus a non-empty `positions[]`. Completeness is then established
independently, by discriminator 5: the whole recorded lot closed, and the
close was sent whole.

**Currency and scale — parity, not a new treatment.** `fills.price` is the raw
broker execution rate, which is what the synchronous path already writes from
`executionPrice` (`order_client.py:2711-2729`), and `positions.avg_cost` is
struck in the same unit. `conversionRate` and `amount` are recorded in the
audit and not used. Any FX correction is the synchronous path's problem as
much as this one's, and belongs to neither ticket.

**Fees.** This response has no fee field. `fills.fees = 0`, which matches the
synchronous close path: `_normalise_close_order_response` reports no fee
either. The audit records `fees_source: "not_reported"`, so a zero here is
labelled as unknown rather than asserted. Any real fee reaches `cash_ledger`
through the sync's delta.

## Schema: capture the pool's average cost at submission (`sql/413`)

`realized_pnl` needs the `avg_cost` the lot was disposed against. Reading it at
booking time, up to an hour later, is wrong whenever the pool changed in
between. That covers:

- a BUY/ADD fill;
- an external re-open that `portfolio_sync` imports with a new `avg_cost`
  (`:764-807`);
- an acquisition the broker has made that is not yet booked locally.

A guard that looks for "later BUY fills" misses the last two and races the
first (rev 1, findings 4-9).

**Fix:** capture it with the lot, in the same claim INSERT that already
records the lot. Add `orders.recommendation_exit_avg_cost NUMERIC(18,6)`, the
`positions.avg_cost` read in the claim transaction. Extend
`orders_recommendation_exit_lot_check`, with the same explicit-NOT-NULL and NaN
discipline, so that:

- a lot row carries a positive, finite `avg_cost`;
- a non-lot row carries NULL.

**A lot whose position has a NULL `avg_cost` is not claimable for a live
EXIT.** Today that would fail the claim INSERT, so `_load_exit_lot`'s caller
must refuse it first, with a refusal audit (`exit_avg_cost_unknown`). Census
(dev, 2026-09-23): `select count(*) from positions where current_units > 0 and
avg_cost is null` = **0** of 6 open positions.

Pre-413 rows read `avg_cost=None` and are refused by discriminator 1.

## Booking discriminator

The booking runs only when **all** of the following hold. Discriminators 1-8
are evaluated on the lookup result and on the row re-read under the lock.
Discriminators 9-10 are enforced inside the write transaction.

1. `load_recommendation_submission_context(...)` returns `recorded`, a
   non-None `exit_lot`, and a non-None submission `avg_cost`. A lot is never
   re-selected (#2942).
2. The close is a success, not a contradiction: `close_detail.status ==
   "filled"`. It is evaluated **before** `_apply_pending_order_verdict`'s
   rejected-with-executions demotion, and that demotion never books.
3. `positions[]` has exactly one entry, and its `positionID` equals
   `exit_lot.position_id`.
4. `rate` is a finite Decimal and `> 0`. A bool is not a number, and NaN, ±inf
   and non-numeric values are refused.
5. `units` is a finite Decimal. Quantised with `_BROKER_UNITS_STEP` /
   `ROUND_HALF_UP`, it is `> 0` and equal to `exit_lot.units`. That makes this
   a whole close of the recorded lot. A partial close stays with a human, the
   release scope #2965 set.
6. No `fills` row exists for this `order_id`.
7. No other order with the same `broker_order_ref` and `broker_environment` is
   `filled`. One broker close is booked once, whichever local row names it.
8. A `positions` row exists for the instrument.
9. **Order CAS (write step 1)** matches `status='pending'`, the same
   `broker_order_ref`, `recommendation_poll_parked_reason IS NULL`, and
   `action='EXIT'`. A row that was parked between batch selection and this
   poll is therefore not booked, and not re-decided either.
10. **Recommendation CAS (write step 4)** matches.

Discriminators 6 and 7 also run inside the transaction, as `NOT EXISTS`
clauses on the order CAS, not only as a pre-check.

**When a refusal parks and when it retries.** A refusal on 1, 2 (rejected
with executions), 3, 5 (units present but unequal), 6, 7 or 8 describes a
property of the order or the answer that re-asking cannot change. It takes
today's `_record_unbooked_fill` park.

A refusal on 4, or on 5 because `units` is absent, is the contract's "when
available". It is **stamped and not parked**, so the row stays pollable and a
later answer that carries the fields can book it. That is the same posture
`lookup_error` has, and it costs one shared read per poll interval. It is not
unbounded: the claim is held, nothing double-submits, and each such poll
writes an ERROR log naming the missing field. ⚠ This means the poller keeps
asking about a close that is confirmed filled but not bookable, so the
operator sees it in the logs, not only in a one-shot audit row.

Every park or stamp from this path writes its audit with
`refusal: "late_fill_not_bookable"`, the failing discriminator's name, and the
compared values. It replaces `_record_unbooked_fill`'s "booking is not
implemented" text, which becomes false once this ships.

## Writes, in one transaction, under the recommendation's submission lock

1. **Order CAS** (discriminators 6, 7 and 9), `rowcount == 1`:
   `SET status='filled', raw_payload_json=…, recommendation_last_polled_at=now`.
   On a miss nothing is written, the transaction rolls back explicitly, and
   the verdict is `no_longer_pending`, the same as `_terminalise_rejected_order`.
   `filled` is outside `idx_orders_recommendation_open_attempt`'s
   `('submitted','pending','uncertain')` predicate (`sql/375:70-73`), so this
   CAS releases the claim. That is correct, because the close is now recorded.
2. **`fills` row** via `_persist_fill`, with `price = rate`,
   `units = quantised units`, `fees = 0`, and `filled_at` chosen as follows:
   - `occurred`, when it parses as an aware timestamp in
     `[orders.created_at, clock_timestamp()]`;
   - otherwise `clock_timestamp()` (the transaction's wall clock, not the
     batch-start `now`).

   `filled_at_source ∈ {broker_occurred, booking_time}` goes into the audit.
   `occurred` absent or implausible is **not** a refusal. It changes only the
   timestamp, never an amount.
3. **Realized P&L:**
   `UPDATE positions SET realized_pnl = COALESCE(realized_pnl, 0) + (rate −
   submission_avg_cost) × units, updated_at = <booking time> WHERE
   instrument_id = …`, with `rowcount == 1` or it raises. This is the
   synchronous path's formula (`order_client.py:1029-1030`), struck against
   the pool as it stood at submission. `current_units` and `cost_basis` are not
   touched. The rounding relationship between `avg_cost × units` and the
   sync's proportional withdrawal is the same one the synchronous path already
   has (`_update_position_exit` docstring, n-ULP bound).
4. **Recommendation CAS:** `execution_pending → executed`. A miss raises, and
   the transaction rolls back (the half-release rule, as in
   `_terminalise_rejected_order`).
5. **`_write_execution_audit`:** PASS, with the explanation `late EXIT fill
   booked from close-order lookup`, plus:
   - the lookup payload;
   - `filled_at_source` and `fees_source`;
   - `conversionRate` and `amount`;
   - the lot-level completion: `exit_scope: "lot"`, `lot_units_selected`,
     `units_closed`, and `position_fully_closed: null`.

   Whether the instrument is fully exited is unknown at booking time, because
   units belong to the sync. It is recorded as unknown rather than guessed.
6. **`enqueue_post_trade_sync`.** It is best-effort, as the synchronous path's
   is. It adds nothing to correctness, because the scheduled sync already
   covers this.

**Not written:** `cash_ledger`, `positions.current_units`,
`positions.cost_basis`, `broker_positions`, `record_estimated_cost` (BUY/ADD
only on the synchronous path).

**Attribution is NOT triggered.** Whether the position is fully closed is not
knowable at booking time: the sync owns units, and several lots may close late
in any order. Triggering on a guess would persist attribution over incomplete
fills. This is the gap every sync-observed close already has. No attribution
sweep exists (`compute_attribution`'s only caller is
`order_client.py:1184`). It is noted on the PR, not fixed here.

New verdict: `filled_booked`. It is terminal and not parked: a `filled` row
leaves `_POLLABLE_ORDER_PREDICATE` by status.

## Provider change

`BrokerCloseOrderDetail` gains
`positions: tuple[CloseOrderPositionFill, ...]`, where each entry carries
`position_id` (int), `rate`, `units` and `occurred`.

- **`positionID` stays strict.** A malformed id still raises
  `BrokerPositionMutationUncertain`, as today (`etoro_broker.py:898-900`), so
  `position_ids`, derived from it, keeps its exact current semantics and
  `strategy_position_manager` is untouched.
- **`rate`, `units` and `occurred` are lenient.** Each is `None` when absent
  or unparseable, and any value that is not a finite, non-bool number reads as
  `None`. Validation happens in the discriminator, not in the parse, so a
  missing field can never break a working recovery path (the class docstring's
  reason).

## Out of scope

- **BUY/ADD late fills** (#2942 slice C proper). Different route
  (`lookup_order`), and a late BUY needs `_persist_broker_position`.
- **Partial closes** (#2965).
- **Attribution sweep**: see above.
- **Existing parked rows.** Census (dev, 2026-09-23):
  `select count(*) from orders where recommendation_id is not null and
  recommendation_poll_parked_reason = 'filled_unbooked'` = **0**, and there
  are 0 recommendation-origin orders of any status. Every such row is
  pre-413, so it has no submission `avg_cost` and discriminator 1 would refuse
  it anyway. If the census is non-zero, those rows stay with a human, as today.

## Acceptance

- **Unit (pure):** a discriminator table covering every refusal, with its
  park-or-stamp classification, plus the booking case.
- **DB:**
  - book-after-sync and book-before-sync each leave `current_units`,
    `cost_basis` and the `cash_ledger` sum untouched by the booking;
  - `realized_pnl` uses the submission `avg_cost` even when `positions.avg_cost`
    has since changed;
  - an order-CAS miss writes nothing;
  - a recommendation-CAS miss rolls back the fill and the P&L;
  - a parked row is not booked;
  - a second order with the same filled ref is refused.
- **Live acceptance (loop-ineligible):** one attended pending→fill on a
  recommendation-origin EXIT. This is the wake already stated on #3007. It also
  records whether `positions[].rate`, `units` and `occurred` are present on the
  live route.
