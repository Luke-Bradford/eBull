# Book a confirmed late EXIT fill on a recommendation order (#3007 part 2)

Status: proposal (spec), 2026-09-23. Refs #3007, #2942 (slice C), #2965.

## Problem

A live recommendation EXIT is submitted through `close_position`. The broker's
acknowledgement carries no price, no units and a numeric `statusID`, so
`execute_order` leaves the order `pending` (#3007 half 1). The poller
(`reconcile_pending_recommendation_orders`) resolves the close through
`GET /api/v1/trading/info/{env}/close-orders/{orderId}` (half 2). When that
lookup says the close filled, `_record_unbooked_fill` parks the row as
`filled_unbooked`. It writes an audit row, books nothing and keeps the claim
held (`app/services/order_client.py:3211`). The order stays `pending` and the
recommendation stays `execution_pending` for good.

Part 1 (`sql/409`, `b14db5a1`) now persists the lot the close addressed
(`orders.recommendation_exit_position_id` / `recommendation_exit_units`), so
booking no longer needs a poll-time lot guess.

## Finding that shapes the design: `portfolio_sync` already books half of it

A late fill is booked up to one poll interval (hourly) after the broker
executed it. By then `portfolio_sync` (5-minute tick, and on every
`enqueue_post_trade_sync`) has usually applied the close from broker state
(`app/services/portfolio_sync.py`):

| ledger effect | synchronous EXIT (`execute_order`) | `portfolio_sync` after a broker-side close |
| --- | --- | --- |
| `positions.current_units` | `_update_position_exit` subtracts | set to the broker's units (`:741`), or zeroed when the instrument is gone (`:877`) |
| `positions.cost_basis` | proportional s104 withdrawal | the same proportional withdrawal (`:736`), zeroed on a full close (`:878`) |
| `positions.realized_pnl` | `(price − avg_cost) × units` | **not accrued, and cannot be** (`:867`: no close price) |
| `cash_ledger` | `order_sell` row, gross − fees | `broker_sync` row for the whole broker-vs-local delta (`:896-909`) |
| `broker_positions` mirror | `_deduct_closed_exit_lot` | replaced wholesale (`_upsert_broker_positions`) |
| `fills` row | written | never |
| order / recommendation terminal state | `filled` / `executed` | never |

So if a late booking ran the synchronous sequence after a sync had already
applied the close, it would **double-count**:

- `current_units` drops by the lot a second time and can go negative;
- the cost pool is withdrawn twice;
- cash is credited twice (the next sync then writes a compensating negative
  `broker_sync` row, so the net is right but the ledger records two false
  events).

Whether the sync has run is a race the poller cannot win reliably.

**Decision: a late booking writes only what `portfolio_sync` cannot, and leaves
units, cost basis, cash and the mirror to the sync.** Those are
broker-observed state, and the sync is already their authority for any
disposal the ledger did not originate (#3017). This holds whichever side of
the race the booking lands on:

- **Sync already ran:** units, cost basis and cash are already correct, and
  the booking adds the fill and the P&L.
- **Sync has not run yet:** the booking adds the fill and the P&L, and then
  `enqueue_post_trade_sync` in the same transaction makes the sync apply units,
  cost basis and cash within seconds. That is the same trigger the synchronous
  path uses (#1593).

## Source rule: what the broker tells us about the fill

`OrderForCloseInfoResponse.positions[]` is `OrderForClosePositionInfoResponse`
in the committed contract (`tests/fixtures/etoro/openapi_v1.375.0.json`). It
has `positionID`, `occurred`, `rate`, `units`, `conversionRate` and `amount`,
and every field except `positionID` is documented as "when available". The
route was observed live once, on 2026-09-22 (#3007, 14:45Z): `statusID=3` and
`positions=[3602456774]`. The per-position fields were not recorded, so their
live presence is **unmeasured**. The parse is therefore lenient (a missing field
reads as `None`) and the booking refuses on `None`. Anything it refuses keeps
today's behaviour: parked, audited, claim held.

There is no fee field on this response. `fees = 0` on the fill row matches what
the synchronous EXIT path already books: `_normalise_close_order_response`
reports no fee either. The cash consequence of any real fee reaches
`cash_ledger` through the sync's delta, which is the authority for cash here.

## Booking discriminator

The booking runs only when **all** of the following hold. Otherwise the row
takes today's `_record_unbooked_fill` path unchanged.

1. `load_recommendation_submission_context(...).recorded` is true and
   `exit_lot` is not None. A pre-409 row, or one with no lot, is refused, and a
   lot is never re-selected (#2942).
2. `positions[]` has exactly one entry, and its `positionID` equals
   `exit_lot.position_id`.
3. Its `units` is present and positive, and it equals `exit_lot.units` at the
   stored grain (`numeric(20,8)`, the `_BROKER_UNITS_STEP` quantisation
   `_deduct_closed_exit_lot` uses). This means a **whole close of the recorded
   lot** only. A partial close stays with a human, which is the release scope
   #2965 set.
4. Its `rate` is present and positive.
5. No BUY/ADD `fills` row exists for this instrument with
   `filled_at > order.created_at`. A re-open between submission and booking
   recomputes `positions.avg_cost`, and `realized_pnl` would then be struck
   against the wrong pool. That case is refused rather than modelled.
6. A `positions` row exists for the instrument. Same guard, same reason as
   `_update_position_exit`'s rowcount check (#3013).

## Writes, in one transaction under the recommendation's submission lock

The lock and the row re-read under it are what `_poll_one_pending_order`
already does.

1. **Order CAS:** `UPDATE orders SET status='filled', raw_payload_json=… WHERE
   order_id=… AND status='pending' AND broker_order_ref=…`, with a
   `rowcount == 1` guard. On a miss, nothing is written and the verdict is
   `no_longer_pending`, the same miss semantics as `_terminalise_rejected_order`.
   `filled` is outside `idx_orders_recommendation_open_attempt`'s predicate, so
   this CAS is what releases the claim. That is correct, because the economic
   close has now been recorded.
2. **`fills` row** via `_persist_fill`: `price = rate`, `units = units`,
   `fees = 0`, `filled_at = occurred` when it parses as an aware timestamp,
   otherwise poll `now`. Which one was used goes into the audit.
3. **`positions.realized_pnl += (rate − avg_cost) × units`**, and nothing else
   on the row: no `current_units`, no `cost_basis`. The UPDATE's rowcount must
   be exactly 1, otherwise it raises and the transaction rolls back.
4. **Recommendation CAS:** `execution_pending → executed`, raising on a miss,
   with the same half-release reasoning as `_terminalise_rejected_order`.
5. **Attribution:** `_maybe_trigger_attribution(conn, instrument_id,
   units_after)`, where `units_after` is decided by whether the sync has
   applied the close. If the exit lot's `broker_positions` row is present with
   units > 0, the sync has not applied it, and `units_after = current_units −
   lot units`. Otherwise `units_after = current_units`. Attribution reads
   `fills`, so it needs step 2 and not the units.
6. **`_write_execution_audit`**: PASS, with the explanation `late EXIT fill
   booked from close-order lookup`, the lookup payload, and
   `filled_at_source ∈ {broker_occurred, poll_time}`.
7. **`enqueue_post_trade_sync`**.

**Not written:** `cash_ledger`, `positions.current_units`,
`positions.cost_basis`, `broker_positions`. See the finding above.
`record_estimated_cost` is also not written. The synchronous path records an
estimated cost only for BUY/ADD, so there is nothing to parity.

New verdict: `filled_booked`. It is terminal and not parked: a `filled` row
leaves `_POLLABLE_ORDER_PREDICATE` by status.

## Provider change

`BrokerCloseOrderDetail` gains `positions: tuple[CloseOrderPositionFill, ...]`,
with `position_id`, `rate`, `units` and `occurred`, each `None` when absent or
unparseable. `position_ids` stays, derived from it, so
`strategy_position_manager` is untouched. The parse stays lenient for the
reason the class docstring already gives.

## Out of scope

- **BUY/ADD late fills** (#2942 slice C proper). Their confirmation comes
  through a different route (`lookup_order`), and a late BUY needs
  `_persist_broker_position`, whose inputs are not a mirror concern.
- **Partial closes** (#2965).
- **A backfill.** Census: `select count(*) from orders where recommendation_id
  is not null and recommendation_poll_parked_reason = 'filled_unbooked'` must
  be run and recorded in the PR. It is expected to be 0: dev has 0
  recommendation-origin orders, per #3007's 19:03Z note.

## Acceptance

- **Unit:** the discriminator table covers each of the 6 refusals plus the
  booking case. Book-after-sync and book-before-sync each leave
  `current_units`, `cost_basis` and the cash total equal to a single
  application of the close. A CAS miss writes nothing.
- **Live acceptance (loop-ineligible):** one attended pending→fill on a
  recommendation-origin EXIT. This is the wake already stated on #3007, and it
  also records whether `positions[].rate`, `units` and `occurred` are present
  on the live route.
