# Book a confirmed late EXIT fill on a recommendation order (#3007 part 2)

Status: proposal (spec), **rev 5**, 2026-09-23. Refs #3007, #2942 (slice C),
#2965, #3320, #3322.

Rev 3 was parked at Codex ckpt-1 round 3 (findings 37 → 23 → 32) because the
close-order fill fields were unmeasured. The attended GBX partial close
(#3007 comment 5791270221) measured them. Rev 4 rebuilds the booking on that
evidence and drops the `CLOSE_ORDER_FILL_FIELDS_VERIFIED` flag, which existed
only to wait for it. Rev 4 drew 54 ckpt-1 findings, mostly on two checks that
compared timestamps across clocks and reference strings across orders. Rev 5
replaces both with same-row identity checks under one lock (see the
disposition table). Slice 1 (`sql/413`, #3319) is merged and unchanged.

## Problem

A live recommendation EXIT is submitted through `close_position`, and the close
is sent whole (`UnitsToDeduct=null`, per the `sql/409` column comment). The
broker's acknowledgement carries no price, no units and a numeric `statusID`,
so `execute_order` leaves the order `pending` (#3007 half 1). The poller
(`reconcile_pending_recommendation_orders`) then resolves the close through
`GET /api/v1/trading/info/{env}/close-orders/{orderId}` (half 2).

When that lookup says the close filled, `_record_unbooked_fill` parks the row
as `filled_unbooked` and books nothing (`app/services/order_client.py:3234`).
The result is permanent: the order stays `pending`, the recommendation stays
`execution_pending`, there is no `fills` row, and nothing is added to
`positions.realized_pnl`.

## Finding that shapes the design: `portfolio_sync` already books half of it

A late fill is booked up to one poll interval (hourly) after the broker
executed it. By then `portfolio_sync` has usually applied the close from broker
state (`app/services/portfolio_sync.py`):

| ledger effect | synchronous EXIT (`execute_order`) | `portfolio_sync` after a broker-side close |
| --- | --- | --- |
| `positions.current_units` | `_update_position_exit` subtracts | set to broker units / zeroed |
| `positions.cost_basis` | proportional s104 withdrawal | proportional withdrawal on the NET units change / zeroed |
| `positions.realized_pnl` | `(price − avg_cost) × units` | **not accrued, and cannot be** |
| `positions.unrealized_pnl` | untouched | refreshed from broker |
| `cash_ledger` | `order_sell` row | `broker_sync` delta row |
| `broker_positions` mirror | `_deduct_closed_exit_lot` | replaced wholesale |
| `fills` row / order + recommendation terminal state | written | never |

A late booking that re-ran the synchronous sequence after a sync would
double-count units, cost and cash. Whether a sync has already run is not
reliably observable, because the mirror is best-effort.

**Decision (settled at rev 2, unchanged):** a late booking writes only what
`portfolio_sync` cannot:

- the `fills` row;
- the `realized_pnl` accrual;
- the terminal order and recommendation states.

Units, cost basis, unrealized P&L, cash and the mirror stay with the sync. The
two writers update DISJOINT columns of the same `positions` row.

Two new effects are accepted:

1. **Realized and unrealized P&L briefly overlap.** Between the booking and the
   next successful sync, `realized_pnl` includes the disposal while
   `unrealized_pnl` still carries the disposed units. The next successful sync
   clears it.
2. **Realized P&L is struck at the submission-time average cost** (`sql/413`).
   The sync later withdraws cost proportionally. The two agree when nothing
   else moved the pool, and they differ when the sync nets (a pre-existing sync
   defect showing through).

The sync's own limitations with a broker-side close are pre-existing, apply
identically today while the close sits unbooked, and are out of scope. They
are the empty-portfolio refusal, the mirror refusal, netting, a stale
snapshot, the cash-sum race and the tolerance band.

## Source rule and evidence

The contract (`tests/fixtures/etoro/openapi_v1.375.0.json`) defines
`OrderForCloseInfoResponse.positions[]` as `OrderForClosePositionInfoResponse`:
`positionID`, `occurred`, `rate`, `units`, `conversionRate`, `amount`. Every
field except `positionID` is documented "when available". `statusID` is an
`integer` with no `enum`.

The attended session on 2026-09-23 (IUSA.L, GBX, `assetCurrencyID 666`)
captured the lookup verbatim for a partial close (`383344846`) and a whole
close (`383339190`), with the trade-history row for each:

- **(a) Presence.** The fields are present only once the close has executed.
  At `statusID 2` the entry carried only `positionID`, and `proceeds` was
  `0.0`. **#3320 (merged) now makes `get_close_order` return `filled` only
  when every entry carries `rate`, `units` and `occurred`, and `proceeds` is
  present.** Otherwise it returns `pending`. So a `filled` detail reaching the
  booking has its fields, by construction.
- **(b) Executed, not echoed.** The partial's `rate` 5816.7 differs from the
  position's `openRate` 5817.71 and equals the history `closeRate`. Its `units`
  0.323024 are the deducted units, not the position's 0.646048. `occurred`
  equals the history close time. The whole close's `rate` 5817.29 and `units`
  0.323024 also equal its history row.
- **(c) Currency.** `rate` is in the **asset** currency.
  `rate × conversionRate × units` = 5816.7 × 0.013303 × 0.323024 = 24.9955,
  and `proceeds` is 24.99. The whole close gives 24.998 against 24.99. Both
  agree with `proceeds` truncated to the cent, on two closes.

### Which currency the booking uses: native, i.e. raw `rate`

The supervisor's queue note (#2437, 08:04Z) said "book via `rate ×
conversionRate`". **That would be wrong for this ledger, and rev 4 books raw
`rate`.** `positions` is denominated in the instrument's native (asset)
currency:

- `portfolio_sync` builds the pool from the broker's `open_price`, which is
  native: `avg_open_price = Σ(open_price × units) / Σ units`, and
  `cost_basis = avg_open_price × units` (`portfolio_sync.py:95-124`, `:805-807`).
  The broker's `openRate` is native: 5817.71 (pence) on the attended IUSA.L
  position.
- Every read converts `positions` values from native to display currency at
  read time (`app/api/portfolio.py`: `convert(..., native_ccy,
  display_currency, rates)`).
- The synchronous EXIT uses the same formula, `realized_pnl += (price −
  avg_cost) × units` (`order_client.py:1052`), with the broker's execution
  price. ⚠ That price's currency has never been observed on a non-USD
  instrument (every attended synchronous fill was USD). The late booking
  does not depend on it; it is noted here because the two paths share the
  formula.

So `(rate − submission_avg_cost) × units` is native on both sides, and it is
the synchronous path's formula. Multiplying `rate` by `conversionRate` would
subtract a native cost from a USD price. `conversionRate`, `amount` and
`proceeds` go into the audit verbatim, for reconciliation.

⚠ #3322 (filed during this work): LSE instruments are priced in pence but
labelled `GBP` in `instruments.currency`. That is a read-side conversion
defect. It does not affect this arithmetic, because `rate` and `avg_cost`
are both in pence.

**Fees.** The response has no fee field. `fills.fees = 0`, as on the
synchronous close path, and the audit records `fees_source: "not_reported"`.
Any real fee reaches cash through the sync's delta.

The currency reading rests on two closes of one instrument, plus the
contract's own separate `conversionRate` field. What is NOT sampled is the
ledger side: `positions` is native by the code above, not by observation.
Residual: a pool built by a synchronous non-USD BUY carries whatever currency
`executionPrice` is in, which is unobserved. That pool would be wrong today,
before any late booking. This spec does not change it.

**Fees.** The response has no fee field. `fills.fees = 0`, as on the
synchronous close path, and the audit records `fees_source: "not_reported"`.
Any real fee reaches cash through the sync's delta. It does not reach
`fills.fees` or `realized_pnl` on either path (pre-existing).

## Scope: whole closes only

A recommendation EXIT always closes the whole lot (`UnitsToDeduct=null`). In
the evidence, a whole close's `positions[].positionID` is the live position's
id (`383339190` → `3602947846`). A partial close reports a NEW slice id
(`3602947861`). Checks 2 and 4 therefore require the exact lot id and the
exact lot units. Any other shape parks, including a split or re-identified
whole close that the two observations did not show. Partial closes are
#2965's.

**Terminality comes from the units, not from `statusID`.** An execution
record for the lot's position id carrying exactly the lot's units is a
complete whole close. A partial execution of a whole close carries fewer units
and fails check 4. So the booking does not rely on "fields present means
final" (r3 #1 / r4 #1).

## Booking discriminator

Evaluated after `get_close_order` returned `status == "filled"` and the
existing instrument-identity check passed (`order_client.py:3625`).

**Every refusal below PARKS** (claim held, audit row committed). A close the
broker reports filled that we cannot book belongs to a human, whichever check
failed. The park removes the row from `_POLLABLE_ORDER_PREDICATE`, so no later
answer can release it.

Pure checks, run before the transaction opens. Parsing exceptions of any kind
(`InvalidOperation`, overflow, a bad timestamp) are refusals too, never
escapes:

1. **Context.** `load_recommendation_submission_context` is `recorded`, with a
   non-None `exit_lot` and `exit_avg_cost` (`sql/413`). Pre-409 and pre-413
   rows fail here.
2. **Exactly one execution, and it is the lot.** `positions[]` has exactly one
   entry, and its `positionID` equals `exit_lot.position_id`.
3. **`rate`.** A finite, non-bool JSON number (it is converted through
   `Decimal(str(value))`, never `Decimal(float)`). It must be `> 0`, and it
   must stay `> 0` when quantised to 6 dp with `ROUND_HALF_UP`. The quantised
   value must fit `NUMERIC(18,6)`: `abs < 10^12`. The booking uses the
   quantised value everywhere (the fill, the P&L and the audit), so the
   stored price is the price the P&L was struck at.
4. **`units`.** A finite, non-bool JSON number, parsed the same way. It must
   **equal `exit_lot.units` exactly** as Decimals, with no quantisation
   (`Decimal("0.323024") == Decimal("0.32302400")`). It must also be exactly
   representable at 6 dp, the scale of `fills.units`. Otherwise the stored
   units would round, so it parks.
5. **`occurred`.** A string that `datetime.fromisoformat` parses, carrying a
   UTC offset. It becomes `fills.filled_at`, because the broker's execution
   time is the disposal's time. It is compared to nothing. Our clock and the
   broker's are different clocks, so no chronology gate exists (r4 #17,
   #42-44).
6. **Arithmetic bound.** `gross = rate × units` and `delta = (rate −
   exit_avg_cost) × units`, both quantised to 6 dp, satisfy `abs < 10^12`.

Locked checks, inside the transaction, after `SELECT … FROM positions WHERE
instrument_id = %s FOR UPDATE`:

7. **The ledger row exists**, and `COALESCE(realized_pnl, 0) + delta` fits
   `NUMERIC(18,6)`.
8. **The pool's average did not move.** `positions.avg_cost ==
   exit_avg_cost`. P&L depends only on the average, and no writer changes it
   on a disposal. `_update_position_exit`, the sync's proportional withdrawal
   (`avg_cost` "unchanged by construction") and the sync's zeroing (which
   leaves `avg_cost` as the historical cost) all preserve it. So an unchanged
   average is the exact witness that the booked P&L matches the pool at
   booking. Any booked acquisition, and any re-open that the sync imports,
   recomputes it, and the row parks. This replaces rev 4's timestamp check.
   Its residual is an acquisition that we never booked and the sync never
   imported. That is invisible to the synchronous path too.
   Over-refusal (an acquisition after the disposal) parks; it is bounded by
   the poll interval, and a human resolves it.
9. **The lot was not already disposed of.** No `fills` row exists on any
   EXIT order with the same `broker_environment` and the same
   `recommendation_exit_position_id`. A broker position is closed at most
   once, and it belongs to one instrument, so the `positions` lock serialises
   every booking of it. This replaces rev 4's reference comparison
   (r4 #23-27). The row's own order is covered by the CAS.

## Writes: one explicit transaction

The poller holds the recommendation's submission lock throughout
(`_poll_one_pending_order`). Before opening the booking transaction it calls
`conn.commit()`, so `conn.transaction()` opens a real transaction and not a
savepoint (r4 #16). **No helper that commits runs inside it.**

1. `positions … FOR UPDATE`, then checks 7-9.
2. **Order CAS**, requiring `rowcount == 1`. It matches `order_id`,
   `status='pending'`, `action='EXIT'`,
   `recommendation_poll_parked_reason IS NULL`, and the
   `broker_order_ref`, `broker_environment`, `instrument_id`,
   `recommendation_id`, `recommendation_exit_position_id`,
   `recommendation_exit_units` and `recommendation_exit_avg_cost` it was
   decided on. It sets `status='filled'`, `raw_payload_json`,
   `recommendation_last_polled_at`.
   `filled` is outside `idx_orders_recommendation_open_attempt`, so this
   releases the claim, correctly.
3. **`fills`**: `price = rate`, `units`, `gross_amount = gross`, `fees = 0`,
   and `filled_at = occurred`. `_persist_fill` gains a `filled_at` parameter.
   It does not commit.
4. **`positions`**: `realized_pnl = COALESCE(realized_pnl, 0) + delta`, and
   `updated_at`. Nothing else.
5. **Recommendation CAS**: `execution_pending → executed`. A miss raises, and
   the transaction rolls back (the half-release rule,
   `_terminalise_rejected_order`).
6. **`_write_execution_audit`** (PASS). Every Decimal and datetime in the
   evidence is stringified before `Jsonb`. The evidence carries the lookup
   payload, the check record, `fees_source`, `conversionRate` / `amount` /
   `proceeds`, and `exit_completion = {exit_scope: "lot", lot_units,
   units_closed, position_fully_closed: null}`.

Commit, then `enqueue_post_trade_sync` best-effort, as on the synchronous
path.

**Lock order.** `positions`, then `orders`, then `trade_recommendations`,
then `decision_audit` (insert). The synchronous EXIT for this recommendation
cannot run concurrently: it needs the recommendation lock the poller holds.
`portfolio_sync` writes `positions` and never an existing `orders` row. The
manual close in `app/api/orders.py` inserts its own new `orders` row, then
updates `positions` (`:178-345`). It never locks an existing order row. So no
writer holds this order's row and then waits on `positions`.

**Failure handling.**

- **A refusal or an exception before commit** (a pure check, a locked check,
  an order-CAS miss, a recommendation-CAS miss, or any other exception inside
  the transaction). The transaction rolls back if it opened. The row then
  parks through a **conditional** park: `UPDATE orders SET … parked_reason
  WHERE order_id = … AND status = 'pending' AND
  recommendation_poll_parked_reason IS NULL`. Only on `rowcount == 1` is the
  refusal audit written, carrying the reason and the check record. On `0`,
  the result is `no_longer_pending`, and nothing is audited. So a concurrent
  resolution is never overwritten (r4 #30).
  `_record_unbooked_fill` becomes that conditional park, with a `reason`
  parameter (`filled_unbooked` stays the stored park value, and the specific
  reason goes in the audit evidence) (r4 #31).
- **The park itself fails.** The exception propagates to the poller's
  existing per-row isolation. The row stays `pending` and unparked, and the
  claim stays held. The next poll re-evaluates it from the start. That is the
  pre-existing posture of every poll failure, and it never releases a claim.
- **The commit's outcome is unknown** (connection lost during `COMMIT`). The
  row is neither parked nor reported. The next poll re-reads it: `filled`
  returns `no_longer_pending`, and `pending` is evaluated again (r4 #34).

**Not written:** `cash_ledger`, `current_units`, `cost_basis`,
`unrealized_pnl`, `broker_positions`, `record_estimated_cost`. Attribution is
not triggered, and estimated cost is not recorded. Both are pre-existing
gaps, noted on the PR. `positions.updated_at` is shared with the sync; the
booking bumps it, and it carries no reconciliation meaning here.

New verdict: `filled_booked`, terminal. Every park stays `filled_not_booked`.

## Also fixed in the same slice

- `_poll_one_pending_order`'s re-read under the lock gains
  `recommendation_poll_parked_reason` and `instrument_id`. A row parked
  between batch selection and the poll returns `no_longer_pending` before
  any broker call. The poller's `ref.isdigit()` becomes an ASCII-digit
  check (`str.isdigit` accepts `"²"`, which `int()` rejects) (r4 #26, #28).
- **Provider id strictness.** `get_close_order`'s `orderID`, `positionID` and
  `instrumentID` accept only a non-bool `int`, or a string of ASCII digits,
  `> 0`. Anything else raises `BrokerPositionMutationUncertain`
  (`lookup_error`, unparked, because no execution is proven yet) (r4 #7).
  `BrokerCloseOrderDetail` gains `positions: tuple[CloseOrderPositionFill,
  ...]` carrying the raw `rate`, `units` and `occurred` values unparsed. The
  booking's checks 3-5 are the only parser, so one place decides.

## Disposition of the review findings

| finding | rev 5 |
| --- | --- |
| r3 #1 / r4 #1 terminality | Exact lot units (check 4), not field presence. |
| r3 #2 evidence | Settled by the attended GBX closes. |
| r4 #2 mixed executed/unexecuted list | Stays `pending`, not booked. A later `rejected` answer that still lists positions parks (`_apply_pending_order_verdict`). A later `rejected` answer that omits them would release the claim. That is pre-existing, and it is noted on #3320. |
| r4 #3 fields never appear | The row stays pollable with the claim held. That is the posture of any unanswered lookup, and it never releases a claim. |
| r4 #4 instrument mismatch unparked | Unchanged from half 2 (reviewed there): a mis-addressed answer is not evidence about our order. |
| r4 #5 rejected + ids parks | A park is fail-safe. It is pre-existing (#3189 finding 3). |
| r4 #6 corroboration | `proceeds` is not a gate. It agrees with `rate × conversionRate × units` truncated to the cent on two closes, and it is recorded in the audit. |
| r4 #8, #35-41 numeric | Checks 3, 4 and 6. `Decimal(str())` is used, parse errors are refusals, the quantised price is used everywhere, the units must be exact at 6 dp, and the evidence is stringified. |
| r4 #9-11 currency | Ledger is native by code; `rate` is asset currency by two observations plus the contract. The residual is stated above. |
| r4 #12 arithmetic | Corrected: cent truncation. |
| r4 #13 split whole close | Parks (fail-safe). |
| r4 #14 rounding `avg_cost` | Same 6-dp `avg_cost` as the synchronous path. Pre-existing. |
| r4 #16 savepoint | `conn.commit()` before `conn.transaction()`. |
| r4 #17-21, #29, #42-44 timestamps | Removed. Check 8 is a same-row comparison, and `occurred` gates nothing. |
| r4 #22 lock order | Stated above. |
| r4 #23-27 duplicates / account | Check 9, keyed on the lot position id, serialised by the `positions` lock. The account is scoped by environment. A single dev deployment holds one demo credential pair. |
| r4 #30-34 park / commit races | Conditional park; park failure; unknown commit (above). |
| r4 #45 `updated_at` | Stated as shared, with no meaning. |
| r4 #46-49 | Accepted pre-existing sync and attribution gaps. |
| r4 #50 backlog | Dev is the only deployment. Census (dev, 2026-09-23): 0 rows `filled_unbooked`, 0 recommendation-origin orders. |
| r4 #51-53 tests | Acceptance, below. |
| r4 #54 | Fixed (checks 2 and 4). |

## Out of scope

- BUY/ADD late fills (#2942 slice C proper).
- Partial closes (#2965).
- An attribution sweep, and estimated-cost recording.
- Every sync limitation listed above, and #3322.

## Acceptance

- **Pure (table):** each refusal of checks 1-6, including a parse exception,
  a bool, a float that rounds to 0 at 6 dp, units that are not exact at 6 dp,
  and a naive timestamp. The book decision uses the verbatim whole-close shape
  of `383339190`.
- **Provider:** strict `orderID` / `positionID` / `instrumentID`; `positions`
  carries the raw fields from both attended bodies.
- **DB:**
  - a booking leaves `current_units`, `cost_basis`, `unrealized_pnl` and the
    `cash_ledger` sum unchanged. It writes one `fills` row (`filled_at =
    occurred`) and `realized_pnl = prior + delta`, for a prior of NULL and a
    prior that is non-zero;
  - `positions.avg_cost` differing from the submission cost parks;
  - an order-CAS miss writes nothing and parks;
  - a recommendation-CAS miss rolls back the fill and the P&L, then parks;
  - a row that is already parked, or already `filled`, is not booked and not
    looked up;
  - a prior EXIT fill on the same `recommendation_exit_position_id` parks;
  - the conditional park does not overwrite a row resolved concurrently.
- **Dev-verify:** the poller runs against dev as a no-op, since there are no
  pollable rows. The first real booking is observed at the next attended
  recommendation EXIT (#2603 acceptance A), and its figure is posted on
  #3007.
