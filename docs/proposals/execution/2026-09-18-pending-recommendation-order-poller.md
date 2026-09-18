# Pending recommendation-order poller — #2942 half 2, slice B

**Status:** proposed 2026-09-18. Branch `fix/2942-pending-recommendation-order-poller`.

## The gap, stated exactly

`execute_order` writes `orders.status='pending'` + `trade_recommendations.status='execution_pending'`
when the broker acknowledges without filling (`order_client.py:2262-2276`). **Nothing ever looks at
that row again.** The claim index `idx_orders_recommendation_open_attempt` (`sql/375`) covers
`('submitted','pending','uncertain')`, so a pending order holds the claim, and every later pass of
`scheduler.py:4859` raises `PriorSubmissionUnresolvedError` for that recommendation — permanently,
including when the broker rejected the order asynchronously seconds later and no order exists at all.

#2942's Required outcome: *"Reconcile submitted and pending attempts to terminal broker facts."*
Half 1 gave the path identity and a claim. Slice A (`f7b840fb`) released the claim for the one case
our own write ordering proves never reached the broker. This slice adds the consumer that asks the
broker.

## Source rule / contract

There is no regulator here; the governing contract is the broker's and our own:

- **Broker status vocabulary** — `strategy_order_reconciliation.py:110-125`, the sets already fixed
  by #2451/#2965 against the eToro order contract. Reused by importing
  `classify_broker_order_status`, not restated. A second copy of this vocabulary is a magic-string
  duplicate of a typed counterpart.
- **The lookup verb** — `BrokerProvider.lookup_order` (`broker.py:701`), which raises
  `BrokerOrderNotFound` (404) / `BrokerOrderLookupError` (transport, non-JSON, bad shape).
- **Informational, therefore reachable unattended** — `refuse_broker_mutation_if_unattended` is
  installed on the six mutating methods only (`etoro_broker.py:455,545,601,667,692,752`); #2645's
  stated second half is that informational calls must stay reachable.

## ⚠ Two findings that bound the slice

**1. `get_order_status` must not be used, and this is the load-bearing design point.**
`etoro_broker.py:848-892` catches `httpx.HTTPStatusError`, `httpx.HTTPError` and `ValueError` and
returns `BrokerOrderResult(status="failed")` for all three. It **never raises**. `"failed"` is
therefore indistinguishable between *"the broker rejected this order"* and *"the network dropped"*.
A poller that terminalised on it would release the claim on a live order after one blip and permit
the second economic order this ticket exists to prevent. `lookup_order` is the verb.

**2. `uncertain` orders are not pollable by `orderId`, structurally.** `broker_order_ref` is written
only by `_update_order_with_broker_result` (`order_client.py:529`), on the successful-response path.
`_park_uncertain_submission` (`:1527`) sets `status` and `raw_payload_json` and nothing else, so an
`uncertain` row carries **no** broker order id — and the 2026-09-17 attended probe established that
`orders:lookup?referenceId=` returns 404 on demo on both API versions even for a FILLED order whose
reference we echoed. So the pollable set is `status='pending'` with a non-null ref. `uncertain` stays
where #2942 already parks it, and this spec says so rather than implying coverage it does not have.

## Full population (dev DB, read-only, 2026-09-18)

```sql
select status, execution_origin, count(*) from orders group by 1,2;   -- ('filled','manual',1)
select count(*) filter (where recommendation_id is not null) from orders;  -- 0
select status, count(*) from trade_recommendations group by 1;        -- considered 74784, rejected 180
```

Zero recommendation-origin orders and zero approved recommendations. The path is live-wired and never
exercised: nothing to backfill, nothing to collide with, and the new column changes no existing row's
meaning.

## Design

### Verdict table — what the poller does with each broker fact

| `lookup_order` outcome | action | claim |
| --- | --- | --- |
| `pending` states (`Pending`, `Received`, `Placed`, `WaitingForMarket`, `PendingTriggeredRate`) | stamp `recommendation_last_polled_at`, leave | held |
| `rejected` states (`Rejected`, `Failed`, `Cancelled`, `Canceled`, `Expired`) | **terminalise** — `orders.status='rejected'`, recommendation → `execution_failed`, audit row | **released** |
| `filled` states (`Filled`, `Executed`) | record the broker fact in `decision_audit`, log ERROR, leave `pending` | held |
| `BrokerOrderNotFound` (404) | stamp, leave | held |
| `BrokerOrderLookupError` (transport / shape) | stamp, leave | held |
| partial-fill states (#2965) / unknown status → `StrategyReconciliationError` | stamp, record, leave | held |

Only one row in that table releases the claim, and it is the row where the broker has told us the
order has ceased to exist. **Every other outcome fails closed**, which is the same direction half 1
and slice A chose.

### ⛔ Why a FILLED verdict is recorded and NOT booked

Booking a late fill is not a small addition: it is `_persist_fill` → `_update_position_buy` /
`_update_position_exit` → `_persist_broker_position` → `_deduct_closed_exit_lot` → `_record_cash_ledger`
→ `_maybe_trigger_attribution` → `enqueue_post_trade_sync`, plus estimated-cost recording. Those calls
sit inside `execute_order`'s transaction and close over `order_params`, `quote_data` and `exit_lot` —
locals that only exist at submission time. Two specific blockers, not a preference:

- **EXIT has no persisted lot.** `_load_exit_lot` resolves the lot at submission; nothing stores which
  lot was closed. Re-selecting at poll time picks a possibly different lot, which is precisely the
  "no instrument/time/FIFO guesses for broker ownership" the ticket forbids.
- **It cannot be dev-verified.** DoD clause 11 wants the operator-visible figure confirmed after the
  change. Producing a real pending→filled recommendation order needs an attended demo session, and
  that is `loop-ineligible`. Shipping unverifiable economic writes onto the money path on test
  evidence alone is the thing this repo's discipline exists to stop.

**Stated unblock for slice C:** one attended pending→fill observation on a recommendation-origin
order — the same observation #2965 needs for the partial-fill ownership lifecycle. Until then a
FILLED verdict leaves the claim held, which is the status quo plus an `ERROR` log and an audit row,
i.e. strictly more visible and no less safe.

### Concurrency

The poller takes `RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS` per recommendation with
`_recommendation_submission_try_lock`, skipping the order when it cannot. Without it the poller could
read `pending`, and a concurrent `execute_order`… could not in fact submit (the claim index refuses),
but the poller could still terminalise a row the submitter is mid-way through resolving. Reusing the
existing per-recommendation key rather than inventing a second one keeps one lock discipline for this
recommendation's whole lifecycle — and it is per-recommendation, so one wedged row cannot starve the
others (#2961's blast-radius finding).

⚠ `try_lock` proves *no OTHER session holds the key*; advisory locks are reentrant. That is exactly
the property wanted here and is already documented at the helper.

### Selection and rotation

```sql
SELECT o.order_id, o.recommendation_id, o.instrument_id, o.broker_order_ref
FROM orders o
WHERE o.recommendation_id IS NOT NULL
  AND o.status = 'pending'
  AND o.broker_order_ref IS NOT NULL
ORDER BY o.recommendation_last_polled_at ASC NULLS FIRST, o.order_id
LIMIT %(limit)s
```

⚠ **The `ORDER BY` is not cosmetic.** #2948 recorded that ordering a bounded backlog on keys that
never change for a non-terminal row is an **absorbing state, not a delay**: once the first `limit`
rows are stuck, row `limit + 1` is never visited again. `recommendation_last_polled_at` is written by
*every* attempt path, including the ones that change nothing, so the scan rotates.

No cooldown and no attempt counter, deliberately: the cadence *is* the rate limit (hourly, `limit=20`
→ at most 20 eToro reads per hour against a 60/min shared budget), and a second retry-backoff
mechanism would be machinery with no measured load to justify it.

### Scheduling

`recommendation_order_reconcile`, lane `source="etoro"`, `Cadence.hourly(minute=7)`,
`prerequisite=_has_pending_recommendation_orders`, `catch_up_on_boot=True`.

- **Lane `etoro`** because the only external call is an eToro read and that lane owns the budget —
  the reason already written on `core_rebalance_observation`. Joining an existing lane costs a
  dispatch thread, not a connection permit (#3159); the dev cluster has zero permit headroom.
- **Minute 7, and hourly rather than every-5-min.** A 5-minute job on a shared lane takes the
  5-min-aligned slots from its lanemates — the #1526/#1527 tick-race, written on this very lane's
  `core_rebalance_observation`. `:07` is not a 5-min-aligned slot and collides with no current
  `etoro` lanemate (`execute_approved_orders` 06:30, `core_rebalance_observation` 22:45).
  ⚠ The cost is stated, not hidden: worst-case time to notice an asynchronous rejection is one hour.
  For a path with zero orders in it that is not a constraint worth a lane split.
- **`catch_up_on_boot=True`** — a restart is exactly when a pending order deserves a look, and the
  job mutates no broker state.
- **Prerequisite gate** so a dormant path spends no lane time and no request budget.

### Job outcome contract

`tracker.row_count` = **orders polled**, not orders terminalised. #3111's lesson, measured: for a
poller the completed unit of work is the POLL, and bucketing on discoveries degraded 98.9% of healthy
runs. `tracker.note` carries the per-verdict counts.

## Schema

One column, `sql/394`:

```sql
ALTER TABLE orders ADD COLUMN IF NOT EXISTS recommendation_last_polled_at TIMESTAMPTZ;
```

NULL = never polled, which sorts first. No backfill (0 rows in scope). No CHECK — it is a timestamp,
not a vocabulary.

## Tests

Pure (no DB):

- `classify_broker_order_status` round-trip over the four documented vocabularies is already covered
  in the strategy module; here assert only that the **verdict mapping** (which verdict releases the
  claim) is what the table above says, via a pure `_pending_order_verdict` helper.

DB-backed:

1. a `rejected` broker status terminalises: `orders.status='rejected'`, recommendation
   `execution_failed`, a `decision_audit` row, and a subsequent `_claim_submission` **succeeds**
   (the claim really lifted);
2. a `pending` broker status leaves everything alone and stamps `recommendation_last_polled_at`;
3. a `Filled` broker status leaves `orders.status='pending'` and the claim HELD, and writes an audit
   row — the explicit refusal-to-book;
4. `BrokerOrderNotFound` and `BrokerOrderLookupError` each leave the row pending and stamp;
5. a non-recommendation (`execution_origin='strategy'`) pending order is **never** selected;
6. an `uncertain` row and a `submitted` row are never selected;
7. a recommendation whose lock is held by another session is skipped, and its row is untouched;
8. rotation: with `limit=1` and two due rows, two successive calls visit **different** orders
   (the #2948 absorbing-state probe).

Revert-probes, each against a green control: drop the `status='pending'` predicate; drop the
`broker_order_ref IS NOT NULL` predicate; swap `lookup_order` for `get_order_status`; drop the
`ORDER BY … NULLS FIRST` rotation key; terminalise on `resolved` instead of `rejected`.

## Not in this slice, and named

- Booking a late fill (slice C, unblock above).
- `uncertain` reconciliation — no client-side identity exists; see finding 2.
- Crash-after-acceptance override (`broker_verb_entered`) — person-gated, #2961 window B.
- async 202.

## ⚠ Codex

Checkpoints 1 and 2 cannot run: both profiles return
`You've hit your usage limit … try again at Sep 19th, 2026 11:57 PM`. Absent by circumstance, not
judgement. Partial mitigation: this slice reuses a reconciliation design that went through ckpt-1 on
#2451/#2948/#2962, and its two novel decisions (the lookup verb, the refusal to book) are each backed
by a citation above rather than by reasoning.
