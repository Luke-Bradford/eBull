# #2942 half 2, slice A — a write-ordering marker for the recommendation submission claim

Status: spec for the slice implemented on `fix/2942-recommendation-submission-marker`.
Half 1 (the claim itself) is `42f392a6`, spec at `2942-recommendation-order-identity.md`.

## The window this closes

`execute_order`'s live branch commits a durable intent row and then calls the broker:

```
_claim_submission()        -> INSERT orders (status='submitted', recommendation_request_id) ; conn.commit()
                              <-- process dies here
broker.place_order(...)    -> or broker.close_position(...)
```

A death inside that window leaves `orders.status='submitted'` for ever. That row holds
`idx_orders_recommendation_open_attempt` (`app/services/order_client.py:1311`,
`sql/375_recommendation_order_identity.sql:70`), so every later scheduler pass
(`app/workers/scheduler.py:4859`) re-selects the still-`approved` recommendation and
`_claim_submission` raises `PriorSubmissionUnresolvedError` — permanently, on a
recommendation the broker never heard of.

Half 1 chose that behaviour deliberately and said why: *"with no verified recovery
capability, refusing to re-submit is the only safe response to an uncertain attempt."*
This slice does not weaken that. It adds the one case where the attempt is **not**
uncertain.

## Source rule

Not an SEC-style reg — the governing rule here is the broker contract and our own
write ordering, and the relevant fact is a **negative** one that was measured rather
than assumed.

- **Reference-keyed recovery does not exist on demo, on either API version.** Measured
  2026-09-17 on the demo account (#2961, comment *"The positive case is now observed,
  and it FAILS"*): a v2-submitted order that **filled** echoed our `referenceId`
  exactly and `orders:lookup?referenceId=` still returned HTTP 404; only `orderId`
  resolved it. The cancel path returns `referenceId: ""`. So the recovery route half 1
  named as the unblock for half 2 is closed, and its own stated alternative applies:
  *"the `submitting`-marker shape … is the only client-side answer available, and
  #2961/#2965/#2942 half 2 should adopt it rather than migrate toward a reference key."*
- **The shape is already landed twice**: `strategy_position_operations.status='submitting'`
  (#2979, closes) and `strategy_order_reconciliation_state.submission_phase` (#2961,
  `sql/392`, core entries). This slice is the third adoption, on `orders`.

⚠ Demo only. Nothing here tests whether a real account indexes references differently
(#2961 records the same caveat).

## Full-population verification

Dev DB, read-only, 2026-09-17:

```sql
select status, count(*) from orders group by 1;                                    -- ('filled', 1)
select count(*) filter (where recommendation_id is not null), count(*) from orders; -- (0, 1)
select count(*) from orders
 where recommendation_id is not null and status in ('submitted','pending','uncertain');  -- 0
select count(*) from orders where recommendation_request_id is not null;            -- 0
select status, count(*) from trade_recommendations group by 1;                      -- considered 74784, rejected 180
```

Zero recommendation-origin orders, zero approved recommendations. The migration needs no
backfill and cannot collide, and the defect is latent rather than live — same standing as
half 1 recorded.

## Design

### 1. Schema — `sql/393_recommendation_submission_phase_marker.sql`

`orders.recommendation_submission_phase TEXT`, CHECK
`IS NULL OR IN ('claim_committed','broker_verb_entered')`.

- Named `recommendation_…` to match `recommendation_request_id` on the same table, and
  so it cannot be misread as #2961's `strategy_order_reconciliation_state.submission_phase`,
  which is a different column on a different table for the core arm.
- **NULL is not "no marker yet"** — it is "this row was not written by the live
  recommendation submission path, or predates this column". Demo synthetic fills, strategy
  orders and the one existing manual row all leave it NULL, and a NULL row is never
  terminalisable. The discriminator requires the affirmative `'claim_committed'`, so every
  pre-existing row keeps exactly today's behaviour and no backfill is required.
- A new column rather than a new `status` value: `status` records what the broker said, and
  "we are about to call" is not a broker answer. It also leaves the claim index predicate
  untouched.

### 2. `'broker_verb_entered'` is named for what it proves

The marker bounds **its own commit instant**, never the statement after it. Between the
marker commit and the socket write sit the unattended guard, credential read, body
construction and `ResilientClient`'s throttle and shared-lock wait. So the value means
*"may subsequently have been entered"*. This is #2961's checkpoint-1 finding applied here
rather than rediscovered (`docs/review-prevention-log.md`, *"A write-ordering marker proves
its own COMMIT, never the statement that follows it"*).

⚠ For the same reason the marker is **not** pushed into the provider via a callback. #2961
measured that placement as buying microseconds in exchange for changing the `BrokerProvider`
protocol and three implementations; it was dropped entirely there and is not revived here.

### 3. The marker alone is not a safe discriminator

Between the claim commit and the marker commit, a **live** submitter's row reads exactly
`'claim_committed'` — identical to a dead one's. Terminalising on the marker alone would
release the claim under a submission that then places a real order: the duplicate-order bug
this ticket exists to prevent, reintroduced by its own fix.

Second condition: `RECOMMENDATION_SUBMISSION_ADVISORY_LOCK = (2942, recommendation_id)`,
session-scoped, taken by `execute_order` **before** `_claim_submission` and held across the
claim, the marker and the provider call. Postgres drops it when the backend dies, so a
successful try-lock is evidence no other session is mid-submission for this recommendation.

- **Keyed per recommendation, not globally.** #2961's second-half finding: a global evidence
  key lets one unrelated in-flight submission starve every other arm. The discriminator only
  ever asks about one recommendation, so the key is that recommendation.
- **What the try proves is "no OTHER session holds it".** Advisory locks are reentrant, so
  `terminalise_unsubmitted_recommendation_attempt`'s own nested try succeeds when
  `execute_order` already holds the key. That is correct here — the proof happens at
  `execute_order`'s acquire, which fails if another session holds it, and the lock is held
  continuously from that instant — but it is a **reasoned exemption**, and it is asserted by
  test rather than assumed.
- **Failure to acquire refuses.** It cannot fall through to `_claim_submission`: the other
  session may not have committed its claim yet, so the unique index would not stop us and we
  would submit the second economic order. `ConcurrentSubmissionInFlightError`.

### 4. Terminal status is `'refused'`, reusing the existing meaning

`_release_claim_after_pre_io_refusal` already writes `status='refused'` for an attempt that
provably never reached the broker (`UnattendedExecutionRefused`). A never-entered verb is the
same claim about the world, so it takes the same status with its own
`raw_payload_json.refusal = 'never_submitted'` and its own audit row. `'refused'` sits outside
the claim index predicate, so the claim lifts; the recommendation stays `approved` and the
next pass submits normally.

Not a new status value: half 1's census found no non-strategy reader of `orders.status`
(reporting, tax_ledger and return_attribution reach orders *through* fills, and a refused
order has no fills row), but growing the vocabulary for a distinction the payload already
carries buys nothing.

### 5. Entry point — the submission path itself, no new job

`execute_order` calls `terminalise_unsubmitted_recommendation_attempt` immediately after
taking the lock and before `_claim_submission`. A stranded recommendation is still `approved`,
so `scheduler.py:4859` already re-selects it every pass; that pass is the sweep. No new job,
no new schedule, no new lane on a cluster already at its connection ceiling.

## What this slice does NOT do

Named so the ticket keeps them:

- **Crash after the broker accepted** (`'broker_verb_entered'`) stays parked. No client-side
  evidence exists for it, and the 09-17 probe closed the only proposed route.
- **`status='uncertain'`** (transport error / 5xx / 408-409-425-429) stays parked. Unchanged
  by design — it has `'broker_verb_entered'` by construction.
- **No pending/`execution_pending` poller.** Half 2's reconciliation requirement is
  untouched; this closes one window inside it.
- **`async 202`** and **repeated-scheduler-run cardinality beyond this window** stay open.

## Tests

Pure-logic where the decision is pure, DB-tier where the predicate is SQL (the `db` marker is
module-scoped, so the DB tests are their own file).

1. The marker is written `'claim_committed'` by the claim INSERT and updated to
   `'broker_verb_entered'` in a **separate commit** before the provider call — asserted by
   commit ordering, because folding them into one transaction makes the separation vacuous.
2. A row left at `'claim_committed'` with no live holder is terminalised to `'refused'`, the
   claim lifts, and a fresh claim succeeds.
3. A row at `'broker_verb_entered'` is **not** terminalised.
4. A NULL-marker row (demo/strategy/manual) is **not** terminalised.
5. A live holder of the advisory lock in another session blocks terminalisation **and** blocks
   the submission (`ConcurrentSubmissionInFlightError`); no orders row is created.
6. The reentrant caller: `execute_order` holding the key terminalises through its own nested
   try, and still owns the key afterwards.
7. The lock is released on every exit path, including the refusal and uncertain paths — a
   session lock leaked onto a pooled connection outlives the request.
