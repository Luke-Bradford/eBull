# #2942 — restart-safe request identity for recommendation orders

Status: proposal, revised after Codex checkpoint 1 (48 findings) and a live eToro portal
re-check. Target: `sql/375_*.sql`, `app/services/order_client.py`,
`app/providers/broker.py`, `app/providers/implementations/etoro_broker.py`,
`app/workers/scheduler.py`.

## Decision: harden, not retire — and ship the claim half only

#2942 offers "retire the legacy broker-writing path, or give it durable identity and
recovery equivalent to the strategy path". #2943 (`68db877e`) hardened the same function
three days earlier and its single caller
(`app/workers/scheduler.py::execute_approved_orders`) has no replacement, so: harden.

The ticket's required outcome has two halves. **Only the first is buildable on current
evidence.**

- **Half 1 — "commit a stable request identity and claim before submission; an uncertain
  attempt cannot create a new economic order."** Fully buildable, fail-closed, self-
  contained. This spec.
- **Half 2 — "reconcile submitted and pending attempts to terminal broker facts."**
  Blocked, see below. Tracked on the issue, not built here.

### Why half 2 is blocked (source rule, not scope convenience)

The strategy path recovers an uncertain submission through
`GET /api/v2/trading/info/{demo|real}/orders:lookup?referenceId=<X-Request-Id>`. The
legacy path does **not** submit through the v2 order endpoint — `place_order` POSTs to v1
`…/trading/execution/market-open-orders/by-{amount,units}` and EXIT POSTs to v1
`…/market-close-orders/positions/{id}`.

Re-read live 2026-09-13:

- [`trading--demo/get-order-information-and-position-details`](https://api-portal.etoro.com/api-reference/trading--demo/get-order-information-and-position-details)
  documents `orderId` ("Numeric order identifier. Mutually exclusive with referenceId")
  and `referenceId` ("Request ID header sent during order submission. Mutually exclusive
  with orderId"). It states **nothing** about which submission endpoints its results
  cover. Coverage of v1-submitted orders is undocumented, in both directions.
- [`trading--demo/close-demo-position-by-units`](https://api-portal.etoro.com/api-reference/trading--demo/close-demo-position-by-units)
  requires `X-Request-Id` but documents **no idempotency or deduplication guarantee** for
  it — the header is described only as "A unique request identifier".

So the premise "the committed UUID makes the legacy submission recoverable" is
**unverified**, and the only thing that would verify it is submitting one demo order
through the v1 endpoint and then looking it up by `referenceId`. That mutates broker
state, which is `loop-ineligible`. Building a reconciler on an assumed capability would
be exactly the invented-source-rule failure the engineering discipline forbids.

This makes half 1's fail-closed posture the *correct* design rather than a compromise:
with no verified recovery capability, refusing to resubmit is the only safe behaviour
available. An uncertain attempt parks the recommendation until an operator resolves it.

## Verified premises (re-checked at `68db877e`; inherited claims re-run, not trusted)

| Claim | Check | Result |
| --- | --- | --- |
| Intent committed before I/O, rec updated only after | `order_client.py:1038`/`:1052` `_persist_submitted_intent` → `conn.commit()` → broker call; rec `UPDATE` at `:1240` | confirmed |
| `place_order` called without `request_id` | `order_client.py:1065` passes none; `etoro_broker.py:230` mints a fresh `uuid4()` when omitted | confirmed |
| Scheduler re-selects pre-existing approved recs | `scheduler.py:4878` `WHERE tr.status = 'approved'`, no attempt filter | confirmed |
| Only a non-unique recommendation index exists | `pg_indexes` on `orders`: `idx_orders_recommendation` non-unique; the unique `idx_orders_strategy_request_id` is confined to strategy origin by `orders_strategy_request_origin_check` | confirmed |
| Legacy `get_order_status` has no production caller | `rg get_order_status app scripts` → provider definitions + tests only | confirmed |
| No recommendation-origin orders exist | ⚠ the first attempt grouped by `execution_origin`, which **cannot answer this** — recommendation orders carry `execution_origin='manual'` (the CHECK allows only `manual`/`strategy`). Re-run keyed on the definition: `count(*) filter (where recommendation_id is not null)` → **0**; whole table is 1 `('manual','filled')` row with `recommendation_id IS NULL` | confirmed, on the second query |
| The claim index can be created | `select recommendation_id, count(*) … where status in ('submitted','pending') group by 1 having count(*)>1` → 0 rows | confirmed |

### New findings while verifying (in scope; both are in the code this PR edits)

1. **Ambiguous outcomes are recorded as terminal.** `etoro_broker.place_order` (`:335`,
   `:345`) and `close_position` (`:514`, `:522`) return `status="failed"` for
   `httpx.HTTPError` (transport) and `ValueError` (non-JSON), and for every
   `HTTPStatusError` including 5xx. `execute_order` then writes the recommendation
   `execution_failed`, which is terminal, so the scheduler never revisits it. This is the
   duplicate-order defect with the sign flipped: an order that actually landed is booked
   as failed and its position is unowned.
2. **`close_position` omits a documented required field.** The portal marks
   `InstrumentID` **required** on the close body; we send only `UnitsToDeduct`. Every
   legacy EXIT would be rejected. Fixed here because it is the same request body this PR
   edits, and leaving a known-broken required field in a line being changed is not
   defensible; called out separately in the PR description.

### `orders.status` reader census (Codex 31 — measured, not assumed)

`rg 'FROM orders|JOIN orders|UPDATE orders|INTO orders' app frontend/src`, then every hit
inspected. Readers of `orders.status` outside the strategy path: **none**.

- Strategy-scoped readers (`strategy_paper_executor`, `strategy_monitoring`,
  `strategy_core_executor`, `api/strategies`, `strategy_control_plane`) all join through
  `strategy_trade_orders` or filter `execution_origin = 'strategy'`, which the
  `orders_strategy_request_origin_check` CHECK keeps disjoint from recommendation rows.
- `reporting.py`, `tax_ledger.py`, `return_attribution.py` join `orders` **through
  `fills`** and never read `status`. An `uncertain` order has no `fills` row by
  construction, so those surfaces are untouched.
- `api/orders.py` writes `status` on its own demo-synthetic path and declares
  `status: str  # "filled", "pending", "failed"` on the response model; it never reads
  the column back for a recommendation order.
- Frontend `orders.status` has no binding. `frontend/src/api/types.ts` already carries
  `"submission_uncertain"` and `"reconcile_required"` in the strategy vocabulary, so the
  concept has precedent in the operator surface.

Conclusion: adding `'uncertain'` to `orders.status` breaks no existing reader.
`orders.status` has no CHECK constraint, so no enum migration is required.

## Design

### 1. Schema — `sql/375_recommendation_order_identity.sql`

```sql
ALTER TABLE orders ADD COLUMN IF NOT EXISTS recommendation_request_id UUID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_recommendation_request_id
    ON orders (recommendation_request_id) WHERE recommendation_request_id IS NOT NULL;

ALTER TABLE orders ADD CONSTRAINT orders_recommendation_request_origin_check
    CHECK (recommendation_request_id IS NULL
           OR (recommendation_id IS NOT NULL AND execution_origin = 'manual'));

-- THE CLAIM: at most one unresolved attempt per recommendation.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_recommendation_open_attempt
    ON orders (recommendation_id)
    WHERE recommendation_id IS NOT NULL
      AND status IN ('submitted', 'pending', 'uncertain');
```

plus `prevent_recommendation_request_id_change()` + `BEFORE UPDATE OF` trigger, mirroring
`sql/285_strategy_order_reconciliation.sql`.

The claim is a **partial unique index, not application logic**, because the failure mode
is a restarted or concurrent second attempt, which an application-level
`SELECT … WHERE status IN (…)` cannot exclude. A recommendation id is minted once per
recommendation, so this forbids exactly the duplicate and nothing else: a genuinely new
recommendation gets a new id and a new claim.

Scope limits of the claim, stated rather than papered over (Codex 4): it freezes *at most
one live attempt per recommendation*. It does not freeze the intent's meaning —
instrument, action and sizing stay mutable — and deleting an unfilled intent would free
the recommendation again. Both are acceptable because nothing in this repo mutates an
order's recommendation FK or deletes order rows; neither operation has a code path.

Migration safety (Codex 5): both indexes are created unguarded and will fail loudly if a
conflicting row exists. That is the intended behaviour — a pre-existing duplicate
unresolved attempt is precisely the defect this ticket exists for and must not be
silently absorbed. The population check above shows none exist.

### 2. `order_client.execute_order`

- `_persist_submitted_intent` mints `uuid4()`, writes it to `recommendation_request_id`
  in the same INSERT, and returns `(order_id, request_id)`. Committed before I/O, never
  rotated. The existing INSERT-then-`conn.commit()` shape and its connection-ownership
  contract are unchanged (Codex 8).
- **Claim violation handling (Codex 6).** A `UniqueViolation` aborts the transaction, so
  the handler must `conn.rollback()` *before* writing anything, and must match
  `exc.diag.constraint_name == "idx_orders_recommendation_open_attempt"` rather than
  catching every uniqueness failure. It then writes a `decision_audit` FAIL row,
  **commits it**, and raises `PriorSubmissionUnresolvedError`. The commit-before-raise is
  the #2943 lesson: `connect_job` rolls back on a raising path, so an uncommitted audit
  row vanishes exactly when it matters.
- The recommendation stays `approved` on a claim refusal — same reasoning as #2943's
  control refusal: the refusal is a statement about the unresolved attempt, not about the
  recommendation's merit.
- `broker.place_order(..., request_id=request_id)` and
  `broker.close_position(exit_pos_id, instrument_id=instrument_id, request_id=request_id)`.
- **Uncertain outcome (Codex 9, 29).** `BrokerOrderSubmissionUncertain` is caught; the
  intent row moves `submitted → uncertain` with the exception's evidence payload in
  `raw_payload_json`, the recommendation moves to `execution_pending`, a `decision_audit`
  row records it, all of it is **committed**, and the exception is re-raised as
  `BrokerSubmissionUncertainError`. Nothing is written to `fills`, `positions` or
  `cash_ledger`. `OrderOutcome` is not extended — the function raises rather than
  returning, so no caller has to learn a fourth outcome.

### 3. Provider

- `BrokerProvider.close_position` gains `*, instrument_id: int | None = None,
  request_id: UUID | None = None`. The eToro implementation sends `InstrumentID` in the
  body (portal-required, see finding 2) and `request_id` on the header. The only
  production caller is `order_client.py:1049`; `api/orders.py::close_position` is a
  demo-synthetic endpoint that never reaches the broker.
- `place_order` and `close_position` raise the **existing**
  `BrokerOrderSubmissionUncertain` (`app/providers/broker.py:92`, already used by the
  strategy path) instead of returning `status="failed"`, for:
  - `httpx.HTTPError` that is not an `HTTPStatusError` (transport),
  - `ValueError` (non-JSON), and any response-normalisation failure — the `_normalise_*`
    call moves inside the guarded region (Codex 37),
  - `HTTPStatusError` with status ≥ 500, or 408 / 409 / 425 / 429.

  Every other 4xx keeps today's `status="failed"` return: the broker answered and the
  answer was a rejection. 408/409/425/429 are carved out because a timeout, a conflict or
  an exhausted-retry throttle does not prove the original request failed (Codex 35).
- The exception carries the response body / error text so the uncertain order row keeps
  the evidence (Codex 38).
- `ResilientClient._request` retries 429/5xx with the **same** `headers` dict, so all
  retries inside one `place_order` call share one `x-request-id` (Codex 36). That is what
  makes those retries safe to the extent the broker honours the header — which, per §"why
  half 2 is blocked", is itself undocumented for these v1 endpoints. Recorded as an open
  question on the issue, not assumed here.

### 3a. Releasing the claim for a refusal that never reached the broker

Found by Codex checkpoint 2, empirically (claim committed, zero HTTP calls).
`refuse_broker_mutation_if_unattended` raises `UnattendedExecutionRefused` at the **top**
of every mutating provider method — before credentials are read and before a request is
built — so no order can exist at the broker. Holding the claim there would park the
recommendation permanently, including after the operator does exactly what the refusal
message asks and re-runs from the main checkout.

So that one exception, and only that one, resolves the intent row to `status='refused'`
(outside the claim predicate, so the claim lifts) and re-raises. The row is resolved
rather than deleted: the attempt happened and belongs in the audit trail. The
recommendation stays `approved`. Any *other* exception out of a broker call may have left
a request on the wire and stays uncertain.

### 4. Scheduler

`execute_approved_orders` counts `PriorSubmissionUnresolvedError` and
`BrokerSubmissionUncertainError` in their own buckets, alongside the existing
`SubmissionControlsRevokedError` bucket, so a parked recommendation is not reported as a
failure (Codex 30). No new submission is attempted while a claim is held — the claim does
that by itself, at the database.

## Tests

Pure-logic first per the repo's tiering; two DB-backed tests for the two things only
Postgres can prove.

| Case | Assertion |
| --- | --- |
| Crash after broker acceptance, then a second scheduler pass | broker submission cardinality **1**; second pass raises `PriorSubmissionUnresolvedError`; recommendation still `approved`; `decision_audit` FAIL row committed |
| Ambiguous transport failure | order `uncertain`, recommendation `execution_pending`, evidence payload retained, **no** fill/position/cash write |
| Next pass over an uncertain attempt | broker never called again |
| 4xx rejection | unchanged — order `failed`, recommendation `execution_failed`, claim released |
| 5xx / 408 / 409 / 425 | uncertain, not failed |
| Non-JSON and malformed-but-200 responses | uncertain, not pending |
| `request_id` reaches the broker | `place_order` and `close_position` both receive the committed UUID; header carries it |
| `close_position` body | contains `InstrumentID` |
| Claim index (DB) | two unresolved intents for one recommendation → `UniqueViolation` on `idx_orders_recommendation_open_attempt`; rollback-then-audit path leaves the audit row committed |
| Immutability trigger (DB) | `UPDATE orders SET recommendation_request_id = …` on an assigned row raises |
| Origin CHECK (DB) | a non-recommendation row carrying the UUID is rejected |
| Claim does not collapse NULLs (DB) | two manual orders (`recommendation_id IS NULL`) coexist — a wrong index predicate would break the operator's manual order path |
| Pre-I/O refusal | `UnattendedExecutionRefused` → order `refused`, claim released, recommendation untouched |

## Out of scope

- **The reconciler** — blocked on an unverified API capability, see above. Half 2 of the
  ticket; the issue stays open.
- Retiring or rewriting the legacy path.
- Relabelling recommendation orders as strategy orders (the ticket forbids it and
  `orders_strategy_request_origin_check` would reject it).
- Cash/exposure reservation for a parked recommendation (Codex 41) — a real gap, but it
  belongs to #2603's allocator, not here.
- `_load_position_id_for_exit` picking the oldest position for an instrument without an
  ownership check (Codex 18) — pre-existing, orthogonal to request identity, noted on the
  issue.
- Any acceptance that mutates broker state. `loop-ineligible`; tests use fakes and
  `refuse_broker_mutation_if_unattended` refuses from this worktree.
