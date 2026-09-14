# The attended demo session — one protocol for seven blocked tickets

Refs #2961 #2965 #2979 #2942 #2993 #3007 #2949.

## Why this document exists

Seven open tickets each end with some form of *"settling this needs one attended demo
order"*, and each states its own fragment of the protocol on its own thread. Nothing
states the whole session, so an operator would have to reconstruct it from seven comment
histories inside a window where mistakes cost a real economic round trip.

Four things that decide whether the session produces **anything at all** are stated on
none of them. P1–P4 below. Each is verified against code here, not asserted.

⚠ This document does not widen any ticket's scope. Every observation in it is one an
existing ticket already asked for. What it adds is the prerequisites, the controls that
make each observation interpretable, and an ordering that does not waste the window.

## P1 — the configuration prerequisite that silently voids the whole session

**`execute_order` chooses the broker or a locally synthesised fill on one flag:**

```python
is_live = runtime.enable_live_trading          # app/services/order_client.py:1449
...
if is_live:      ... broker.place_order(... request_id=request_id)
else:            ... broker_result = _synthetic_fill(...)
```

With `enable_live_trading = false` — **today's state** — executing an approved
recommendation performs **no broker call**, sends **no `x-request-id`**, and produces a
synthetic fill that looks like a successful execution in the app. Every observation below
would be vacuous, and nothing in the UI would say so.

The complete set the session needs, from `decide_submission_controls`
(`app/services/execution_guard.py:370`) and `execute_order`:

| control | needed | today | why |
| --- | --- | --- | --- |
| kill switch | cleared | **active since 2026-06-28** | `_check_kill_switch`, refuses every action including EXIT |
| `enable_auto_trading` | `true` | **false** | `_check_auto_trading` |
| `enable_live_trading` | `true` | **false** | selects broker vs synthetic — the vacuity risk above |
| `etoro_env` | `demo` | `demo` ✅ | this, not the flags, is what keeps the session off real money |
| `fx_rates` + `portfolio_sync` jobs | enabled | check | `_assert_safety_layers_enabled_for_buy_add` refuses BUY/ADD otherwise |
| transaction-cost provenance | complete | check | `_assert_transaction_cost_complete_for_buy_add` |

⚠⚠ **`enable_live_trading` does not mean real money here.** It means *"reach the broker
rather than simulate"*. The environment is selected separately by `etoro_env`, which is
`demo`. Reading the flag as the money switch is the mistake most likely to stop this
session happening; reading it as harmless is the mistake most likely to matter later.
Both halves need saying.

⚠ **The loop does not set any of these.** Per the R6 queue, clearing the kill switch and
enabling auto-trading are the operator's, and this document exists so that page can be
made once with the full list rather than three times.

⚠ **Revert them after the session.** A session that ends with the flags left on has
armed the scheduler, which processes *all* approved recommendations on its next pass —
not just the one the operator meant to execute.

## P2 — the EXIT will close the WRONG lot unless the instrument is chosen for it

`_load_exit_lot` (`app/services/order_client.py:206`) selects **one** lot:

```sql
WHERE instrument_id = %(iid)s AND units > 0 AND is_buy AND position_id > 0
ORDER BY open_date_time ASC, position_id ASC LIMIT 1
```

FIFO-oldest. Two consequences, and the second is the one that wastes the session:

1. **Immediately after the BUY there is no eligible lot at all** *if the instrument had
   none before*. `_persist_broker_position` writes `-order_id` as a synthetic id
   (`:621`), and #3006 deliberately excluded those (`position_id > 0`) because a
   synthetic id is our record of a fill, not a handle the broker can close. The real id
   arrives only with the next `portfolio_sync`. The EXIT before that sync returns
   `no broker-closeable long lot found` and books a `failed` order.
2. **If the instrument already has an older long lot, the EXIT closes THAT one** — not
   the position the session just opened. The close-leg observations would then describe a
   different lot, and the session would leave its own position open.

**Therefore: pick an instrument with zero existing `broker_positions` rows.** The
observer's `--phase baseline` reports the eligible-lot set per instrument so this is read,
not assumed. If no such instrument is acceptable, the session is still runnable but the
close leg must be interpreted against the lot `_load_exit_lot` actually picked, and the
observer records which one that is.

⚠ Not "use the core sleeve's instrument": `SELECTED_CORE_INSTRUMENT_ID` is `None`
(`app/services/strategy_core_selection.py:34`). No core instrument is selected yet, and
selecting one is a different ticket.

⚠ The recommendation path is the **v1** writer. The core sleeve uses
`place_demo_core_order` (v2), with different authority and reconciliation. **This session
measures the v1 legacy path.** What it settles for the core path is stated per-row in the
verdict table and is narrower than "the core path works".

## P3 — #2961 needs the NEGATIVE, and one successful lookup cannot supply it

Every ticket currently reduces this to *"does `orders:lookup?referenceId=` cover v1
submissions"*, and treats a 200 as the unblock. That is not what #2961 asks. #2961 needs
**a `not_found` to be guaranteed complete** — i.e. absence must prove the broker never
accepted the order. A single successful lookup of an order that *was* accepted says
nothing about that direction.

Three arms are needed, and only together do they bound the false-negative window:

| arm | what | when |
| --- | --- | --- |
| **N — negative control** | `lookup_order(reference_id=<freshly minted UUID, never submitted>)` → expect not-found | ⚠ **runs unattended, today.** Needs no session. |
| **P — positive** | the UUID the app actually committed for the BUY | step 2 |
| **L — latency** | P repeated at increasing offsets from submission | steps 2, 2b |

Arm N establishes the endpoint answers *"no"* rather than erroring or 200-ing on garbage.
Arm L bounds how long after acceptance a 404 can still be a **false** negative — which is
the number #2961's discriminator has to be safe against. Without L, a 404 observed at
t+10s is uninterpretable and a fail-closed discriminator built on it is unsafe.

**The `orderId` control.** Alongside each P read, run
`lookup_order(order_id=<orders.broker_order_ref>)`:

| `referenceId` | `orderId` | reading — and what remains uncontrolled |
| --- | --- | --- |
| 200 | 200 | v1 submissions **are** keyed by `referenceId`. Still needs arms N and L before a 404 may be treated as evidence of non-acceptance. |
| 404 | 200 | the order **is** in the v2 index; the `referenceId` key is absent **or** lagging. An `orderId` reconciler is viable — ⚠ but only for crashes *after* `broker_order_ref` was persisted, which is not the window #2942 half 2 is about. |
| 404 | 404 | consistent with v1 being invisible to v2 lookup. ⚠ Also consistent with a wrong id, retention, or a longer lag. Not by itself a migration mandate. |
| 200 | 404 | contradictory; record and do not interpret. |

⚠ **`BrokerOrderDetail.reference_id` is an ECHO of the query argument**
(`_parse_order_detail(raw, reference_id=reference_id)`, `etoro_broker.py:940`). It is not
a broker acknowledgement, and reading it as one is circular — the same trap as
`response_currency` on #2603. Any claim that the broker returned our UUID must come from
the raw body.

## P4 — the counter baseline is necessary, and it is not sufficient

Read `get_closed_position_event_counts()` in the **same run**, immediately before the
close. The 2026-09-13 figures on #2993 are not a baseline: the counter keys on the
gateway GCID, which spans `realCid` and `demoCid`, so anything on the real account since
then moves it for an unrelated reason.

⚠ **State the limits with the observation, because the delta is weaker evidence than
#2993 implies:**

- `+1` on the close-year bucket shows the demo close **is included**. It does **not**
  show the counter is demo-*exclusive* — a combined real+demo counter gives the same
  reading.
- `+1` does **not** by itself establish that `…/trade/demo/history` omits rows. That
  needs the same close to be checked for presence in the history response; the observer
  records both so the comparison is on the exact close, not on totals.
- **Unchanged** is not evidence of non-demo scope either: delayed publication, caching,
  an excluded asset bucket, or a close that did not complete all produce it.
- The counter is bucketed `(closeYear, assetType)`. Compare **per bucket**; a delta of 0,
  >1, or negative is a recordable outcome, not an error.

## Source rule — the endpoints, and what is undocumented about them

| what | where | documented? |
| --- | --- | --- |
| v1 open writer | `POST …/execution/demo/market-open-orders/by-{amount,units}` (`etoro_broker.py:445`) | ⚠ **absent from `openapi_v1.375.0.json`** while named in 6 other operations' `sharedWith` (#2946 finding 1) |
| v1 close writer | `POST …/market-close-orders/positions/{positionId}` (`:652`) | present; requires `X-Request-Id`, states **no** dedup guarantee for it |
| v2 lookup | `GET …/info/demo/orders:lookup?{orderId,referenceId}` (`:894`) | defines `referenceId` as *"Request ID header sent during order submission"*; **silent on which submission endpoints it covers** |
| v1 close-order read | `GET /api/v1/trading/info/demo/close-orders/{orderId}` (`:795`) | present |
| closed-events counter | `GET /api/v1/data/positions/closed-events/history` (`:1140`) | user-scoped by gateway GCID, **no env segment** |
| v1 order info | `{info_prefix}/orders/{ref}` via `get_order_status` (`:848`) | ⚠ also absent from the pinned document — its failure is not a clean negative control |

The idempotency sentence lives on the *create-order* page only and must not be
generalised to the close page. v1-vs-v2 coverage is undocumented in both directions,
which is why it is measured rather than reasoned about.

## What is operator-manual and what is scripted

`tests/test_unattended_broker_mutation_guard.py` carries an empty `_EXEMPT_SCRIPTS` whose
comment reads: *"An entry here would mean an unattended run reaches the order path, which
is the condition this file exists to prevent — it needs an operator decision, not a
commit."*

So **every broker mutation is operator-performed through the app's normal order path**,
and nothing under `scripts/` submits, closes or edits.
`scripts/probe_attended_demo_session.py` performs read-only DB queries and informational
broker reads only (`lookup_order`, `get_order_status`, `get_demo_close_order`,
`get_closed_position_event_counts`, `get_trade_history`).

⚠ `get_closed_position_event_counts` is informational in behaviour but is **not** in the
guard test's `_INFORMATIONAL` set — that set partitions the `BrokerProvider` protocol
surface, and this method is on the concrete eToro class only. Behaviourally read-only;
not covered by that particular assertion.

The split is also the cheap one: `_claim_submission` (`order_client.py:1168`, #2942 half
1) mints and durably commits the `x-request-id` **before** the broker call, so the
operator executes a recommendation and the UUID is already in
`orders.recommendation_request_id`. No hand-built headers.

⚠ One application call may issue **more than one broker POST**: `_submit` retries 429/5xx
with the same UUID. "One economic order" is guaranteed only if the writer deduplicates on
`X-Request-Id` — which for the close writer is explicitly undocumented. Record the
attempt count from the request log rather than assuming one.

## The session

Run the observer from the **main checkout** (`~/Dev/eBull`) so the reads and the
mutations see one database, one credential set and one revision. It is read-only, so the
worktree guard is not what is being relied on here.

```
PYTHONPATH=. uv run python -m scripts.probe_attended_demo_session \
    --phase <baseline|negative-control|open|close|residual> \
    --session-id <YYYY-MM-DD-NN> [--order-id <orders.order_id>] \
    [--history-min-date <copied verbatim from the baseline file>] \
    --out tests/fixtures/etoro/attended_<session-id>_<phase>_<n>.json
```

Every invocation writes its own file; `--session-id` ties them together, and `--out`
refuses to overwrite. Repeated polls must not overwrite each other — the latency arm is a
comparison *between* reads, so a run that keeps only the last has destroyed the
measurement it was taken for.

⚠ **Copy the baseline's `history_min_date` into the close phase's `--history-min-date`.**
A relative `--history-days` window is re-evaluated per invocation, so the two phases do
not read the same range and a trade near the lower boundary can leave it for that reason
alone — a difference in the #2993 diff that the session did not cause. The observer marks
every relative window `history_min_date_is_absolute: false` rather than letting that pass
silently.

### Step 0a — `--phase negative-control` — ✅ **ALREADY RUN, 2026-09-14. Do not re-run.**

Arm N. Costs one request, needs no configuration change, and if it fails the session
should not be scheduled: an endpoint that does not cleanly answer "no" cannot support
#2961's discriminator whatever else is observed.

**Result: PASS.** `tests/fixtures/etoro/attended_negative_control_2026-09-14.json`.
Two independently minted, never-submitted UUIDs both returned HTTP 404 on
`GET /api/v2/trading/info/demo/orders:lookup?referenceId=…`, decoding to
`BrokerOrderNotFound` → `outcome: not_found`. So the endpoint does distinguish
"no such order" from a 200 or an error, and the session is worth booking.

⚠ **What this does NOT establish, stated because it is the easy over-read:** that a 404
for a *submitted* UUID means non-acceptance. It rules out the failure mode where the
endpoint 200s or errors on an identity it has never seen — which would have killed
#2961's discriminator outright — and nothing more. Arms P and L remain required, and
without arm L a 404 observed shortly after submission is still uninterpretable.

### Step 0b — `--phase baseline`, before any mutation

Counter (P4 baseline), a **bounded** trade-history window, and the eligible-exit-lot set
per candidate instrument (P2).

⚠ Bounded, not epoch. `get_trade_history(HISTORY_EPOCH)` is an unbounded paced pagination
loop on lane G's 3.33 s floor, and making it an attended prerequisite can consume the
window. Use one explicit window inside the documented maximum, and **the identical window
in the close phase** — two differently-scoped reads cannot be diffed.

### Step 0c — operator sets P1's controls, and records the before-state

### Step 1 — one BUY, operator-executed. Record `orders.order_id`.

⚠ Do not invoke the scheduler job to do this: it processes *all* approved recommendations
and evaluates proposed ones first. Execute the single intended recommendation.

### Step 2 — `--phase open --order-id <id>` — arms P and L, plus the `orderId` control

Reads `recommendation_request_id`, `broker_order_ref`, `status`, `raw_payload_json` from
`orders`, then each broker call **in isolation** so an expected not-found on one does not
prevent the rest. Records HTTP status, exception type and parse outcome separately —
`lookup_order` can reject a *successful* response during parsing, and `get_order_status`
folds transport failures into `status="failed"`, so a bare parsed object cannot
distinguish "broker said no" from "we could not read what it said".

### Step 2b — repeat step 2 at a later offset (arm L)

At least two offsets. A 404 that becomes a 200 is a **lag** finding, not a coverage
finding, and it changes every row of P3's table. Record submission-relative timestamps;
if the last offset is still 404, the outcome is *"not visible within T"*, not *"absent"*.

### Step 3 — `portfolio_sync`, then confirm the exit lot (P2)

Re-run `--phase baseline` scoped to the instrument. The lot the EXIT will select must be
positive-id, `is_buy`, `units > 0`, and — if the session is to describe its own position
— must be the one just opened. The observer reports the selected lot explicitly.

### Step 4 — one EXIT, operator-executed, for that instrument

### Step 5 — `--phase close --order-id <exit orders.order_id>`

1. **`orders.raw_payload_json` for the EXIT row — decides #3007 at zero broker cost.**
   `_build_result` stores the response body verbatim (`etoro_broker.py:2016`,
   `raw_payload=raw_payload`) and `_update_order_with_broker_result` persists it
   unmodified (`order_client.py:440`).
   ⚠ The discriminator is narrower than #3007's wording. A **numeric** `statusID` is not
   by itself proof of a mis-booked fill: `str(1)` is not a `_STATUS_MAP` key so it falls
   to `"pending"`, but the pinned close description also distinguishes *submission* from
   *execution*, so a pending acknowledgement may be correct at that instant. What decides
   it is the pair — the response shape **and** whether a later acknowledgement ever
   completes the fill/accounting lifecycle (step 5.2, repeated). Missing
   `executionPrice`/`units` independently prevents a fill regardless of status mapping;
   record which of the three is the binding one.
2. `get_demo_close_order(order_id=<ref>)`, **repeated until executed/rejected or a stated
   deadline**. A single 200 pending is not close-completion evidence.
   ⚠ It marks any non-empty affected-position list `filled` when no error code is
   present, so match the **specific** position id and its units — do not trust the
   normalised status. ⚠ Compare any returned `referenceID` to the submitted UUID for
   equality; the adapter requires a UUID and will reject an otherwise-informative
   non-UUID string, so capture the raw value too.
3. Arms P/L again for the **exit** UUID plus the `orderId` control. ⚠ Separate question
   from step 2: the close page documents no dedup guarantee for `X-Request-Id`, so
   open-writer coverage must not be generalised to it.
4. Counter + the **same bounded** history window, diffed per bucket against step 0b, and
   the specific close checked for presence in the history rows (P4).

### Step 6 — `--phase residual`, before standing down

Residual broker positions and open orders, `fills`, `cash_ledger`, recommendation
statuses, and any unresolved order claim. Then revert P1's flags. A session that ends
without this can leave exposure or a wedged claim that the next loop run inherits as a
mystery.

## Verdict table — observation → ticket

| observation | decides | how |
| --- | --- | --- |
| arms N + P + L, with the `orderId` control | **#2942 half 2**, **#2961** | P3's table, and only with N and L present. Arm P alone establishes positive coverage and nothing about absence. |
| step 5.3 arms on the **exit** UUID | **#2979**, **#2949** rd3 | close-writer coverage; not inferable from step 2 |
| step 2 executions array across polls | **#2965** | see below — opportunistic |
| step 5.1 raw close body (+ 5.2 completion) | **#3007** | the shape, and whether any acknowledgement ever completes the lifecycle |
| step 5.2 repeated close-order read | **#2979** | whether a broker-executed close is acknowledgeable by order id after the fact |
| step 5.4 per-bucket counter delta + presence of the specific close in history | **#2993** | P4, with its limits |

## What this session does NOT settle

- **#2965's partial fill is opportunistic and probably will not occur.** A market order
  in a liquid instrument fills whole. **Recommendation: do not chase it.** The arms that
  turn on `referenceId` are worth far more, and reaching for a partial fill means picking
  an instrument for the experiment rather than for the mandate. Record every poll
  regardless; if no partial state is observed, #2965 stays open on it and the session
  says so rather than inferring.
  ⚠ Two further limits if one *is* observed: `remainingUnits` is a property of the
  position execution row, not unfilled order quantity, so it must not be read as "the
  order is still working"; and the immutability contract #2965 turns on covers
  `openingData.units` / `avgPrice` / `executionTime` / `fees`, so a poll pair must be
  compared on those fields, not on ids alone.
- **v1 evidence does not validate the v2 core writer.** #2961 and #2979 are core-path
  tickets; what this session gives them is the *endpoint's* behaviour, not their writer's.
  Any transfer to the core path is an explicit assumption and must be written as one.
- **#2961's override question is untouched.** If arms N/P/L land in P3 row 1 the
  automatic discriminator becomes buildable. Otherwise the override remains an operator
  call this session has not made.
- **Nothing here establishes real-environment behaviour**, and the counter's real-vs-demo
  scope is the very thing under test.

## Acceptance

One observation file per phase and per poll under `tests/fixtures/etoro/`, sharing a
`--session-id`. Each verdict-table row resolves to **settled** or **inconclusive, and
why** — inconclusive is an outcome, not a failure, and is the honest result for any arm
whose control did not run. Each of the seven tickets then closes, or states what still
blocks it, citing an observation file rather than a re-derivation.

⚠ Capability evidence is not a fix. Settling an arm unblocks the corresponding ticket's
*work*; it does not complete it, and none of these observations exercises restart
recovery.
