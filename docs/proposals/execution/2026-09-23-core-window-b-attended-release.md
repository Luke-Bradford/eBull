# Attended, demo-only release of a window-B core authority (#2961)

Status: spec · 2026-09-23 · Refs #2961, #2942, #2844, #2437 ·
**One parameter awaits a supervisor confirmation (`T`, see Correction 2). Everything else is
buildable.**

This implements the supervisor decision posted on #2961 at 2026-09-23 00:02Z: *"an ATTENDED,
DEMO-ONLY release is authorised. Real stays fail-closed."* The decision's risk acceptance is
load-bearing: *"On demo, a double order is bounded, fake-money exposure, and the #2844 sandbox
invariant caps it."* This design reduces that risk. It does not eliminate it, and it says where.

Three corrections to the decision as written follow. Corrections 1 and 3 are **strictly tighter**
than the decision: they add refusals and remove none. Correction 2 is a finding that one of the
decision's own conditions cannot be met.

## The state

Window B is a core ENTRY whose `mark_core_submission_entered` committed
(`submission_phase = 'broker_verb_entered'`, `app/services/strategy_core_executor.py:452-493`),
after which the process died before the broker response was persisted. So:

- `orders.broker_order_ref` is NULL;
- the reconciliation `state` is non-terminal;
- `strategy_trades.status` is `planned`. (The first draft said `submitted`; the executor writes
  `submitted` only together with `broker_order_ref`, and the #2949 harness's real crash reads `planned`.)

Window A (`authority_committed`) is already terminalised on write-ordering evidence by
`terminalise_unsubmitted_core_entry` (`app/services/strategy_order_reconciliation.py:808-892`,
#3162). Window B cannot use that evidence: the marker bounds only its own commit. A
`referenceId` lookup cannot settle it either, because a v2 order that filled still returned 404
(the 2026-09-17 probe, recorded in that docstring).

Census on the dev DB: `select submission_phase, state, count(*) from
strategy_order_reconciliation_state group by 1,2` returns `broker_verb_entered / resolved / 1`.
**No window-B row is stranded today.**

## Correction 1: the witness must include pending orders

A position-only witness releases an order the broker accepted but has not filled. A market order
entered outside the session sits pending until the open; the attended AAPL order `383320185` was
`pending/WaitingForMarket` at 2026-09-23 00:01Z. Once such an order is released, the next
authority doubles the position at the open.

The account payload carries pending orders in `clientPortfolio.ordersForOpen` and
`clientPortfolio.orders` (both parsed at `app/providers/implementations/etoro_broker.py:1820-1909`).
The witness refuses on:

- any `ordersForOpen` entry on the instrument with `mirrorID == 0`;
- **any** `orders` entry on the instrument. The parser applies no mirror filter to `orders`, and
  this design does not invent one.

## Correction 2: `T` cannot be measured from arm L, because arm L was never run. Supervisor, please confirm the by-construction value.

The decision fixes `T` as *"the longest observed submit→visible lag across the session fixtures
(arm L), times a stated safety factor"*. The committed fixtures (`tests/fixtures/etoro/attended_*.json`)
are two baseline reads and one negative control. **No fixture carries an arm-L poll.**

The full population of lag evidence in the repo:

- **One timed order.** `select o.created_at, te.executed_at from orders o join trade_events te on
  te.raw_payload->>'orderID' = o.broker_order_ref and te.event_kind = 'open'` returns one row:
  order `382257232`, SPY.RTH, 3.77 s from authority commit to broker `openDateTime`, in hours.
  This is commit→fill, not acceptance→portfolio publication.
- **One untimed observation.** A close-order lookup returned 404, then `filled` "seconds later"
  (#2961, 2026-09-22 14:46Z).

**Recommendation: `T = 600 s` by construction**, frozen as
`WINDOW_B_VISIBILITY_WAIT` under `WINDOW_B_RELEASE_RULE_VERSION = "core-window-b-v1"`.

- **Derivation:** 600 s is two orders of magnitude over the only timed lag in the repo.
- **The costs are asymmetric:**
  - **Too short:** a double demo order, which the decision already accepts as bounded.
  - **Too long:** ten minutes of attended waiting, on a wedge that today never recovers.
- ⚠ This is a construction, not a measurement. It is stated as one, and it does not claim to
  bound the broker's worst case.
- **Tightening `T`:** the next attended session runs arm L against the portfolio (protocol
  step 2b). It polls `get_account_risk_snapshot` at +0 / +5 / +15 / +60 / +300 s after a submit,
  records the first appearance in `ordersForOpen` or `positions`, and bumps the version.
  **Producer:** any attended session. It is producible on demand.

The implementation lands with this value. If the supervisor answers on #2961 that `T` must wait
for arm L, the script refuses with `window_b_visibility_wait_unconfirmed` until the constant is
set. The rest of the act is unaffected.

## Correction 3: the advisory lock proves the DB session is dead, not the sender

`CORE_SUBMISSION_ADVISORY_LOCK` is session-scoped, so its being free proves only that the
submitter's Postgres backend is gone. A process that lost its connection can still be inside
`ResilientClient`'s POST retry. On a 429, `Retry-After` is server-chosen and has no client bound
(`app/providers/resilient_client.py:349-367`).

`sql/410` adds three nullable columns to `strategy_order_reconciliation_state`:
`submission_entered_at timestamptz`, `submission_entered_pid integer` and
`submission_entered_host text`. `mark_core_submission_entered` writes them in its existing
UPDATE, from `clock_timestamp()`, `os.getpid()` and `socket.gethostname()`. No backfill is needed
(see the census above). The release refuses:

- when any of the three is NULL;
- when the host is not this host;
- when the pid is not positive;
- unless `os.kill(pid, 0)` raises **`ProcessLookupError`**. Any other outcome refuses, including
  success, `PermissionError` and any other `OSError`.

PID reuse can only make a dead sender look alive (a refusal). ⚠ The hostname is not a PID
namespace: a sender inside a container that reports the same hostname would be misjudged. This
deployment runs every writer as a host process (launchd jobs daemon, the uvicorn API), and the
spec states that as an assumption rather than proving it.

⚠ **Sender death bounds our sends, not broker processing.** A request already on the wire can be
accepted after the process dies. `T` is what covers that tail. Beyond `T`, it is the residual the
decision accepts.

## The act: `release_window_b_core_entry(conn, *, order_id, operator_id, attestation, clock, broker_factory)`

Every condition must hold, and each failure is a named refusal. Checks run in this order.

1. **Input.** The `attestation` must be non-empty after strip and ≤ 2000 characters. `order_id`
   is explicit.
2. **Accident controls, not attendance proof.** Refuse when:
   - `is_linked_worktree()` is true;
   - `sys.stdin.isatty()` is false. The headless loop has no TTY.

   ⚠ Neither check proves a human is present. Automation in the main checkout with a pseudo-TTY
   passes both. The hard line is the loop prompt's prohibition. These controls catch a confused
   run.
3. **Demo only.** Refuse unless all three hold: `settings.etoro_env == 'demo'`,
   `orders.broker_environment = 'demo'`, and the credential environment is `demo`.
4. **Candidacy, read before any lock.** Use the window-A predicate with the phase as a parameter,
   **plus** `trade.status = 'planned'`, `o.status = 'submitted'`, and `(SELECT count(*) FROM
   strategy_trade_orders WHERE order_id = o.order_id) = 1`. That last clause closes the
   one-order-many-trades case.
5. **Locks.** The connection must be IDLE with no transaction open; otherwise refuse and never
   commit on the caller's behalf. Try `CORE_SUBMISSION_ADVISORY_LOCK`, then the per-order lock
   (module order). Refuse if either is busy. ⚠ The lock is reentrant, so the act refuses if the
   calling session already holds the core key. That is checked in `pg_locks` for
   `pg_backend_pid()` before the try.
6. **Re-read candidacy under both locks.** The act proceeds only if the row still matches.
7. **Sender dead** (Correction 3). The observation instant is `t_dead`.
8. **Wait**, on a **monotonic** clock. The deadline is `monotonic(t_dead) + T`, and
   `submission_entered_at + T` must also have passed on the wall clock. Both locks stay held, so
   no core submission can start meanwhile. ⚠ This blocks core submissions for at least `T`. That
   is accepted: the act is attended, and a stranded authority already blocks every core
   submission through `core_trade_in_flight`.
9. **Witness.** Build the broker from **the order's own `api_key_credential_id` /
   `user_key_credential_id`**. Refuse if they no longer resolve. This binds the witness to the
   submitting account. Take one `get_account_risk_snapshot()`, which is informational. Refuse if
   any of these holds:
   - its `observed_at` is before the wait deadline;
   - `clientPortfolio` is missing, or `positions`, `ordersForOpen` or `orders` is missing or is
     not a list;
   - any entry on the instrument fails field validation: integer `instrumentID`, integer
     `positionID`, tz-aware `openDateTime`, integer `mirrorID` when present;
   - a direct position on the instrument has `openDateTime ≥ orders.created_at − 60 s`. The 60 s
     is a clock-skew allowance between the DB clock and the broker clock. By construction it
     makes the check refuse more, never less;
   - any pending order matches Correction 1.

   The evaluator is a **pure function** of `(raw_payload, instrument_id, authority_created_at)`.
   ⚠ A position opened before the authority passes. That is safe only if a fill cannot be added
   to an existing position while keeping its `openDateTime`. **Implementation must verify this
   in `openapi_v1.375.0.json`**: the open-order request must carry no target-position field. If
   it does, the rule becomes "refuse on any unowned position on the instrument".
10. **Terminal write, in one transaction, both locks held.** The window-A statements, each
    **conditional on the candidate shape and asserted to affect exactly one row**, with
    `last_error_code = 'core_authority_released_attended'`. Plus one `decision_audit` row:
    - `stage = 'core_window_b_release'`;
    - `pass_fail = 'PASS'`;
    - `model_version = WINDOW_B_RELEASE_RULE_VERSION`;
    - `evidence_json` holds: the order, trade and intent ids; the sender identity and `t_dead`;
      the wait applied; the snapshot's `observed_at`; **every** position and pending order on
      the instrument **as read, unfiltered** (so the timestamp exemption can be audited); the
      payload's total position and pending counts; `operator_id`; and the attestation verbatim.

    If any assertion fails, the transaction rolls back and the act refuses.
11. **Refusals from step 4 onward are audited durably.** After the rollback, in their own
    committed transaction, outside the lock helpers' cleanup: a `decision_audit` row with
    `pass_fail = 'FAIL'`, the refusal slug, and whatever evidence the act had gathered.

The surface is `scripts/release_window_b_core_entry.py --order-id N --operator-id ID --attestation
"..."`. It prints the verdict. There is no API endpoint, because an endpoint is reachable by
anything holding a session.

### Residual risk, named

A PASS does **not** prove the broker never received the order. It leaves:

1. **Acceptance or publication later than `T`.** A request on the wire when the sender died, and
   processed after the witness read. If it fills, it becomes an unowned position and the next
   authority doubles exposure. On demo, that is bounded by #2844 and accepted by the decision.
2. **Filled and fully closed inside the wait**, for example by stop-loss. No exposure remains,
   but the realised P&L is unbooked against the strategy. `trade_events` ingests the close,
   owned by no one.
3. **A stale or internally inconsistent snapshot.** For example, a pending→position transition
   that omits the order from both arrays. There is no freshness watermark beyond `observed_at`.

This is why the real environment stays refused.

## Tests

- **Pure, table-tested witness evaluator.** Cases:
  - an empty account passes;
  - a position after `created_at − 60 s` refuses;
  - an older position passes, and appears in the evidence;
  - a non-mirror `ordersForOpen` entry refuses; a mirror one passes;
  - an `orders` entry refuses, whatever its `mirrorID`;
  - a missing array refuses; an array that is not a list refuses;
  - a malformed entry on the instrument refuses;
  - `observed_at` before the deadline refuses.
- **Pure:** sender liveness (only `ProcessLookupError` is death; own pid refuses;
  `PermissionError` refuses; a non-positive pid refuses; the wrong host refuses; NULL refuses),
  attestation validation, and the wait arithmetic under an injected monotonic clock.
- **DB, one test per new SQL mechanism:**
  - PASS: a `broker_verb_entered` row, a dead sender and a clean fake witness give `rejected` /
    `failed`, one PASS audit row, and broker mutations 0;
  - a core lock held by another session refuses, and the trade is unchanged;
  - a caller that already holds the core key refuses;
  - candidacy changed between the pre-lock read and the in-lock read refuses;
  - a refusal after step 4 leaves a committed FAIL audit row;
  - `mark_core_submission_entered` writes the three identity columns.
- **Unchanged:** the window-A tests. The predicate is shared through a parameter.

## Out of scope

#2942's recommendation-path twin (low priority per the decision). There is no change to
`reconcile_backlog`, to the lookup-miss refusal, or to any unattended path, and no frontend.

## Codex checkpoint 1: disposition

27 findings on the first draft. The accepted ones are folded in above:

- account binding;
- `orders` has no mirror filter;
- ESRCH-only liveness;
- the monotonic wait;
- candidacy re-read under the locks;
- row-count assertions;
- durable refusal audit;
- unfiltered evidence;
- snapshot completeness and field validation;
- the reentrant-holder refusal;
- the trade/order status predicate;
- the one-trade-per-order predicate;
- clock skew;
- the widened residual list;
- the added tests.

The rest are either named as residuals or assumptions, or escalated:

- *"600 s is invented"*: agreed, and escalated as Correction 2 with a recommendation.
- *"Sender death does not bound broker processing"*: agreed, and named as residual 1.
- *"Worktree detection does not prove attendance"*: agreed, and restated as an accident control
  with a TTY check added. Nothing short of a human can prove attendance.
- *"Same-hostname container"*: stated as a deployment assumption.
- *"Locks held during an unbounded 429 sleep"*: accepted, for the reason given in step 8.
