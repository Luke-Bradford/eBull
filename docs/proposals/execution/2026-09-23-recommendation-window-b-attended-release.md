# Attended, demo-only release of a stranded recommendation claim (#2942)

Status: spec · build-ready (Codex ckpt-1 converged, 8 rounds) · 2026-09-23 · Refs #2942, #2961, #2844, #2437

This implements the supervisor decision posted on #2942 at 2026-09-23 00:02Z: *"crash-after-acceptance
takes #2961 window B's policy. Attended, demo-only release; second independent witness from
`portfolio_sync` after a measured lag bound; `decision_audit` row; real env refuses. … Build #2961's
version first and reuse it."*

#2961's version is merged: `app/services/strategy_core_window_b_release.py` (#3311, `d6e41bca`),
spec `docs/proposals/execution/2026-09-23-core-window-b-attended-release.md`. **This spec is a
delta against that one.** Every step it does not restate works exactly as it does there, and so
do its three corrections: the witness includes pending orders, `T` is fixed by construction, and
sender death is proved by ESRCH, not by a free advisory lock.

"Second independent witness from `portfolio_sync`" is read as #2961 read it: one informational
`get_account_risk_snapshot()` (`/pnl`), which is the same endpoint `portfolio_sync` consumes,
taken after the wait. The act writes nothing to `broker_positions`.

## The two stranded states on this path

The recommendation claim (`sql/375`) is the `orders` row holding
`idx_orders_recommendation_open_attempt`, a unique partial index keyed on `recommendation_id` alone,
held while `status IN ('submitted','pending','uncertain')`. A legacy row with a NULL
`recommendation_request_id` can hold it too, and this act refuses such a row. Three of the stranded shapes are already
resolved unattended:

- **Window A**: `recommendation_submission_phase = 'claim_committed'`. Released on
  write-ordering evidence by `terminalise_unsubmitted_recommendation_attempt`
  (`app/services/order_client.py`, #3165).
- **`pending` with a `broker_order_ref`**: polled by `recommendation_order_reconcile` (#3168).
- **Pre-I/O refusal**: released inline by `_release_claim_after_pre_io_refusal`.

Two shapes have no path today, and this act is for them:

| state | predicate | recommendation status | what ended the sends |
| --- | --- | --- | --- |
| **W** (window B) | `status='submitted'`, `recommendation_submission_phase='broker_verb_entered'`, `broker_order_ref IS NULL` | `approved` | the sender process died after the marker; see Delta 1 for the live-sender W shapes, which refuse |
| **U** (uncertain) | `status='uncertain'`, same phase, `broker_order_ref IS NULL` | `execution_pending` | the provider call raised (`BrokerOrderSubmissionUncertain`, or after this build any other exception) and `_park_uncertain_submission` committed |

W's recommendation stays `approved`, so every `execute_approved_orders` pass re-selects it and
raises `PriorSubmissionUnresolvedError`. U's is `execution_pending`, which no consumer polls,
because the poller needs a `broker_order_ref` and U has none (`_park_uncertain_submission` writes
only `status` and `raw_payload_json`).

**Census on the dev DB**, the only deployment: `select count(*) from orders where recommendation_id
is not null` returns **0**. Nothing is stranded there today, and no backfill is needed.

## Delta 1: sends-ended proof and anchor, per state

**W** uses #2961's proof unchanged: the recorded sender pid on the recorded host is dead
(ESRCH only). `sql/412` adds three nullable columns to `orders`:
`recommendation_submission_entered_at timestamptz`,
`recommendation_submission_entered_pid integer` and
`recommendation_submission_entered_host text`. `mark_recommendation_submission_entered` writes
them in its existing UPDATE, from `clock_timestamp()`, `os.getpid()` and `socket.gethostname()`.

**U cannot use sender death.** The recommendation executor runs inside the long-lived jobs-daemon
child, so the sender pid is normally still alive and the check would refuse for ever. It does not
need that proof, though. `_park_uncertain_submission` runs only after the provider call has
returned by raising, so `ResilientClient`'s retries for that attempt are over and no further sends
can come from it. The park commit is therefore the proof. `sql/412` adds
`recommendation_parked_at timestamptz` and `recommendation_park_message text`.
`_park_uncertain_submission` writes both in its existing UPDATE: `clock_timestamp()` and `str(exc)`.
U refuses when `parked_at` or `park_message` is NULL (`recommendation_release_park_unrecorded`).
The two are written by one UPDATE, so a half-recorded park is not a normal shape, and it refuses.

⚠ The `decision_audit` row that `_park_uncertain_submission` writes today cannot serve as the
anchor. Its `decision_time` is `execute_order`'s `now`, captured at step 1, before the provider
call, and it can be minutes early under a 429 `Retry-After`.

**One wait function for both states** (ckpt-1 #9, #10, #14). The act observes the sends-ended
event under the lock: for W that is sender death, for U the recorded park. The observation instant
is `t_obs`, taken on the local monotonic clock and on the wall clock. #2961's
`window_b_wait_remaining` is reused unchanged, with `dead_monotonic = t_obs` and
`entered_at = entered_at` for W or `parked_at` for U. The monotonic leg alone guarantees `T` of
real time after the act saw the event, whatever either wall clock does. The wall leg adds `T` after
the recorded instant. The witness `not_before` is `max(recorded instant, t_obs wall) + T`, as in
#2961. The wait issues no DB reads, so the connection stays IDLE between the locked re-read and
the terminal transaction.

**Unexpected provider exceptions park as U** (ckpt-1 #15). Today, only
`BrokerOrderSubmissionUncertain` and `UnattendedExecutionRefused` are caught around `place_order`
and `close_position`. Any other exception propagates and leaves a W-shaped row while the daemon
lives, so the sender-death proof refuses for ever. The build adds an `except Exception` arm after
the two existing ones. It parks the row `uncertain` with evidence
`{"exception": type(exc).__name__, "repr": repr(exc), "raw_payload": getattr(exc, "raw_payload", None)}`,
plus `"str": str(exc)`, all untruncated, so the scan sees everything the exception's `repr`,
`str` and `raw_payload` carried. The
column is `jsonb`, and a `repr` over 64 KiB is replaced by `{"repr_truncated": true}`, which the
scan refuses by name. It then raises
`BrokerSubmissionUncertainError(...) from exc`, as the existing arm does, so the scheduler counts
it `uncertain` rather than `failed`. The sends have ended, because the call returned by raising,
and the outcome is unknown, which is what `uncertain` means.

- If the park's state transaction fails, the W shape remains, and it refuses.
- If the state commits and only the audit that follows fails, the row is U. Both are safe.

⚠ **W shapes with a live sender refuse, and that is safe.** The sender check refuses them.
- **Marker commit ambiguous at the client.** The commit was acknowledged by Postgres but failed at
  the client.
- **Broker call succeeded, persistence failed.** The call succeeded but response persistence
  (`_update_order_with_broker_result`) failed.

Once the daemon restarts, such a row does become a W candidate, and nothing in its shape marks
it. It is caught the way any landed order is caught: by the witness. A landed open shows a
position after the authority, a pending one shows in the pending arrays, and a landed EXIT's lot
is gone. Only the named residuals remain.

**Payload tightening, W and U alike** (round 3 #3, #4; round 4 #3, #4, #5). The scan refuses
`recommendation_release_payload_has_ref` if `orderid`, `positionid` or `reprtruncated` appears in
**any** of these views of `raw_payload_json`:

0. **The inputs:** `raw_payload_json` and `recommendation_park_message`. Both live on the order
   row, so both sit inside the terminal CAS. The park's `decision_audit` explanation is **not** an
   input: its fixed prefix embeds `order_id=` and would refuse every U row (round 6 #1). The
   message is `str(exc)` for both arms, so text that exists only in an exception message is
   scanned (round 5 #4).
1. the raw serialised text, `json.dumps(payload, ensure_ascii=False)`;
2. the same text after decoding every `\uXXXX` escape it contains, wherever it sits;
3. the text of the recursively parsed form, where each string value that parses as JSON is
   replaced by its parsed form, and this repeats.

Each view is lower-cased and reduced to `[a-z0-9]` before matching. View 1 keeps what parsing
would lose, such as the first of two duplicate keys. View 2 catches escaped keys inside prose.
View 3 catches keys split across a serialised body.

⚠ This is an **accident control, not a decoder.** A reference encoded in a form none of the views
normalise, base64 for example, passes. So does one lost before storage: `jsonb` keeps only the
last of duplicate keys in a stored object, so view 1 recovers duplicates only inside embedded
serialised strings, never in the stored object itself (round 5 #5). That is why the operator must SEE the payload **before**
the act can terminalise, for W and U alike (round 5 #6):
- `--show` prints the candidate, the full payload, the park message, and the SHA-256 of
  `json.dumps([payload, park_message], sort_keys=True, ensure_ascii=False)`. It takes no lock and
  writes nothing.
- The release requires `--payload-sha256 <hex>`. Under the lock, it refuses
  `recommendation_release_payload_not_inspected` unless the hash matches the same digest computed
  from the locked re-read.

The payload is also recorded verbatim in the evidence. The price of the scan is also
refusing a null-valued key or prose such as "order identifier". `raw_payload_json` is part of the
terminal CAS, so a payload changed during the wait misses.

`T` is `WINDOW_B_VISIBILITY_WAIT`, **imported** from the #2961 module rather than redefined. The
recommendation rule version is **derived**: `f"recommendation-window-b-v1+{WINDOW_B_RELEASE_RULE_VERSION}"`.
A core change that bumps its version therefore changes this one mechanically (ckpt-1 #28).

## Delta 2: the witness, per action

**BUY / ADD** (`place_order`): #2961's `evaluate_window_b_witness`. It refuses on **any** `positions` entry
on the instrument, mirror or direct, opened at or after `orders.created_at − 60 s`; on any
non-mirror `ordersForOpen` entry on the instrument; on any `orders` entry on it; and on any
malformed or incomplete payload. Its premise, that an open cannot add to an existing position,
holds for `place_order` for the same reason #2961 recorded: the open body sends no position field.

⚠ `orders.created_at` is `execute_order`'s application `now`, taken at step 1, before the claim.
An early `created_at` widens the "opened after" window, so it refuses more. An application clock
running ahead of the broker's by more than the 60 s allowance would narrow it. That is #2961's
skew residual, unchanged.

**Shared evaluator hardening** (ckpt-1 #22, #23; round 2 #14, #15). `evaluate_window_b_witness` is
tightened in place, and each change only adds refusals:
- **Alias values.** Each present alias of `instrumentID`, `positionID` or `mirrorID` must be,
  independently, a strict `int`: not a `bool`, not a `float`. When two aliases are present they
  must be equal.
- **Ids.** A `positionID` must be `> 0`. A `mirrorID` must be `≥ 0`.
- **Observation time.** A naive or non-`datetime` `observed_at` refuses
  `window_b_witness_observed_at_invalid`.

Entries on other instruments are still skipped once their instrument id has been read. Only the
EXIT target search (below) looks across instruments.

Because the change is strictly tighter, core's version bumps to `core-window-b-v2`, and the
derived recommendation version follows.

**EXIT** (`close_position(position_id)`): a close targets one broker `positionID`, so the witness
asks whether **that lot** is untouched. A new pure evaluator,
`evaluate_recommendation_exit_witness(raw_payload, *, instrument_id, position_id,
recorded_units, observed_at, not_before)`. It applies the shared completeness and field
validation, then refuses on:

- **The target search**, over **every** `positions` entry whatever its instrument: more than one
  entry carrying `position_id`, or one carrying it on another instrument, is malformed. Every
  entry carrying the id goes into the evidence, unfiltered;
- **the lot absent** (`recommendation_release_exit_lot_gone`), because the close may have landed.
  **This is the load-bearing check.** This path always closes the lot WHOLE
  (`close_position(..., units_to_deduct=None)`), so this attempt's close, if it landed, removes the
  lot. It never shrinks it;
- **the lot present but no longer a long with `mirrorID == 0`**, where "long" means `isBuy` is
  exactly the JSON boolean `true`. Missing, null, string or numeric values are malformed
  (`recommendation_release_exit_lot_identity_changed`). `_load_exit_lot` selects `is_buy` lots with
  a positive id. The mirror clause is a witness-side tightening; it does not claim an upstream
  invariant;
- **invalid `units`**: missing, `bool`, not parseable as a finite `Decimal`, not positive, or
  carrying more than 8 **significant** decimal places once trailing zeros are removed, since
  `1.000000000` compares exactly with `1.00000000` (malformed);
- **`units` not exactly equal** to `orders.recommendation_exit_units`
  (`recommendation_release_exit_lot_changed`). This check is secondary. Something ELSE (a manual
  partial) may have touched the lot, and refusing is safe. Because `broker_positions` can be
  stale, equality proves agreement with the cache, not an untouched lot. It does not need to
  prove more, because this attempt's own effect is caught by the presence check;
- any `orders` entry on the instrument, and any non-mirror `ordersForOpen` entry on it.

An EXIT row with `recommendation_exit_position_id IS NULL` refuses
(`recommendation_release_exit_lot_unrecorded`). The `sql/409` CHECK allows NULL, but the live claim
always passes the lot for EXIT (`execute_order`, the EXIT arm).

⚠ **EXIT residual, corrected** (ckpt-1 #6, #7, #8). A close can only reduce exposure, never add
to it. But a released EXIT is not harmless:
- **A queued close.** A pending close has no documented home in `/pnl`. So a close queued outside
  market hours can pass this witness and execute at the open.
- **An unbooked landed close.** A landed or queued close is unbooked: no position deduction, no
  cash-ledger row, no attribution. `trade_events` ingests the close, owned by no one.
- **A later lot closed.** A later EXIT recommendation, which is a fresh guard-approved decision,
  may close the next FIFO lot.

The last two are the EXIT analogue of #2961's residual 2. The first is an outstanding future
broker side effect with no core equivalent. All three are why EXIT is released only on demo.

## Delta 3: account binding by recorded credential ids

Core authorities carry their credential ids through `strategy_core_eligibility_proofs`.
Recommendation orders do not today. `execute_approved_orders` loads the operator's pair ONCE per
run with `_load_etoro_credentials` and reuses it for every order, so the account an order was sent
to is not recoverable from any current-state rule (ckpt-1 #1, #2, #4). The binding therefore has
to be recorded:

- `sql/412` adds `recommendation_api_key_credential_id uuid` and
  `recommendation_user_key_credential_id uuid` to `orders`, both FKs to `broker_credentials(id)`
  and both nullable. `NULL` means *unrecorded*, which covers every non-live row and every live row
  written before 412. It refuses.
- **An id is an immutable account binding.** A credential row's `ciphertext` is written once by
  the INSERT in `app/services/broker_credentials.py`. The only UPDATEs anywhere set `revoked_at`,
  `last_used_at` or health columns (`broker_credentials.py:343`, `master_key.py:328`). Rotation
  revokes the old row and inserts a new one.
- **New loader.** `_load_etoro_credentials_with_ids` in the scheduler returns the two
  `LoadedCredential`s, each with plaintext and id, loaded by `load_credential_with_id_for_provider_use`.
  `_load_etoro_credentials` and its other callers stay unchanged.
- **Construction contract.** `execute_approved_orders` builds its `EtoroBrokerProvider` from those
  two objects' plaintext and, in the same function, passes their ids to `execute_order` as
  `broker_credential_ids`. `_claim_submission` writes them in the claim INSERT. On the live path
  they are **required**, and a missing pair is refused before the claim, exactly as `broker_env`
  is (#3189 finding 4b's pattern). A unit test asserts that the ids recorded are those of the
  plaintext the broker was built with.
- **Rotation refuses while a claim needs the account** (round 3 #5; round 4 #6, #7, #8).
  `revoke_credential` already takes the core key and refuses `CredentialInUse` for an unresolved
  core order. Its existing EXISTS gains one more arm: an `orders` row holding the recommendation
  claim (`status IN ('submitted','pending','uncertain')`) with either recorded id equal to
  `credential_id`. No new lock is added. The claim INSERT itself refuses pre-I/O, with the audited
  `SubmissionControlsRevokedError`, unless both ids are unrevoked at INSERT time. It checks this in
  the INSERT's own statement.

  ⚠ **Named race, which fails closed.** The revoke and the claim do not share a lock. A revocation
  whose EXISTS ran just before a concurrent claim INSERT committed can still leave a claim with a
  revoked id. The release then refuses `credentials_unresolved`, and that claim has no release
  path, which is today's state for every W/U row. Row-level serialisation was specified in round 3
  and withdrawn: it introduced a batch-revocation deadlock, an isolation dependency and a convoy
  behind core's key. Rotation is an attended operator act, and the census is 0.
- **Witness factory.** It takes the two recorded ids. It resolves the owning `operator_id` from
  the rows themselves, and refuses `recommendation_release_credentials_unresolved` unless all of
  these hold:
  - both rows exist and are unrevoked;
  - their labels are `api_key` and `user_key`;
  - both are `environment='demo'`;
  - both have the same `operator_id`;
  - each is still that operator's live row for its label.

  It builds the broker through a recommendation-side factory. That factory shares
  `_order_account_broker`'s body with a `caller` parameter, so credential-access audits read
  `recommendation_window_b_release`, not `core_window_b_release`. The factory also translates the
  core's `window_b_credentials_unresolved` into `recommendation_release_credentials_unresolved`,
  including failures raised after its own preliminary checks (round 8 #5, #6).
  The CLI's `--operator-id` is the attestor, recorded in evidence. It does not select the account.
- `orders.broker_environment` must also be `'demo'`. NULL refuses.

## Delta 4: the stale-approval re-submission race (ckpt-1 #5)

`execute_order` reads the recommendation as `approved` at step 1 and takes the claim much later,
under the key. A release that commits in between would let a paused executor resume and submit a
second order, because the released claim no longer blocks its INSERT. The fix is in
`execute_order`, under the key and before `_claim_submission`: re-read
`trade_recommendations.status` for this `recommendation_id` and refuse
`recommendation_no_longer_approved` unless it is still `approved`, raising
`RecommendationNoLongerApprovedError`. That refusal is pre-I/O, so it
takes no claim. `RecommendationNoLongerApprovedError` subclasses `SubmissionControlsRevokedError`,
the class `_assert_submission_controls` already raises. It is raised after a committed `_write_refusal_audit` row, so the scheduler counts it `refused`.
Since the release changes status only while holding the same key, an executor that holds the key
sees either the pre-release or the post-release status, never a torn one.

**Two branches skip the claim today and must first respect an outstanding one** (round 2 #16;
round 3 #1, #2; round 4 #1, #2). They are the synthetic branch (live trading off) and the live EXIT arm when
`_load_exit_lot` finds no lot. Both currently persist an order and move the recommendation on
without looking for a claim. So a W or U row taken while live was on could be overtaken: a
synthetic fill, or a no-lot `execution_failed` booked beside a real unresolved attempt, after
which the W or U row fails candidacy for ever. Both branches now first run the claim index's own
predicate for this `recommendation_id`. When a row matches, they raise
`PriorSubmissionUnresolvedError` after the same committed `_write_refusal_audit` row the existing
`_claim_submission` raise site writes, so the scheduler's `unresolved` count stays truthful.

**Serialisation: the key spans every branch, from acquisition to the step-4/5 commit** (round 4
#1, #2; round 5 #1, #2, #3). A check alone races any claim or release that commits after it.
Today the live path drops the key as soon as the provider call returns and persists outside it.
So executor B can re-check `approved` and INSERT its claim in the gap after executor A's
persistence lifts A's claim, and then send a second order. The build therefore changes the key's
scope in `execute_order`. On **every** branch (live claim, live no-lot EXIT, synthetic) the
per-recommendation key is taken once, before any check, and released only after the step-4/5
transaction has made a **top-level** `conn.commit()`. The `conn.transaction()` block must not be
nested inside an implicit transaction the checks opened. So the checks are followed by a commit,
or run in their own `conn.transaction()`, before the persistence block starts. The key's cleanup
path then has nothing left to roll back.

- **Window A is unaffected.** Its discriminator is "`claim_committed` while the key is free", and
  holding the key longer only delays when a dead submitter's row becomes releasable. It never
  makes a live one releasable.
- **The live no-lot arm** runs its claim check after `terminalise_unsubmitted_recommendation_attempt`,
  so a provable window-A row has already been released.
- **Compare-and-set on the recommendation.** The step-5 `UPDATE trade_recommendations` is
  compare-and-set on `status='approved'` in the synthetic branch and in the live no-lot arm. Both
  have no broker response to lose, and a miss raises and rolls back. The live claim branch's
  step-5 UPDATE stays unconditional: it runs under the key, after its own claim, and rolling it
  back would discard a real broker response.
- **Key-helper fix** (round 5 #8; round 6 #2, #3). `_recommendation_submission_try_lock` today
  acquires the session key and commits **before** its `try`/`finally`, and its cleanup calls
  `rollback()` outside the guarded unlock. Any exception there can leak a session lock onto a
  pooled connection. The fix covers the whole path. If any of the acquire `execute`/`fetchone`,
  the acquire commit, the cleanup `rollback()` or the unlock raises, the helper calls
  `conn.close()`, which makes Postgres drop every session lock that backend held, and the
  original exception propagates. `psycopg_pool` discards a closed connection on return instead
  of reusing it.
- **A committed execution survives a cleanup failure** (round 7 #5). `execute_order` records
  that its step-4/5 commit returned. If the key helper's cleanup then raises, having already
  closed the connection so no lock survives, the error is logged and the `ExecuteResult` is
  returned. The scheduler's post-call `conn.commit()` is skipped when `conn.closed`. A filled or
  pending order is therefore never counted `failed` because of an unlock hiccup. The build sweeps
  every `execute_order` caller for post-return use of `conn` and guards each one the same way
  (round 8 #1).
- **Check order under the key** (round 6 #6; round 7 #4, #6). On every live branch the order is:
  1. `terminalise_unsubmitted_recommendation_attempt`, as today, so a provable window-A row is
     released and never wedges the next check;
  2. the unresolved-claim predicate (`PriorSubmissionUnresolvedError`);
  3. Delta 4's status re-check.

  The synthetic branch runs all three too (round 8 #2). Otherwise a stranded `claim_committed`
  row would wedge synthetic execution for as long as live trading stays off. Under the key, then, an outstanding claim is reported
  as unresolved even when the recommendation's status has moved to `execution_pending`. ⚠ Step 1
  of `execute_order` still runs before the key. A U park that commits between the scheduler's
  selection and step 1 is therefore counted `refused` (`RecommendationNoLongerApprovedError`),
  not `unresolved`. Both counts are truthful: nothing is submitted, and the claim stays held.
- **Status moved before `execute_order` started** (round 6 #7). Step 1's refusal of an existing
  but non-`approved` recommendation raises the same refused-class exception as Delta 4
  (`RecommendationNoLongerApprovedError`, a `SubmissionControlsRevokedError`). A missing
  recommendation keeps its `ValueError`. The build greps every `execute_order` caller for a
  `ValueError` handler around step 1, and preserves that caller's mapping for the non-approved
  case. So a release landing between the scheduler's selection
  and step 1 counts `refused`, not `failed`. Like every `SubmissionControlsRevokedError` the
  scheduler treats as already audited, it is raised only after a committed `_write_refusal_audit`
  row (round 8 #3).
- **Busy key** (round 5 #10). A busy key raises the existing `ConcurrentSubmissionInFlightError`.
  `execute_approved_orders` gains a handler that counts it `unresolved`, beside
  `PriorSubmissionUnresolvedError`, instead of letting it fall through to `failed`.

## The act: `release_recommendation_window_b(conn, *, order_id, operator_id, attestation, broker_factory, environment, clock, …)`

The step numbers are #2961's.

1–3. **Input, accident controls, demo.** Unchanged. `validate_attestation` and
   `attendance_refusal` are reused. `environment` must be `'demo'`, and the connection must be
   IDLE. The per-order demo check is `orders.broker_environment = 'demo'` plus Delta 3.
4. **Candidacy, read before any lock** (ckpt-1 #13). W or U, as defined in the table above, plus:
   - `recommendation_id IS NOT NULL` and `recommendation_request_id IS NOT NULL`;
   - `o.action IN ('BUY','ADD','EXIT')`, equal to the recommendation's `action`;
   - `o.instrument_id` equal to the recommendation's;
   - the recommendation status the table requires;
   - both recorded credential ids not NULL.

   Anything else refuses `recommendation_release_not_a_candidate`.
5. **Locks.** The evidence key is the per-recommendation advisory key
   `(RECOMMENDATION_SUBMISSION_ADVISORY_LOCK_NS = 2942, recommendation_id)`, taken with
   `_recommendation_submission_try_lock`. A busy key refuses, because a live submitter or a
   scheduler pass holds it. The key is reentrant, so a caller already holding it refuses first,
   checked in `pg_locks` for `pg_backend_pid()`. There is no per-order reconciliation lock on this
   path: `recommendation_order_reconcile` selects only `status='pending'` rows with a
   `broker_order_ref`, and neither W nor U matches.
6. **Re-read candidacy under the lock.** Unchanged. The whole detail tuple must match.
7. **Sends ended** (Delta 1). W: sender death, as in #2961. U: `recommendation_parked_at` is
   recorded. Both: the payload scan. `t_obs` is taken here.
8. **Wait**, per Delta 1's single function. The key stays held, so this recommendation cannot
   re-submit meanwhile. Unlike #2961's global core key, it blocks nothing else.
9. **Witness.** One `get_account_risk_snapshot()` through `broker_factory(api_id, user_id)`, bound
   per Delta 3, and judged per Delta 2 by action.
10. **Terminal write, in one transaction with the key held.** Each statement is compare-and-set
    over **every field the act relied on** (ckpt-1 #12), and asserted to affect exactly one row:
    - `UPDATE orders SET status='rejected'`, with a `WHERE` that matches all of: `order_id`, the
      state's `status`, `broker_order_ref IS NULL`, `recommendation_submission_phase =
      'broker_verb_entered'`, and `IS NOT DISTINCT FROM` the values read for `recommendation_id`,
      `created_at`, `instrument_id`, `action`, `broker_environment`, `raw_payload_json`,
      `recommendation_park_message`, both
      credential ids, the three sender columns, `recommendation_parked_at`,
      `recommendation_exit_position_id` and `recommendation_exit_units`.
      `rejected` leaves the claim index's predicate, so the claim lifts. `raw_payload_json` is
      left as it is: U's ambiguous response is evidence.
    - `UPDATE trade_recommendations SET status='execution_failed' WHERE recommendation_id=%s AND
      status=<'approved'|'execution_pending'> AND instrument_id=%s AND action=%s`.
    - The PASS `decision_audit` row comes **last**: `stage='recommendation_window_b_release'`,
      `model_version` = the derived version, with `recommendation_id` and `instrument_id` set.
      `evidence_json` holds #2961's fields, plus (ckpt-1 #27): `state` (W/U), `action`,
      `recommendation_request_id`, both credential ids with their `created_at`, `t_obs` (wall),
      `parked_at` or the sender identity, the full `raw_payload_json` and park message as read (W and U),
      the accepted `payload_sha256`, and for an EXIT the
      lot id, its recorded units and every `positions` entry carrying that id.

    ⚠ The recommendation goes terminal, **not** back to `approved`. Window A re-arms it by
    leaving it `approved`, which is sound because no order can exist in window A. Here one may,
    so an automatic re-submission of a stale approval would spend the double-order residual
    without anyone choosing to. Delta 4 closes the race in which a paused executor does that
    anyway. The portfolio manager generates new recommendations on its own cadence, and those go
    through the guard again.
11. **Refusals from step 4 onward are audited.** As in #2961: a committed FAIL row, written in its
    own transaction after the key is released, with the same stage and model version. Any
    non-`WindowBRefused` exception raised anywhere from step 4 through step 10 is converted to
    `recommendation_release_internal_error` and audited, never propagated unaudited (ckpt-1 #23).
    This includes the evaluator, the lock checks, the terminal SQL and the PASS INSERT.
    - **An exception raised by the terminal COMMIT itself** is reported
      `recommendation_release_outcome_indeterminate`, not as a refusal (round 3 #7). The flag is
      captured in a local variable **before** any cleanup runs. A later exception from the key
      helper's `rollback()` or unlock on a broken connection cannot overwrite it (round 4 #9).
    - **The success flag is captured the same way** (round 5 #7). Once `COMMIT` has returned, a
      cleanup exception is logged and the result is PASS with its `decision_id`.
    - The script closes its connection in a `finally`, and process exit drops any session key in
      any case. The release
      may have committed. Recovery is to re-run: a candidacy miss on an order that already has a
      PASS row for this stage (looked up by `evidence_json->>'order_id'`) reports `already_released`
      with that `decision_id`.
    - **The FAIL row is best-effort** (round 3 #8). It is written on the same connection, so a lost
      connection or a failed FAIL INSERT can leave no row. The script always prints the slug and
      the evidence to stdout, and exits non-zero. The attended operator's terminal is the record
      of last resort.

The surface is a script, in two invocations:

```
scripts/release_recommendation_window_b.py --order-id N --show
scripts/release_recommendation_window_b.py --order-id N --operator-id ID \
    --payload-sha256 <hex from --show> --attestation "..."
```

The script prints one of three verdicts and exits with a matching code: `RELEASED` (0),
`REFUSED <slug>` (1), or `INDETERMINATE` (2, meaning re-run to learn the outcome) (round 8 #4).
There is no API endpoint.

### Residual risk, named

- **BUY/ADD:** everything in #2961's list applies — acceptance or publication later than `T`; a
  fill fully closed inside the wait; a stale snapshot — plus the skew residual in Delta 2.
- **EXIT:** a queued close executing later; an unbooked landed close; a later EXIT recommendation
  closing the next lot (Delta 2). A close cannot add exposure.
- **Account binding assumption.** Credential ids are immutable, as shown in Delta 3. This rests on
  code, not on a constraint. Changing how rotation works would need to revisit it.
- **Exposure bound** (ckpt-1 #29). Core's residual is capped by #2844's sandbox. The recommendation
  path is **not** inside #2844, whose sandbox covers mandate/core capital. The bound here is
  narrower: demo money only, and `_assert_submission_controls` still gates every later
  submission. The supervisor decision of 00:02Z accepted the #2961 policy for this path by name.
  This spec states the difference rather than hiding it.
- **Rollout** (ckpt-1 #26; round 2 #22). A row written by a jobs daemon still running pre-412 code
  carries no sender, park or credential columns, and it refuses. An OLD executor paused before its
  claim would lack Delta 4's re-check. **Precondition, in the script's `--help` and the runbook:**
  the jobs daemon has been restarted onto a build containing this change before any release. It
  is attended, so the operator confirms it. The dev census is 0.

The real environment stays refused.

## Tests

- **Pure:**
  - `evaluate_recommendation_exit_witness`: lot present and unchanged passes; lot absent refuses;
    units differing at the 8th dp refuses; units bool, NaN, infinite, zero, negative or >8
    significant dp refuse, and `1.000000000` against `1.00000000` passes; `isBuy` missing, null,
    `"true"` or `1` refuses;
    a duplicate position id refuses; the lot on another instrument refuses; the lot as a mirror or
    a short refuses; a pending order on the instrument refuses; a malformed entry, a missing array
    and a pre-deadline read refuse.
  - The shared evaluator's hardening: a negative mirror; a non-positive position id; conflicting
    aliases; `bool`, `float` and `null` alias values (a present null alongside a valid alternate
    alias included); a naive or non-`datetime` `observed_at`.
  - The U payload-ref scan: each spelling, nested in a dict, in a list, as a float and inside a
    serialised string, refuses; a clean payload passes.
  - The wait function with `t_obs` for U: a `parked_at` in the future or long past still waits
    `T` on the monotonic leg.
- **DB, one per new mechanism:**
  - **PASS matrix**, parametrised over {W, U} × {BUY, ADD, EXIT}, one fixture builder: each case
    gives `rejected` / `execution_failed`, one PASS row, and broker mutations 0. A second release
    of the same order reports `already_released` with the first `decision_id`.
  - **Refusals, parametrised over the missing proof:** a live sender (W); sender columns NULL (W);
    `parked_at` NULL (U); credential ids NULL; `broker_environment` NULL or `'real'`. Each refuses,
    leaves a committed FAIL row, and changes nothing.
  - The recommendation UPDATE missing (its status moved) rolls back the orders UPDATE too.
  - **CAS mutations, parametrised:** `parked_at`, `raw_payload_json` and `created_at` each changed
    between the locked re-read and the write. Each misses, rolls back and audits FAIL.
  - **Candidacy, parametrised:** NULL `recommendation_request_id`; order/recommendation action
    mismatch; instrument mismatch; the wrong recommendation status for W and for U; a row changed
    between the unlocked and the locked read. Each refuses.
  - **U with a LIVE sender pid passes**, as does U with NULL sender columns: U never consults
    sender death.
  - **Terminal-COMMIT failure, both ways:**
    - Injected after the server committed: reports `outcome_indeterminate`, and a re-run reports
      `already_released`.
    - Injected before commit: reports `outcome_indeterminate`, and a re-run proceeds as a fresh
      candidate.
    - A cleanup exception after either keeps `outcome_indeterminate`.
  - **Key scope:** executor A holds the key through its persistence commit. An executor B started
    after A's provider call returns refuses busy. Once A has committed, B's outcome depends on A's
    result, parametrised: a terminal result (filled or rejected) gives
    `recommendation_no_longer_approved`, and a `pending` result, which keeps the claim, gives
    `PriorSubmissionUnresolvedError` (round 8 #7). A's step-5 rows are visible from another connection
    once A releases the key.
  - **Commit succeeded, then cleanup raised:** the result is PASS.
  - **Every changed persistence branch:**
    - a synthetic success and a live no-lot EXIT are both visible from another connection before
      the key is released;
    - a no-lot CAS miss rolls back its order row.
  - **Scheduler:** a busy key counts `unresolved`, not `failed`, and the loop continues to the next
    recommendation. A recommendation released between selection and step 1 counts `refused`.
  - A clean U row, whose park message is an ordinary exception text, is NOT refused by the scan.
  - **Key-helper failure sites, parametrised:** acquire `execute`, acquire `fetchone` (the key is
    taken server-side before the result is consumed), acquire commit, cleanup
    `rollback()`, and unlock. At each, the session key is not held afterwards (checked in
    `pg_locks` from another session), the connection is closed, and an exception raised in the
    body is the one that propagates.
  - **A committed execution whose unlock raises** returns its `ExecuteResult`, and the scheduler
    counts it by outcome, not as `failed`.
  - **Unexpected release exceptions:** an evaluator exception and a PASS-INSERT exception are each
    injected. Each becomes an audited `internal_error`, both terminal UPDATEs roll back, and the
    FAIL row is durable while the connection is usable.
  - **`--payload-sha256` mismatch** refuses `payload_not_inspected`. So does a park message
    changed after `--show`, with the payload unchanged.
  - **Park message:**
    - a reference present only in the message refuses;
    - a message changed between the locked re-read and the write makes the CAS miss;
    - both exception arms persist `str(exc)`. The generic arm is exercised at BOTH provider call
      sites (`close_position` and `place_order`), preserving `str`, `repr` and `raw_payload`, and
      at the 64 KiB `repr` boundary, which produces the truncation marker (round 8 #8);
    - a U row with `parked_at` set and a NULL message refuses `park_unrecorded`.
  - A U row whose park audit INSERT failed after the state committed is releasable: the order-row
    evidence is complete.
  - Synthetic mode with a stranded `claim_committed` row releases it and proceeds.
  - The CLI verdict and exit code for each of RELEASED, REFUSED and INDETERMINATE (both commit
    cases).
  - **Best-effort FAIL:** with the FAIL INSERT forced to raise, the script still prints the slug
    and the evidence and exits non-zero.
  - **W payload:** a W row whose payload carries `orderID` refuses. So do the normalisation
    counterexamples: a duplicate key, `\u`-escaped keys inside prose, a serialised body, and a
    truncated `repr`.
  - A busy recommendation key refuses, and nothing changes. A caller that already holds it refuses.
  - Recorded credential ids that are no longer the live pair refuse. So do mixed-operator ids and
    swapped labels. The attestor `--operator-id` differing from the owner still passes: it
    attests, it does not select.
  - The scheduler construction contract: the recorded ids are those of the plaintext the broker
    was built with (unit test with a fake loader). A live `execute_order` with no or partial
    `broker_credential_ids` raises before any claim or broker call, for BUY and for EXIT.
  - Rotation: `revoke_credential` on a recorded id of an unresolved claim raises
    `CredentialInUse`. A claim INSERT against a revoked id refuses pre-I/O with an audit row.
  - Overtaking: with a W row present, and separately with a U row reached by parking it **after**
    a paused executor has read `approved` (a U recommendation is `execution_pending`, so a static
    fixture cannot reach the check), the synthetic branch
    and the live no-lot EXIT arm both raise `PriorSubmissionUnresolvedError` with an audit row,
    and nothing is persisted. With the recommendation key held by another session, both refuse as
    busy. That is the serialisation the key provides against a claim or a release committing
    after the check.
  - Changed evidence between the locked re-read and the write (a changed `parked_at`) makes the
    CAS miss: rolled back, FAIL audited.
  - A refusal after step 4 leaves a committed FAIL row.
  - Delta 4: an executor that read `approved` before a release refuses
    `recommendation_no_longer_approved` after it, with no claim taken.
  - `mark_recommendation_submission_entered` writes the three identity columns;
    `_park_uncertain_submission` writes `recommendation_parked_at`; the new `except Exception` arm
    parks `uncertain` with the exception's payload and raises `BrokerSubmissionUncertainError`;
    the claim INSERT writes both credential ids; the synthetic step-5 CAS miss rolls back.
  - **Wait wiring:** with an injected clock and a fake broker, the witness is called only after
    both deadlines, the recommendation key is held throughout, and the connection is IDLE at the
    witness call.
- **Unchanged:** the #3165 window-A tests, the #3168 poller tests and the #2961 tests. The #2961
  tests are updated only where the version string changed.

## Out of scope

- Async 202 (`pending` with a ref), which the poller already owns.
- Any unattended path, any change to `terminalise_unsubmitted_recommendation_attempt`, and any
  frontend.
- Booking a landed-but-released order. That is residual 2, as in #2961.
- A W row whose broker call succeeded but whose persistence failed. Its handling is in Delta 1:
  refused while its sender lives, caught by the witness after a restart. Nothing here books it.

## Codex checkpoint 1: disposition

**Round 1: 40 findings.** Accepted and folded in:

- #1, #2, #4 → Delta 3, recorded credential ids;
- #5 → Delta 4;
- #6, #7, #8 → the corrected EXIT residual;
- #9, #10, #14 → Delta 1's single wait function with a monotonic leg and no DB reads while
  waiting;
- #12 → the full-evidence CAS;
- #13 → candidacy integrity;
- #15 → `except Exception` parks as U;
- #16–#19 → EXIT validation;
- #21 → the prose fix;
- #22, #23 → the evaluator hardening and the internal-error audit;
- #24 → the payload scan now looks at values;
- #25 → the prose fix;
- #26, #29 → named residuals;
- #27 → evidence fields;
- #28 → the derived version;
- #31–#40 → the tests above, where each exercises a new mechanism.

Not adopted:

- #3: superseded, since binding no longer rests on timestamps.
- #11: #2961's skew residual, named.
- #20: a stale cache refuses, which is safe.
- #30: accepted residuals.
- The rest of #31–#40 duplicate existing #2961 or #3165 coverage of shared code, per the lean-test
  rule.

**Round 2: 37 findings.** Accepted and folded in:

- #1, #2 → CAS adds `raw_payload_json` and `created_at`;
- #3 → exact units, >8 dp refuses;
- #4 → presence is the load-bearing EXIT check, because the close is whole;
- #5 → the prose no longer claims an upstream invariant;
- #6, #7 → the generic arm keeps the payload and raises `BrokerSubmissionUncertainError`;
- #8, #9, #10 → the live-sender W shapes, named and caught by the witness;
- #11, #12, #24 → the substring payload scan;
- #13, #14, #15 → the hardening;
- #16 → the synthetic CAS;
- #17 → the refusal class and audit;
- #18–#21 → Delta 3 immutability, loader and factory;
- #22 → the rollout precondition;
- #23 → the audit covers steps 4–10;
- #25–#29 → prose;
- #30–#36 → the tests above.

Not adopted:

- #37: a disposition-format complaint.
- The balance of #31–#35 duplicates shared-code coverage.

**Round 3: 15 findings, all accepted.**

- #1, #2 → the overtaking branches respect the claim;
- #3, #4 → the normalised scan, applied to W and U alike;
- #5, #6 → the rotation row locks;
- #7, #8 → the indeterminate outcome and best-effort FAIL;
- #9 → significant decimal places;
- #10 → prose;
- #11–#15 → the tests.

**Round 4: 17 findings.** Accepted:

- #1, #2 → the key spans check and persist;
- #3, #4, #5 → the three-view scan and the untruncated `repr`;
- #6, #7, #8 → the round-3 row locks withdrawn, and the race named as fail-closed;
- #9 → the indeterminate flag captured before cleanup;
- #10–#17 → the tests, refusal classification and W evidence.

**Round 5: 10 findings, all accepted.**

- #1, #2, #3 → the key spans acquisition to the top-level commit on every branch, with a CAS on
  the no-lot branch;
- #4 → the park explanation is scanned, and the generic arm stores `str`;
- #5 → the duplicate-key boundary is stated;
- #6 → `--show` plus `--payload-sha256`;
- #7 → the success flag;
- #8 → the key-helper fix;
- #9 → the paused-executor U fixture;
- #10 → the busy-key handler.

**Round 6: 10 findings, all accepted.**

- #1, #4, #5 → `recommendation_park_message` on the order row replaces the audit-explanation
  input. It is scanned, CAS-covered, hashed and recorded;
- #2, #3 → the helper closes the connection on any failure in its lock path;
- #6 → the check order;
- #7 → `RecommendationNoLongerApprovedError` at step 1;
- #8, #9, #10 → the tests.

**Round 7: 9 findings, all accepted.**

- #1 → the obsolete audit-row test replaced;
- #2, #3, #8 → the tests;
- #4 → the check order qualified, with the pre-key step-1 case named;
- #5 → a committed execution survives a cleanup failure;
- #6 → window-A terminalise runs first;
- #7 → NULL message refuses;
- #9 → the full CLI shown.

**Round 8: 9 findings, all accepted.**

- #1 → the caller sweep;
- #2 → synthetic window-A;
- #3 → the step-1 audit;
- #4 → the three-verdict CLI;
- #5, #6 → the recommendation factory;
- #7, #8, #9 → the tests.

**Converged.** The finding count ran 40 → 37 → 15 → 17 → 10 → 10 → 9 → 9. Round 8 raised no
new unsafe-release path; every item was a test, prose, attribution or wiring detail. The design
is frozen for the build.
