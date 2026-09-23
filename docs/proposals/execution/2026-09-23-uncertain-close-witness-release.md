# Releasing an uncertain engine close on the broker's whole-close witness (#2979 half a)

Status: spec r3 · 2026-09-23 · Refs #2979, #2965, #2603, #2437. Extends the #2965 r3 key
(`2026-09-23-broker-closed-ownership-release-rev3.md`, built in #3312). That doc's key,
evidence and locks are inherited and not restated.

## The wedge, as it stands on `51e9d5d8`

Our close verb was entered and its outcome is unknown. Two writers produce this state, and
the rows they leave are identical:

| writer | op `status` / `last_error_code` | trade | exit `orders.status` |
| --- | --- | --- | --- |
| `_resume_operation`, a close found at `submitting` (`strategy_position_manager.py:652-681`) | `reconcile_required` / `crash_before_submission_identity` | `reconcile_required` | `submitted` |
| `_submit_close`, `BrokerPositionMutationUncertain` (`:1055-1076`) | `reconcile_required` / `broker_close_uncertain` | `reconcile_required` | `submitted` |

When the broker did act (7b, and the "taken" arm of the uncertain test), the position is
gone. Every later `manage_owned_position` pass reaches the `position is None` branch
(`:1373`) and calls `_release_whole_broker_close`. That call refuses `operation_unresolved`,
because `load_whole_close_evidence` counts any `reconcile_required` close
(`broker_closed_release.py:217-220`). Ownership stays `active`. `resolve_engine_capital_usage`
then refuses the join on every cycle (`engine_capital_ownership_unwitnessed`). Separately,
the core preflight's `core_operation_outstanding` (`strategy_core_preflight.py:370-378`,
twin in `strategy_core_executor.py:842-852`) refuses every core BUY for as long as that op
exists, on an active or a released ownership (#2603 sell-leg spec §4). Nothing clears
either condition.

## Why the #2965 witness settles it

1. **The verb is position-addressed and whole.** `close_demo_strategy_position` POSTs
   `/market-close-orders/positions/{P}` with `UnitsToDeduct: null`
   (`etoro_broker.py:827-828`). Its only possible broker effect is closing P whole.
2. **The #2965 key proves P was closed exactly once, whole.** It requires one open witness
   of the unaltered position, one `etoro_history` close row covering all of it, and no
   sibling slice under the entry order.
3. So whatever our op did, no exposure remains for it. Either our close was that one close,
   or it failed, no-oped, or still sits queued against a position that is gone. ⚠ A queued
   close is NOT excluded (Codex r1-1) and does not need to be. It is addressed to P by
   path, P is closed, and eToro position ids are not reused. A later rebuy opens a new id
   that it cannot address. A close can only reduce the position it names, so the most it
   can do is fail.

What stays unknowable is **whose** close it was. History rows carry no close reason and no
close-order id. Dev evidence: the latest `etoro_history` row's keys are `closeRate`,
`closeTimestamp`, `orderId` (the OPENING order), `stopLossRate`, `takeProfitRate` and the
P&L fields. So the spec never marks our op `applied` and never marks the exit order
`filled`. Either would invent a fill.

7a is unaffected. A close that never reached the broker leaves P present unless something
else closed it. In that case the witness is truthful regardless, and release is correct
(r1-22). `intent_persisted` is terminalised first (`close_never_submitted`, `:625-651`), and
the release happens on a later pass under the existing key. `submitting` means the verb MAY
have been entered (the marker commits before the call, r1-21). That is the same uncertainty,
and the same argument covers it.

## Design

### 1. Migration `sql/414_close_op_broker_witness.sql`

```sql
ALTER TABLE strategy_position_operations
    ADD COLUMN IF NOT EXISTS broker_close_witnessed_at TIMESTAMPTZ;
ALTER TABLE strategy_position_operations
    ADD CONSTRAINT strategy_position_operations_broker_witness_shape CHECK (
        broker_close_witnessed_at IS NULL
        OR (operation_type = 'close' AND status = 'reconcile_required'
            AND last_error_code IS NOT NULL
            AND last_error_code IN ('crash_before_submission_identity', 'broker_close_uncertain'))
    );
```

The constraint is dropped `IF EXISTS` and re-added, which is the repo idiom (`sql/413`), so
a rerun is a no-op (r1-34). `IS NOT NULL` is load-bearing: `NULL IN (…)` is NULL, and a CHECK accepts
NULL (r2-1). The CHECK names the two codes so a divergent close can never be stamped
(r1-7). It cannot require a released ownership, because that is a cross-table fact.
Instead, the stamp has exactly two writers, both in the transaction that releases the
ownership (§4, §4b).

The column means that P's whole close was witnessed at the broker while this op stood
unresolved. The op's `status` and `last_error_code` are untouched. They still record that
our outcome is unknown.

### 2. Evidence split (`load_whole_close_evidence`)

A row is **uncertain-eligible** when all of these hold. It is a `close` at
`reconcile_required` with `broker_close_witnessed_at IS NULL`. Its code is in
`UNCERTAIN_CLOSE_ERROR_CODES = ("crash_before_submission_identity",
"broker_close_uncertain")`, compared with `= ANY`, so a NULL code is not eligible (r1-14).
And its exit order's persisted response does NOT name another position. That response is
`orders.raw_payload_json -> 'orderForClose' ->> 'positionID'`: NULL passes, and any text
other than P's decimal passes as "another" and fails closed. The last condition exists
because `broker_close_uncertain` also covers "response identity does not match intent"
(`etoro_broker.py:855-858`), and `persist_response` stores the body before that raise
(r1-2).

The evidence carries `uncertain_close_operation_ids`, the exact eligible ids. Every other
unstamped `reconcile_required` close, plus every in-flight op, is counted in
`blocking_operation_count` as today. `close_order_did_not_affect_exact_position` stays
blocking. It is written for ANY non-pending, non-exact close-order answer, a broker
`rejected` included (`strategy_position_manager.py:724-761`, r1-32). Releasing that shape too
is plausible but out of scope.

### 3. Verdict (`evaluate_whole_close`)

The verdict is unchanged except for its reason. When every other condition passes and
`uncertain_close_operation_ids` is non-empty, it releases with `UNCERTAIN_CLOSE_RELEASE_REASON =
"broker_closed_after_uncertain_close"`. Any number of uncertain closes is accepted, because
point 1 holds for each of them. The distinct reason keeps the audit honest: this was not
necessarily an external close.

### 4. Write (`_release_whole_broker_close`)

`_release_whole_broker_close` returns the verdict's reason (or `None`) instead of `bool`,
writes that reason as `release_reason`, and the caller at `:1374-1375` returns it (r1-6).
Order inside the existing write transaction (r1-12):

1. the ownership UPDATE (guard `status='active'`; `None` → return with nothing written);
2. only then, when ids are present: `UPDATE … SET broker_close_witnessed_at = now()
   WHERE position_operation_id = ANY(ids) AND ownership_id = %s` plus the full eligibility
   predicate. The row count must equal `len(ids)`, else `StrategyPositionManagerError`,
   which rolls back step 1 as well. It targets the evaluated ids, not a recount (r1-11), and
   uses DB `now()`, not the pre-lock `observed_at` (r1-31);
3. the trade write, as today.

Evidence and write run in separate transactions under the SAME held
`_paper_allocator_lock` + `_position_lock` (`:1356`), which is #3312's existing
discipline. Step 2's re-predicate catches any row that changed in between (r1-10).

### 4b. `_finish_close` stamps too (r1-5)

A later close that the close-order lookup proves `filled` on exactly `(P,)` releases through
`_finish_close`. An earlier uncertain op on the same ownership would otherwise block the
core preflight forever. That is today's wedge in its other form. `_finish_close` therefore
runs step 2 over that ownership's eligible rows, without an equality check (it holds no
evidence set), in the same transaction. The same point-1 argument applies: our verb is a
whole close (`UnitsToDeduct: null`) and it filled on P.

### 5. Readers skip a witnessed op

The three `reconcile_required`-close predicates gain `AND op.broker_close_witnessed_at IS NULL`:

- `broker_closed_release.py:217-220`, which point 2 replaces;
- `strategy_core_preflight.py:377`;
- `strategy_core_executor.py:849`.

§4's rule ("any core ownership, active or released") is kept as written. The op stops
counting because its broker effect is exhausted (point 3), not because its ownership is
released.

### 6. Executor outcome map

`map_core_close_outcome` (`strategy_core_executor.py:670`) maps the new reason as it maps
`RELEASE_REASON`, to `("refused", "core_position_closed_by_broker")`. Today this is
unreachable: a new sell intent on the ownership is refused by the preflight while the
uncertain op is unstamped. It is mapped anyway so a future path cannot fall through to
`core_rebalance_close_not_started`.

### Not changed

- **The exit `orders` row stays `submitted`.** That is the truthful state of our order.
  No reader treats a strategy EXIT at `submitted` as outstanding: the `'submitted'` readers
  are the entry-side window-B releases and the executors' own write sites, and
  `reconcile_backlog` cannot see an EXIT at all (#2979 fact 1).
- **A stamped op's `status` never moves again.** `reconcile_required` is terminal.
  `_terminal` only updates in-flight rows (`:431-445`). The other status writers
  (`mark_close_submitting`, `_submit_close`'s `submitted` write) act on an op still inside its
  own serialized submission, which never follows its terminal write (r1-9, r2-18).
- **The linked sell intent's recorded outcome** stays `reconcile_required`. That is the
  truthful historical verdict on OUR close (r1-16).
- **Multi-ownership trades** keep #3312's rule: the trade closes only when no active
  ownership remains (r1-19).
- **The inherited #3312 key** (history completeness, sibling normalisation, raw vs stored
  close timestamp, snapshot coherence and writers outside the locks, r1-25..29 and
  r2-4..8) is DEFERRED, not reopened here. This spec adds no new dependency on it: the
  uncertain op changes WHICH ops block, not what the witness must prove. Only `_int` is
  fixed. `"²".isdigit()` is true and `int("²")` raises, and so does a digit string past
  Python's conversion limit. It now requires `isascii()` and catches `ValueError`
  (r1-30, r2-9).
- **P&L and capital.** Realised P&L already comes from the witness row R
  (`strategy_engine_capital.py:133-148`), and the key refuses when R is unpriced.
- No broker call is added. The release reads stored rows only.

## Tests

All in the #2949 harness or pure, as below. The inverted 7b needs TWO passes after the
crash: resume (→ `reconcile_required`), then release (r1-20).

- **Pure evaluator:** a whole close with one or two uncertain ids → release, new reason.
  Zero ids → today's reason. Any blocking op → `operation_unresolved`, whatever the ids.
  Uncertain ids plus an unpriced or partial witness → the witness refusal (r2-11, r2-16).
- **DB (the #2949 harness):** `test_scenario_7b_…` is inverted per #2979's acceptance.
  Seed the `etoro_sync` open row and the `etoro_history` whole-close row the fake broker
  implies. The next pass then gives `applied` / `broker_closed_after_uncertain_close`:
  ownership `released`, trade `closed`, op stamped and still `reconcile_required`, exit
  order still `submitted`, `close_calls == 1`. The core allocator returns a verdict and no
  longer refuses `engine_capital_ownership_unwitnessed`. Without the history row,
  behaviour is today's (`owned_position_missing`).
- The same inversion for the "taken" arm of
  `test_an_uncertain_close_holds_the_position_…`.
- 7a / 7d tests run unchanged (acceptance clause 2).
- `close_order_did_not_affect_exact_position` plus a whole-close witness → still refused.
- Preflight: a witnessed op no longer sets `core_operation_outstanding`. An unwitnessed one
  on a released ownership still does.
- Migration: the CHECK rejects a stamp on a non-close, non-`reconcile_required` or
  divergent-code row. A rerun is a no-op.
- Eligibility (DB): a `broker_close_uncertain` whose `raw_payload_json` names another
  position → blocking. A NULL code → blocking. Both codes together → release. An
  already-stamped row → ignored. Another ownership's op → not counted.
- Stamp mismatch (a row made ineligible between evidence and write) → raises, nothing
  written (ownership still `active`).
- `_finish_close` after an earlier uncertain op → op stamped, preflight clear.
- Executor's outstanding query: the same witnessed/unwitnessed pair as the preflight. A
  witnessed op beside an unwitnessed divergent close on another core ownership → still
  outstanding (r2-14).
- `map_core_close_outcome`: the new reason unlinked → `core_position_closed_by_broker`. A
  linked uncertain op → `reconcile_required`. An id mismatch still wins (r2-15).
- §4b negative: a divergent-code op on the same ownership stays unstamped (r2-13).
- `_int("²")` and a 5,000-digit string → `None`.

## Acceptance (live)

A demo core close whose acknowledgement is lost cannot be produced on demand, so this is a
WATCH item, not a wake condition. Cutover (r2-2, r2-3): on 2026-09-23 dev holds **no close
op in any status** (`select status, last_error_code, count(*) from
strategy_position_operations where operation_type='close' group by 1,2` → no rows). So no
pre-existing released-with-uncertain op needs a backfill. Re-run that query after the
jobs restart onto the merge, which closes the old-worker window. Ongoing census (r1-35):
`select last_error_code, broker_close_witnessed_at is not null, count(*) from strategy_position_operations where operation_type='close' and status='reconcile_required' group by 1,2`.
