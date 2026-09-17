# #2961 — terminalise a core ENTRY authority the broker verb never reached, on our own write ordering

Status: proposal, rewritten after Codex checkpoint 1 (20 findings; the spec was rewritten,
not patched). Autonomy loop, 2026-09-17, from `ba39da31`.

## What this is, and what it deliberately is not

This is **not** the `POST /strategies/core-sleeve/abandon-authority` design killed at Codex
checkpoint 1 on 2026-09-13
(`docs/proposals/execution/2026-09-13-abandon-unsubmitted-core-authority.md`). That design
rested on two **broker observations** — a fresh `orders:lookup?referenceId=` miss (G2) and
the absence of an unowned position on the instrument (G3) — which are correlated, have
clean false negatives, and can never prove non-acceptance. It was correctly refused as a
refusal bypass wearing an attestation string.

This rests on a fact about **our own write ordering**: a marker committed between the
authority commit and the broker verb, so its absence proves the verb was never entered.
That is the same argument `app/services/strategy_position_manager.py:517-522` already makes
for closes, shipped by #2979:

> `mark_close_submitting` commits BEFORE the broker verb is entered, so a close still
> sitting at `intent_persisted` never reached it (#2979). That is a fact about our own
> write ordering, not an inference about the broker, which is what makes it safe to
> terminalise cleanly.

No capital is released on any row whose marker says the verb may have been entered.

## Why this is buildable now, and was not on 2026-09-13

#2961's Scope made the fix conditional on settling `orders:lookup?referenceId=` coverage.
That is now settled — **negatively, on both API versions** — by the consented demo probe of
2026-09-17 (issue #2961, comment *"The positive case is now observed, and it FAILS"*): a
v2-submitted order that **filled** echoed our `referenceId` exactly and was still HTTP 404
on `orders:lookup?referenceId=`; only `orderId` resolved it. That probe's own conclusion
names this design:

> the `submitting`-marker shape (commit an intent marker immediately before the broker
> verb) is the only client-side answer available, and #2961/#2965/#2942 half 2 should adopt
> it rather than migrate toward a reference key.

## ⚠⚠ Premise correction 1 — the issue body and the harness describe different windows

The issue body states the fault as *"the engine dies after `execute_core_rebalance` commits
the durable authority … and before `place_demo_core_order`. Broker mutation count: 0."*

The harness injects that fault **inside** `place_demo_core_order`, at the first statement of
the fake provider (`tests/fixtures/core_restart.py:538`). `mutation_calls == 0` is therefore
a fact the **harness** knows by construction, not one the application could derive.

**Consequence, stated up front because it changes this ticket's acceptance:
`test_scenario_2_stranded_authority_is_reached_unattended_and_still_cannot_resolve` does
NOT invert.** Its fault lands after the marker, so it stays unresolvable — and that is the
correct outcome, not a shortfall. It becomes this change's **negative control**: the proof
that the marker is a discriminator and not a blanket release. #2961's acceptance as written
cannot be met without weakening the gate, and the corrected acceptance is in this document.

## ⚠⚠ Premise correction 2 — the recoverable boundary is the MARKER COMMIT, not the socket write

The first draft of this spec put the marker inside the provider via a `mark_submitting`
callback, arguing it bought the whole span up to `self._http_write.post(...)`. Codex
falsified that on three counts and the callback is dropped entirely:

- `broker_verb_entered` means *"may subsequently enter"*, not *"entered"*. The boundary a
  marker can prove is its own commit, wherever it sits.
- The provider point is **not** the last client-side instant: `ResilientClient`
  (`app/providers/resilient_client.py:276`) throttles, waits on a shared lock and builds the
  request *after* it, as does header construction (`etoro_broker.py:618`).
- It buys **less** than claimed over executor placement. `_submit_core_authority` acquires
  the per-order reconciliation lock at `strategy_core_executor.py:406` and only then calls
  `_submit_core_authority_locked`, so that blocking acquire precedes the marker under
  *either* placement. The callback would have bought the unattended-mutation guard, the env
  check and body construction — microseconds — in exchange for changing the
  `BrokerProvider` protocol and three concrete providers.

So the marker goes in `_submit_core_authority_locked`, one statement before the provider
call, and there is **no provider API change**.

**Window A (auto-terminalised): authority commit → marker commit.** Its dominant term is the
blocking `reconciliation_order_lock` acquire, which deliberately sets no `lock_timeout`
(`strategy_order_reconciliation.py:272-277`), so under contention it waits for a reconciler's
full provider HTTP timeout. Uncontended it is two round trips.

**Window B (not resolvable by this mechanism): marker commit → response persisted.**
⚠ Phrased as *"this mechanism cannot resolve it"* rather than *"permanently unresolvable"* —
the stronger claim exceeds the evidence.

## Source rule / precedent, cited rather than reasoned out

- **Two distinct commits.** `strategy_position_manager.py:854-857` is explicit that folding
  the marker into the intent transaction makes the separation vacuous.
- **A public, module-level marker function is required by the harness, not a style choice.**
  `strategy_position_manager.py:805-807`, and `tests/fixtures/core_restart_child.py:54-61`
  is the mechanism: the child monkeypatches `mark_close_submitting` to die before it. The
  new fault point reuses that exact shape.
- **`CORE_SUBMISSION_ADVISORY_LOCK` already documents the span this needs.**
  `app/services/broker_credentials.py:316-319`: *"A core mutation holds this key through the
  broker response."* Acquired session-scoped by `core_submission_lock` at
  `strategy_core_executor.py:547`, held to `:772`.
- **Postgres releases advisory locks when a backend dies** — already relied on and stated at
  `strategy_order_reconciliation.py:224-228`.
- **The terminal-state guard exists.** `_record_failure`
  (`strategy_order_reconciliation.py:443`) preserves a terminal state, its timestamp, broker
  status and error code, and preserves the failed trade — #2964's guard. The 09-13 verdict
  made this a prerequisite; it is satisfied, and is asserted by test rather than assumed.

## Design

### 1. Schema — one additive column

```sql
ALTER TABLE strategy_order_reconciliation_state
    ADD COLUMN IF NOT EXISTS submission_phase TEXT
        CHECK (submission_phase IS NULL
               OR submission_phase IN ('authority_committed', 'broker_verb_entered'));
```

`NULL` = *not written by the core entry path, or predates this column* — the alpha arm,
manual orders, core **closes** (see the scope note below), and every row that exists today.
**`NULL` is never terminalisable**, so a legacy row keeps exactly today's behaviour and no
backfill is needed or attempted.

⚠ A new column rather than a new `state` value: `state` describes **what the broker said**,
and "we are about to call" is not a broker answer. It also leaves
`strategy_order_reconciliation_resolved_shape` and the backlog partial index untouched.

Population before writing it (dev DB, 2026-09-17): `strategy_order_reconciliation_state`
**0 rows**, `strategy_trades` **0 rows**, `orders WHERE execution_origin='strategy'` **0
rows**. Stated rather than dressed up as a passing check — with zero rows the column's
semantics cannot be validated against stored data. What it does establish is that the
migration cannot break an existing row.

### 2. Scope of the proof — core ENTRIES only

`CORE_SUBMISSION_ADVISORY_LOCK` is **not** held by every core mutation. `manage_owned_position`
→ `_submit_close` (`strategy_position_manager.py:927`) commits core *close* authority under
the allocator and position locks and does not take this key. Closes are out of scope here —
they carry their own marker in `strategy_position_operations.status` (#2979) and never write
`submission_phase`, so they are `NULL` and untouchable by this path. The claim is stated as
*core entries*, not *core mutations*.

### 3. Writer 1 — the authority commit declares the phase

`execute_core_rebalance` (`strategy_core_executor.py:749-752`), same statement, same
transaction, no extra round trip:

```python
conn.execute(
    "INSERT INTO strategy_order_reconciliation_state (order_id, submission_phase) "
    "VALUES (%s, 'authority_committed')",
    (order_id,),
)
```

### 4. Writer 2 — the marker, with an explicit success contract

```python
def mark_core_submission_entered(conn, *, order_id: int) -> None:
```

- **Requires an idle connection.** `conn.transaction()` inside an open transaction opens a
  savepoint, not a transaction, so the "commit" would not be durable. Refuse rather than
  silently downgrade.
- `UPDATE … SET submission_phase='broker_verb_entered', updated_at=now()
  WHERE order_id=%s AND submission_phase='authority_committed'`.
- **Requires exactly one affected row**, and raises otherwise. A zero-row UPDATE must not be
  allowed to fall through into the POST: a row that could not record "about to call" must
  not then call.
- Called from `_submit_core_authority_locked` on the statement before
  `broker.place_demo_core_order(...)`, **outside** the `try` that classifies broker failures,
  so a marker failure surfaces as itself rather than as
  `BrokerOrderSubmissionUncertain`.

### 5. The discriminator, and the liveness witness it needs

**The marker alone is not sufficient.**

```
T0  submitter commits the authority          (phase = authority_committed)
T1  _submit_core_authority acquires reconciliation_order_lock   [BLOCKING, no lock_timeout]
T2  mark_core_submission_entered commits     (phase = broker_verb_entered)
T3  provider call
```

A reconciler arriving in `(T0, T1)` finds the per-order lock free, reads
`'authority_committed'`, and on the marker alone would terminalise a **live** submission —
which then places a real order against a row already `rejected` with its capital released.
That is the doubled-position outcome this ticket exists to prevent.

**`CORE_SUBMISSION_ADVISORY_LOCK` is the second condition.** Session-scoped, held across
`T0..T3` by `execute_core_rebalance` and by `resume_core_submission`, released by Postgres
when the backend dies.

⚠⚠ **What a successful try-lock proves, exactly: that no OTHER session holds the key.**
Advisory locks are reentrant, so a caller that already owns it — `resume_core_submission`,
which holds `core_submission_lock` and then reconciles — will succeed on its own nested try.
That is correct here rather than a hole, because the attended resume is forbidden to
resubmit and is therefore not a submitter in flight; but it is a *reasoned* exemption, and
it is asserted by test rather than left to be discovered. It is **not** a claim about process
liveness in general: it also requires the submitter to use the same connection for the marker
and to refuse the POST after a database failure, which §4's contract enforces.

Order of operations — candidacy is read **before** any lock is taken:

1. **Cheap read: is this row a candidate at all?** `submission_phase = 'authority_committed'`.
   Not a candidate → return `None` and let ordinary reconciliation proceed untouched.
   ⚠ This ordering is load-bearing: taking the global core key first would let one unrelated
   core submission cause **alpha and legacy** orders to skip reconciliation entirely.
2. `pg_try_advisory_lock(CORE_SUBMISSION_ADVISORY_LOCK)` — fail → a core entry is in flight;
   raise `StrategyReconciliationBusy` (the batch's existing skip path), change nothing.
3. `try_reconciliation_order_lock(order_id)` — busy → `StrategyReconciliationBusy`.
   ⚠ The per-order lock is **always last** (`strategy_order_reconciliation.py:13`); the order
   here keeps that true. Both are try-locks, so neither can deadlock against the submitter's
   blocking acquire.
4. **Re-read and validate under both locks** (§6), then write, then release both.

Both locks are released on every path, including the busy and error paths.

### 6. The predicate — writer assumptions are not schema guarantees

All required, re-read under both locks:

| condition | why |
| --- | --- |
| `orders.execution_origin = 'strategy'` | not a manual order |
| `strategy_trade_orders.purpose = 'entry'` | not an exit/stop/ratchet leg |
| `strategy_trades.core_rebalance_intent_id IS NOT NULL` | core arm, not alpha |
| `submission_phase = 'authority_committed'` | the write-ordering fact |
| `state NOT IN ('resolved','rejected')` | not already terminal |
| `orders.broker_order_ref IS NULL` | ⚠ redundant-looking but **not** redundant: `_apply_detail` (`:711-718`) also writes it, so `_persist_core_acceptance` is not the only writer. The first draft claimed it was; that was false. |
| no `strategy_order_position_executions` rows for the order | nothing was executed |
| no active `strategy_position_ownership` for the trade | nothing is owned |
| the trade has exactly **one** linked order | the executor's single-entry shape; refuses anything else rather than reasoning about siblings |

### 7. The terminal write — one atomic transaction

Reuses the vocabulary `_submit_core_authority_locked` already writes on a **definite**
rejection (`strategy_core_executor.py:445-458`), in the module's stated lock ordering
(`orders` → reconciliation state → `strategy_trades`), inside **one** `with conn.transaction():`
while both locks are still held:

- `orders.status = 'rejected'`
- reconciliation: `state='rejected'`, `reconciled_at=now()`, `last_attempt_at=now()`,
  `attempt_count=attempt_count+1`, `last_error_code='core_authority_never_submitted'`,
  `updated_at=now()`
- `strategy_trades.status = 'failed'`, `updated_at=now()`

⚠ Atomicity is required, not tidy: a partially committed terminal reconciliation row drops
out of the backlog index while its trade still holds capital, which is a worse state than the
one being repaired.

No new terminal vocabulary is invented; `last_error_code` is the only distinguishing field,
which is what keeps existing consumers correct without change.

### 8. Call site

Inside `reconcile_strategy_order` (`strategy_order_reconciliation.py:736`), before the
existing `try_reconciliation_order_lock`, so it covers both callers:

- `reconcile_backlog` → `scheduler.strategy_paper_cycle` — the unattended path;
- `_reconcile_core_authority` → `resume_core_submission` — the attended resume.

**Zero broker calls.**

### 9. Read surface

`core_authority_is_stranded` (`strategy_core_executor.py:239-277`) still documents the
`referenceId`-coverage question as open and implies no terminalisation surface exists. Its
docstring is corrected, and the lifecycle read at `app/api/strategies.py:3117` is checked so
a terminalised row is not labelled *incomplete/error*. Minimal: this slice does not redesign
the operator page.

## What this does and does not buy

**Buys.** A crash in window A terminalises on a subsequent reconciliation pass: the authority
reaches a terminal state, `core_pending_committed` releases (`strategy_engine_capital.py:334`
excludes failed, terminal, unowned entries), `core_trade_in_flight` stops refusing, and the
row leaves `enforce_reconciliation_slo`'s overdue count so the global
`strategy_execution_blocks(source='order_reconciliation')` row can clear — with broker
mutations at zero.

⚠ *"a subsequent pass"*, not *"the next"*: backlog cooldown applies to `error`/`not_found`
candidates, batch limits and busy rows delay selection, and repeated global-lock contention
can defer a candidate. No bound is claimed.

⚠ Clearing the block is a **separate** action — `enforce_reconciliation_slo` must recompute
before a fresh execution is admitted, and any other overdue order correctly keeps the block
active. The acceptance asserts both.

**Does not buy.** Window B. A crash between the marker commit and the persisted response
still wedges core and alpha exactly as today.

⚠ *"No order is ever resubmitted"* is **not** claimed unconditionally: `ResilientClient`
retries POSTs on 429/5xx at transport level, below this design. A pre-existing hazard sits
there — acceptance, then 5xx, then a retry returning 4xx reaches the provider's
"definite rejection" branch and releases capital despite `broker_verb_entered`. Not
introduced here, not fixed here; recorded on the PR.

**The second-order gain is the one worth naming.** The 09-13 verdict's open question — *"is
the operator willing to authorise a manual override that releases a stranded core authority's
reserved capital, accepting a residual double-position risk?"* — is genuinely person-gated.
After this change it is only ever asked about window-B rows. Rows carrying **no** residual
risk are resolved by the machine and never reach the operator. That is a prerequisite for the
override being safe, not a substitute for it.

## Acceptance

1. **New fault `after_authority_commit_before_marker`** (`tests/fixtures/core_restart.py`
   `FaultPoint`, armed in `core_restart_child.py` by monkeypatching
   `strategy_core_executor.mark_core_submission_entered` to die before it — the exact shape
   `after_close_intent_before_marker` already uses). After the kill:
   `submission_phase='authority_committed'`, broker `mutation_calls == 0`;
   `reconcile_backlog` terminalises with zero broker calls; `enforce_reconciliation_slo`
   recomputed clears the block; a fresh `execute_core_rebalance` is **admitted** rather than
   refused `core_trade_in_flight`; capital released.
2. **Negative control — `test_scenario_2_…` keeps its behaviour.** Its fault lands after the
   marker, so `submission_phase='broker_verb_entered'` and `reconcile_backlog` must **not**
   terminalise it. Its docstring changes to record why; its assertions do not.
3. **Legacy/NULL control.** A row with `submission_phase IS NULL` in the same unresolved
   state is never terminalised. Alpha-arm and manual-origin controls likewise.
4. **Live-submitter control.** With `CORE_SUBMISSION_ADVISORY_LOCK` held on a second
   connection, the path declines and changes no row.
5. **Nested-resume control.** `resume_core_submission` holds the key and its nested try
   succeeds; assert the outer holder still owns it afterwards and `core_submission_lock`'s
   unlock-ownership assertion does not fire.
6. **Per-order contention.** Order lock held elsewhere → `StrategyReconciliationBusy`, no
   write.
7. **Terminal durability, tested directly.** A later `_record_failure` against the
   terminalised order preserves `state`, `reconciled_at` and
   `last_error_code='core_authority_never_submitted'`. ⚠ A second `reconcile_backlog` pass
   does **not** test this — terminal rows are excluded from selection — so the guard is
   exercised at the helper.
8. **Unrelated overdue order keeps the block** after the candidate is terminalised.
9. **Marker contract.** Non-idle connection refused; zero-row UPDATE raises and the POST does
   not happen.
10. Existing scenarios 1, 3, 4, 6, 8 and the whole close-recovery module unchanged.

⚠ Scenario 3's existing recovery goes through the fake's *successful* reference lookup — the
capability the 09-17 probe showed production does not have. That control stays (it tests a
different thing), and the inconsistency is recorded rather than silently inherited.

## Out of scope, named so it is not quietly absorbed

- The operator override for window B (#2961's person-gated half) — unchanged, still open.
- #2965's partial-fill ownership — still blocked on an attended partial fill.
- #2942 half 2 (legacy recommendation writer) — same marker shape applies, not done here.
- The `ResilientClient` 5xx-retry→4xx capital-release hazard — recorded, not fixed.
- Migrating `place_order` (v1, manual) to v2 — the 09-17 probe was explicit this *"would not
  deliver what these tickets want"*.
