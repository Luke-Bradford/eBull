# Unattended reconciliation for the core/cash arm — #2962

**Status: BLOCKED at checkpoint 1. The first design was killed before implementation; nothing was
built.** Base: `f78acbb3`.

**Evidence in:** `docs/proposals/execution/2026-09-13-core-restart-acceptance.md` (gap G-2),
measured by `tests/test_2949_core_restart_recovery_db.py`.
**Prerequisite out:** #2964. **Incidental defect out:** #2965.

## The problem, which is real and unchanged

Core order reconciliation works and nothing schedules it.

- `resume_core_submission` has exactly one production caller, `app/api/strategies.py:3823`, inside
  `rebalance_core_sleeve`.
- `reconcile_backlog` — the only unattended reconciliation caller, reached from
  `app/workers/scheduler.py::strategy_paper_cycle` — excludes every core order:
  `AND trade.core_rebalance_intent_id IS NULL`,
  `app/services/strategy_order_reconciliation.py:532`.

Measured against a positive control: a non-core order in the same `unresolved` state is returned by
the identical call; the core order is not. ⚠ Not restart-specific — the fault-free baseline also
ends at `pending`, so every successful core submission needs the same follow-up.

## Why the first design was wrong

It proposed making the exclusion data-driven: admit a core order when its eligibility proof's
credential ids still match the live pair, on the theory that the exclusion stood in for the attended
route's account-provenance refusal (`app/api/strategies.py:3801-3811`).

**Both halves of that theory are false.**

### 1. Provenance is already closed, in the database

`sql/373_core_unresolved_credential_guard.sql` installs a `BEFORE UPDATE OF revoked_at OR DELETE`
trigger on `broker_credentials` that raises whenever the row is named by an eligibility proof whose
core order is non-terminal:

```sql
WHERE OLD.id IN (proof.api_key_credential_id, proof.user_key_credential_id)
  AND state.state NOT IN ('resolved','rejected')
```

So the credential pair an unresolved core authority names **cannot be revoked or replaced**. The
swap the attended route defends against cannot occur for exactly the orders this ticket wants to
reconcile. A provenance predicate would have been dead code, and the proposed acceptance test
"revoke the credentials, expect exclusion" is unconstructible.

### 2. The exclusion is about SERIALISATION, and that reason is recorded

#2948's spec, `docs/proposals/execution/2026-09-13-reconciliation-backlog-fairness.md:236-239`:

> **`_record_failure` can overwrite a terminal row under concurrency** (pre-existing; same
> single-lane argument). […] Production has exactly one scheduled caller (`strategy_paper_cycle`)
> on the `strategy_execution` job lane, which serialises it; **a second reconciler would be a new
> deployment decision, not a regression introduced here.**

The core arm already has a second reconciler — the attended route. It does not race the scheduled
job only because the job excludes core orders. **Today there is exactly one reconciler per order
class, and the exclusion is what makes that true.** Removing it makes core the first class with
two, which is precisely the "new deployment decision" that document names.

The write path is not ready for it:

- `_record_failure` (`strategy_order_reconciliation.py:134`) is an `ON CONFLICT DO UPDATE SET state
  = EXCLUDED.state, reconciled_at = NULL` with **no terminal guard and no row lock**.
- `_apply_detail`'s terminal-regression guard (`:333`) does not reach the caller as a raise:
  `reconcile_strategy_order` catches it and converts it into `_record_failure(... "ambiguous" ...)`
  (`:470-478`) — performing the overwrite the guard exists to prevent.
- Selection commits immediately after `fetchall()`, so nothing claims a row and two callers can
  select the same one.
- Three paths take the same three rows in different orders (`_apply_detail`: order → state → trade;
  the submission-uncertainty writer: trade → state; `_record_failure`: state → trade).
- A job resolving an order between broker acceptance and `_persist_core_acceptance`
  (`strategy_core_executor.py:186`) leaves that writer setting `state='pending'` without clearing
  `reconciled_at`, which `sql/285_strategy_order_reconciliation.sql:75`'s CHECK refuses — failing
  the attended request.

The original spec's "no new lock, stated rather than assumed" section argued from the idempotency of
`_record_execution` and `_claim_entry_execution`. Those two writers *are* idempotent; the argument
was wrong because it never looked at `_record_failure`, which is the one that is not.

### 3. Two smaller claims in the first draft were also wrong

- *"the SLO still sees it, so the condition surfaces"* — false in the core-only configuration, and
  contradicted by this session's own #2949 report: `refresh_strategy_health` clears the
  `order_reconciliation` block and never calls `enforce_reconciliation_slo` when no enabled paper
  deployment supplies a policy (`strategy_paper_runtime.py:235-250`).
- *"alpha is unaffected"* — too strong. Core rows would consume the same bounded `limit` slots and
  delay alpha reconciliation.

## What #2962 should be, after this

A **scheduling** change, once concurrency is somebody else's solved problem:

1. **#2964 first** — make the write path safe for a second reconciler: terminal guard on
   `_record_failure`, a claim at selection, one lock order, and the
   submission-vs-reconciliation CHECK race. Acceptance is deterministic concurrent-connection tests.
2. **Then #2962 becomes small:** drop the exclusion, pass the paper cycle's credentials through, and
   re-run #2949's harness with `reconcile_backlog` in place of `resume_core_submission`. No
   provenance predicate — the DB trigger already holds that invariant, and a second check that
   cannot fire is worse than none because it reads as protection.

Still not in scope at that point: unattended core **entry**. Recovery is a read plus a state write;
re-entry stays behind #2843's flag inside #2844's boundary.

⚠ One consequence to carry forward: resolving core orders in the paper cycle can clear the global
`order_reconciliation` block, and the same cycle then evaluates alpha entries. That is an indirect
exposure change and belongs in #2962's PR description when it is written, not discovered afterwards.

## Incidental finding — #2965

A partially-filled core entry claims exact ownership while its reconciliation row is still
`pending` (`_apply_detail:341-348` claims unconditionally on state), and
`load_engine_capital_authority:278-280` **raises** on that combination — taking down
`execute_core_rebalance`, the mandate writer and `core_rebalance_observation` until the order
resolves. Independent of this ticket; found in the same pass. #2949's scenario 4 does not catch it
because its fake broker returns no executions while pending.

## Process note

This spec existed for one Codex checkpoint-1 pass and was killed by it. That is the checkpoint
working as intended — the cost was one document, against an implementation that would have added a
second reconciler to a write path documented as unsafe for one.
