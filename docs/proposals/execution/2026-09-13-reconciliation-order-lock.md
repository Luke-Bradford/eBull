# #2964 items 2-4 — one per-order lock and one stated lock ordering

Status: proposal (2026-09-13). Items 2, 3 and 4 of #2964. Item 1 shipped in `7b833d69` (PR #2974).

## Goal

Make the reconciliation write path safe for more than one reconciler, so #2962 (unattended core
reconciliation) becomes a scheduling change rather than a concurrency change.

## Source rule

No published rule fixes broker-order reconciliation concurrency — searched, none exists, none
borrowed. The design is therefore fixed **by construction** from the repo's own settled shapes:

- `app/services/strategy_position_manager.py::_position_lock` — the established shape for a
  per-BIGINT-row **session** advisory lock: `pg_advisory_lock(hashtextextended(identity, seed))`,
  with an unlock-ownership assertion in `finally`.
- `docs/review-prevention-log.md` "Two writers sharing an advisory lock must use a BYTE-IDENTICAL
  lock-key SQL" — *"Never cast a BIGINT id to int4 in a lock key."* `orders.order_id` is BIGINT, so
  the two-int4 form used by `core_submission_lock` is not available.
- `docs/review-prevention-log.md` "Advisory lock scope vs concurrent writers" — *"every code path
  that mutates the same invariant must also take the same lock. An advisory lock is cooperative."*
- `app/jobs/locks.py` — why **session**-scoped: the lock must survive `conn.commit()`, and Postgres
  releases it automatically if the connection dies, so a crashed worker cannot wedge an order.
- `docs/review-prevention-log.md` "Architectural-invariant claims about Postgres advisory locks must
  be empirically tested" — every semantic claim below is pinned by a DB test, not by reasoning.

## What the issue proposed, and why half of it does not hold

> the analogous shape here is `FOR UPDATE SKIP LOCKED` on selection, or a claim column.

**`FOR UPDATE SKIP LOCKED` cannot work here.** `reconcile_strategy_order` refuses a non-idle
connection (`strategy_order_reconciliation.py:545-548`) because broker I/O must not run inside a DB
transaction, so `reconcile_backlog` MUST `conn.commit()` after `fetchall()`. A row lock is released
by that commit, before a single broker call. It would protect nothing across the window that
matters. Any claim here has to survive commits — which is exactly what a session advisory lock is
and a row lock is not.

A claim **column** would work but needs a lease duration (an invented constant), a stale-claim
reaper, and a migration — all to re-implement what Postgres already gives free, including the
crash-release the reaper would exist to emulate.

## Design

### Item 2 — the claim: one per-order session advisory lock

```python
RECONCILIATION_ORDER_LOCK_SEED = 2964

@contextmanager
def reconciliation_order_lock(conn, order_id: int) -> Iterator[None]:       # blocking — submitters
@contextmanager
def try_reconciliation_order_lock(conn, order_id: int) -> Iterator[None]:   # raises Busy — reconcilers
```

- **Two entry points, ONE lock-key SQL.** Both delegate to a single private key-builder, so the
  prevention log's byte-identical-key requirement holds structurally rather than by a grep tripwire.
  A test asserts no other module spells `hashtextextended` against this identity prefix.
- **Reconcilers try, submitters block.** ⚠ This asymmetry is the whole design, not a convenience.
  A reconciler that loses can skip — the row is re-selected next cycle. A **submitter that loses
  cannot skip**: its `orders` + reconciliation row are already committed (the crash-safe
  durable-before-I/O design), so refusing would abandon a durable authority that `resume_*` is
  deliberately forbidden to resubmit, wedging the arm permanently. Codex ckpt-1 finding 16.
  The submitter's wait is bounded by the only thing that can hold the lock against it: one
  reconciler's broker lookup, i.e. the provider HTTP timeout.
  ⚠ An earlier draft justified try-only with *"an advisory lock is not subject to `lock_timeout`"*.
  That is **false** — advisory lock waits go through the normal lock-wait machinery and `lock_timeout`
  does apply (PG `src/backend/storage/lmgr/proc.c`). Do not repeat it.
- **The helper owns its transaction.** With `autocommit=False` the acquire `SELECT` itself opens a
  transaction. The helper therefore requires an IDLE connection on entry and `conn.commit()`s
  immediately after the acquire — **on the contention path too**, or the caller's next statement runs
  inside a stray transaction and `reconcile_strategy_order`'s idle check fails. Codex ckpt-1 2-4.
- **Not re-entrant, and loudly so.** A second `pg_try_advisory_lock` in the same session succeeds and
  bumps the hold count, so the inner release would not actually release. The helper checks
  `pg_locks` for this backend holding the key (`objsubid = 1` for a one-int8 key — the `core_lock_held`
  rule inverted) and raises on a nested acquire. Codex ckpt-1 6.
- **Collision direction is safe.** A `hashtextextended` collision makes two *unrelated* orders
  serialise: a reconciler skips one row for one cycle, a submitter waits one lookup. Never two
  reconcilers on one order.
- **`reconcile_strategy_order` takes the lock FIRST**, before its identity read. That ordering also
  closes the duplicate-poll case (Codex ckpt-1 19): two workers that selected the same due row now
  read the state *under* the lock, so the loser sees the terminal state the winner just wrote and
  short-circuits instead of re-polling the broker.
- **`reconcile_backlog` catches `StrategyReconciliationBusy` per row and skips it**: no
  `_record_failure` (that would invent an error that did not happen) and no `last_attempt_at`
  advance, so the skipped row is re-selected first next cycle rather than starved. This catch must
  precede the existing `except Exception` or the generic handler swallows it. The skipped count is
  logged — #2948's absorbing-state lesson applies here too: with `b` busy rows the declared bound
  degrades to `ceil(due_rows / (limit - b))`, and a silently-empty batch must not look like an empty
  backlog.
- **The backlog's unexpected-error fallback re-acquires.** `_record_failure` in
  `reconcile_backlog`'s `except Exception` runs *after* `reconcile_strategy_order` has unwound and
  released, so it would otherwise write unlocked (Codex ckpt-1 7). It re-takes the try-lock; a busy
  re-acquire skips the write rather than racing it.

### Item 3 — one stated lock ordering

Stated once in the `strategy_order_reconciliation` module docstring, and obeyed by every writer:

```
advisory  1. PAPER_ALLOCATOR -> CORE_MANDATE -> CORE_SUBMISSION  (existing, unchanged)
advisory  2. _position_lock(broker_position_id)                  (existing)
advisory  3. reconciliation_order_lock(order_id)                 (new; ALWAYS acquired last)
rows      4. orders
          5. strategy_order_position_executions   (ascending broker_position_id)
          6. strategy_position_ownership          (ascending broker_position_id)
          7. strategy_order_reconciliation_state
          8. strategy_trades
```

⚠ The row order is `_apply_detail`'s **actual** order, read off the code, not a preference. A first
draft of this spec put the reconciliation row before the executions loop; `_apply_detail` writes the
executions and ownership rows first (`:484-491`) and the state row after (`:493`). Writing the
ordering down wrong is worse than not writing it down, so it is transcribed, then every other writer
is moved to match. Codex ckpt-1 11.

`strategy_trades` is last because it is the only row two *different* orders of the same trade both
touch; with it last, a wait is a queue, not a cycle.

**Ascending `broker_position_id` is load-bearing, not tidiness.** Two orders whose broker details
name the same two positions in opposite order (`[P,Q]` and `[Q,P]`) each insert one
`strategy_position_ownership` row and then block on the other's unique-index conflict — a deadlock
across two *different* per-order locks, which the per-order lock cannot prevent. `_apply_detail`
therefore iterates `sorted(detail.position_executions, key=position_id)`. Codex ckpt-1 13.

**The FK cycle does not exist, and the reason is the stated order.** Inserting a
`strategy_order_reconciliation_state` row takes a `KEY SHARE` FK lock on its `orders` row, which
conflicts with `FOR UPDATE`. A writer that inserted state *without* first locking `orders` could
deadlock against `_apply_detail`. `ensure_strategy_request_id` — the only such initialiser — already
takes `FOR UPDATE OF o` before its insert (`:141-173`), so it conforms. Codex ckpt-1 12.

Writers that currently violate the row order, all of which write `strategy_trades` before
`strategy_order_reconciliation_state`:

| site | current | becomes |
| --- | --- | --- |
| `strategy_core_executor.py` `_submit_core_authority` uncertain branch | trade → state | state → trade |
| `strategy_core_executor.py` `_submit_core_authority` reject branch | orders → trade → state | orders → state → trade |
| `strategy_core_executor.py` `_submit_core_authority` bare-`except` branch | trade → state | state → trade |
| `strategy_core_executor.py` `_persist_core_acceptance` | orders → trade → state | orders → state → trade |
| `strategy_paper_executor.py` `_resume_uncertain_submission` reject branch | orders → trade → state | orders → state → trade |
| `strategy_paper_executor.py` `_execute_fired_paper_signal_locked` reject branch | orders → trade → state | orders → state → trade |

`_apply_detail`, `_record_failure` and `ensure_strategy_request_id` already conform.

### Item 4 — the submission/reconciliation race

The named symptom (`_persist_core_acceptance` setting `state='pending'` on a row a concurrent
reconciler just resolved, which `sql/285`'s `strategy_order_reconciliation_resolved_shape` CHECK
refuses, failing the attended request) is **structurally closed by item 2**, not by a second guard:
the submitter holds the per-order lock from before broker I/O until after persistence, so no
reconciler can interleave.

That is the right fix rather than a terminal `CASE` on those UPDATEs, because two of them are
terminal→terminal (`state='rejected'` over a concurrently-`resolved` row) which item 1's guard would
*not* catch — preserving the existing terminal state is not the correct outcome there either. The
only sound answer is that the interleave cannot happen.

⚠ These are cooperative locks, so the submitters must take them or the reconciler's lock protects
nothing. The three submission paths that write reconciliation state after broker I/O all acquire it:

- `strategy_core_executor.py::_submit_core_authority` + `_persist_core_acceptance` (one hold,
  wrapped at the call site so the lock spans submit *and* persist);
- `strategy_paper_executor.py::_resume_uncertain_submission`;
- `strategy_paper_executor.py::_execute_fired_paper_signal_locked` (acquired after the durable
  insert commits — `order_id` does not exist before it — and held across the broker call).

⚠ The paper *accept* branch writes no reconciliation row at all (only `orders` + `strategy_trades`),
so the backlog resolves it. That is unchanged; it is inside the lock hold anyway.

### Caller integration

`StrategyReconciliationBusy` escaping to `app/api/strategies.py`'s core-resume endpoint would be an
HTTP 500 (it catches `StrategyCoreExecutionError`). Contention is a retryable condition, not a
server fault, so the endpoint maps it to **409** with the order id. Codex ckpt-1 20.

## Acceptance

Per the issue: deterministic **concurrent-connection** tests, and each asserts on the broken
configuration too, or it cannot fail.

⚠ **No start-`Barrier` races.** A barrier guarantees a common start, not an overlap, and a barrier
placed inside a broker stub hangs when only one caller reaches it (Codex ckpt-1 22). Every case below
instead has connection A **hold the lock outright** and then runs the subject on connection B, so the
interleave under test is the one asserted, every run.

1. Connection A holds the per-order lock; connection B's `reconcile_strategy_order` for the same
   order raises `StrategyReconciliationBusy` and performs no broker call. Control arm: with A idle,
   B proceeds — so the test can fail.
2. Connection A holds the lock; `reconcile_backlog` on B skips the row — `last_attempt_at`,
   `attempt_count` and `state` all unchanged (no invented `_record_failure`), and the other due rows
   in the same batch are still reconciled.
3. The submitter blocks rather than refusing: connection A holds the lock, B's submit path waits,
   A releases, B completes. Asserts the strand in Codex ckpt-1 16 cannot happen.
4. Item 4 end-to-end without the lock (control arm): a reconciler resolving the order inside the
   submitter's window makes `_persist_core_acceptance`'s `state='pending'` raise `CheckViolation` on
   `strategy_order_reconciliation_resolved_shape`. With the lock the same schedule serialises and
   the CHECK holds.
5. Empirical PG-semantics pins (prevention log #1619 — none of these are asserted from documentation):
   the lock survives `conn.commit()`; it is released when the holding connection closes;
   `hashtextextended` of the same identity is equal across sessions; a `bigint` `order_id` above
   2³¹ keys correctly.
6. Transaction hygiene: the connection is IDLE after acquire, after a contention refusal, and after
   an exception in the body; a nested acquire on one connection raises rather than silently
   double-counting; a lost lock at release raises.

## Not in scope — named, not fixed

- The three adjacent defects from #2964's item-1 close-out (`_apply_detail`'s `FOR UPDATE OF o` not
  locking the joined state row; `resolved → resolved` remaining mutable; `last_attempt_at` moving
  backwards). The first is *materially reduced* — every `_apply_detail` caller now holds the
  per-order lock — but the row-lock gap itself is unchanged and is not claimed fixed.
- **Exit/close orders are not reconciled at all.** `strategy_position_manager::_submit_close` writes
  `orders.broker_order_ref` under the allocator + position locks and never creates a
  `strategy_order_reconciliation_state` row, so `reconcile_backlog` (which selects *from* that table)
  cannot see them. No new race is introduced; the gap is pre-existing. Codex ckpt-1 8, 9.
- **Trade-lifecycle overwrite between the position manager and entry reconciliation** (reconciliation
  writing `open`/`submitted` over a concurrent `closing`/`closed`). Different locks, pre-existing,
  and not a concurrency property the per-order lock claims. Codex ckpt-1 21.
- `strategy_control_plane`'s exported `record_order_position_execution` / `claim_exact_position` /
  `release_exact_position` and `order_client._update_order_with_broker_result` have no strategy-origin
  production callers; they are unguarded writers on paper only. Codex ckpt-1 10, 14.
