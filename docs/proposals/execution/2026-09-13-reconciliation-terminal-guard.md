# `_record_failure` must not demote a terminal reconciliation row (#2964 item 1)

Status: proposal — **post-Codex-ckpt-1 revision. The first draft's own fix still destroyed terminal
evidence and leaked an error into the operator API; both are corrected below.** Refs #2964 (item 1
only), #2962 / #2949 (which queue behind it), #2948 (whose spec first recorded the hazard).

## Scope — item 1 of four, and why it ships alone

#2964 lists four items: (1) a terminal guard on `_record_failure`, (2) a claim or row lock at
selection, (3) one lock ordering across three writers, (4) the `_persist_core_acceptance` race.

This is **item 1 only** — the one that stands alone, because it makes the *write* refuse to destroy
a terminal outcome regardless of who calls it. Items 2-4 admit a second reconciler and need item 1
underneath them either way.

⚠ **Item 1 alone does NOT unblock #2962**, and neither does item 2 alone. #2964's goal — *"so #2962
becomes a scheduling change rather than a concurrency change"* — needs items 2 AND 3, since the lock
ordering is what makes two claimants safe rather than merely disjoint. The first draft implied item
2 sufficed; it does not. Stated here so the issue is not read as unblocked when this merges.

## The premise, re-falsified — one correction and one over-claim

#2964's body says `_apply_detail`'s terminal guard *"does not reach the caller as a raise:
`reconcile_strategy_order` catches `StrategyReconciliationError` and converts it into
`_record_failure(... "ambiguous" ...)`"*.

The catch is real (`strategy_order_reconciliation.py:513-521`; note `except
StrategyReconciliationError, StrategyOwnershipError:` is parenthesis-free and valid under PEP 758 on
py3.14). But the **public path** short-circuits first:

```python
prior_state = identity["reconciliation_state"]
...
if prior_state in _TERMINAL_RECONCILIATION_STATES:
    return ReconciliationResult(...)          # :474-481, before ANY broker call
```

For the guard at `:373` to fire, `_apply_detail`'s re-read must see a terminal state the top-level
read did not — and the two reads sit in **different transactions**, because `reconcile_strategy_order`
commits immediately after the identity read (`:468`). So reaching the overwrite *through
`reconcile_strategy_order`* requires a concurrent resolver.

⚠ **The first draft then over-claimed in the other direction** — *"a single-threaded test cannot
reproduce this"* — and Codex refuted it. `_record_failure` is an ordinary function: seed a terminal
row, call it directly, and the demotion happens with one thread. **The helper's contract is testable
single-threaded; only the end-to-end schedule needs two connections.** That distinction decides the
test design, and getting it backwards would have meant a thick concurrency harness where a table
test does the work.

### Existing corruption: none, measured

```
select state, count(*) from strategy_order_reconciliation_state group by state;   -- 0 rows
select count(*) from strategy_order_reconciliation_state;                         -- 0
```

The table is **empty on dev**, so there is nothing to repair and no migration is needed. This lands
before any data exists, which is the cheapest possible moment and is why no detection/repair path is
specified.

### What the schema does and does not catch

`sql/285_strategy_order_reconciliation.sql:75-78` requires `reconciled_at IS NOT NULL` exactly when
`state` is terminal. `_record_failure` demotes `state` and clears `reconciled_at` **together**, so
the corruption is perfectly CHECK-consistent at every step — the constraint describes a valid *row*
and says nothing about a valid *transition*.

⚠ **`ON CONFLICT DO UPDATE` does take a row lock on the conflicting row** — the first draft said "no
row lock", which is wrong literally. There is no *pre*-lock, and the distinction matters
constructively: the lock the upsert itself takes is held to end-of-transaction, so a decision made
from its `RETURNING` value is safe against another state writer **provided the dependent write stays
in the same transaction**. Every call site already wraps `_record_failure` in `with
conn.transaction()`, so that holds.

## The fix

An arriving failure for a terminal order is an **observation, not a state transition**. The first
draft implemented that by preserving `state` and `reconciled_at` while still assigning
`broker_status` and `last_error_code` from the incoming failure. **Codex showed that is still
destructive, twice over:**

- **It erases terminal evidence.** All five call sites pass `broker_status=None` (default), so
  `broker_status = EXCLUDED.broker_status` would replace a terminal `Filled` / `Rejected` with
  `NULL`, leaving the row's provenance a mixture of the resolve and the late failure.
- **It leaks into the operator API.** A retained `last_error_code` makes `app/api/strategies.py:3063`
  add a `*_reconciliation_error` entry to the trade's lifecycle `incomplete_reasons` — **for a
  `resolved` trade**. And nothing ever clears it, because the terminal early-return path never
  writes: the row would carry an error indefinitely while every subsequent
  `ReconciliationResult` reported none.

So on the terminal branch the write is narrowed to the **attempt counters only**:

| field | terminal branch | non-terminal branch |
| --- | --- | --- |
| `state`, `reconciled_at` | preserved | today's behaviour |
| `broker_status`, `last_error_code` | **preserved** | today's behaviour |
| `last_payload_sha256`, `position_count`, `first_unresolved_at` | untouched (already) | untouched |
| `last_attempt_at`, `attempt_count`, `updated_at` | updated | updated |

```sql
INSERT INTO strategy_order_reconciliation_state (
    order_id, state, last_attempt_at, attempt_count, broker_status, last_error_code, updated_at
) VALUES (
    %(order_id)s, %(state)s, now(), 1, %(broker_status)s, %(error_code)s, now()
)
ON CONFLICT (order_id) DO UPDATE SET
    state           = CASE WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                           THEN strategy_order_reconciliation_state.state
                           ELSE EXCLUDED.state END,
    reconciled_at   = CASE WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                           THEN strategy_order_reconciliation_state.reconciled_at
                           ELSE NULL END,
    broker_status   = CASE WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                           THEN strategy_order_reconciliation_state.broker_status
                           ELSE EXCLUDED.broker_status END,
    last_error_code = CASE WHEN strategy_order_reconciliation_state.state = ANY(%(terminal)s)
                           THEN strategy_order_reconciliation_state.last_error_code
                           ELSE EXCLUDED.last_error_code END,
    last_attempt_at = now(),
    attempt_count   = strategy_order_reconciliation_state.attempt_count + 1,
    updated_at      = now()
RETURNING state
```

Paired `CASE` rather than a `DO UPDATE ... WHERE`: a `WHERE` that skips would leave the late failure
with **no trace at all**, replacing a corruption with a silence, and would return no row.

⚠ Codex confirmed the two semantics this rests on: the `CASE` expressions read the *existing* row so
both CHECK branches stay satisfied and the row stays excluded from both partial indexes
(`285:81`, `376:18`); and `RETURNING` on the `ON CONFLICT` path returns the **resulting** row,
including preserved fields.

### Three mechanical corrections the first draft would have failed on

⚠ **`%(terminal)s` must be a `list`, not the `frozenset`.** `_TERMINAL_RECONCILIATION_STATES` is a
`frozenset` and psycopg rejects it as a parameter. Pass `sorted(_TERMINAL_RECONCILIATION_STATES)`.

⚠ **The whole statement converts to named placeholders.** The existing `VALUES` clause uses
positional `%s`; mixing the two raises *"positional and named placeholders cannot be mixed"*.

⚠ **"Three readers of the terminal set" was wrong.** SQL does not read the Python constant at all —
the CHECK, both partial indexes, `_apply_detail`'s SQL, and the backlog/SLO predicates each
hard-code terminal membership independently. Passing the constant as a parameter keeps *this*
statement in step with the Python definition and nothing more; adding a terminal state would still
require touching the SQL sites by hand. Said plainly rather than implying a single source that does
not exist.

### The dependent write and the return value

**The trade-status write is gated on the returned state.** ⚠ It is *not* "unconditional" as the
first draft said — `:209-218` already excludes `closed` and `failed` trades, and that predicate is
preserved rather than replaced. The new gate is additional: a terminal reconciliation row must not
flag its trade `reconcile_required`.

**`ReconciliationResult.state` becomes the stored state**, not the attempted one. Today the function
returns what it *tried* to write; after this change those differ exactly in the guarded case, and a
caller believing the demotion happened is the same defect one layer up. `ReconciliationResult.state`
is typed `ReconciliationState`, wider than the `Literal["not_found", "ambiguous", "error"]`
parameter, so the stored value fits.

**The guarded branch logs at WARNING** with the order id, the preserved state and the incoming error
code. Since the error detail is deliberately no longer stored, the log is the only place it exists —
and a late failure arriving for a settled order is worth an operator seeing.

## Tests

**DB tier, single-threaded on the helper** — Codex's correction: `_record_failure` is directly
callable, so the contract is a table test, not a harness.

- the matrix: **2 terminal states × 3 failure states** — `state`, `reconciled_at`, `broker_status`
  and `last_error_code` all preserved, `attempt_count` **exactly +1**, `last_attempt_at` moved;
- the **non-terminal control** for each of `unresolved` / `pending` / `not_found` / `ambiguous` /
  `error`: state moves, `reconciled_at` stays `NULL`, `broker_status` and `last_error_code` take the
  new values — without this every terminal assertion could hold because nothing writes at all;
- **absent row** → plain INSERT, unchanged;
- `first_unresolved_at`, `last_payload_sha256` and `position_count` are **immutable** on both
  branches;
- the trade gate **in both directions**: a terminal row leaves `strategy_trades.status` alone, a
  non-terminal one sets `reconcile_required` — and a `closed`/`failed` trade is still excluded, so
  the pre-existing predicate is not silently dropped (the "enumerate what a narrowing gate rejects"
  rule);
- the **returned** `ReconciliationResult.state` equals the stored state in the guarded case.

**DB tier, two connections** — one test, for the end-to-end schedule only:

B resolves the row and **commits before entering the barrier** (⚠ a barrier alone does not order a
commit — the first draft's fixture was underspecified and could deadlock, since A holding the
upsert's row lock would block B). A then runs the public `reconcile_strategy_order` against a broker
stub that raises, and the terminal row must survive. Bounded `Barrier(timeout=...)`, and the worker
thread's exception is re-raised in the test body rather than swallowed.

**Revert-probe, in the suite rather than only in the PR**: the same fixture against the unguarded
statement demotes the row, so the guarded assertions cannot pass for the wrong reason.

## Out of scope, stated so the issue is not read as closed

- #2964 items **2, 3, 4**.
- ⚠ **`_apply_detail` is NOT proven concurrency-safe and this change does not make it so.** Its
  `FOR UPDATE OF o` locks only `orders`, not the joined `strategy_order_reconciliation_state` row,
  so a statement waiting behind another resolver can carry an older joined snapshot past the guard.
  Fixing `_record_failure` cannot protect that path — it is item 2/3 work and needs its own DB
  regression.
- ⚠ **`resolved → resolved` remains mutable** in `_apply_detail`: the guard at `:373` compares
  `prior_state != state`, so a delayed duplicate detail can replace `reconciled_at`, the payload
  hash and the position count, and re-set an entry trade to `open` over a later lifecycle change.
  Calling that guard simply "correct" would conceal this.
- ⚠ **`last_attempt_at` can move backwards.** `now()` is transaction-START time, so a transaction
  that began before a concurrent one can write after it and rewind the timestamp that #2948's
  round-robin orders on. Pre-existing for every caller, a fairness effect rather than a correctness
  one, and changing it would perturb ordering #2948 has just shipped. Named, not fixed.
- `attempt_count` overflow at `2147483647` and `sql/286`'s text-length bounds abort the whole
  statement rather than being caught here; the five callers all supply valid strings.
