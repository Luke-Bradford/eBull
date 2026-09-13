# #2948 — bounded fairness and retry backoff for the strategy reconciliation backlog

Status: spec for implementation (2026-09-13).

## Defect (re-verified, not inherited)

`reconcile_backlog` (`app/services/strategy_order_reconciliation.py:486`) selects
`ORDER BY state.first_unresolved_at, state.order_id LIMIT %s`. Both sort keys are
immutable for a non-terminal row:

- `first_unresolved_at` is `DEFAULT now()` at insert (`sql/285_strategy_order_reconciliation.sql`)
  and is written by **no** statement in the module — grepped: the only column writes are
  `state`, `last_attempt_at`, `reconciled_at`, `attempt_count`, `broker_status`,
  `position_count`, `last_error_code`, `last_payload_sha256`, `updated_at`
  (`:125-135` `_record_failure`, `:333-350` `_apply_detail`).
- `order_id` is the primary key.

So the selected set is a pure function of the non-terminal row set. While the oldest
`limit` orders stay non-terminal (`not_found` / `ambiguous` / `error` / `pending`), order
`limit + 1` is never visited — for any number of cycles. That is an absorbing state, not a
delay.

Live census on the dev DB at spec time (read-only): `select state, count(*) from
strategy_order_reconciliation_state group by 1` returns **0 rows**, and
`strategy_execution_policies` / `strategy_deployments` are both empty. The defect is
**latent in a wired path** — reproduced synthetically by the operator's audit, and
confirmed here by source inspection rather than by production damage.

⚠ The same class was already fixed once in the sibling batch in the same lifecycle:
`app/services/strategy_paper_runtime.py:434-437` rotates the owned-position batch by
five-minute slot, with the comment *"A fixed `ORDER BY ... LIMIT` would protect the same
oldest positions forever and starve later positions once the sleeve grows past the cap."*
The reconciliation batch never received it.

## Source rule

No published rule fixes a broker order-reconciliation retry cadence — searched, none
exists, and none is borrowed. Two documented constraints DO bind and the policy is
derived from them rather than invented:

1. **Venue budget.** `.claude/skills/data-sources/etoro-api.md` § "Stable facts": ordinary
   trading reads are **60/min shared** (live portal, re-verified 2026-08-11); 429 returns
   `{"errorCode": "TooManyRequests"}` with **no guaranteed `Retry-After`**. `orders:lookup`
   is an ordinary trading read, so every wasted poll of a permanently-dead order consumes
   the same shared budget the live path needs. Absent a server-supplied retry hint, the
   client must supply its own — which is the standard documented discipline (capped
   exponential backoff; AWS Architecture Blog, *Exponential Backoff and Jitter*; Google
   SRE ch. 22 "Handling Overload").
2. **Our own cadence.** `strategy_paper_cycle` is registered
   `Cadence.every_n_minutes(interval=5)` at `app/workers/scheduler.py:2322`. The retry
   base is anchored to that cycle, so a first retry costs no added latency.

Everything else is fixed **by construction** and stated below, per CLAUDE.md
("Where a published formulation genuinely does NOT exist, say so explicitly and fix the
rule by construction").

## Decision — fairness and backoff, decided together

The ticket warns that "merely increasing LIMIT moves the same defect to a larger backlog".
It does. The two levers are decided together as follows:

**Fairness = round-robin on a key the work mutates.** `last_attempt_at` is already set to
`now()` on *every* attempt path (failure `:130`, success/pending `:343`). Ordering by it
ascending, NULLS first, makes the batch a strict FIFO over attempt time: an attempted row
moves to the back of the queue by construction, and a never-attempted row (NULL) is always
at the front. No new column, no reset of `first_unresolved_at`, so the existing SLO age is
untouched.

**Backoff = capped exponential, for venue budget only — never for fairness, and never on a
state that made progress.** Fairness comes from the ordering alone; the backoff exists
solely so a permanently dead order stops consuming a 60/min shared trading read every
rotation. Two constraints on its shape:

- It is an *exclusion* (`due` predicate), never a de-prioritisation, so it cannot
  re-introduce starvation: an excluded row becomes due at a bounded time and then sorts to
  the front on age.
- It applies **only to the no-progress states** `not_found` / `ambiguous` / `error`.
  `_apply_detail` increments the same lifetime `attempt_count` when a poll *succeeds* and
  leaves the order `pending` (`:345`), so keying the delay off `attempt_count` alone would
  exponentially deprioritise exactly the orders that are progressing — a partially filled
  order discovering its remaining executions would wait up to the cap. `unresolved` and
  `pending` are therefore always due.

### Selection (new)

```sql
WHERE state.state NOT IN ('resolved', 'rejected')
  AND o.execution_origin = 'strategy'
  AND trade.core_rebalance_intent_id IS NULL
  AND (
        state.state NOT IN ('not_found', 'ambiguous', 'error')
     OR state.last_attempt_at IS NULL
     OR state.last_attempt_at <= now() - make_interval(secs => least(
            %(retry_cap_seconds)s::double precision,
            %(retry_base_seconds)s::double precision
                * greatest(power(2, least(state.attempt_count - 1, 30)) - 0.5, 0)))
  )
ORDER BY state.last_attempt_at ASC NULLS FIRST, state.first_unresolved_at, state.order_id
LIMIT %(limit)s
```

`least(attempt_count - 1, 30)` bounds the exponent so `power` cannot overflow on a
pathological `attempt_count`; `greatest(..., 0)` keeps a pathological `attempt_count = 0`
row (non-NULL timestamp, zero attempts — unreachable through this module, since every
write that sets `last_attempt_at` also increments) immediately due rather than negatively
delayed.

**The `- 0.5` is the whole point of the construction, not decoration.** A delay that is an
exact multiple of the cycle period lands exactly on a grid boundary and is then missed by
any execution latency at all: the batch selected at 00:00 records its attempts at
00:00:02, so at the 00:05 selection only 299.8 s have elapsed and a 300 s delay excludes
the row — the "next cycle" retry silently becomes the one after. Every delay is therefore
**k cycles less a half cycle**, which tolerates up to 150 s of drift between a selection's
grid time and the attempt it records. Caught by Codex at checkpoint 1.

Constants, by construction, as defaults on an explicit input (the same shape as
`enforce_reconciliation_slo`'s `max_unresolved_seconds`, which the module already
documents as "an explicit deployment input, not a made-up constant"):

| input | default | derivation |
| --- | --- | --- |
| `retry_base_seconds` | `300` | one `strategy_paper_cycle` (`scheduler.py:2322`). Delays are `base * (2^(n-1) - 0.5)`, so the first retry is due half a cycle after the attempt and lands on the very next cycle. |
| `retry_cap_seconds` | `3450` | 12 cycles less the same half-cycle slack — the longest polling gap a dead order may impose. Reached at attempt 5. |

Resulting sequence at the defaults (seconds, verified against dev Postgres):
`150, 450, 1050, 2250, 3450, 3450, …` — i.e. `0.5, 1.5, 3.5, 7.5, 11.5, 11.5 …` cycles.

Validation: `retry_base_seconds >= 1`, `retry_cap_seconds >= retry_base_seconds`, raising
`ValueError` otherwise (the module's existing style). The inputs are **not** threaded
through `run_strategy_paper_cycle`: the one production caller has no reason to override
them, and an unused parameter chain is a worse contract than a documented default.

### Declared bound

Stated conditionally, because the unconditional form is false (Codex ckpt-1). Under a
**finite stable** eligible backlog of `N` rows, batch limit `L`, monotonic timestamps and
no aborted batch:

> once a row is **due**, it is selected within `ceil(N_due / L)` completed selection
> cycles.

Two consequences the earlier draft got wrong and which the tests assert:

- **The backoff delay and the queue wait are additive, not a max.** A row's total wait
  after an attempt is its delay *plus* up to `ceil(N_due/L)` cycles, because rows that
  were already due keep the front of the queue.
- **A never-attempted row is due immediately but not necessarily next cycle.** With more
  than `L` NULL rows, `L` of them go first and the rest wait their rotation.

Arrivals cannot displace attempted rows indefinitely in this deployment: one cycle admits
at most `signal_limit = 5` new entries against `reconciliation_limit = 20`
(`app/services/strategy_paper_runtime.py:418-419`), so at most a quarter of each batch can
be fresh NULL rows. The reproduced 20-dead + 1-recoverable case at `L = 20` reaches order
21 on **cycle 2**, versus never today.

⚠ The backoff cap is a **polling cadence floor, not a resolution SLO**, and it is not a
maximum observed gap either — scheduler drift, batch duration, a skipped cycle or an
outage all add to it. The safety control remains `enforce_reconciliation_slo`, which
blocks new strategy entries on `first_unresolved_at` age and is not affected by this
change. A cooldown can hold an already broker-resolved order past that threshold and
prolong an active block; that is the safe direction (the block refuses new entries) and is
accepted.

### Poison row — the second starvation vector

`reconcile_backlog` builds its result with `tuple(reconcile_strategy_order(...) for row in
rows)`. `reconcile_strategy_order` catches `BrokerOrderNotFound`, `BrokerOrderLookupError`,
`StrategyReconciliationError` and `StrategyOwnershipError` — anything else (a psycopg
error, an unmodelled broker exception) escapes, aborts the generator mid-batch and
propagates to the caller. Every row after the failing one is skipped, `last_attempt_at` is
**not** advanced for the failing row, so the next cycle selects the same row first and
fails the same way: a second absorbing state, reached without any backlog at all.

The batch therefore reconciles row by row with a per-row guard: on an unexpected
exception, `conn.rollback()` (the escaping exception may have left an aborted
transaction), record `state = 'error'`, `error_code = 'reconcile_unexpected_error'` so the
attempt clock advances, and continue to the next order. `_record_failure` already marks
the trade `reconcile_required`, and the SLO block still fires on age, so the outcome stays
fail-closed. `BaseException` is deliberately not caught.

### Index

The existing partial index leads with `first_unresolved_at` and no longer matches the
`ORDER BY`. Add a matching one and keep the old one (it still serves
`enforce_reconciliation_slo`'s `min(first_unresolved_at)` + age filter):

```sql
CREATE INDEX IF NOT EXISTS idx_strategy_order_reconciliation_fairness
    ON strategy_order_reconciliation_state (last_attempt_at NULLS FIRST, first_unresolved_at, order_id)
    WHERE state NOT IN ('resolved', 'rejected');
```

`NULLS FIRST` must be declared: Postgres defaults ascending indexes to `NULLS LAST`, which
would not match the query's ordering.

## What is deliberately NOT changed

- `first_unresolved_at` — never written, so the SLO age cannot be reset to make health look
  green (ticket acceptance).
- The `limit` bound, the core-path exclusion (`trade.core_rebalance_intent_id IS NULL`),
  the `execution_origin = 'strategy'` filter, terminal-state semantics, request UUIDs and
  per-account authority: all untouched.
- `enforce_reconciliation_slo` and the entry block it drives.
- No jitter. Jitter exists to de-correlate *independent* clients; there is exactly one
  reconciler process on one five-minute cadence, so jitter would add nondeterminism to the
  tests and buy nothing.

## Tests (DB-backed, `tests/test_strategy_order_reconciliation.py`)

Existing `_seed_trade` is split into `_seed_deployment` + `_seed_order` so a test can seed
`n` orders against one deployment; `_seed_trade` stays as a wrapper, so the four existing
tests are untouched.

⚠ Advancing a Python `now=` does not advance SQL `now()`, so every test manipulates
`last_attempt_at` / `attempt_count` with an explicit `UPDATE` rather than by sleeping.

1. **Rotation over multiple cycles.** `L = 3`, 7 orders, broker always `BrokerOrderNotFound`.
   Assert the exact visited `order_id` list per cycle and the exact broker-call count; assert
   every order is visited within `ceil(7/3) = 3` cycles; assert a newly inserted order is
   visited within its own rotation; assert a resolving order leaves the backlog and is never
   visited again; assert `first_unresolved_at` is unchanged for every row.
2. **Backoff due-filter.** Set `attempt_count` and `last_attempt_at` per row, then assert
   the selected set excludes rows inside their capped delay and includes them once outside
   it — by exact visited IDs and broker-call count. Includes the boundary row (exactly one
   cycle after its attempt, `attempt_count = 1`, which must be due) and a `pending` row
   with a high `attempt_count` that must **never** be excluded.
3. **Poison row.** One order whose reconciliation raises an unmodelled exception; assert
   the batch still visits every later order, that the poison row's `last_attempt_at`
   advanced and its state is `error`, and that it does not monopolise the next cycle.

## Known, accepted, and out of scope (all surfaced by Codex ckpt-1)

- **Concurrent invocations can select the same rows.** The selection commits immediately
  after `fetchall()`, so nothing claims a row. Production has exactly one scheduled caller
  (`strategy_paper_cycle`) on the `strategy_execution` job lane, which serialises it; a
  second reconciler would be a new deployment decision, not a regression introduced here.
- **`_record_failure` can overwrite a terminal row under concurrency** (pre-existing; same
  single-lane argument).
- **`attempt_count` is `INTEGER` and is never clamped on increment.** At the capped
  cadence a row accrues ~12 attempts/hour, so `2^31` attempts is ~20,000 years. Accepted.
- **The partial index can scan cooling rows before satisfying `LIMIT`.** The eligible
  backlog is bounded by the sleeve's concurrent-position cap, so the scan is small.
- **A permanently dead row still blocks new entries forever** — fair polling supplies no
  terminal disposition, and #2948 explicitly requires the entry block be preserved. The
  disposition question belongs with #2949's scenario matrix (safe stop vs automatic
  recovery), not here.
- **A longer reconciliation delay has a lasting effect on candidates**: the caller keeps
  evaluating signals while blocked and `_persist_rejection`
  (`app/services/strategy_paper_executor.py:567`) records a funding rejection that removes
  the signal from future selection. Pre-existing, and the direction of this change is to
  shorten delays, not lengthen them.

## Blast radius

`reconcile_backlog` has one production caller
(`app/services/strategy_paper_runtime.py:431`) plus tests. No API shape changes. The
migration is index-only and `IF NOT EXISTS`.
