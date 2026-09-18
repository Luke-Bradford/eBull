# Durable execution-slot wait — #3159 clause 2

Status: implemented on `fix/3159-durable-execution-slot-wait`.
Issue: #3159 clause 2 — *"whatever the instance is waiting on is recorded when it waits."*

## Problem

`_job_execution_slot` (`app/jobs/runtime.py`) is deliberately the outermost boundary of every
runtime entry path: *"parameter-error audit writes, gate/prerequisite checks, advisory locks, the
job body, and terminal writes all occur only after a slot is held"* (`wrapped()`). The prelude
stamps `job_runs.started_at` **inside** that boundary. So a fire that parked on the semaphore for
19 minutes and a fire that was admitted instantly produce byte-identical rows, and the wait exists
only in the daemon's stderr plus a process-local in-memory snapshot
(`execution_slot_wait_snapshot`) that a restart discards.

Measured, dev DB, 7 days to 2026-09-18:

| measurement | value |
| --- | ---: |
| `job_runs` rows | 13,054 |
| `status='skipped'` with `max_instances_active` | **567 (4.3%)** |
| distinct jobs carrying those skips | **11** |

`strategy_halt_feed_refresh` (119 of the 567) is the only one PR #3167 moved off the shared general
lane. For three of the remaining ten, the job's own body cannot account for the collision at all:

| job | cadence | max body 7 d | `max_instances_active` |
| --- | ---: | ---: | ---: |
| `jobs_retry_sweeper` | 300 s | **0.20 s** | 74 |
| `jobs_liveness_watchdog` | 900 s | **0.56 s** | 9 |
| `expected_filings_poller` | 900 s | **1.93 s** | 6 |

`finished_at - started_at` measures the body only, because `started_at` is stamped after the slot.
An instance still active at the next fire, whose body is under 2 s of a 300–900 s window, spent
that window in time no stored column measures.

⚠ **No concurrency proxy is cited for this.** The obvious join — *is some peer run spanning the
skip* — returns **567/567**, which is a check that cannot fail: with 13k rows in 7 days something
is nearly always running. The previous comment's lane-scoped 105/119 is the honest figure and it
covers the halt feed only.

## Source rule

No external rule governs an internal instrumentation column, so the treatment is fixed by this
repo's own precedents, cited rather than reasoned out:

1. **Carrying a value from a wrapper into the prelude** — module-scoped `contextvars.ContextVar`
   with an explicit set/reset token. Three existing carriers do exactly this: `_prelude_run_id`,
   `_params_snapshot_var`, `_invoker_request_context` (`app/jobs/runtime.py`), and
   `_run_scheduled_body` already uses the `set()`/`reset(token)` idiom for the params snapshot.
2. **Nullable, no default.** `docs/review-prevention-log.md` — *"a `NOT NULL DEFAULT` column makes
   `count(col)` the row count, always"*, and *"a default value is not a measurement"*. A `DEFAULT 0`
   would make every un-instrumented row read as an admitted-instantly row.
3. **`numeric`, not `double precision`** — `job_runs` carries no float column today; three decimal
   places is what the existing log line already prints (`"acquired %s execution capacity after
   %.3fs"`).

## Design

### Schema — `sql/397`

```sql
ALTER TABLE job_runs ADD COLUMN execution_slot_wait_seconds numeric(12,3);
```

⚠ Precision **12**, not 9, for a failure-mode reason rather than a range estimate: `numeric(9,3)`
caps at 999999.999 (11.6 days) and a wait past the cap raises `numeric field overflow` *inside the
prelude's INSERT*, i.e. an instrumentation column able to fail the job it instruments — in exactly
the pathological-wait case it exists to observe. Verified 2026-09-18 that psycopg3 adapts a Python
`float` into this column with no explicit cast (`1140.4567 → 1140.457`, `0.0 → 0.000`,
`None → NULL`).

Nullable, no default, no index (every consumer is a bounded-window scan already filtering on
`started_at`).

Three-valued on purpose, and this is the load-bearing part of the design:

| value | means |
| --- | --- |
| `NULL` | the row was **not** written from inside an execution slot (see *uncovered rows*) |
| `0.000` | a slot was held and admission was immediate |
| `> 0` | admission blocked for this many seconds **before** `started_at` |

Recording `0.000` on the fast path is what makes the column answer a question. Leaving the fast
path `NULL` would collapse *"admitted instantly"* into *"not instrumented"*, and every zero-wait
run would be indistinguishable from an opt-out row — the same shape as the `count()`-on-`NOT NULL`
trap, one level up.

### Runtime

- `_job_execution_slot` computes the wait it already logs (`time.monotonic() - wait.started_monotonic`,
  `0.0` on the non-blocking acquire) and publishes it on a new ContextVar
  `_execution_slot_wait_seconds` for the duration of the `yield`, resetting with the token in
  `finally`.
  ⚠ **The reset is what makes `None` mean "no slot is held".** APScheduler's `ThreadPoolExecutor`
  reuses worker threads and a ContextVar's value lives in the thread's top-level context, so
  without it every read taken on that thread after the fire returns the previous fire's figure and
  the column's NULL — the value that reports its own coverage — is unreachable for any reader
  outside a slot.
  ⚠⚠ **It is NOT what stops one fire's wait being attributed to the next fire on the same thread,
  and the first draft of this spec said it was.** The `set` is unconditional, so a later slot entry
  always overwrites. Revert-probe P1 (delete the reset) fails
  `test_an_immediate_admission_publishes_a_zero_wait_not_none` and does **not** fail the leak test
  the claim named. Both tests are kept; each now states what it actually discriminates.
  The token form (rather than `set(None)`) matches the idiom `_run_scheduled_body` already uses for
  `_params_snapshot_var` and keeps a nested entry from clearing its caller's value.
- `current_execution_slot_wait_seconds()` reads it. Deliberately **not** a `consume_*` popper like
  its three neighbours: the value's lifetime is the slot's, not the reader's, and the slot owns
  both ends of it. A popping reader would also silently zero the second reader if one is ever
  added.
- `_run_prelude` passes it into all four `INSERT INTO job_runs` branches (fence-held × params-snapshot).

### Uncovered rows, stated rather than discovered later

The column is `NULL` on:

- `_PRELUDE_OPT_OUT_JOBS` — `bootstrap_orchestrator` (own state machine) and
  `orchestrator_full_sync` / `orchestrator_high_frequency_sync`, which write `sync_runs`, not
  `job_runs`. ⚠ `orchestrator_high_frequency_sync` carries 76 of the 567 skips, so its wait stays
  invisible after this change.
- `record_job_skip` rows written outside the prelude — the registry-default param-validation abort
  and `_record_lane_busy_skip`. Both are inside the slot, so both *could* carry it; neither is a
  run whose own `started_at` clause 1 reads, and covering them means widening
  `ops_monitor.record_job_skip`'s signature. Out of scope, named here so it is a decision rather
  than an omission.
- `_tracked_job`'s `record_job_start` fallback (bootstrap stage invokers, direct test calls) —
  those paths hold no slot, so there is no wait to record.

### What this does NOT do

- It does not shorten any wait. #3159's producer half bounds the halt feed's wait by moving lanes;
  the general lane still has **one permit shared by 49 jobs** and this change does not touch that.
- It does not record *which peer* held the permit. With the wait on the row, the holder is
  derivable — the lane peer whose `[started_at, finished_at]` spans `started_at - wait` — so
  storing a peer name would duplicate a join, and it would be wrong whenever the permit changed
  hands mid-wait.
- It does not retro-fill clause 1's before-figure. The column has no history;
  `scripts/measure_3159_halt_feed_lane.py` keeps its proxy walk for that, and after one US session
  the same figure becomes a direct read of this column.

## Acceptance

1. A blocked slot entry observes a positive wait *inside* the slot; the peer that was admitted
   immediately observes `0.0`.
2. The value does not leak across two slot entries on the same thread (the pooled-thread case).
3. The prelude persists it on both the `running` and the fence-`skipped` INSERT branch, with and
   without a params snapshot.
4. A `job_runs` row written outside the prelude keeps `NULL`.

Met by 3 pure tests (`tests/test_jobs_runtime.py::TestConnectionBudgetExecutionGate`) + 5 DB tests
(`tests/test_jobs_runtime_fence.py`). Control green first: 10 collected / exit 0 on the pure class,
5 on the DB additions. Revert-probes against that control:

| probe | fails |
| --- | --- |
| P1 — delete the ContextVar `reset` | 1 (`…publishes_a_zero_wait_not_none`) |
| P2 — fast path publishes `None` instead of `0.0` | 3 |
| P3 — publish a constant `0.0` in place of the measurement | 2 |

## Codex checkpoints

Both unavailable this session — `codex exec` returns `You've hit your usage limit … try again at
Sep 19th, 2026 11:57 PM` on both profiles. Recorded as missing, not skipped. The ckpt-1 question
list was run by hand and changed two things in this spec: the pooled-thread leak (which the first
draft handled with a bare `set(None)` rather than a token, so a nested slot entry would have
cleared the outer one's value), and the fast-path `0.000`-vs-`NULL` decision, which the first draft
left `NULL` and would have made the column unable to distinguish its own absence.
