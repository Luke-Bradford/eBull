# Reserved execution lanes need their own APScheduler executor

**Issue:** #2985. **Protects:** #2833's prospective hourly quote population.
**Status:** proposal, 2026-09-13. Revised after Codex checkpoint 1.

## The filed premise is false

#2985 says *"a jobs-daemon restart orphans an in-flight `quotes_refresh` and nothing
re-runs it"*. Measured on the dev DB, every clause of that is wrong:

1. **No `quotes_refresh` run was orphaned.** All 12 `error_msg like 'orphaned%'` rows in
   the last 3 days belong to other jobs (`daily_candle_refresh` ×5, `daily_portfolio_sync`
   ×2, `fundamentals_sync`, `daily_financial_facts`, `core_rebalance_observation`,
   `core_eligibility_refresh`, `strategy_intraday_harvest`). `quotes_refresh` has **0**.
2. **Something does re-run it.** `quotes_refresh` carries `catch_up_on_boot=True`
   (`app/workers/scheduler.py:931`), so `JobRuntime._catch_up` fires it whenever the
   last *successful* run's next cadence slot is already past. Two such off-schedule
   fires exist in the last 14 days (09-12 22:35:40, 09-13 04:56:43), both `success`.
3. **Fix direction (b) — "enqueue an immediate catch-up for reaped interval jobs" — is
   already implemented for this job**, and direction (a) (drain before restart) addresses
   a cause that did not occur: the process never went down.

## What actually lost the bucket

One bucket is missing inside the live window — `2026-09-13 03:00Z`, absent for all 10
candidates. (The 08-26 → 09-12 block is the known offline gap, not this.)

`quotes_refresh` rows for 2026-09-13 00:00–06:00Z:

| started_at | status | reason (full `error_msg`) |
| --- | --- | --- |
| 02:23:00 | success | — |
| **03:23:00** | **skipped** | `misfire: … worker reached it 2801.5s late, past misfire_grace_time` |
| **04:23:00** | **skipped** | `misfire: … worker reached it 872.9s late, past misfire_grace_time` |
| 04:56:43 | success | boot catch-up — wrote bucket **04**, not 03 |
| 05:23:00 | success | — |

⚠ The lateness is the decisive column and it is easy to lose: it lives at the **end** of
`error_msg`, so any `left(error_msg, 50)` in a diagnostic query truncates it away.

### The chain, with the step that each number pins

1. **50 of the 63 registered jobs share one execution permit.**
   `JOBS_GENERAL_NON_SEC_MAX_CONCURRENCY = 1` (`app/db/pg_settings.py:163`); counted by
   lane: general **50**, `sec_rate` 11, `paper_lifecycle_reserved` 1, `quote_observation
   _reserved` 1.
2. **A job waiting for that permit holds an APScheduler worker thread while it waits.**
   `wrapped()` (`app/jobs/runtime.py:2433-2441`) is the registered job func and its
   outermost statement is `_job_execution_slot`, whose fallback is an **unbounded blocking**
   `slots.acquire()` (`runtime.py:764-780`).
3. **The pool is 10 threads** — APScheduler 3.11.2's default executor is
   `ThreadPoolExecutor(max_workers=10)` (verified against
   `BaseScheduler._create_default_executor`); `JobRuntime` registers no named executor, so
   every recurring job shares it.
4. **Fires therefore queue for tens of minutes.** The 03:23 fire was dequeued
   **2801.5 s (46.7 min)** after its slot, at ≈04:09:41; the 04:23 fire **872.9 s
   (14.5 min)** after, at ≈04:37:33. `executors/base.py::run_job` compares that lateness
   against `misfire_grace_time=1` and raises `EVENT_JOB_MISSED` without running the body.
5. **Boot catch-up at 04:56 sampled a fresh quote**, so it wrote bucket **04**.
   `sample_bucket` truncates `observed_at` to the hour
   (`app/services/strategy_core_quote_observation.py:78-82`), and a quote is a
   point-in-time observation — **hour 03 cannot be reconstructed by any retry.**

### Three alternative explanations, and what rules each out

Checkpoint 1 correctly objected that a mix of `max_instances_active` and misfire rows can
be produced with a *free* worker pool, and that my first draft asserted saturation rather
than showing it. The stored lateness settles it:

- **Scheduler-thread stall** (e.g. `_on_job_max_instances` writes to Postgres synchronously
  on the scheduler thread, `runtime.py:1636`). Real, and it does delay dispatch — but it
  produces *seconds*. The measured 2801.5 s is three orders of magnitude larger. It is
  also the wrong shape: a dispatch stall delays submission, whereas `run_job`'s lateness is
  measured when a **worker** picks the job up.
- **"If 03:23 were still queued at 04:23, the 04:23 fire would report
  `max_instances_active`, not an independent misfire."** Correct, and the timestamps show
  it did not overlap: 03:23 was dequeued at ≈04:09:41, and 04:23 was submitted afterwards.
  Two independent misfires are exactly what serial dequeuing predicts.
- **Process down / restart.** Ruled out by a discriminator rather than by absence: the
  04:56:43 catch-up ran on `_manual_executor` (`runtime.py:1877`), a *different*
  `concurrent.futures` pool. The process was alive and making progress on that pool during
  the same window in which APScheduler fires sat 872 s queued. Only the APScheduler pool
  was jammed.

Prior occurrence, same shape: 2026-08-25 03:23 misfired **10466.3 s (2.9 h)** late — the
window the prevention log records as `orchestrator_full_sync` holding `general_non_sec`
for three hours.

### Why the existing reservation did not hold

#2934 gave `quotes_refresh` a reserved semaphore (`_QUOTE_OBSERVATION_EXECUTION_SLOTS`)
*and* its own source lane (`source="etoro_quotes"`), and the prevention-log entry
*"A reserved execution slot does not bypass the job's source lock — audit both admission
layers"* records that work. Both layers were fine here — and untested, because a misfired
fire never reaches the wrapper, the semaphore or the lock.

**There is a third admission layer nobody traced: the APScheduler worker pool.** A job
parked on another lane's semaphore still owns a worker thread, so one lane's backlog
starves every other lane's *threads* no matter how their semaphores and locks are
partitioned. The reservation is real but sits one layer below the point of contention.

A fourth candidate was checked and ruled out as the *misfire* cause: `EtoroMarketDataProvider`'s
shared throttle is a per-request 1.1 s clock (`_ETORO_RATE_LIMIT_LOCK` guarding
`_ETORO_RATE_LIMIT_CLOCK`, `etoro.py:77-78`), and a misfire happens before the body. It can
still lengthen the runs that cause the backlog; it is not exonerated as a contributor,
only as the proximate cause.

## Why this matters to #2833, precisely

`scripts/verify_2833_core_selection.py::_population_for` takes, per selected date, the
**first and last `observed` bucket for that instrument** and requires every bucket in that
span to be present and observed; otherwise `incomplete_population`.
`selected_dates = common_dates[:5]`, and `_common_dates` counts only dates with observed
rows. Consequences, stated to the code rather than to the trading session:

- A hole is fatal only if it is **interior** to that instrument's observed span. A hole at
  the leading or trailing edge shortens the span and passes silently.
- "Outside market hours" is not a safe category: an out-of-session hour bracketed by
  observed buckets is interior, and therefore fatal.
- A date with a fatal hole is still a *common* date, so it consumes one of the five slots
  and cannot be swapped for a later clean one.

`common_dates_observed` is currently **1** of 5 (2026-09-13 is a **Sunday**; the last
trading day with observations is 09-11). Four more qualifying sessions are needed. The
existing mitigation — a deploy-freeze note on #2437 — does not address this failure at all:
the 09-13 outage involved no deploy and no restart.

## Change

### 1. One source of truth for lane routing

Extract the `if/elif` inside `_job_execution_slot` into a pure
`execution_lane_for(job_name) -> str`, preserving today's precedence exactly: `sec_rate`
source first, then the two job-name checks, then general; an unknown job name (`source_for`
raises `KeyError`) keeps falling back to general. `_job_execution_slot` maps lane →
semaphore and keeps emitting the same lane labels the heartbeat already publishes. The
semaphore layer and the executor layer then cannot drift — otherwise they are two
independent copies of one predicate.

### 2. A dedicated APScheduler executor per reserved lane

```
executors = {
    "default": APSchedulerThreadPool(_DEFAULT_EXECUTOR_MAX_WORKERS),
    EXECUTION_LANE_PAPER: APSchedulerThreadPool(JOBS_PAPER_LIFECYCLE_MAX_CONCURRENCY),
    EXECUTION_LANE_QUOTE: APSchedulerThreadPool(JOBS_QUOTE_OBSERVATION_MAX_CONCURRENCY),
}
```

`add_job(..., executor=_scheduler_executor_alias(job.name))`, which returns the lane alias
for the two reserved lanes and `"default"` for `sec_rate` and general.

⚠ **Import trap.** `app/jobs/runtime.py` already imports
`concurrent.futures.ThreadPoolExecutor` for `_manual_executor`. APScheduler needs
`apscheduler.executors.pool.ThreadPoolExecutor`; passing the `concurrent.futures` class to
`BackgroundScheduler` fails, and re-pointing the existing import would break
`_manual_executor.submit`. Import the APScheduler class under an explicit alias.

⚠ **An unknown alias registers fine and fails when the job is due** — APScheduler resolves
the executor at dispatch, not at `add_job`. So the test must assert the alias is present in
the scheduler's executor registry, not merely that `job.executor` holds the expected string.

### 3. `misfire_grace_seconds` on `quotes_refresh` — second layer, not the fix

`misfire_grace_time=1` means any dispatch delay above one second discards a lost-forever
bucket, and sub-2-second misfires are real in this process (`strategy_paper_cycle` has one
at **1.1 s**). #2880 added the per-job field for exactly this class. Ceiling derived from
the bucket: a fire at minute 23 is only useful inside its own hour, admission requires
`now - run_time <= grace`, so `grace < (60 - 23) * 60 = 2220` → **2219 s**, computed from
the registered cadence minute rather than written down.

⚠ **This is a second layer and it would NOT have saved bucket 03**: the measured lateness
was 2801.5 s, past any within-bucket ceiling. It covers the seconds-scale dispatch delays
that change (2) cannot (a scheduler-thread stall). ⚠ It also does **not** guarantee the
sample lands in the bucket — grace gates wrapper admission, and the body then runs for a
further ~40–90 s (n=244: median 40.2 s, p95 76.3 s), so a very late admission can stamp the
next bucket. Because the insert is `ON CONFLICT (instrument_id, sample_bucket) DO NOTHING`
that is never corrupting, only wasteful.

### 4. Out of scope, recorded

- **The general lane itself.** 50 jobs on 1 permit is the underlying pressure. Sizing the
  default pool to the general semaphore would convert ~50 jobs' "queue then run" into
  "discard" — far too wide for this ticket.
- **Moving the listener DB writes off the scheduler thread** (`_on_job_max_instances`,
  `runtime.py:1636`). A real dispatch-latency source; change (3) makes it non-fatal for
  `quotes_refresh`. Noted on the issue, not fixed here.
- **`jobs_liveness_watchdog` is itself on the general lane and the default executor**, so
  the detector can be starved by the condition it exists to catch, and it counts skip rows
  as activity. Noted on the issue, not fixed here.

### Sizing rule (derived, not chosen)

A reserved lane's executor must have at least as many workers as its semaphore permits, or
the pool becomes the tighter bound and re-creates the starvation inside the lane. Both
reserved semaphores are 1 and **each reserved lane has exactly one registered job**
(asserted), so one worker per lane is both necessary and sufficient. The assertion is the
derivation (`max_workers == JOBS_*_MAX_CONCURRENCY`, and exactly one job per reserved
lane), so bumping a semaphore or adding a second job to a reserved lane fails a test
instead of silently re-introducing the bug.

### Connection budget: no term changes, and why

The prevention log requires an explicit statement whenever execution slots and a pool
maximum move together. **Neither moves here.** Body concurrency is still bounded by the
same four semaphores (SEC 4 + general 1 + paper 1 + quote 1 = 7 bodies), which is what
`app/db/pg_settings.py` budgets. An APScheduler executor hands out a *thread*; the
semaphore still gates entry to the body and therefore to every connection the body opens.
Adding threads to a lane already capped at one body cannot raise concurrent body
connections. ⚠ Narrow exception, unchanged by this PR: the misfire/max-instances listeners
write outside the semaphores via the bounded background-write seam — that was already true
and no term of it moves.

⚠ **Not claimed: "general and SEC behaviour is unchanged."** Their *configuration* is
unchanged, and they keep the whole 10-worker default pool. But removing two jobs from that
pool's queue does change contention and therefore which general fires survive their grace —
in their favour, and by two fires an hour at most.

## Rejected alternatives

- **Grace alone.** Measured lateness 2801.5 s exceeds any within-bucket ceiling. Kept as a
  second layer (§3), rejected as the fix.
- **Non-blocking semaphore acquire (skip instead of park).** Frees the worker and is
  arguably the right long-term shape, but turns ~50 general jobs' queueing into discards.
- **Draining `dev_reload` before restart (#2985 direction (a)).** No restart occurred.
- **Touching the verifier or the declaration.** Both are sealed by digest
  (`DECLARATION_SHA256`; the verifier hashes itself into every output). Changing
  `_population_for` after seeing the data is tuning, barred by the spike rules.

## Acceptance

1. **The failure is reproduced and fixed as a test**: with the general semaphore held and
   every default-executor worker parked on it, a reserved-lane fire still runs its body.
   Asserted per reserved lane independently. This is the exact shape that failed at 03:23.
2. Each reserved executor alias is present in the scheduler's executor registry, and its
   `max_workers` equals its lane's `JOBS_*_MAX_CONCURRENCY`.
3. Every registered recurring job's executor alias equals `_scheduler_executor_alias(name)`;
   `sec_rate` and general jobs resolve to `"default"`.
4. Exactly one registered job maps to each reserved lane.
5. `execution_lane_for` preserves SEC-first precedence and the unknown-name → general
   fallback.
6. `quotes_refresh`'s `misfire_grace_seconds` is strictly less than the seconds from its
   registered cadence minute to the end of its hourly bucket.
7. No `JOBS_*_MAX_CONCURRENCY`, `SEC_LANE_MAX_CONCURRENCY`, pg pool constant, `coalesce`,
   `max_instances` or default `misfire_grace_time` changes.
8. Dev verification is on the **persisted evidence**, not on "the job ran": after the change
   is live, every hourly bucket between the first and last observed bucket of a session is
   present for all 10 candidates.

⚠ **Not claimed:** that this makes an hour un-losable. If `quotes_refresh` itself wedges, if
the host is down for an hour, or if the provider returns stale/invalid quotes, the bucket is
still lost and no retry can rebuild it. This removes the one cause that is *measured* to
have lost a bucket in the live window.
