# Lane-busy retry — scheduled fires recover transient cross-job lane contention

**Issue:** #1538 · **Relates:** #1534 (per_cik lane extraction), #1536 (watchdog can't heal lane starvation), #1064 (same-source serialise), #1530 (schedule_missed de-noise)

## Problem

A scheduled fire acquires a **source-level** `JobLock` —
`pg_try_advisory_lock(hashtext('job_source:{lane}'))`, NON-blocking. When the
lane is already held it raises `JobAlreadyRunning`; the scheduled-fire handler
(`app/jobs/runtime.py`, the `except JobAlreadyRunning` at the `with JobLock(...)`
site) logs "skipped" and **waits for the next cadence** — losing the whole
period.

On an over-subscribed lane this starves periodic producers. `sec_rate` has ~24
jobs including `sec_atom_fast_lane` (every 5 min, holds the lane <1s) and heavy
drainers. Any periodic producer whose slot aligns with a lane peer (most do —
`:00`, `:35`, hourly/daily on 5-min boundaries) loses the race and skips. On dev:
`sec_per_cik_poll` skipped 17h+ (fixed by #1534 — own lane); `sec_filing_documents_ingest`
fires ~every 2h instead of hourly; daily_index / 8k / form3 / master_idx are
exposed identically.

## Root cause

The skip-whole-period on **transient cross-job** lane contention.
`max_instances=1` (`app/jobs/runtime.py`) already prevents a job overlapping
*itself*, so the source-level lock serializes only DIFFERENT jobs — the skip is
a cross-job artifact, not self-overlap protection. Per-job lane extraction
(#1534) treats one symptom at a time; the mechanism is generic to every lane.

## Design — in-lock bounded retry (extracted helper)

Extract the scheduled-fire run into a small, testable helper
`_fire_scheduled_with_lane_retry(database_url, job_name, run, *, backoff, sleep=time.sleep)`
that wraps the existing `with JobLock(...): run()` in a bounded retry. The
inline `except JobAlreadyRunning` site in `app/jobs/runtime.py` calls the helper.

Two invariants the helper must hold:

1. **Retry ONLY the acquire, never the body.** `JobAlreadyRunning` is raised by
   `JobLock.__enter__` (acquire) — verified at `app/jobs/locks.py:324` — BEFORE
   the body runs. But the `except` wraps the whole `with`, so a `JobAlreadyRunning`
   surfaced from *inside* the invoker/prelude must NOT be retried (the body
   already partially ran). Guard with an `acquired` flag set as the first
   statement inside the `with`; on `JobAlreadyRunning` with `acquired=True`, treat
   as a body error (log + stop), do not retry.
2. **Bound concurrent waiters** so retry sleeps cannot drain the scheduler pool
   (see Safety). A module-level `BoundedSemaphore` gates the wait; if no slot is
   free, skip immediately (== today's behaviour).

```python
# app/jobs/runtime.py (shape, not final formatting)
_LANE_BUSY_RETRY_BACKOFF: tuple[float, ...] = (0.25, 0.5, 1.0)   # 3 retries, ~1.75s max
_MAX_CONCURRENT_LANE_WAITERS = 3
_LANE_WAIT_SLOTS = threading.BoundedSemaphore(_MAX_CONCURRENT_LANE_WAITERS)

def _fire_scheduled_with_lane_retry(database_url, job_name, run, *, backoff=_LANE_BUSY_RETRY_BACKOFF, sleep=time.sleep):
    slot = False
    try:
        for attempt in range(len(backoff) + 1):
            acquired = False
            try:
                with JobLock(database_url, job_name):
                    acquired = True
                    run()
                return                                  # ran this period
            except JobAlreadyRunning:
                if acquired:
                    raise                               # body raised it — NOT a lane skip; caller logs
                if attempt == 0 and not _LANE_WAIT_SLOTS.acquire(blocking=False):
                    logger.info("scheduled fire of %r skipped: lane busy, no retry slot free", job_name)
                    return                              # too many concurrent waiters — skip now
                slot = slot or attempt == 0 and True    # (set when slot acquired)
                if attempt < len(backoff):
                    sleep(backoff[attempt]); continue
                logger.info("scheduled fire of %r skipped after %d lane-busy retries: lane held the whole window", job_name, len(backoff))
                return
    finally:
        if slot:
            _LANE_WAIT_SLOTS.release()
```

(The `slot` bookkeeping is shown roughly; final code sets it precisely when the
semaphore is acquired and releases exactly once.) The caller keeps its existing
`except JobAlreadyRunning` (now only reached if the body raised it),
`except OrchestratorFenceHeld`, and `except Exception` arms unchanged.

### Why it works

The dominant collision is with `sec_atom_fast_lane` (holds the lane <1s). The
first acquire at `:35:00` loses; the retry ~0.25–0.5s later finds the lane free
and **runs the same period**. Recovery is within the first period — far under
the ~2-cadence `schedule_missed` de-noise threshold (#1530) — so the row never
goes red. No verdict change required.

### Safety / bounds

- **Worst case = today.** Lane held longer than the ~1.75s window (a ~2-min heavy
  drainer), OR no waiter slot free → log skip exactly as now. No new failure mode.
- **Scheduler-pool protection (Codex ckpt-1).** APScheduler runs executor size
  10, `misfire_grace_time=1`, `coalesce=True`, `max_instances=1`
  (`app/jobs/runtime.py:956`). Sleeping workers risk delaying *other* due jobs
  past the 1s misfire grace. Mitigations: (a) short backoff (~1.75s max, vs the
  job runtimes already on the pool — e.g. filing_documents ~1m44s — so the
  incremental hold is small); (b) `BoundedSemaphore(3)` caps total concurrent
  sleepers at 3, so retries can never occupy more than 3 of 10 workers; excess
  collisions skip immediately. This is a deliberate tradeoff, not a free lunch.
- **No self-overlap special-casing.** Retrying is safe whether the lane is held
  by another job (likely frees) or this job's own manual run (retries exhaust →
  skip). Bounded either way.
- **#1184 re-entrancy (Codex ckpt-1).** `_HELD_SOURCES` is per-ContextVar and NOT
  cross-thread (`app/jobs/locks.py`), so a same-context nested same-lane acquire
  bypasses Postgres entirely (no `JobAlreadyRunning`, never reaches the retry);
  scheduled/manual sibling workers on different threads still collide at Postgres
  and ARE the case this retry helps. Retry semantics are unaffected by the bypass.
- **Manual triggers unchanged.** The manual-trigger path keeps its immediate
  `JobAlreadyRunning` → 409. Only the scheduled-fire caller wraps with the helper.
- **`max_instances=1` interaction.** The retry occupies the run slot for ≤~1.75s;
  for all current scheduled cadences (≥ every-5-min) that is ≪ the period, so a
  next-cadence fire is not expected to be affected. (Not an absolute guarantee —
  it holds because cadences ≫ the retry window, not by construction.)

### Not in scope

- No change to lane membership (`sec_rate` keeps its members; per_cik stays on
  its own lane from #1534).
- No change to `JobLock` semantics itself (still non-blocking acquire); the retry
  lives in the scheduled-fire caller so manual/orchestrator acquire paths are
  untouched.
- The #1536 watchdog enhancement is separate (this reduces how often a job
  reaches a true stall, but doesn't replace the watchdog).

## Testing (fast tier, DB-free)

The helper takes an injected `sleep` and runs against a fake `JobLock`, so all
tests are pure (no DB, no real sleeping).

- **Retry-then-run:** fake `JobLock` raises `JobAlreadyRunning` on the first N
  `__enter__`s then succeeds; assert the `run` callable fires once and `sleep`
  was called N times with the expected backoff prefix.
- **Exhaust-then-skip:** `JobLock.__enter__` always raises; assert `run` never
  fires, `sleep` called `len(backoff)` times, skip logged once.
- **Body raises `JobAlreadyRunning` → NOT retried (Codex ckpt-1):** `JobLock`
  acquires (sets `acquired`), the `run` callable raises `JobAlreadyRunning`;
  assert exactly one attempt, `sleep` never called, and it surfaces as a body
  error (not a lane-skip) to the caller.
- **Waiter-slot exhaustion → immediate skip (Codex ckpt-1):** pre-acquire all
  `_MAX_CONCURRENT_LANE_WAITERS` slots; a lane-busy fire skips on attempt 0
  without sleeping; assert `sleep` never called and the slot count is unchanged
  afterwards (the helper released nothing it didn't take).
- **Slot released on every exit path:** after retry-then-run, exhaust-then-skip,
  and body-raises, assert `_LANE_WAIT_SLOTS` is back to full capacity (no leak).

## Rollout / verify

Restart dev jobs proc onto the change. Confirm `sec_filing_documents_ingest`,
`sec_daily_index_reconcile`, `sec_8k_events_ingest`, `sec_form3_ingest` fire on
their full cadence (no skipped periods) across an aligned `sec_atom_fast_lane`
tick; verify their Processes verdicts read current. Spot-check the log for
"skipped after N lane-busy retries" — should be rare (only genuine long holds).
