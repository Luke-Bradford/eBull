# Jobs out of API process — design (#719)

## Goal

Move `JobRuntime` (APScheduler + manual-trigger executor) and the sync
orchestrator dispatcher from inside the FastAPI process to a dedicated
long-running process. After this change:

- API restarts (uvicorn `--reload`, redeploy, crash) MUST NOT kill jobs in flight.
- Job hangs MUST NOT consume API threads, API DB conns, or API event-loop time.
- Postgres is the only inter-process coupling — no HTTP, no shared memory, no Redis.
- The new architecture must not itself become brittle: durable triggers, supervised
  listeners, multi-component health, locked startup ordering.

## Non-goals

- Replacing `BackgroundScheduler` / APScheduler. Same scheduler, new process.
- Multi-process job parallelism. One scheduler process is sufficient.
- Job re-implementation. `app/workers/scheduler.py` invokers stay byte-for-byte.
- Production / deployment story. eBull is single-operator dev; production design is out of scope.

## Architecture

### Process topology

| Process | Owns | Restart cadence |
| --- | --- | --- |
| `app.main` (uvicorn) | HTTP API, request DB pool (`db_pool`, `audit_pool`) | High (every dev edit) |
| `app.jobs` (entrypoint) | `JobRuntime` (BackgroundScheduler + manual executor), sync orchestrator executor, reaper, queue dispatcher, listener supervisor, heartbeat writer | Low (manual / on schedule code change) |

Both connect to the same `ebull` Postgres database. Both use the hardened
`_open_pool` helper extracted to `app/db/pool.py` so the jobs process imports
it without pulling in FastAPI.

### Trigger transport: durable queue + NOTIFY

`LISTEN/NOTIFY` alone is lossy — events sent while the jobs process is down
or reconnecting are dropped silently. Avoid that by writing every trigger to
a durable queue table FIRST and using NOTIFY only as a wakeup hint.

New table `pending_job_requests`:

```sql
CREATE TABLE pending_job_requests (
    request_id    BIGSERIAL PRIMARY KEY,
    request_kind  TEXT NOT NULL CHECK (request_kind IN ('manual_job', 'sync')),
    job_name      TEXT,                   -- populated for request_kind='manual_job'
    payload       JSONB,                  -- populated for request_kind='sync' (scope)
    requested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    requested_by  TEXT,                   -- operator id / "service-token" / "scheduler-catchup"
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'claimed', 'dispatched', 'completed', 'rejected')),
    claimed_at    TIMESTAMPTZ,
    claimed_by    TEXT,                   -- jobs-process pid + boot timestamp
    error_msg     TEXT
);

CREATE INDEX idx_pending_job_requests_unclaimed
  ON pending_job_requests (requested_at)
  WHERE status = 'pending';
```

Flow:

- **API** (`POST /jobs/{name}/run` or `POST /sync`):
  1. Validate the request (job name in registry, scope shape valid).
  2. INSERT into `pending_job_requests` with `status='pending'`.
  3. `pg_notify('ebull_job_request', request_id::text)`.
  4. Return `202` with `{"request_id": N}` so the operator can correlate.
- **Jobs process listener** (one thread, one channel — handles both `manual_job` and `sync` request kinds; payload distinguishes):
  1. `LISTEN ebull_job_request` on its own dedicated `psycopg.Connection`.
  2. **NOTIFY-driven path**: on each notify, parse `request_id` from payload, then `UPDATE pending_job_requests SET status='claimed', claimed_at=now(), claimed_by=$boot_id WHERE request_id=$1 AND status='pending' RETURNING request_kind, job_name, payload`. Empty result = already claimed by another path (boot-drain or poll); skip.
  3. **Poll fallback path** (every 5s, regardless of notify activity): `UPDATE pending_job_requests SET status='claimed', claimed_at=now(), claimed_by=$boot_id WHERE request_id = (SELECT request_id FROM pending_job_requests WHERE status='pending' AND requested_at > now() - interval '24 hours' ORDER BY requested_at LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING request_kind, job_name, payload`. Loops until the SELECT returns no row. `SKIP LOCKED` means concurrent listener restarts don't deadlock.
  4. Dispatch by `request_kind`:
     - `manual_job` → `runtime.submit_manual(job_name)` (uses the `JobRuntime`'s manual `ThreadPoolExecutor`).
     - `sync` → submit `run_sync(scope, trigger)` to a dedicated `ThreadPoolExecutor` owned by the jobs process (sized for one concurrent sync at a time, since the orchestrator's partial unique index on `sync_runs` already serialises starts).
     **The status transition to `dispatched` happens INSIDE the executor task, not here.** Each dispatch wrapper's first action (after the executor picks up the future) is to open the linked run row (`job_runs` or `sync_runs`) with `linked_request_id` populated, then UPDATE `pending_job_requests.status='dispatched'`. This pins the recovery contract: a row only reaches `dispatched` when its run exists.
  5. On dispatch submit error (executor refused, e.g. shutting down): the listener catches and `UPDATE ... SET status='rejected', error_msg=$msg`. Submission errors are rare; in-flight crashes are handled by boot-drain.

NOTIFY is the low-latency wakeup; the 5s poll is the safety net for dropped notifies (network blip, listener reconnect window). Both paths use the same atomic claim semantics; whichever fires first wins.

### Singleton enforcement

Boot-recovery is only safe under a single live `app.jobs` process. Two
processes racing on the same queue would each treat the other's `claimed_by`
as stale and replay live rows.

The entrypoint acquires a session-scoped Postgres advisory lock on a
**dedicated, long-lived `psycopg.Connection` held for the lifetime of the
process** — never a pool checkout, never a `with` block:

```python
# app/jobs/__main__.py — entrypoint sketch
fence_conn = psycopg.connect(settings.database_url)  # NOT from the pool
acquired: bool = fence_conn.execute(
    "SELECT pg_try_advisory_lock(%s)", (JOBS_PROCESS_LOCK_KEY,)
).fetchone()[0]
if not acquired:
    logger.error("another app.jobs process holds the singleton lock; exiting")
    fence_conn.close()
    sys.exit(2)
# fence_conn is held in module-global scope until shutdown(); no `with`,
# no pool. Released on graceful close OR Postgres-detected dead session.
```

The connection is closed only by the SIGTERM/SIGINT handler at the very end
of shutdown, after every other subsystem has stopped. This guarantees the
lock outlives every other resource the process owns. Running the entrypoint
twice is a hard exit rather than silent double-replay of queue rows.

`JOBS_PROCESS_LOCK_KEY` is a constant `BIGINT` shared between API and jobs
process (defined in `app/jobs/locks.py` alongside the existing per-job
lock keys).

### Boot-drain + crash recovery

Before the listener thread starts, the entrypoint runs a **boot-drain** that
addresses two failure modes:

1. **Triggers landed during downtime**: rows in `status='pending'` from any prior boot.
2. **Triggers stranded mid-flight by a previous crash**: rows in `status='claimed'` or `status='dispatched'` whose `claimed_by` is a stale boot id (safe because of the singleton-enforcement section above).

Boot-drain query (run once, before the listener starts and before scheduler.start):

```sql
UPDATE pending_job_requests pjr
SET status = 'pending', claimed_at = NULL, claimed_by = NULL
WHERE pjr.status IN ('claimed', 'dispatched')
  AND pjr.claimed_by IS DISTINCT FROM $current_boot_id
  AND pjr.requested_at > now() - interval '24 hours'
  AND NOT EXISTS (
    -- Don't replay manual_job rows whose linked job_runs is terminal.
    SELECT 1 FROM job_runs jr
    WHERE jr.linked_request_id = pjr.request_id
      AND jr.status IN ('success', 'failure')
  )
  AND NOT EXISTS (
    -- Don't replay sync rows whose linked sync_runs is terminal.
    SELECT 1 FROM sync_runs sr
    WHERE sr.linked_request_id = pjr.request_id
      AND sr.status IN ('complete', 'failed', 'partial', 'cancelled')
  );
```

Then the listener's poll-fallback claim query (oldest-pending with `FOR UPDATE SKIP LOCKED`) runs in a tight `until empty` loop to drain every freshly-pending row before the listener thread proper starts. The dispatch path each claim takes is identical to runtime listener dispatch.

Per-row crash safety contract:

- The dispatch wrapper for `request_kind='manual_job'` opens a `job_runs` row with `status='running'` as its first action, populating `linked_request_id`. The wrapper then UPDATEs `pending_job_requests.status='dispatched'`. Both writes happen in the same autocommit conn; crash anywhere in this prefix → row stays `claimed`, boot-drain replays.
- The dispatch wrapper for `request_kind='sync'` opens a `sync_runs` row (existing `_start_sync_run` already does this), populates `sync_runs.linked_request_id`, then UPDATEs `pending_job_requests.status='dispatched'`. Same crash semantics.
- After the invoker returns, the wrapper transitions the linked run's status to terminal AND `pending_job_requests.status='completed'`. Crash here → the boot-drain `NOT EXISTS` clauses see the terminal run and do not replay.
- Replay is at-least-once. Idempotency is required of every job (already locked into the contract by #657 — same property already required for crash-recovery of in-process scheduled fires).

TTL: rows older than 24h are not replayed. They remain in the table with their last status for operator inspection. A nightly `requests_retention_sweep` job (added in this PR) deletes terminal rows older than 30 days.

### Listener supervision

Each listener thread is supervised by the jobs process main loop. The loop:

- Tracks the listener's `last_event_at` timestamp (updated on every notify, every poll tick, and every successful claim attempt).
- If `now() - last_event_at > 60s` and no recent activity, restart the listener (close the conn, reopen `LISTEN`, resume).
- Each restart logs at WARNING and increments a counter exposed to the heartbeat row.

Supervised: the LISTEN thread, the orchestrator listener (if separate), the heartbeat thread itself. Each has an idle-with-no-progress threshold; main loop restarts whichever stalls.

### Multi-component heartbeat

`job_runtime_heartbeat` is a single row keyed by subsystem name:

```sql
CREATE TABLE job_runtime_heartbeat (
    subsystem      TEXT PRIMARY KEY,         -- 'scheduler' | 'manual_listener' | 'queue_drainer' | 'main'
    last_beat_at   TIMESTAMPTZ NOT NULL,
    pid            INTEGER NOT NULL,
    process_started_at TIMESTAMPTZ NOT NULL,
    notes          JSONB                     -- listener restart count, last claim timestamp, etc.
);
```

Each subsystem writes its own row every 10s. The API `/system/jobs` endpoint
reads ALL rows; `jobs_process_state` is `"healthy"` only if every expected
subsystem has `now() - last_beat_at < 60s`. A single stale subsystem = `"degraded"`,
all stale = `"down"`. The frontend can render per-subsystem detail.

This addresses the listener-died-but-heartbeat-thread-still-up failure mode:
the listener has its own row, and a stalled listener fails the health check
even when the main process is fine.

### `/system/jobs` next-fire-time

Existing code already gets this right. `app/api/system.py:210` calls
`compute_next_run(job.cadence, now)` when the runtime is absent — that's
"next future occurrence of the cadence past now", correct under any
job_runs history. The current `runtime.get_next_run_times()` path is the
fallback; this PR removes it and the existing fallback becomes the only
path. No new computation needed.

### Operator visibility for queued requests

A trigger that is rejected before any `job_runs` / `sync_runs` row is created
must still be visible. Today the operator polls `/jobs/runs` and would see
nothing for a request the dispatcher rejected. Without a visible read path,
silent loss is the operator's experience even when the queue captured the
failure correctly.

New endpoint `GET /jobs/requests`:

- Returns the most recent N rows from `pending_job_requests` (default 50).
- Supports `?request_id=N` (exact, returns 0 or 1 row), `?status=pending|claimed|dispatched|completed|rejected`, `?job_name=...`, and `?request_kind=manual_job|sync` filters.
- Each row exposes: `request_id`, `request_kind`, `job_name`, `requested_at`,
  `requested_by`, `status`, `claimed_at`, `error_msg`, `linked_run_id`.

Existing `/jobs/runs` rows gain an optional `linked_request_id` column so the
operator can pivot from a run to its triggering request and vice versa.
Migration adds `job_runs.linked_request_id BIGINT REFERENCES pending_job_requests(request_id) ON DELETE SET NULL`.

The `POST /jobs/{name}/run` and `POST /sync` 202 responses include
`{"request_id": N}` so the operator can immediately query
`GET /jobs/requests?request_id=N` without polling `/jobs/runs` waiting for a
run row that may never appear.

### Sync orchestrator dispatch

The API today calls `submit_sync(scope, trigger)` which uses
`_executor_ref` set during lifespan startup from `job_runtime._manual_executor`.
That executor must move to the jobs process.

After this PR — three distinct functions to avoid the naming collision the
old design had:

- **`publish_sync_request(scope, trigger) -> request_id`** (NEW; in
  `app/services/sync_orchestrator/dispatcher.py`). Called by the API. Inserts
  into `pending_job_requests` with `request_kind='sync'` and the scope
  serialised to JSON; emits `pg_notify('ebull_job_request', request_id::text)`;
  returns the `request_id`. **This is the only function the API ever calls
  for async sync.**
- **`run_sync(scope, trigger) -> SyncResult`** (UNCHANGED; in
  `app/services/sync_orchestrator/executor.py`). Synchronous — plans,
  executes, finalises in the caller's thread. Used by tests, CLI scripts,
  and inside the jobs process when the dispatcher claims a `sync` request.
  Its body opens the `sync_runs` row with `linked_request_id` populated.
- **`submit_sync` is DELETED.** Anything calling it on the API side moves to
  `publish_sync_request`. Anything calling it inside the jobs process moves
  to `run_sync` (the dispatcher invokes `run_sync` on its own executor).
- `set_executor` is removed entirely. The jobs process owns its executor;
  the API doesn't need one.

This means the file list grows by:

- `app/services/sync_orchestrator/dispatcher.py` (new): publisher + queue claim helpers used by both processes.
- `app/api/sync.py` (modify): swap `submit_sync` for `publish_sync_request`.
- `app/services/sync_orchestrator/executor.py` (modify): delete `set_executor` and `submit_sync`; `run_sync` stays unchanged and is what the dispatcher invokes on a sync-only `ThreadPoolExecutor`.
- `app/services/sync_orchestrator/__init__.py` (modify): remove `set_executor` from `__all__`.

### Startup ordering (jobs process entrypoint)

LOCKED order — every previous in-process attempt got bitten by a different ordering bug. Pin it.

```text
1.  Logger + signal handlers + stop_event installed.
2.  Hardened DB pool opened (_open_pool('jobs_pool', min_size=1, max_size=4)).
3.  SINGLETON FENCE acquired on a dedicated long-lived psycopg.Connection
    (NOT from the pool). pg_try_advisory_lock(JOBS_PROCESS_LOCK_KEY).
    If the lock is already held, log FATAL and sys.exit(2). Every
    later step assumes the singleton: boot-drain's "claimed by stale
    boot_id" reset is only safe because no concurrent live process
    exists.
4.  Reaper runs (reap_orphaned_syncs(reap_all=True)).
    — MUST be before any catch-up or boot-drain so stale sync_runs are
      terminal before prereq checks read them.
5.  JobRuntime CONSTRUCTED (no scheduler.start yet — object init wires
    the manual ThreadPoolExecutor and the in-process inflight locks).
    Sync ThreadPoolExecutor created (max_workers=1; the orchestrator's
    partial unique index on `sync_runs` already serialises starts) and
    held on the entrypoint module — separate from JobRuntime so its
    lifecycle is independently shutdown-able.
6.  Queue stale-row recovery (boot-drain step 1): UPDATE pending_job_requests
    rows in 'claimed' / 'dispatched' from prior boots back to 'pending'
    per the crash-recovery rules above.
7.  Queue boot-drain step 2: tight loop claiming all currently-pending
    rows and submitting them through runtime.submit_manual /
    `run_sync`. Targets the executors that step 5 created (manual +
    sync). Runs BEFORE scheduler.start() so a scheduled-fire's catch-up
    cannot race with a queued operator trigger.
8.  scheduler.start() — registers cron triggers, kicks BackgroundScheduler.
9.  JobRuntime._catch_up() (already wired into start()).
10. Listener thread started (LISTEN ebull_job_request + 5s poll fallback loop).
11. Heartbeat threads started (one per supervised subsystem).
12. Main loop sleeps on stop_event with periodic supervision.
```

Shutdown order (reverse, with timeouts at each step):

```text
1. Stop accepting new claims (listener.stop, including its 5s poll loop).
2. Stop scheduler (scheduler.shutdown(wait=False) — same as today).
3. Stop manual executor (cancel_futures=True — same as today).
4. Stop sync executor (cancel_futures=True; in-flight sync run is
   killed by process exit and the boot reaper terminates its
   sync_runs row on next boot, same contract as scheduled jobs).
5. Heartbeats stop.
6. Pool closes.
7. Singleton fence connection closes LAST — once it closes, Postgres
   releases the advisory lock and another `app.jobs` invocation can
   start. Closing earlier risks a fresh process launching while this
   one is still tearing down.
```

### API lifespan changes

- Remove: `start_runtime`, `shutdown_runtime`, `set_executor`, `reaper.reap_orphaned_syncs(reap_all=True)`, `_boot_freshness_sweep` (relocates — see below).
- `app.state.job_runtime` is no longer set; reads update.
- `app.state.db_pool` and `app.state.audit_pool` unchanged.
- `_LIFESPAN_STATE_FLAGS` in `tests/smoke/test_app_boots.py` drops `'job_runtime'`.

### Boot freshness sweep relocation

Today `app/main.py::_boot_freshness_sweep` fires `submit_sync(SyncScope.behind(), trigger='boot_sweep')` on every API boot. Per the isolation goal, the API initiates no work — this hook moves to the jobs entrypoint as step 8.5 (between `_catch_up()` and the listener thread):

- `app/jobs/boot_sweep.py` (new): `def run_boot_freshness_sweep() -> None` calls `run_sync(SyncScope.behind(), trigger='boot_sweep', linked_request_id=None)` directly on the jobs process's sync executor (NOT through the queue — it's an internal-to-jobs-process action with no operator request to track). All exceptions logged + swallowed, same best-effort contract as today.
- `EBULL_SKIP_BOOT_SWEEP` env gate stays, read by the jobs entrypoint.
- The `boot_sweep` trigger value already exists in `SyncTrigger`. No type change.

Operator-visible behaviour change: every-API-restart-triggers-a-sweep becomes every-jobs-process-restart-triggers-a-sweep. Since the jobs process restarts much less often, freshness recovery happens less aggressively. That is the correct trade — repeated boot sweeps from a dev edit storm were wasted work; one sweep per actual jobs-process restart is the right cadence.

## Files to create / modify

### Create

- `app/jobs/__main__.py` — entrypoint, ordered startup.
- `app/jobs/listener.py` — supervised LISTEN dispatcher (manual jobs + sync requests on the same channel; payload distinguishes by `request_kind`).
- `app/jobs/heartbeat.py` — multi-subsystem heartbeat writer.
- `app/jobs/supervisor.py` — main-loop supervisor that restarts stalled subsystems.
- `app/db/pool.py` — extracted `_open_pool` + `_POOL_CONNECTION_KWARGS`.
- `app/services/sync_orchestrator/dispatcher.py` — `publish_sync_request` + claim helpers.
- `sql/{NN}_job_runtime_heartbeat.sql` — heartbeat table.
- `sql/{NN}_pending_job_requests.sql` — durable queue table.
- `tests/smoke/test_jobs_process_boots.py` — drives the entrypoint as a callable; asserts startup ordering, heartbeat rows, listener alive.
- `tests/test_jobs_listener.py` — mocked NOTIFY iterator + DB.
- `tests/test_jobs_heartbeat.py` — heartbeat loop unit test.
- `tests/test_jobs_queue_boot_drain.py` — boot drain claims pending rows from the table.
- `tests/test_jobs_listener_supervision.py` — stalled listener detected and restarted.
- `tests/test_api_jobs_publish.py` — `POST /jobs/{name}/run` writes `pending_job_requests` row + issues NOTIFY.
- `tests/test_api_sync_publish.py` — `POST /sync` writes a `request_kind='sync'` row instead of calling the in-process executor.
- `tests/test_api_jobs_requests.py` — `GET /jobs/requests` returns rows + supports filters.
- `tests/test_jobs_queue_recovery.py` — boot-drain replays stale `claimed`/`dispatched` rows from a prior boot. Asserts BOTH branches of the recovery NOT EXISTS clause: (a) a `manual_job` row whose `linked_request_id` matches a terminal `job_runs` row is NOT replayed; (b) a `sync` row whose `linked_request_id` matches a terminal `sync_runs` row (status in {complete, failed, partial, cancelled}) is NOT replayed; (c) a `sync` row whose `sync_runs` is still 'running' (mid-flight crash) IS replayed; (d) a row whose `requested_at` is older than 24h is NOT replayed regardless of state.

### Modify

- `app/main.py` — drop `start_runtime`, `shutdown_runtime`, `set_executor`, reaper. Keep `_open_pool` import (now from `app/db/pool.py`).
- `app/api/jobs.py` — rewrite to use `pending_job_requests` + NOTIFY. Returns 202 with `request_id`. 404 still returned for unknown names (validate against the imported registry). Adds `GET /jobs/requests` endpoint with filter params.
- `app/workers/scheduler.py` — register a new `requests_retention_sweep` job (daily) that deletes terminal `pending_job_requests` rows older than 30 days; existing 8-K / fundamentals / etc. invokers unchanged.
- `sql/{NN}_job_runs_linked_request.sql` — adds `linked_request_id BIGINT REFERENCES pending_job_requests(request_id) ON DELETE SET NULL` to `job_runs`; `_tracked_job` updated to populate it when the dispatch wrapper passes it down.
- `sql/{NN}_sync_runs_linked_request.sql` — adds the parallel `linked_request_id` column to `sync_runs`. `_start_sync_run` is updated to accept and populate `linked_request_id` from the dispatcher wrapper; `run_sync`'s signature gains an optional `linked_request_id` argument that the in-process callers (`workers/scheduler.py` scheduled-fire entrypoints, tests) pass `None` and the dispatcher passes the claimed `request_id`.
- `app/api/system.py` — remove `runtime.get_next_run_times()` path; the `compute_next_run(now)` fallback becomes canonical. Add `jobs_process_state` derived from heartbeat rows.
- `app/api/sync.py` — swap `submit_sync` for `publish_sync_request`.
- `app/services/sync_orchestrator/executor.py` — delete `set_executor` and `submit_sync` entirely. `run_sync` stays unchanged and becomes the canonical worker entry the dispatcher invokes per claimed `sync` request.
- `app/services/sync_orchestrator/__init__.py` — drop `set_executor` from exports.
- `app/jobs/runtime.py` — extract `submit_manual(name)` public method (encapsulates inflight lock + executor.submit) so the listener calls it cleanly.
- `tests/smoke/test_app_boots.py` — drop `'job_runtime'` from `_LIFESPAN_STATE_FLAGS`; assert it is NOT set.
- `.vscode/tasks.json`, `Makefile`, `stack-restart.ps1`, `README.md` — add `stack: jobs` task / target.
- `docs/settled-decisions.md` — Process topology section.
- `docs/review-prevention-log.md` — "Don't add lifespan-side scheduling" entry.

### Delete

- The lifespan-startup `set_executor(job_runtime._manual_executor)` line.
- The lifespan-startup reaper call (moves to jobs entrypoint).

## Migration

Hard cut, no env flag. Single-operator dev — no rolling deploy concern.

1. `git pull` + `uv sync` + `uv run python scripts/migrate.py` (heartbeat + queue tables).
2. Restart all three VS Code tasks (backend / frontend / jobs).
3. Existing `job_runs` history is preserved (same table, same writers).

## Test plan

### Unit

- `test_jobs_listener.py`: feeds notifies through a mocked psycopg `Connection.notifies()`, asserts each `request_kind='manual_job'` claim ends in `runtime.submit_manual(name)` and each `request_kind='sync'` claim ends in `run_sync(scope, trigger, linked_request_id=...)` submitted to the sync executor. Malformed payloads, unknown job names, already-claimed rows: each logs a single WARNING and does not call any executor.
- `test_jobs_heartbeat.py`: drives the multi-subsystem loop for ~3 ticks (with a fake clock), asserts UPSERT against each subsystem row.
- `test_jobs_queue_boot_drain.py`: pre-seeds `pending_job_requests` with two pending rows, calls the boot drainer, asserts both are claimed and dispatched + the rows transition to `status='dispatched'`.
- `test_jobs_listener_supervision.py`: simulates a listener stall (no events for >60s with mocked clock), asserts the supervisor restarts it and the heartbeat row records the restart.
- `test_api_jobs_publish.py`: `POST /jobs/{name}/run` against a real `db_pool`, asserts a row appears in `pending_job_requests` AND a notify on `ebull_job_request` is observed by a side LISTEN connection. 404 path: unknown names hit before the insert.
- `test_api_sync_publish.py`: `POST /sync` writes `request_kind='sync'` with the serialised scope; the response carries the `request_id`.
- Existing `test_job_runtime_*.py`: `set_executor` references in tests must be replaced with the new shape (or those tests deleted if they covered the lifespan integration that no longer exists).

### Integration / smoke

- `tests/smoke/test_jobs_process_boots.py`: imports the entrypoint as a function, drives it under a stop event, asserts:
  - startup order observable via log captures (`reaper` before `scheduler.start` before `LISTEN`),
  - heartbeat rows for each subsystem within 5s,
  - LISTEN thread receives a test-injected NOTIFY and dispatches,
  - stop_event triggers shutdown within 10s,
  - APScheduler is registered with all expected jobs.
- `tests/smoke/test_app_boots.py`: still drives the FastAPI lifespan; asserts `job_runtime` is absent from `app.state` and `/system/jobs` returns sensibly when the jobs process is down (heartbeat rows missing).

### Manual

- Restart backend during a long-running job; confirm `job_runs` row continues, finishes, and a new `job_runs` row is not orphaned to `error_category='orchestrator_crash'`.
- Stop jobs process; `POST /jobs/{name}/run` returns 202 with a request_id and the row sits in `pending_job_requests` with `status='pending'`. Restart jobs process; the row claims+dispatches within ~seconds via boot-drain.
- Inject a malformed NOTIFY (`pg_notify('ebull_job_request', 'not-a-number')`); jobs process logs a single WARNING and continues.
- Manually `pg_terminate_backend()` the listener's connection; supervisor restarts within 60s and the next NOTIFY is delivered.

## Open questions to resolve in spec review (Codex round 2)

1. **Boot-drain TTL**: 24h is operator-friendly but means a long-stopped jobs process resumes at most a day's worth of triggers. Confirm or tighten.
2. **No API-side `run_sync` callers**: audit `app/api/sync.py` + every other `app/api/*.py` for `run_sync` imports. The design REQUIRES zero API-side callers — any in-process `run_sync(...)` from the API thread would re-introduce API-side execution and violate the isolation goal. If the audit finds a caller, that call site must be rewritten to `publish_sync_request(...)` as part of this PR. (`run_sync` legitimately stays in the codebase for the jobs-process dispatcher and for tests/CLI scripts; it just must not be reachable from a FastAPI handler.)
3. **Boot-drain ordering vs. scheduler catch-up**: catch-up reads `job_runs` to find overdue jobs; boot drain claims explicit operator triggers from the queue. If the queue has a pending trigger for a job that catch-up is also about to fire, the job's in-process inflight lock prevents double-run. Confirm by inspection.
4. **Heartbeat subsystem set**: locked to {`scheduler`, `manual_listener`, `queue_drainer`, `main`}? Or extensible?
5. **Windows signal**: VS Code task termination on Windows sends `CTRL_BREAK_EVENT` (or kills the process). Confirm the entrypoint handles whatever signal the dev environment actually sends; on Windows the recommended path is `signal.signal(signal.SIGBREAK, ...)` plus a console-control handler.

## Rollback

If a critical bug surfaces after merge:

1. Revert the merge commit on `main`.
2. Restart all three VS Code tasks. JobRuntime is back in the API process.
3. The heartbeat + pending_job_requests tables stay — orphan, harmless. Drop in a follow-up.

## Settled-decision additions (post-merge)

Add to `docs/settled-decisions.md`:

> ### Process topology (settled)
>
> - The FastAPI process serves HTTP only. No APScheduler, no manual-trigger executor, no orchestrator executor, no reaper.
> - The jobs process owns APScheduler, the manual-trigger executor, the sync orchestrator executor, the reaper, the queue dispatcher, and the heartbeat writer.
> - Inter-process communication is Postgres-only: durable rows in `pending_job_requests`, NOTIFY on `ebull_job_request` as a wakeup hint.
> - Both processes use the hardened `_open_pool` helper at `app/db/pool.py`.
> - Triggers are durable: every `POST /jobs/{name}/run` and `POST /sync` writes a row before NOTIFY, so a trigger sent while the jobs process is restarting is replayed on boot rather than lost.

## Acceptance

- All four CI checks (lint, pytest, review, supply-chain) green on the PR.
- Codex pre-push review: no remaining correctness or coverage gaps.
- Smoke test for jobs process passes.
- Manual: API restart during a long-running job leaves the job's `job_runs` row at `status='running'` (not orphaned to `error_category='orchestrator_crash'`) and the row finishes normally when the job completes.
- Manual: `pg_terminate_backend()` the listener; the next NOTIFY still dispatches within 60s.
