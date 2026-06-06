# Job-level retry/backoff for transient failures

Issue: #1509 (T3 of epic #1508). Spec-first; Codex ckpt-1 (this doc) + ckpt-2 (plan) before code.
Builds on: #1512 (verdict, shipped 5b38dde) · #1511 (audited manual-queue activation, shipped 8267c7c).
Proposal: `docs/proposals/ui/admin-processes-self-healing-health.md` §3 decision 3.

## Problem

A failed scheduled job has **no near-term retry**. It waits a full cadence for its
next natural fire (daily ≤24h, weekly ≤7d, yearly ≤12mo). A 30-second network blip on
a daily job = a red row for ~24h. The shipped `pending_retry` status only means "the
*next natural fire* covers the failed scope" (`scheduled_adapter._status_for`) — it is
cadence-bound, not a real backoff. The "will retry HH:MM" verdict promised by #1512's
docstring (`health_verdict.py:30-37`) has no data source yet.

## Goal

On failure, classify **transient** vs **permanent**:

- **Transient** (network/timeout/429/source-down/internal blip) → schedule a near-term
  retry on capped exponential backoff, surfaced as **self_healing** "will retry HH:MM".
- **Permanent** (auth/schema-drift/db-constraint/missing-key) → no retry → **attention**
  immediately. (Stops #1516's `NumericValueOutOfRange` from retry-storming.)

Bounded attempts; exhausted transient → permanent (attention). Auditable re-fire.

## Scope / exclusions

Retry/backoff covers only jobs that finalize through `record_job_finish`
(`job_runs` terminal rows). **Excluded** (Codex ckpt-1): self-tracked orchestrator jobs
that resolve terminal status from `sync_runs`, not `job_runs` — the same exclusion set
the liveness watchdog uses (`JOB_ORCHESTRATOR_FULL_SYNC`,
`JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC`, `scheduler.py:4252`). Sync-run-level retry is a
separate mechanism, out of scope here. The sweeper iterates only `SCHEDULED_JOBS` minus
that set, so a stray `job_runs` row for an unregistered name is never re-dispatched.

## Decisions

### D1 — classifier = existing `REMEDIES[category].self_heal`

`app/services/sync_orchestrator/layer_types.py` already maps every `FailureCategory`
to `Remedy.self_heal: bool`. Reuse it verbatim — single source of truth, no new table:

| Category | self_heal | retry? |
|---|---|---|
| RATE_LIMITED, SOURCE_DOWN, DATA_GAP, UPSTREAM_WAITING, INTERNAL_ERROR | True | **transient → retry** |
| AUTH_EXPIRED, SCHEMA_DRIFT, DB_CONSTRAINT, MASTER_KEY_MISSING | False | **permanent → attention** |

`error_category` is already computed on the failure path (`_tracked_job` →
`classify_exception` → `record_job_finish`). DB_CONSTRAINT=permanent ⇒ #1516 finra
overflow never retries (correct — needs the schema fix, not a storm).

### D2 — state on `job_runs` (not a sidecar)

Migration `sql/183` (or next free if #1516's `sql/183` lands first — rebase):

```sql
ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS attempt       SMALLINT NOT NULL DEFAULT 1;
```

- `next_retry_at` — set on the **failed** row when a retry is scheduled; `NULL` = no
  retry (permanent, exhausted, or success). PG14+ const-default ⇒ metadata-only rewrite.
- `attempt` — this run's attempt number within the current failure streak (1 = first
  natural fire). Persisted for observability + backoff math.
- Partial index for the sweeper's hot read:
  `CREATE INDEX IF NOT EXISTS job_runs_due_retry_idx ON job_runs (next_retry_at) WHERE next_retry_at IS NOT NULL;`
- Idempotent shape-check guard (information_schema) per repo migration convention.

### D3 — set `next_retry_at` in `record_job_finish` (failure path)

`record_job_finish` already receives `error_category`. Extend the failure branch:

1. `is_transient = (category is not None and REMEDIES.get(category, _NO_RETRY).self_heal)`
   — `None` / unknown category ⇒ no retry (never index with `[]`; cannot KeyError).
2. Streak count = consecutive prior `status='failure'` rows for this job, counting back
   from this run until the first non-failure **terminal** row. Exact invariants:
   - exclude the current `run_id`;
   - only rows with `started_at < this run.started_at` (tie-break `run_id <`),
     `ORDER BY started_at DESC, run_id DESC`;
   - `success` / `skipped` / `cancelled` **break** the streak (stop counting);
   - `running` rows are ignored (not terminal);
   - `attempt = streak + 1` (this failure is attempt N).
3. If transient **and** `attempt <= MAX_ATTEMPTS`: `next_retry_at = now + backoff(attempt)`.
   Else (permanent, or attempts exhausted): `next_retry_at = NULL` → row stays `failed` →
   verdict attention.
4. Write `next_retry_at` + `attempt` in the same UPDATE.

Counting the streak (vs threading `attempt` through dispatch/prelude) keeps the change
local to `record_job_finish` — no new param on `record_job_start`/the manual-queue payload.
`_NO_RETRY = Remedy(..., self_heal=False)` is the module sentinel for the `.get` default.

Backoff (tunable consts in `ops_monitor`):

```
backoff(attempt) = min(BASE * FACTOR**(attempt-1), CAP)
BASE=300s (5m) · FACTOR=3 · CAP=3600s (1h) · MAX_ATTEMPTS=4
# RATE_LIMITED: BASE_RATE_LIMITED=900s (15m) — never retry into a still-held window (#1484 caveat)
```

attempt 1→5m, 2→15m, 3→45m, 4→60m(cap); attempt 5 ⇒ exhausted ⇒ attention.

**Out of scope:** the orphan reaper (`reap_orphaned_job_runs`) writes its own UPDATE,
not via `record_job_finish`, so crash-reaped runs do **not** auto-retry (conservative;
unchanged). Skip/cancel/success paths set `next_retry_at=NULL`.

### D4 — re-fire via a periodic audited sweeper (not APScheduler one-shot)

New scheduled job `jobs_retry_sweeper`, `Cadence` every 5m, `source="db"`,
`catch_up_on_boot=False`. Body (template: `jobs_liveness_watchdog`, `scheduler.py:4237`):

1. Read due retries **off the partial index** (no full latest-terminal scan):
   ```sql
   SELECT run_id, job_name, started_at, attempt
     FROM job_runs
    WHERE next_retry_at IS NOT NULL
      AND next_retry_at <= now()
      AND status = 'failure'
    ORDER BY job_name, started_at DESC, run_id DESC
   ```
   (`next_retry_at` is only ever set on a `failure` row, so the `status` predicate is a
   cheap guard, not the access path.) Drop any `job_name` not in `SCHEDULED_JOBS` minus
   the orchestrator exclusion set (scope guard above).
2. Per candidate, in its **own** `conn.transaction()` (psycopg3 abort-safety, mirrors
   `post_bootstrap_activation`) — all checks + publish + clear are **atomic**:
   a. `SELECT ... FOR UPDATE` the failed row; re-assert it is still the latest terminal
      row for the job (no newer terminal `run_id`) **and** `next_retry_at` still set/due
      (a concurrent sweep or natural run may have superseded it).
   b. Skip (rollback) if the job already has a `running` row or a live
      `pending_job_requests` row — the in-flight request is the primary dedup, so two
      sweeps can never double-dispatch.
   c. Validate `job_name` against the invoker registry (publish contract), then
      `publish_manual_job_request_with_conn(conn, job_name,
      requested_by="system:retry_backoff", process_id=job_name, mode="iterate")`
      (caller-owned `conn` first arg — `dispatcher.py:149`) — `process_id`/`mode` set so the existing
      process-scoped queue probes (`queue_stuck`, hub state, fences) see the retry row;
      a nullable `process_id` would make it invisible to them (Codex ckpt-1).
   d. `decision_audit` row (`stage='retry_backoff'`, action `RETRY`, evidence =
      job/attempt/category).
   e. **Advance, do NOT clear:** `UPDATE job_runs SET next_retry_at = now +
      _DISPATCH_RECHECK_SECONDS (15m) WHERE run_id=<failed row>`. Clearing is unsafe —
      the manual queue can reject the request **asynchronously after this commit**
      (bootstrap gate / per-job prereq / full-wash fence in `listener.py` / `runtime.py`)
      with no new terminal run to restamp the retry (Codex ckpt-2 HIGH). Advancing keeps
      the failed row as its own durable backstop: a genuine new run supersedes it (the
      latest-terminal check in (a)/(2a) clears the stale row), while an async rejection is
      re-dispatched once the window elapses — no loss, bounded to one request per window.
   Because (a)–(e) share one tx, a *synchronous* publish failure (e.g. a full-wash
   `UniqueViolation` fence) rolls everything back and the row is retried next sweep,
   unchanged.
3. The retried run is a fresh `job_runs` row; its `attempt` is recomputed by D3's streak
   count. A concurrent natural cadence fire arriving after commit is a harmless idempotent
   duplicate (re-enqueue bodies are idempotent, per #1511).
4. Best-effort: a per-candidate exception logs + continues (durable backstop = next
   natural cadence fire, exactly as today).

Why a sweeper, not an APScheduler `date` job: re-uses the durable audited manual-queue
(survives restart, respects the universal gate, auditable per the non-negotiables);
APScheduler one-shots are in-memory, lost on restart, and bypass the audit trail.

**#1484 caveat (rate-limit/gate):** v1 leans on backoff (RATE_LIMITED gets the 15m base,
so the window has passed by re-fire) + the universal gate (steady-state `complete`, and
the manual-queue path honors it). Lane-aware "is this source's rate budget currently
exhausted?" pre-check is **T4 (#1510)** territory — noted, deferred, cross-ref #1484.

### D5 — verdict wiring (`compute_verdict` stays pure)

`compute_verdict` gains two precomputed inputs (pattern: #1511's `watermark_is_fresh`):

```python
retry_in_flight: bool = False      # next_retry_at IS NOT NULL (scheduled recovery exists)
retry_at_display: str = ""         # "HH:MM" when future; "" when already due
```

**`retry_in_flight = next_retry_at IS NOT NULL`** — past OR future (Codex ckpt-1: a
due-but-not-yet-swept retry, `next_retry_at <= now()` in the ≤5m sweeper gap, is still
scheduled recovery; gating on "future only" would flicker the row red for up to 5m).
The reason text distinguishes the two: future → `f"will retry {retry_at_display}"`;
due (empty display) → `"retrying shortly"`.

Adapter/API choke point computes them (clock lives there, not in the pure fn):
`ProcessRow` gains `next_retry_at: datetime | None` (populated by `scheduled_adapter`
from the latest terminal row, like #1511's `source_watermark_fresh`); `_convert_row`
(`app/api/processes.py`) derives `retry_in_flight` + label and passes both.

Precedence change (preserves the Codex-ckpt-1 invariant — genuine wedges still win):

```
retry_in_flight suppresses ONLY schedule_missed (a pending retry IS the missed-schedule fix).
queue_stuck / mid_flight_stuck / watermark_gap still outrank → attention
   (a wedged queue means the retry itself may be stuck; do not paint it self-healing).
Then: status in {failed, pending_retry} AND retry_in_flight
   → (self_healing, True, "will retry HH:MM"  if future else "retrying shortly")
```

Existing cadence-based `pending_retry` ("retry scheduled") stays as the fallback when no
`next_retry_at` is set. Exhausted/permanent `failed` (no retry_in_flight, no actionable
stale) → attention "last run failed" (unchanged).

## Where (verified file:line)

- `sql/183_job_runs_retry_backoff.sql` — new migration (D2).
- `app/services/ops_monitor.py:325 record_job_finish` — backoff + classify (D3); new
  `_backoff_seconds` + consts; streak SELECT.
- `app/workers/scheduler.py` — `JOB_RETRY_SWEEPER` const + `ScheduledJob` registration
  (near `:946`) + `jobs_retry_sweeper()` body (D4); `app/jobs/runtime.py:243` invoker wiring.
- `app/services/processes/health_verdict.py:71 compute_verdict` — D5 inputs + precedence.
- `app/services/processes/__init__.py` — `ProcessRow.next_retry_at` field.
- `app/services/processes/scheduled_adapter.py` — populate `next_retry_at`.
- `app/api/processes.py _convert_row` — derive `retry_in_flight`/label, pass to verdict.

## Tests

- `_backoff_seconds` table-test (attempt→seconds, cap, RATE_LIMITED base).
- `record_job_finish`: transient sets `next_retry_at`+`attempt`; permanent (DB_CONSTRAINT)
  does not; exhausted (attempt>MAX) does not; success/skip clears.
- `compute_verdict` table-test extended: `failed`+retry_in_flight → self_healing "will
  retry HH:MM"; `failed`+retry_in_flight+queue_stuck → attention (invariant);
  schedule_missed suppressed by retry_in_flight.
- Sweeper: due row → one manual-queue request + audit + `next_retry_at` cleared;
  running/in-flight job skipped; per-candidate tx isolates a failure.

## DoD

Lint/format/pyright/pytest green. Codex ckpt-1 (spec) + ckpt-2 (plan). Smoke
(`test_app_boots`) since lifespan registers the new job. No new universal-gate carve-out.
PR records: migration applied on dev, a forced-transient-failure row gaining
`next_retry_at`, sweeper re-enqueue audited, verdict rendering "will retry HH:MM".
