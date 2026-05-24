# Atomic bootstrap run enqueue + retry-failed + operator audit

**Date:** 2026-05-13
**Issue:** [#1139](https://github.com/Luke-Bradford/eBull/issues/1139)
(Task B of [#1136](https://github.com/Luke-Bradford/eBull/issues/1136) audit)
**Status:** Draft — pending Codex review

## 1. Problem

`POST /system/bootstrap/run` (`app/api/bootstrap.py::run_bootstrap`)
and `POST /system/bootstrap/retry-failed`
(`app/api/bootstrap.py::retry_failed`) call two side-effecting
functions back-to-back:

1. `start_run(conn, ...)` (or `reset_failed_stages_for_retry(conn, run_id=...)`)
   — runs inside its own `with conn.transaction():` block. Commits on
   success: `bootstrap_state.status='running'`, `bootstrap_runs` row
   inserted (or stages reset), pending stage rows seeded.
2. `publish_manual_job_request(JOB_BOOTSTRAP_ORCHESTRATOR, ...)` —
   `app/services/sync_orchestrator/dispatcher.py:105`. Opens a fresh
   `psycopg.connect(..., autocommit=True)` connection and inserts a
   `pending_job_requests` row + `pg_notify` for the listener.

If step (1) commits and step (2) raises (DB blip, pool exhausted,
insert/constraint failure, NOTIFY failure), the singleton stays at
`status='running'` with seeded `pending` stages and **no orchestrator
queue row**. From the API's perspective: 500. From the operator's
perspective: bootstrap stuck "running" with nothing draining; only
escape is a manual SQL or a `/mark-complete` (which is 409-gated by
`status='running'`, so won't even work).

Same shape for `/retry-failed`: reset commits, publish fails, run is
stranded.

Audit §3 in #1136 flagged this as a real window. Codex agreed in the
spec review for #1138.

Secondary defect, same audit clause: `start_run(operator_id=None, ...)`
is hardcoded in the API even when the request bears an authenticated
operator id. `bootstrap_runs.triggered_by_operator_id` therefore
always reads NULL for user-triggered runs, breaking audit forensics
("which operator kicked off this rebuild?"). The `/cancel` endpoint
already extracts the operator UUID via `_operator_uuid(request)`;
`/run` just doesn't use it.

## 2. Goal

Eliminate the stuck-run window by making run-creation/reset + queue
publish a **single transaction** on a single connection. Both
mutations land or neither lands. Propagate operator identity into
`bootstrap_runs.triggered_by_operator_id` for the audit trail.

## 3. Non-goals

- A boot-reaper sweep (issue option (c)). Once the txn is atomic the
  reaper has nothing to sweep — there is no "started but never
  queued" state to recover. If a later decoupled-queue path appears
  (cross-DB queue, message broker), a reaper is the correct fix at
  that point; punt to a follow-up ticket then.
- Auto-recovery of orphan rows from any pre-fix stranded runs.
  Cleanup is a one-shot explicit-SQL operator step on dev DB; the
  exact statements are in §6.
- Refactoring `cancel_run` / `force_mark_complete` to also wrap a
  publish — they don't publish to the queue in the current flow.

## 4. Design

### 4.1 Shared-connection publish helper

Add to `app/services/sync_orchestrator/dispatcher.py`:

```python
def publish_manual_job_request_with_conn(
    conn: psycopg.Connection[Any],
    job_name: str,
    *,
    requested_by: str | None = None,
    process_id: str | None = None,
    mode: Literal["iterate", "full_wash"] | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    """Same as publish_manual_job_request but uses the caller's
    connection. Caller is responsible for the surrounding transaction
    / commit. INSERT + pg_notify both stay buffered until the caller
    commits — Postgres flushes NOTIFY on commit, so the listener sees
    the wakeup exactly when (and only when) the queue row is durable.
    """
```

Implementation: same INSERT + pg_notify SQL as the existing helper,
just executed on `conn.cursor()` instead of `psycopg.connect(...)`.
No autocommit override; the caller owns the txn.

The existing `publish_manual_job_request(...)` stays — only the
bootstrap API needs the shared-conn variant. Other callers (jobs UI,
processes endpoints) still open their own conn.

### 4.2 API wrap

Refactor `run_bootstrap`:

```python
operator_id = _operator_uuid(request)
try:
    with conn.transaction():
        run_id = start_run(
            conn,
            operator_id=operator_id,
            stage_specs=get_bootstrap_stage_specs(),
        )
        requested_by = _identify_requestor(request)
        request_id = publish_manual_job_request_with_conn(
            conn,
            JOB_BOOTSTRAP_ORCHESTRATOR,
            requested_by=requested_by,
        )
except BootstrapAlreadyRunning as exc:
    raise HTTPException(409, ...)
```

`start_run` already opens its own inner `with conn.transaction():` —
psycopg3 nests that as a SAVEPOINT. If `publish_manual_job_request_with_conn`
raises, the SAVEPOINT is gone with the outer rollback and **nothing
commits**. Single-flight is preserved (the inner txn still acquires
the `FOR UPDATE` lock on `bootstrap_state`).

Same shape for `retry_failed`: drop the pre-lock `read_state` call
(it's racy — Codex R1 W §6 / §7). Change
`reset_failed_stages_for_retry`'s signature to **derive** the target
run id from the singleton under lock — no `run_id` parameter, no
caller-supplied stale read. Then wrap the helper +
`publish_manual_job_request_with_conn` in one `with conn.transaction():`
on the API side. The helper returns `(run_id, reset_count)` so the
API has both for the response body.

### 4.3 `reset_failed_stages_for_retry` hardening (Codex R1 BLOCKING §2 + §3, R2 BLOCKING §1)

Current signature:

```python
def reset_failed_stages_for_retry(conn, *, run_id: int) -> int:
```

New signature:

```python
def reset_failed_stages_for_retry(conn) -> tuple[int, int]:
    """Reset failed + later-numbered same-lane stages for retry.

    Derives the target ``run_id`` from ``bootstrap_state.last_run_id``
    under the same ``FOR UPDATE`` lock that the state-check uses.
    Caller passes nothing.

    Returns ``(run_id, reset_count)`` on success. ``reset_count`` is
    0 when the singleton is in a resettable status but the latest
    run has no failed stages — the helper does NOT flip state in
    that case (nothing to retry) and the API maps it to 404.

    Raises (precedence order — first matching wins):
      BootstrapNoPriorRun      — singleton.last_run_id IS NULL,
                                 regardless of status (API 404,
                                 preserves the existing /retry-failed
                                 "no prior bootstrap run to retry" 404
                                 contract — Codex R3 WARNING §2 /
                                 R4 WARNING §1).
      BootstrapAlreadyRunning  — singleton.status == 'running' (API 409).
      BootstrapNotResettable   — singleton.status in {pending, complete};
                                 i.e. not in {partial_error, cancelled}.
                                 (API 409 ``bootstrap_not_resettable``.)
    """
```

`BootstrapNoPriorRun` precedes the status check on purpose: a fresh
install (`pending + last_run_id NULL`) and a wipe-then-mark-partial
(`partial_error + last_run_id NULL`) both deserve the same operator
message — "there is no run to retry; trigger /run first" — and the
existing endpoint returned 404 for both. Keeping the 404 shape
preserves any FE/runbook that already depends on it.

Resolution shape (precedence top-to-bottom):

| Singleton state at FOR UPDATE                                          | Helper return / raise                                            | API status                       |
| ---                                                                    | ---                                                              | ---                              |
| `last_run_id IS NULL` (any status)                                     | raises `BootstrapNoPriorRun()`                                   | 404 `no_prior_run`               |
| `running` (last_run_id set)                                            | raises `BootstrapAlreadyRunning(run_id)`                         | 409 `bootstrap_running`          |
| `pending` or `complete` (last_run_id set)                              | raises `BootstrapNotResettable(status)`                          | 409 `bootstrap_not_resettable`   |
| `partial_error` or `cancelled`, valid `last_run_id`, no failed stages  | returns `(last_run_id, 0)`  (state untouched)                    | 404 `no_failed_stages`           |
| `partial_error` or `cancelled`, valid `last_run_id`, ≥1 failed stage   | returns `(last_run_id, n)` and flips state back to `running`     | 202                              |

`BootstrapRunIdMismatch` from the previous draft is dropped — once
the helper derives `run_id` under lock there is no stale id to
compare against.

Both call sites updated:

- `app/api/bootstrap.py::retry_failed` — drop the pre-lock
  `read_state` call entirely. Call the no-arg helper. Catch
  `BootstrapAlreadyRunning` → 409 (existing). Catch
  `BootstrapNotResettable` → 409 `bootstrap_not_resettable`. Catch
  `BootstrapNoPriorRun` → 404 (existing 404 text preserved). On
  return: if `reset_count == 0` → 404 `no failed stages to retry`
  (existing 404 text preserved). On `reset_count > 0` → enqueue
  publish, return 202.
- `app/api/processes.py::_apply_bootstrap_iterate_reset` (Codex R2
  BLOCKING §2 + R3 BLOCKING §1) — drop the local
  `SELECT last_run_id` pre-read; the helper is now authoritative.
  Catch:
  - `BootstrapAlreadyRunning` → `_conflict("bootstrap_already_running")` (existing).
  - `BootstrapNotResettable` → `_conflict("bootstrap_not_resettable")` (new — without this the call site would surface 500).
  - `BootstrapNoPriorRun` → `_conflict("bootstrap_not_resumable")` (preserves the helper's current 409 text).
  - Return value: if `reset_count == 0`, **do not enqueue the
    orchestrator** — surface `_conflict("bootstrap_no_failed_stages",
    advice="latest run has no failed stages to iterate")` so the
    processes shim doesn't fire a no-op orchestrator run. Pre-fix
    the helper raised on no-failed silently and the caller still
    enqueued; the new shape makes the no-op explicit (Codex R3
    BLOCKING §1).

Add two new exception classes in `bootstrap_state.py`:
`BootstrapNotResettable` (carries `status` for the response detail)
and `BootstrapNoPriorRun` (parameterless). Keep
`BootstrapAlreadyRunning` (unchanged).

Existing direct callers of the helper (Codex R4 WARNING §2) that
must update from `reset_failed_stages_for_retry(conn, run_id=...)`
→ `reset_failed_stages_for_retry(conn)` and unpack the tuple
return:

- `tests/test_bootstrap_cancel.py:479`
- `tests/test_bootstrap_cancel.py:663`
- `tests/test_bootstrap_state.py:188`
- `tests/test_bootstrap_state.py:220`
- `tests/test_bootstrap_flow_integration.py:210`

Each call site is in a test that already seeds the singleton
appropriately (sets `last_run_id` before calling the helper); the
update is mechanical — drop the kwarg, unpack `(run_id, count)`,
compare `count` to the existing assertion. No semantic change for
the cancelled-stage retry path that those tests cover.

### 4.4 Operator identity propagation (Codex R1 BLOCKING §4 — scope to /run only)

`start_run`'s `operator_id` parameter type is `str | None`. The DB
column is `UUID REFERENCES operators(operator_id)`. Widen to `UUID
| None` for type-clarity and pass the parsed UUID through on the
**`/run` path only**.

`/retry-failed` re-uses the existing `bootstrap_runs` row; updating
its `triggered_by_operator_id` would corrupt the original-run audit
("who started this run?" → would silently change to the retry
operator's UUID). Don't touch the column on retry. Existing audit of
who-triggered-retry can ride on application logs (already emitted by
`logger.info("bootstrap: retry-failed ...")`); a separate audit
column is out of scope and filed as follow-up tech-debt if needed.

Reuse the existing `_operator_uuid(request)` helper on the API side
(it already handles "not a UUID → log + audit as NULL" for malformed
state). Pass into `/run` only.

Service-token-only paths leave `triggered_by_operator_id` `None`
honestly — documented in the `start_run` docstring as "NULL is
correct for service-token initiated runs; only operator-session
callers populate the column".

### 4.5 NOTIFY semantics under shared txn

The existing autocommit version fires NOTIFY immediately on each
statement. The shared-conn variant defers NOTIFY to outer commit
(PostgreSQL's actual semantics — `pg_notify` queues are flushed at
commit boundary). The orchestrator listener observes the wakeup the
moment the queue row is durable — strictly safer than the
fire-then-commit ordering of the old code, which could in principle
have woken the listener for a row that hadn't yet committed (psycopg
autocommit puts each statement in its own txn so it commits before
the NOTIFY queue flush, but the ordering was racier in the
multi-statement case). The new shape is correct-by-construction.

## 5. Tests

### 5.1 Mock-level (unit, in `tests/test_api_bootstrap.py`)

- **Existing happy-path test stays** — covers normal flow.
- **New: publish failure rolls back start_run**. Patch
  `app.api.bootstrap.publish_manual_job_request_with_conn` to raise.
  Assert response is 500 (or 503 — see §5.3) AND that `start_run`'s
  effects don't leak (the mock conn's transaction context manager
  saw an exception). Since these are mocks, the verification is "the
  conn's `__enter__`/`__exit__` were entered + exited with an
  exception"; real rollback semantics belong in the integration test.
- **Same for retry-failed.**
- **New: operator UUID passed through**. Stub request middleware to
  set `request.state.operator_id = "<uuid>"`. Assert `start_run` is
  called with `operator_id=<UUID>` (not None).

### 5.2 Real-DB integration (new file `tests/test_bootstrap_atomic_enqueue.py`)

Uses `ebull_test_conn` per `feedback_test_db_isolation`.

- **rollback_on_publish_failure** — call the API handler via
  TestClient with a monkeypatch that makes
  `publish_manual_job_request_with_conn` raise. After the call,
  query `bootstrap_state.status`, `bootstrap_runs`, `bootstrap_stages`
  — all unchanged (no `running` row, no seeded stages). Also assert
  no `pending_job_requests` row exists for the bootstrap job.
- **commit_on_success** — opposite: both tables populated, queue row
  present, NOTIFY fires. Verified by a dedicated `LISTEN` connection
  that has issued `LISTEN ebull_job_request` and committed BEFORE
  the API call; poll `conn.notifies()` for up to 5s after the
  publishing commit (the autocommit-era `~100ms` budget was flaky).
- **retry_failed_rollback** — seed a `partial_error` run with a
  failed stage, monkeypatch publish to raise, call
  `/retry-failed`, assert stage stays `error` (not reset to
  `pending`) and state stays `partial_error`.
- **retry_failed_targets_singleton_last_run_id** (Codex R1 WARNING
  §7 — superseded shape) — seed run 1 in `partial_error`, then
  insert run 2 in `partial_error` and explicitly set
  `bootstrap_state.last_run_id = 2`. Call `/retry-failed`. Assert
  the helper resets stages on run 2 (not run 1) and that run 1's
  stage rows are untouched. The new helper derives the target run
  from the singleton under lock — there is no stale caller-supplied
  id to mismatch against, so the race the audit flagged is
  structurally impossible.
- **retry_failed_no_prior_run** — singleton at `partial_error`,
  `last_run_id IS NULL` (e.g. a wipe-then-mark-partial-error edge
  case). Call `/retry-failed`; assert 404 — existing contract
  preserved (Codex R3 WARNING §2). Helper raises
  `BootstrapNoPriorRun`.
- **retry_failed_pending_state_no_prior_run_404** (Codex R4 WARNING
  §1) — singleton at `pending` with `last_run_id IS NULL` (the
  fresh-install state). Call `/retry-failed`; assert 404 (the
  no-prior-run branch precedes the status check — preserves the
  existing 404 contract for fresh installs).
- **retry_failed_pending_with_orphan_last_run_409** — synthetic
  fixture where singleton is `pending` but `last_run_id` is set
  (shouldn't happen in normal flow but possible via raw SQL).
  Assert 409 `error="bootstrap_not_resettable"` — status check
  fires after no-prior-run check is bypassed.
- **retry_failed_no_failed_stages** — singleton at `partial_error`,
  `last_run_id` set, but all stages already `success`. Helper
  returns `(run_id, 0)`. API maps to 404, **does NOT enqueue**
  (assert `pending_job_requests` count unchanged) — pre-fix
  behaviour: also no enqueue (current API already 404-guarded).
- **retry_failed_wrong_status** — singleton at `complete`. Call
  helper directly; assert `BootstrapNotResettable`; assert state
  still `complete`. API-level twin: call `/retry-failed`; assert
  409 `error="bootstrap_not_resettable"`.
- **iterate_reset_processes_endpoint_widened_catch** (Codex R2
  BLOCKING §2 — implementation finding) — the processes endpoint's
  PR-#1071 precondition gate at
  `app/api/processes.py::_check_bootstrap_iterate_preconditions`
  catches `complete`/`pending` *before* the trigger reaches
  `_apply_bootstrap_iterate_reset`, surfacing
  `bootstrap_not_resumable` (lines 911-917). The helper-level
  `BootstrapNotResettable` catch (lines 1072-1076) is therefore
  reachable only via a TOCTOU race between the precondition gate
  and the singleton FOR UPDATE — not via a straightforward complete-
  state call. The defense-in-depth catch is still required (without
  it the race surfaces as 500); the explicit complete-state test in
  the original spec is unreachable in practice and is dropped in
  favour of:
  - The existing `test_trigger_bootstrap_iterate_from_pending_returns_409`
    (precondition gate hit; preserved behaviour).
  - **iterate_reset_processes_no_failed_stages_no_enqueue** below
    (helper-level no-op-no-enqueue path; exercises the new code).
- **iterate_reset_processes_no_failed_stages_no_enqueue** (Codex
  R3 BLOCKING §1 + WARNING §4) — singleton at `partial_error`,
  no failed stages. Hit the processes iterate shim. Assert 409
  `error="bootstrap_no_failed_stages"` AND that
  `pending_job_requests` has no new orchestrator row (the
  processes endpoint must not enqueue on the no-op).
- **iterate_reset_processes_no_prior_run** (Codex R3 WARNING §4)
  — singleton at `partial_error`, `last_run_id IS NULL`. Hit the
  iterate shim. Assert 409 `error="bootstrap_not_resumable"`
  (preserves the existing processes-endpoint text).
- **operator_uuid_persisted** — call `/run` with a stubbed operator
  session A; assert `bootstrap_runs.triggered_by_operator_id =
  <uuidA>`. Then (Codex R4 NIT §3 + R5 NIT §1) transition the
  run to a fully-consistent retryable state by directly setting
  one stage to `error`, flipping `bootstrap_runs.status =
  'partial_error'` with `completed_at = now()`, AND flipping
  `bootstrap_state.status = 'partial_error'` — keeps the run +
  singleton consistent (don't drive a real orchestrator round in
  this test). Call `/retry-failed` with a
  *different* operator session B; assert the call succeeds AND
  that `bootstrap_runs.triggered_by_operator_id` is still
  `<uuidA>` — NOT overwritten to B (Codex R1 BLOCKING §4
  regression guard). The retry path does not read or write the
  column at all in the new code, but the test pins the invariant.

### 5.3 Error-code shape (Codex BLOCKING §1 — phase-aware)

Catching a bare `Exception` around the combined block mislabels
`start_run` failures (FK violation, schema drift, programming error)
as `queue_publish_failed`. Phase-aware shape:

- `start_run` raises `BootstrapAlreadyRunning` → 409 (existing
  contract, unchanged).
- `start_run` raises anything else → propagate untouched; FastAPI
  turns it into 500 with the original stack in logs. No re-wrap.
- `publish_manual_job_request_with_conn` raises
  `psycopg.OperationalError` → 503 with `{"error":
  "queue_publish_failed"}`. Transient; FE renders retry toast.
- `publish_manual_job_request_with_conn` raises anything else →
  propagate untouched → 500. Programming error or constraint
  violation; needs a fix, not a retry.

Concrete shape:

```python
try:
    with conn.transaction():
        run_id = start_run(conn, operator_id=operator_uuid, stage_specs=...)
        try:
            request_id = publish_manual_job_request_with_conn(
                conn, JOB_BOOTSTRAP_ORCHESTRATOR, requested_by=requested_by,
            )
        except psycopg.OperationalError as exc:
            logger.exception("bootstrap: queue publish failed (transient)")
            raise HTTPException(503, {"error": "queue_publish_failed"}) from exc
except BootstrapAlreadyRunning as exc:
    raise HTTPException(409, ...)
```

The inner `HTTPException` raise inside `with conn.transaction():`
propagates out — psycopg sees the exception and rolls the outer
txn back. FastAPI then serves the 503.

## 6. Migration / rollout

- No schema change.
- No data migration. Pre-existing stranded `running` rows (if any
  exist in dev/prod from before this fix) cannot be cleared via
  `/mark-complete` — that endpoint is 409-gated by
  `status='running'` (Codex R1 WARNING §5 — earlier spec wording
  was wrong). Operator cleanup path is explicit SQL on dev DB,
  honouring the existing CHECK constraint
  (`status IN ('pending','running','complete','partial_error','cancelled')`
  per `sql/129_bootstrap_state.sql` + `sql/136_bootstrap_runs_cancel.sql`):

  ```sql
  -- Stale-run cleanup (use only if you confirm no orchestrator is draining)
  UPDATE bootstrap_state SET status='partial_error' WHERE id=1 AND status='running';
  UPDATE bootstrap_runs  SET status='partial_error', completed_at=now()
   WHERE status='running';
  UPDATE bootstrap_stages SET status='error', last_error='stranded-pre-1139'
   WHERE status IN ('pending','running');
  ```

  (`bootstrap_stages.status` accepts `'error'`; only the parent
  `bootstrap_runs.status` is constrained — Codex R2 BLOCKING §3.)

  Then `/mark-complete` or `/retry-failed` is reachable again. This
  is a one-shot transitional cleanup; post-merge no new stranded
  rows can be created.
- Backwards-compatible: callers of `publish_manual_job_request`
  (non-bootstrap) are not touched.

## 7. Smoke / verification

`uv run pytest tests/test_api_bootstrap.py tests/test_bootstrap_atomic_enqueue.py tests/smoke/test_app_boots.py -q`

Manual verification on dev DB:

1. Start dev stack.
2. `curl -X POST :8000/system/bootstrap/run -H 'X-Service-Token: ...'`
   — expect 202 with run_id + request_id.
3. Inspect: `psql -c "SELECT status FROM bootstrap_state"` shows
   `running`; `psql -c "SELECT request_id FROM pending_job_requests
   ORDER BY request_id DESC LIMIT 1"` shows the new row.
4. Operator-side audit: `psql -c "SELECT triggered_by_operator_id
   FROM bootstrap_runs ORDER BY id DESC LIMIT 1"` is the operator's
   UUID (not NULL).

## 8. Risk / rollback

Risk: the shared-conn variant of publish bypasses the autocommit
contract. If a future caller copies the pattern but forgets the
surrounding `with conn.transaction():`, the queue insert will sit
in an open implicit psycopg txn until pool checkin, which commits.
The risk is "row arrives later than expected" — never destructive.
Mitigation: the helper is named explicitly `_with_conn` and the
docstring says "caller owns the txn".

Rollback: revert the PR. No DB shape change, no data migration.

## 9. Out-of-scope follow-ups

- A reaper exists in skeleton at `app/jobs/runtime.py` for orphan
  process cleanup; if a future cross-DB or message-broker queue
  splits this txn back apart, add `bootstrap` to its sweep then.
- Task C / #1140 (capability + rows_written gates) is unblocked by
  #1138 and lands separately.
