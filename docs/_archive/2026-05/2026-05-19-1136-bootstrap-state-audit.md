# #1136 — Bootstrap state-machine audit (Phase 4 trim) + operator-visibility endpoint

> Created **2026-05-19** as Phase A.3 of
> [`docs/superpowers/plans/2026-05-19-post-1208-cleardown.md`](../plans/2026-05-19-post-1208-cleardown.md).
> Partial-closes #1136 (umbrella stays open for the deferred items in §6).
> Same cadence as #1218 / #1010 (PR #1220 / PR #1222).

## 1. Problem

`bootstrap_state` has sat at `partial_error` since 2026-05-17 (run_id=3).
Operator drive-back to `complete` requires hitting "Retry failed" from
the admin UI, but the operator currently has no readout that answers
the two T9-POST questions cleanly:

1. **Which stages failed and which of those failures are
   operator-retryable** (i.e. `/system/bootstrap/retry-failed` would
   actually advance them) versus structural blockers that need a code
   fix first?
2. **What is the current retry-availability of the run overall** —
   would the next click do anything, or would it 4xx?

The existing `GET /system/bootstrap/status` endpoint returns the
per-stage rows verbatim (status / last_error / archive_results /
bulk_manifest), but does NOT compute a `retryable` predicate and does
NOT summarise whether `/retry-failed` would succeed. The operator has
to read every `last_error` and mentally replay the precedence rules
inside `reset_failed_stages_for_retry` to know what a retry will do.

There is also one stale-data failure mode the retry path cannot heal
on its own (see §3 below — S21 in run_id=3).

## 2. Goal

Three deliverables:

1. **Audit:** confirm the run_id=3 failure set, classify each failure
   as patched / retryable-by-current-contract / out-of-scope-deferred,
   and record the classification in this spec so reviewers can read
   the closure rationale without log-grepping.
2. **Operator endpoint:** `GET /system/bootstrap-status` (mirror of
   the `/system/postgres-health` lean readout shape) returning
   `(stage_key, status, last_error, retryable, attempt_count,
   completed_at)` per stage + a top-level summary + a
   `retry_available` / `retry_blocked_reason` pair so the operator can
   drive T9-POST without log-grepping.
3. **Dispatch hardening:** the dispatcher resolves `job_name` from
   `_BOOTSTRAP_STAGE_SPECS` by `stage_key` instead of reading it from
   `bootstrap_stages.job_name`. Removes the one stale-data failure
   mode the retry path cannot self-heal (S21 in run_id=3). Existing
   DB column stays in place for forensic audit; it just stops being a
   dispatch input.

## 3. Audit of run_id=3 — failure classification

```text
state: partial_error  last_run_id=3  last_completed_at=2026-05-17 05:30:37 UTC
```

Stages 1-15 success. Stages 16-24 below. Per-lane minimum-failed
`stage_order` (drives reset scope — see §4.2):

* `sec_rate` lane: `MIN(stage_order WHERE status IN
  ('error','blocked','cancelled')) = 17` → reset on retry touches
  S17-S22 (S16 stays put).
* `db` lane: same MIN = 23 → reset touches S23-S24 (S8-S12 stay put).

| Stage | Status | attempt | Failure mode | Patched by | Phase A.3 decision |
|---|---|---|---|---|---|
| 16 `sec_def14a_bootstrap` | **pending** | 2 | Run finalised with this stage still pending. `finalize_run` counts only error/blocked/cancelled toward `partial_error`; a stuck `pending` is silently passed through. The retry-failed reset is scoped to `stage_order >= MIN(sec_rate failed) = 17`, so S16 at order 16 is **never re-dispatched** by a retry click — that is the root of "stuck pending" for run_id=3. | nothing yet | DEFER — separate ticket; needs dispatcher root-cause investigation (why the run finalised with S16 still pending in the first place). Operator-visible via the new endpoint as `retryable=False, last_error=null`. |
| 17 `sec_business_summary_bootstrap` | error | 2 | `JobLock` contention — competing scheduled cron / manual run held the source advisory lock. Error message literally tells the operator to retry after the other run completes. | nothing — by design | KEEP behaviour; surface as `retryable=True`. Retry-failed reset puts it back to `pending`, next dispatch reclaims the lock. |
| 18 `sec_insider_transactions_backfill` | error | 2 | Same as S17. | nothing | Same as S17. |
| 19 `sec_form3_ingest` | error | 2 | Same as S17. | nothing | Same as S17. |
| 20 `sec_8k_events_ingest` | error | 2 | Same as S17. | nothing | Same as S17. |
| 21 `sec_13f_recent_sweep` | error | 2 | `unknown job_name 'bootstrap_sec_13f_recent_sweep'` — PR1c #1064 renamed the canonical to `JOB_SEC_13F_QUARTERLY_SWEEP` but the DB row's `job_name` was set at run-creation time and never updated. The dispatcher reads `_INVOKERS.get(stage.job_name)` (`bootstrap_orchestrator.py:1771`), the stale name returns `None`, the stage is marked error before any work runs. Retry-failed leaves `job_name` untouched, so the next retry hits the same wall. | **#1136 / this spec — §4.3 dispatch hardening** | FIX in scope. |
| 22 `sec_n_port_ingest` | error | 2 | Same as S17 (lock contention). | nothing | Same as S17. |
| 23 `ownership_observations_backfill` | blocked | 0 | `missing capability institutional_inputs_seeded; no surviving provider met rows floor 1 (providers: sec_13f_ingest_from_dataset=success [rows_processed=NULL], sec_13f_recent_sweep=?)` — bulk provider landed `success` but its `rows_processed` is NULL, so the strict-gate floor of 1 (#1140) marks the cap dead. | nothing yet | DEFER — separate ticket. Real ETL bug: the bulk ingester wrapper does not propagate row counts to `mark_stage_success`. Operator-visible via the new endpoint as `retryable=True` mechanically, **but the retry is expected to re-block immediately**: the retry resets only S23-S24 (db-lane reset floor=23), leaves the upstream success rows alone, and the cap-eval reads the same NULL-row-count providers and marks the cap dead again. Self-healing requires the row-count fix to land first. |
| 24 `fundamentals_sync` | blocked | 2 | `missing capability fundamentals_raw_seeded; no surviving provider met rows floor 1 (providers: sec_companyfacts_ingest=success [rows_processed=NULL])` — same shape as S23. | nothing yet | DEFER — same ticket as S23 (bulk providers' row-count propagation). `retryable=True` mechanically; re-blocks until row-count fix lands. |

**Net Phase A.3 fix budget:** one dispatch-hardening change (§4.3,
covers S21) + one operator endpoint (§4.1 + §4.2) + regression tests
(§5). Everything else either retries-clean today (S17-S20, S22), or
retries-and-re-blocks-honestly (S23-S24 — the new endpoint flags the
"retryable but expected to re-block" shape as `retryable=True`
because that matches what `/retry-failed` will actually do; the
operator's downstream observation that the re-block recurred is the
trigger to chase the row-count followup), or falls to a follow-up
ticket (S16 stuck-pending).

## 4. Design

### 4.1 Endpoint

`GET /system/bootstrap-status` lives in `app/api/system.py` (next to
`/system/postgres-health`). Pydantic response model:

```python
class BootstrapStatusSummary(BaseModel):
    total: int
    pending: int
    running: int
    success: int
    error: int
    blocked: int
    skipped: int
    cancelled: int


class BootstrapStatusStageOverview(BaseModel):
    stage_key: str
    stage_order: int
    lane: LaneApi          # reuse from app/api/bootstrap.py
    status: StageApiStatus # reuse from app/api/bootstrap.py
    last_error: str | None
    attempt_count: int
    completed_at: datetime | None
    retryable: bool


class BootstrapStatusOverview(BaseModel):
    state_status: BootstrapApiStatus
    current_run_id: int | None
    last_completed_at: datetime | None
    summary: BootstrapStatusSummary
    retry_available: bool
    retry_blocked_reason: Literal[
        "bootstrap_running",
        "no_prior_run",
        "state_not_resettable",
        "no_failed_stages",
    ] | None
    stages: list[BootstrapStatusStageOverview]
    collected_at: datetime
```

Auth: same `require_session_or_service_token` dependency the rest of
the `/system/*` router uses.

Failure posture (mirrors `/system/postgres-health`):

* DB unreachable → 503 with detail `"bootstrap status unavailable"`.
* No prior run → 200 with `state_status="pending"`,
  `current_run_id=None`, `stages=[]`, `retry_available=False`,
  `retry_blocked_reason="no_prior_run"`.

Snapshot read happens inside one `conn.transaction()` so the state +
run + stage rows come from a single READ COMMITTED window — same
contract as `_build_status_response` in `app/api/bootstrap.py`.

**Run-id pinning (Codex 1b §2):** the endpoint reads stages keyed
off `bootstrap_state.last_run_id`, NOT `ORDER BY bootstrap_runs.id
DESC LIMIT 1`. The two diverge transiently — `start_run` inserts a
new `bootstrap_runs` row inside a transaction that also flips the
singleton to that new id, but a reader observing in between (or a
post-restart sweep that re-seeded a row without touching the
singleton) would point at the wrong run. The retry semantics this
endpoint advertises target the singleton's `last_run_id` because
that is what `reset_failed_stages_for_retry` reads. So:

```python
state = read_state(conn)
if state.last_run_id is None:
    snap = None
else:
    snap = read_run_with_stages(conn, run_id=state.last_run_id)
```

A new helper `read_run_with_stages(conn, *, run_id)` lives next to
`read_latest_run_with_stages` in `app/services/bootstrap_state.py`
— same query, parameterised on `run_id` instead of `ORDER BY id
DESC LIMIT 1`. If the run row vanished (manual DB cleanup) the
helper returns `None` and the endpoint surfaces
`current_run_id=state.last_run_id` with `stages=[]` —
operator-visible "stale pointer" rather than a misleading old run.

### 4.2 Retryable computation

Pure function in `app/services/bootstrap_state.py` (new
`compute_retryable_view`, exported), unit-tested without a live DB.
Inputs: `BootstrapState`, `RunSnapshot | None`. Output: the
`(retry_available, retry_blocked_reason, per_stage_retryable: dict)`
triple the endpoint serialises.

Per-stage `retryable` is True iff `reset_failed_stages_for_retry`
would set this stage's `status` back to `pending` on the next call.
Per the helper's SQL ([bootstrap_state.py:718-749]
(../../../app/services/bootstrap_state.py)) the reset walks **every
same-lane row with `stage_order >= MIN(stage_order)` over failed
rows in that lane** — regardless of the row's own current status.
A `success` stage downstream of a same-lane failure is reset to
`pending` along with the failures. A `pending` stage upstream of
the first failure is NOT reset.

The predicate therefore is:

1. `state.status in ("partial_error", "cancelled")`, AND
2. `stage.lane` has at least one row in
   `(error, blocked, cancelled)`, AND
3. `stage.stage_order >= MIN(stage_order)` over those failed
   same-lane rows.

The stage's own current status is irrelevant to the predicate — it
is what the SQL reset will do, not what the row reads as today.

`retry_blocked_reason` precedence (first match wins, mirroring the
helper's exception precedence inside the singleton `FOR UPDATE`):

1. `state.last_run_id is None` → `"no_prior_run"`.
2. `state.status == "running"` → `"bootstrap_running"`.
3. `state.status not in ("partial_error", "cancelled")` →
   `"state_not_resettable"`.
4. No stage in `(error, blocked, cancelled)` →
   `"no_failed_stages"`.
5. Otherwise → `None` and `retry_available=True`.

**Worked example — run_id=3** (validates the implementation against
the live state in §3):

* `state.status="partial_error"`, `last_run_id=3` → not blocked at
  the state level.
* `sec_rate` lane: failed rows = {S17 error, S18 error, S19 error,
  S20 error, S21 error, S22 error}, `MIN=17`. → S16 (order 16) is
  **not** retryable (16 < 17 — that is the root of "stuck pending,"
  not "pending status itself"). S17-S22 all retryable.
* `db` lane: failed rows = {S23 blocked, S24 blocked}, `MIN=23`. →
  S23, S24 retryable. S8-S12 (success, orders 8-12) untouched
  because 8-12 < 23.
* `init` / `etoro` lanes: no failed rows → no resets in those lanes.
  S1, S2 retryable=False.

So the endpoint will report `retryable=True` for S17-S24 and
`retryable=False` for S1-S16 plus the success stages outside the
failed-lane reset window.

### 4.3 Dispatch hardening — resolve job_name by stage_key

Today `app/services/bootstrap_orchestrator.py:1771` reads
`_INVOKERS.get(stage.job_name)` where `stage` is the DB row. A
catalogue rename (PR1c #1064 → `JOB_SEC_13F_QUARTERLY_SWEEP`) cannot
backfill the in-DB column on existing runs, so a retry of an old run
hits an unknown job_name and errors before invocation.

Fix: build a `stage_key → spec` map from `_BOOTSTRAP_STAGE_SPECS` at
the top of `run_bootstrap_orchestrator`, look up `spec.job_name` by
`stage.stage_key`, and **fail closed** if the stage_key is not in the
catalogue (Codex 1a §5 — falling back to the DB row's `job_name` for
an unknown stage_key would dispatch a stale stage without canonical
params / lane / caps, the worst-of-both-worlds).

```python
spec_by_key = {spec.stage_key: spec for spec in _BOOTSTRAP_STAGE_SPECS}
# ...
canonical_spec = spec_by_key.get(stage.stage_key)
if canonical_spec is None:
    # Catalogue trim removed this stage_key but live DB rows still
    # exist (e.g. an old `dividend_calendar` row from a pre-#719
    # install). Refuse to dispatch — silently invoking
    # stage.job_name from the DB row would lose canonical params,
    # CapRequirement, and lane semantics. Mark error so the
    # operator sees the gap.
    #
    # mark_stage_error has `AND status = 'running'`; against a
    # `pending` row it no-ops silently and the stage would survive
    # into finalize_run still pending (Codex 1b §1). Run the
    # pending → running → error sequence the existing unknown-
    # job-name path already uses at bootstrap_orchestrator.py:1778.
    with psycopg.connect(database_url) as conn:
        mark_stage_running(conn, run_id=run_id, stage_key=stage.stage_key)
        mark_stage_error(
            conn,
            run_id=run_id,
            stage_key=stage.stage_key,
            error_message=(
                f"stage_key {stage.stage_key!r} not in current bootstrap catalogue; "
                f"row job_name={stage.job_name!r} is stale and dispatch is refused"
            ),
        )
        conn.commit()
    continue
effective_job_name = canonical_spec.job_name
invoker = _INVOKERS.get(effective_job_name)
```

`effective_job_name` must flow through every downstream consumer that
currently reads `stage.job_name`, not just the `_INVOKERS` lookup
(Codex 1a §3). Concretely:

* `validate_job_params(..., job_name=effective_job_name, ...)` at
  `bootstrap_orchestrator.py:1643` — otherwise the validator looks up
  a registry entry under the stale name and the stage still fails
  before invocation.
* `_RunnableStage.job_name = effective_job_name` at
  `bootstrap_orchestrator.py:1791` — propagates to `_run_one_stage`
  and the `_snapshot_job_runs_max_id(job_name=...)` rows-resolution
  helper.
* Structured log at info level when `effective_job_name !=
  stage.job_name`: `"bootstrap dispatcher: stage %s remapped
  stored_job_name=%r → effective_job_name=%r (catalogue rename)"`
  (Codex 1a §4). Preserves the forensic trail; the DB row is the
  long-term audit record, the log line records the runtime decision.

The DB column `bootstrap_stages.job_name` stays as the
audit-snapshot of "what the run was created to dispatch" — never
overwritten, never used for dispatch decisions. The audit value of
that column is preserved (Codex 1a §4 concern: do not silently
rewrite historical rows).

Single-site change. No schema migration. No data fix-up script — the
next retry of run_id=3 (T9-POST) self-heals because the dispatcher
no longer cares about the stale string. The forensic record of
`bootstrap_stages.job_name='bootstrap_sec_13f_recent_sweep'` survives
in the DB for anyone reading the run's history.

## 5. Tests

### 5.1 Pure-function tests (`tests/test_bootstrap_retry_view.py`, new)

`compute_retryable_view`:

* Empty state (no run) → `retry_available=False, reason="no_prior_run", stages=[]`.
* `state.status="running"` → `reason="bootstrap_running"`, every stage
  `retryable=False`.
* `state.status="complete"` → `reason="state_not_resettable"`.
* `state.status="pending"` with `last_run_id=None` →
  `reason="no_prior_run"`.
* `state.status="partial_error"` with all stages success →
  `reason="no_failed_stages"`.
* `state.status="partial_error"` with one stage in `error` →
  `retry_available=True`, that stage `retryable=True`, others
  `retryable=False` (different lane).
* Lane-downstream propagation: stages S17/S18 in same lane both
  `error`; S19 in same lane `success` but `stage_order > min(failed)`
  → S19 `retryable=True` (helper will reset it too).
* **Own-status irrelevance** (Codex 1b §3): a `pending` row in the
  same lane as a failure, with `stage_order >= MIN(failed_order)`,
  is `retryable=True`. The predicate looks at lane + order, not the
  row's own status.
* `pending` stage in a lane with another lane's failure →
  `retryable=False` (helper only touches the failed lane).
* Pre-min-order rows are NOT retryable even when same-lane failures
  exist: place S16 in `pending`, S17 in `error`, both in `sec_rate`;
  S16 has `stage_order < min(failed)` → `retryable=False`. Exact
  shape of run_id=3 S16.
* `cancelled` state with `cancelled` stages → all cancelled stages
  `retryable=True`.

### 5.2 API tests (`tests/test_api_bootstrap_status_overview.py`, new)

* 200 with `state="pending"` shape (no run).
* 200 with `state="partial_error"` shape — sum of summary counters
  equals total; `retry_available=True`; at least one stage
  `retryable=True`.
* 503 when `read_state` raises `psycopg.Error` (DB unreachable).
* Auth: 401 without session token.
* **Run-id pinning** (Codex 1c MEDIUM): mock `read_state` to return
  `last_run_id=42` and patch the new `read_run_with_stages(conn,
  run_id=...)` helper. Assert the endpoint passes 42 (not the
  latest-id-by-DESC). A second test seeds a `read_state` with
  `last_run_id=42` plus a `read_run_with_stages` that returns `None`
  (run row vanished); endpoint should still 200 with
  `current_run_id=42, stages=[]` — operator-visible stale pointer.

Mock pattern mirrors `tests/test_api_bootstrap.py`'s
`_install_conn()` + `dependency_overrides`.

### 5.3 Dispatch-hardening regression — `tests/test_bootstrap_orchestrator.py` (extend)

* `test_dispatch_resolves_job_name_from_spec_by_stage_key`: seed a
  `bootstrap_stages` row with `stage_key="sec_13f_recent_sweep"` and
  `job_name="bootstrap_sec_13f_recent_sweep"` (the deleted wrapper).
  Patch `_INVOKERS` to register only the canonical
  `JOB_SEC_13F_QUARTERLY_SWEEP`. Assert the dispatcher resolves the
  spec-side name AND param validation sees that name (Codex 1b §5):
  the test fixture seeds `params={"source_label":
  "sec_edgar_13f_directory_bootstrap"}` which is allow-listed via
  `JOB_INTERNAL_KEYS` for the canonical `sec_13f_quarterly_sweep`
  job; under the stale `bootstrap_sec_13f_recent_sweep` name the
  validator has no registry entry and rejects the param. The test
  asserts the stage reaches success (invoker called once), not just
  that the unknown-job-name branch wasn't taken.
* `test_dispatch_unknown_stage_key_fails_closed` (Codex 1b §4): seed
  a `bootstrap_stages` row with `stage_key="dividend_calendar"` (a
  catalogue-trimmed key from a pre-#719 install) and any `job_name`.
  Assert the dispatcher marks the stage `error` (NOT silently
  `pending`) with an error message containing the stale
  `stage_key`. Validates the mark_stage_running + mark_stage_error
  pair from §4.3 (against the silent-no-op pitfall in Codex 1b §1).

### 5.4 Catalogue-invariant — existing `tests/test_bootstrap_orchestrator_source_registry.py`

Existing test asserts every `_BOOTSTRAP_STAGE_SPECS.job_name` is
registered in `_INVOKERS`. Confirmed still passing on main; this PR
does not weaken it.

## 6. Out of scope — follow-up tickets to file at merge

Three findings the audit surfaced that are NOT addressed here:

1. **S16 stuck-pending root cause** — `finalize_run` and / or the
   dispatcher loop should sweep any non-terminal stage to `error` /
   `blocked` before finalising, or refuse to finalise. Needs
   replay-against-DB investigation. File a `tech-debt` ticket
   referencing run_id=3 S16 as the exemplar.
2. **Bulk ingester `rows_processed=NULL`** — `sec_13f_ingest_from_dataset`
   and `sec_companyfacts_ingest` land `success` without writing a row
   count to `mark_stage_success`, defeating the strict-gate floor.
   File an `area: filings` ticket referencing run_id=3 S23/S24 as the
   exemplars; #1140 (Task C) is the original strict-gate ticket and
   should cross-reference.
3. **JobLock contention with standalone crons during bootstrap retry**
   — S17/S18/S19/S20/S22 all errored with "another instance holds the
   advisory lock; retry after the other run completes." Behaviour is
   correct (lock prevents double-dispatch) and the error message
   directs the operator. No follow-up needed unless the noise becomes
   an operator-UX papercut, in which case the orchestrator can grow a
   `wait-for-lock-with-timeout` mode in a separate ticket.

Issue #1136 stays open after this PR merges; the umbrella's remaining
items (#1041 slow-connection, #649 self-healing freshness, #1064
admin hub follow-ups) stay tracked as documented in §1 of the plan.

## 7. Rollout

1. Branch `feature/1136-bootstrap-state-audit`.
2. Implement §4.1 + §4.2 + §4.3 + §5 in one PR.
3. Pre-push: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`.
4. Codex 2 on the diff. Push only after clean.
5. Bot review + CI poll.
6. Merge on APPROVE-on-latest + CI green.
7. Update auto-memory + close-out comment on the issue.
8. **T9-POST follow-up** (operator, not this PR): hit
   `/system/bootstrap/retry-failed`. The §4.3 dispatch hardening means
   S21 now invokes correctly. S17/S18/S19/S20/S22 retry-claim the
   locks (standalone crons have long-since released them). S16
   remains `pending` and is flagged by the new endpoint as
   `retryable=False` — that one's the followup-ticket case.

## 8. Acceptance

1. `GET /system/bootstrap-status` returns 200 with the §4.1 shape
   against the dev DB.
2. `retry_available=True` matches reality:
   `/system/bootstrap/retry-failed` returns 202 (not 404 / 409).
3. Per-stage `retryable` against run_id=3 reads as predicted by §4.2
   worked example: `retryable=True` for S17-S24, `retryable=False`
   for S1-S16 (S16 = before-first-failed-same-lane-order, not because
   it is pending).
4. New tests in §5 all pass.
5. No regression in existing `/system/bootstrap/status` shape — the
   richer endpoint is left untouched.
6. Bot review on the latest commit returns APPROVE and CI is green.
7. Auto-memory + issue close-out comment posted with merge SHA.
