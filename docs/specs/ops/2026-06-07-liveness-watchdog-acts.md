# Liveness watchdog acts — re-enqueue stalled jobs + wire stall into status/verdict

Issue: #1510 (T4 of epic #1508). Extends #1500/#1507 (`job_liveness`), builds #649.
Relates #1484 (rate-limit backoff), prevention-log 1217 (terminal-state) + 1341 (universal gate).

## Problem

`jobs_liveness_watchdog` (#1507) DETECTS stalled jobs — zero `job_runs` rows over
K=3 cadence cycles despite ≥1 lifetime row and not running — but only **logs**
them. Two gaps remain:

1. **No action.** A silently-stopped scheduled job stays stopped until an operator
   notices. We have the audited manual-queue path (`publish_manual_job_request_with_conn`)
   used by `job_retry` (#1509) and `post_bootstrap_activation` (#1511) — wire it in.
2. **Invisible at the top line.** `app/api/system.py::_derive_overall_status` rolls
   up `failure` → `down` and `running`/stale-layer → `degraded`, but a stalled job's
   latest terminal row is an OLD `success`, so `last_status='success'` → `overall_status`
   stays `ok`. The standalone `/system/job-liveness` shows the stall; the headline does not.

The per-row Processes verdict (#1512) already paints a stalled job `attention` via the
`schedule_missed` stale reason — honest. This spec makes that row read `self_healing`
once the watchdog has re-enqueued it.

## Why a stalled job is safe to kick (cause-awareness, load-bearing)

The detector counts **any** `job_runs` status as a fire (#1507 design note). A job
blocked by the bootstrap gate or a per-job prerequisite writes a `skipped` row on every
scheduled fire (`record_job_skip`) → `recent > 0` → it is **never in the stalled set**.
Therefore a job that IS stalled has genuinely stopped firing (scheduler wedge, crash-loop,
mis-schedule) — re-enqueuing is the correct remedy, not a re-entry into a held limit.

The kick itself flows through the **universal bootstrap-state gate** that runs before
every dispatch path (prevention-log 1341). If the system is mid-bootstrap or the job's
prereq is unmet, the dispatcher rejects the request (`bootstrap_not_complete` etc.) — so
a kick that should not run does not run. That is the issue's "blocked by gate →
Needs-attention, not a retry storm" path, handled downstream for free.

## Storm bound (#1484)

Two guards, both mirrored from `job_retry`:

* **In-flight dedup + in-tx stall recheck.** Skip a candidate that already has a live
  `manual_job` `pending_job_requests` row or a `running` `job_runs` row. Detection
  (`find_stalled_jobs`) runs BEFORE the act loop, so a natural cadence fire could complete
  in the gap and the dedup probes would still be false at kick time (Codex ckpt-1 #3) —
  therefore, inside the per-candidate transaction, **re-assert the stall predicate**
  (`find_stalled_jobs` for this one job, or an inline "zero rows in the K-window") before
  publish. A job that fired in the gap is no longer stalled → skip. This closes the
  detect→act race; two watchdog fires (15 min apart) cannot double-dispatch.
* **Cooldown via `decision_audit`.** Before kicking, check for a `liveness_kick` audit
  row for this job newer than `max(cadence_period, COOLDOWN_FLOOR=6h)`. If one exists and
  the job is STILL stalled, the previous kick did not take (rejected by gate, or the
  scheduler is dead) → **do not re-kick**; the row stays `schedule_missed → attention`,
  (any kick request has aged past the §D freshness window so it no longer paints
  self-healing) — the honest "blocked" surface. This bounds re-dispatch to once per
  cadence/6h, not once per 15-min watchdog fire.

  Rationale for the floor: a daily job's cadence (24h) already self-bounds; a 5-min job's
  cadence does not, so the 6h floor prevents a wedged high-frequency job from being kicked
  every watchdog tick. (A genuinely-recovered kick writes a row → leaves the stalled set →
  the cooldown is moot.)

## Design

### A. Act service — `app/services/job_liveness_act.py`

Mirrors `app/services/job_retry.py::sweep_due_retries`. Autocommit conn; one
`conn.transaction()` per candidate (psycopg3 abort-safety — prevention `psycopg3_savepoint_commit`).

```python
def act_on_stalled_jobs(conn, *, stalled, eligible, now) -> ActResult:
    # stalled: Sequence[StalledJob] from find_stalled_jobs
    # eligible: Mapping[str, Cadence]  (SCHEDULED_JOBS minus orchestrator_*) —
    #           the cadence is needed for the per-job cooldown floor + in-tx recheck
    # returns ActResult(kicked: list[str], blocked: list[str])
    for job in stalled:
        cadence = eligible.get(job.job_name)
        if cadence is None:                           # not eligible (orchestrator_* / unknown)
            continue
        with conn.transaction():
            # In-tx stall recheck (Codex ckpt-1 #3): a natural fire may have landed
            # between detect and here. Re-assert zero rows in the K-window for THIS job;
            # if it fired, it is no longer stalled → skip.
            if not _still_stalled(conn, job.job_name, cadence, now):
                continue
            if _has_running_run(conn, job.job_name) or _has_active_request(conn, job.job_name):
                continue                              # in-flight dedup — defer
            if _kicked_within_cooldown(conn, job.job_name, cadence, now):
                blocked.append(job.job_name); continue  # already tried, still stalled ⇒ blocked
            publish_manual_job_request_with_conn(
                conn, job.job_name, requested_by='system:liveness_kick',
                process_id=job.job_name, mode='iterate')
            _write_liveness_audit(conn, job)          # decision_audit stage='liveness_kick'
            kicked.append(job.job_name)
    return ActResult(kicked, blocked)
```

`_still_stalled` reuses `job_liveness.window_start_for` + the same count query
`find_stalled_jobs` runs (lifetime≥1, recent==0, not running) for a single job — keep it a
thin wrapper so detection and recheck share the exact predicate (no drift).

`_kicked_within_cooldown` reads `decision_audit WHERE stage='liveness_kick' AND
evidence_json->>'job_name'=:name AND decision_time >= :cooldown_start`. `cooldown_start =
now - max(cadence_period(cadence), 6h)`; cadence looked up per job (passed in with each
`StalledJob` or via the eligible registry map).

**Decision (Codex):** duplicate `_has_running_run` / `_has_active_request` (the 4-line
queries) into `job_liveness_act` rather than extracting a shared module — they are tiny
local predicates and extraction creates coupling without enough shared behaviour.

### B. Watchdog body — `app/workers/scheduler.py::jobs_liveness_watchdog`

After `evaluate_liveness`, call `act_on_stalled_jobs` with the same eligible set the
retry sweeper builds (`SCHEDULED_JOBS` minus the two orchestrator names). Log
`kicked` / `blocked`. `tracker.row_count = len(stalled)` unchanged.

### C. overall_status — `app/api/system.py::_derive_overall_status`

Add param `stalled_job_names: set[str]`. New rule, between the `failure→down` and
`stale-layer→degraded` checks:

```python
if stalled_job_names: return "degraded"   # a job silently stopped firing
```

A stall is `degraded`, never `down` (it is recoverable + may already be self-healing).
The `/system/status` handler computes the stalled set by calling `find_stalled_jobs(conn,
jobs, now)` where `jobs` is the **orchestrator-excluded** registry — the SAME
`{JOB_ORCHESTRATOR_FULL_SYNC, JOB_ORCHESTRATOR_HIGH_FREQUENCY_SYNC}` exclusion the watchdog
uses (Codex ckpt-1 #2). Without it, sync-runs-tracked jobs (which never write `job_runs`)
would false-stall and permanently degrade the headline.

**Decision (Codex):** call `find_stalled_jobs` directly, not a cached watchdog result —
caching would make the headline stale and add invalidation semantics. One extra count query
per eligible job on the status endpoint, served by `idx_job_runs_name_started`
(`sql/014_ops_monitor.sql:18`); acceptable for an operator endpoint that already does N
`check_job_health` round-trips.

### D. verdict — `compute_verdict` + `ProcessRow` + adapter + `_convert_row`

Codex ckpt-1 #1: a live `manual_job` request only flips status→`running` when the latest
terminal was a `failure` (`scheduled_adapter.py:211-214`). A stalled job's latest terminal
is an old `success`(→`ok`) or `skipped`(→`idle`), so a kick does NOT make it `running` —
relying on a `status=="running"` branch would fail acceptance #1. Instead **mirror the
existing T3 `retry_in_flight` pattern**: a liveness kick IS the fix for a missed schedule,
so it suppresses `schedule_missed` exactly like a retry does, then a dedicated branch
returns `self_healing`. No adapter status machine change.

* `ProcessRow` gains `liveness_kick_in_flight: bool = False`.
* `scheduled_adapter` sets it via a **dedicated EXISTS probe** (Codex ckpt-1 #4 — do not
  reuse an unordered `LIMIT 1` requester read; multiple live manual requests can coexist):

  ```sql
  SELECT EXISTS (
      SELECT 1 FROM pending_job_requests
       WHERE request_kind = 'manual_job' AND job_name = %(name)s
         AND requested_by = 'system:liveness_kick'
         AND status IN ('pending','claimed','dispatched')
         AND created_at >= %(fresh_floor)s )      -- freshness bound, Codex ckpt-1 #5
  ```

  `fresh_floor = now - _LIVENESS_KICK_FRESH_WINDOW` (30 min = 2× watchdog interval). A kick
  request that has sat `pending`/`claimed` beyond the window is itself wedged (queue not
  draining / scheduler dead) → the probe returns False → the row falls back to its honest
  `schedule_missed`/`queue_stuck` → `attention` surface rather than masking as
  self-healing forever (Codex ckpt-1 #5). Process-wide death remains owned by
  `supervisor.py`/heartbeat (#719); this age-bound just stops a stuck kick from painting
  green indefinitely.

* `compute_verdict` gains `liveness_kick_in_flight: bool = False`:
  * In the suppression block (next to `retry_in_flight`): drop `schedule_missed` from
    `actionable` when `liveness_kick_in_flight` — the kick is the in-flight fix. Genuine
    wedges (`queue_stuck` / `mid_flight_stuck` / `watermark_gap`) are NOT dropped, so the
    actionable block still returns `attention` for them (ckpt-1 invariant preserved).
  * A dedicated branch AFTER the actionable block, BEFORE the status-only branches:

    ```python
    if liveness_kick_in_flight:
        return ("self_healing", True, "re-enqueued, recovering")
    ```

  A blocked stall (cooldown active, no fresh in-flight kick) keeps `schedule_missed` →
  `attention` — the honest surface, no new branch.

`_convert_row` passes `row.liveness_kick_in_flight` through.

## Out of scope

* Scheduler-process-wide death (a watchdog cannot report its own stall — owned by
  `supervisor.py` / heartbeat, #719).
* Changing K, the cadence-window math, or `find_stalled_jobs` (T4 only consumes it).
* `/system/job-liveness` response shape — it already lists stalled jobs; optionally add
  `kicked`/`blocked` to its body (nice-to-have, not required by acceptance).

## Acceptance (from #1510)

1. Stalled job, eligible, no in-flight request, outside cooldown → one audited kick →
   row reads `self_healing` "re-enqueued, recovering"; `decision_audit` stage
   `liveness_kick` row written. ✅ A/B/D
2. Stalled job kicked-and-still-stalled (gate-rejected / dead scheduler), inside
   cooldown → NOT re-kicked → stays `attention` (`schedule_missed`). ✅ storm-bound
3. `overall_status` returns `degraded` when ≥1 job is stalled. ✅ C

## Tests (lean — pure where possible)

* **Pure** `test_health_verdict`: `liveness_kick_in_flight=True` + `status='running'` →
  `self_healing`; + an actionable wedge (`queue_stuck`) → still `attention` (invariant).
* **Pure** `test_derive_overall_status`: non-empty stalled set → `degraded`; empty → unchanged.
* **Pure/seam** `act_on_stalled_jobs`: with a fake/seamed conn — eligibility filter,
  in-flight dedup, cooldown bound, audit written. Prefer table-tested decision helpers
  (`_kicked_within_cooldown` boundary) over a full DB harness; ONE `-m db` integration
  test exercising publish + audit end-to-end.
* **Dev-verify**: trigger the watchdog on dev (`POST /jobs/jobs_liveness_watchdog/run` is
  unauth in dev — run via the in-process path used for the finra backfill), confirm a
  genuinely-stalled job gets one `liveness_kick` audit row + the Processes row flips to
  self_healing; confirm `/system/status` `overall_status` reflects any stall.
