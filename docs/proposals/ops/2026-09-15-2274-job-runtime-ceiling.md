# #2274 — a wall-clock runtime ceiling, as a VERDICT and not a status transition

Scope: the `max_runtime` option only. Not the heartbeat, not the no-progress watchdog, not
`dev_reload`'s reap-age refusal, not the consecutive-orphan-reap verdict. (The predecessor
doc numbers this option 2 and the ticket body lists it first; the options are an unnumbered
bullet list, so it is named here rather than numbered.)

Ordering comes from this ticket's own researched answer
(`docs/proposals/ops/2026-09-14-2274-tracked-job-heartbeat.md` §3): a real heartbeat
*removes* the stand-in `stale_detection` rule 4 provides, because a job that ticks forever
reads *Working* forever. The ceiling is what makes the heartbeat safe to have, so it lands
first — and the only form of it that has that property is one the heartbeat cannot mute.

## 1. The defect

`jobs_liveness_watchdog` (`app/workers/scheduler.py`) already finds every still-`running`
row and logs, for any older than 2h:

> `"possible wedge; the #1474 reaper will terminalise it past its threshold"`

**That sentence is false.** `reap_orphaned_job_runs` (`app/services/ops_monitor.py:717`)
has exactly one production caller — `app/jobs/__main__.py:1088`, at boot, with
`reap_all=True`. Its steady-state `timeout` predicate is documented in its own docstring as
existing *"for a future periodic watchdog"*, and no periodic watchdog ever called it. Short
of a process restart nothing acts on a wedged row, and the operator is told otherwise.

That is #2274's item 1: *"a running job with zero progress is a steady state, not a
terminal one … `_tracked_job` has no wall-clock ceiling and the liveness watchdog only
logs."*

## 2. The design that was specced first, and why it is wrong

Revision 1 of this document proposed the ticket's literal wording — terminalise the
`job_runs` row (`status='failure'`) once it passes its ceiling, from the watchdog. Codex
checkpoint 1 killed it, and the two decisive findings were verified by hand against the
code rather than taken on trust:

**⚠⚠ `job_runs.status='running'` is not a display state. It is a mutual-exclusion signal.**
`app/api/processes.py::_has_active_job_run` refuses a manual trigger while a row is
running, and its docstring says why:

> *"Any trigger that arrives during that window must still see the active run and refuse
> rather than (a) double-enqueue an iterate or (b) **reset watermarks under the running
> worker's feet** (Codex pre-push BLOCKING)."*

Terminalising a row whose worker thread is still alive removes that guard, so a full-wash
reset can run underneath live work. The v1 design would have re-opened a bug a prior Codex
pass had explicitly blocked. The same row also gates operator cancel
(`scheduled_adapter` drops `can_cancel`, the endpoint returns `no_active_run`), suppresses
`act_on_stalled_jobs` re-enqueues, and suppresses `job_retry`'s `_has_running_run`
protection — all of which a premature terminal status unlocks while the work continues.

**The boot reaper is not a precedent for it, either.** Its safety argument is explicitly
that *the owning process died* — it runs at boot step 4, before anything dispatches. It
documents nothing about terminalising a row whose thread is alive, and Python cannot
force-kill that thread (`app/jobs/job_connection.py` header, same limitation).

**⚠ A claim in revision 1 was also simply false.** It said a permanently-`running` row
defers `dev_reload` indefinitely. It does not: `_LIVE_JOB_SQL` requires
`last_progress_at IS NOT NULL`, so a row with a NULL heartbeat — which is every row on
every job but one — never defers a reload at all.

## 3. What actually ships: rule 5, `runtime_ceiling`

`app/services/processes/stale_detection.py` is a **pure-logic** four-rule stale model whose
rule 4 is already the "running too long" alarm:

```
mid_flight_stuck: status == "running"
                  AND COALESCE(last_progress_at, started_at) < now() - threshold
```

The ceiling is its sibling, and the difference is the whole point:

```
runtime_ceiling:  active_run_started_at IS NOT NULL
                  AND active_run_started_at      < now() - RUNTIME_CEILING_S
```

⚠ Gated on the **active run**, not on `status`. Codex checkpoint 2 found the
`status == "running"` version unreachable on a halted system:
`scheduled_adapter._status_for` returns `disabled` FIRST when the kill switch is on,
*before* it consults `has_running_row`, so the reason would never be emitted and its
`_WEDGE_STALE` membership (below) would be dead code. Halting the schedule does not end a
run that started before the halt, which is exactly when a ceiling breach matters. The
adapter builds `active_run` from the running row regardless of status, and every
non-running status reaches `compute` with `None`, so this gate is both stricter and
correct.

**It does not consult the heartbeat.** Rule 4 is muted the moment a producer ticks; rule 5
cannot be, because it measures the run's age and nothing else. That is precisely the
property §3 of the predecessor doc says the ceiling must supply before a heartbeat is safe
to switch on, and it supplies it without asserting anything false about any row.

Consequences, stated as the reason this shape was chosen: no row is mutated, so the
mutual-exclusion guard holds, cancel survives, no retry or liveness-kick churn is unlocked,
`dev_reload` gains no new power to SIGKILL live work, and no `sync_runs` / queue /
telemetry state goes inconsistent with `job_runs`. The operator gets the alarm; the system
keeps the truth.

### Files

1. `app/services/processes/stale_detection.py` — `RUNTIME_CEILING_S` beside the module's
   other rule constants, and rule 5. Pure; no new DB probe (`active_run_started_at` is
   already a parameter, already supplied by both adapters that have an active run).
2. `app/services/processes/__init__.py` — `"runtime_ceiling"` on `StaleReason`.
3. `app/services/processes/health_verdict.py` — the reason joins `ACTIONABLE_STALE`,
   `_WEDGE_STALE` (it is a genuine wedge, so it must stay `attention` under the kill switch
   — a halt does not un-stick a run that is already in flight), `_REASON_ORDER` and
   `_REASON_LABEL`; plus a headline branch placed **before** the `mid_flight_stuck` one so
   the stronger claim wins when both fire.
4. `app/workers/scheduler.py::jobs_liveness_watchdog` — the aged-running WARNING stops
   promising a reaper that does not exist, and keys on the ceiling.
5. `frontend/src/api/types.ts`, `frontend/src/components/admin/processStatus.ts` and the
   admin fixtures mirror — the union and the chip label.
6. `frontend/src/components/admin/ProcessRow.tsx` — the elapsed-since-heartbeat suffix on
   the verdict-reason line moves behind one `hasHeartbeatSuffix` predicate, and
   `runtime_ceiling` suppresses it. Codex checkpoint 2 again: `mid_flight_stuck` almost
   always fires alongside the ceiling, and the old condition keyed only on its presence, so
   a 25-hour run that ticked ten minutes ago rendered *"running past its runtime ceiling
   10m"* — an unrelated duration welded to an age claim. One predicate rather than two
   copies, so the memo signature and the rendered line cannot disagree.

### Mechanism scope

Rule 5 fires on `mechanism == "scheduled_job"` only.

- `ingest_sweep` passes `active_run_started_at=None` by construction (sweeps have no own
  active run), so it is unreachable there anyway.
- `bootstrap` is **excluded deliberately**. A one-time install drives 17 stages including
  multi-GB archive seeds; it has no cadence to bound a ceiling against and no measured
  duration distribution, and it already carries a `mid_flight_stuck` signal via its 1,800 s
  `stale_thresholds` override. Mirrors rules 1 and 2, which gate on mechanism for the same
  kind of reason.

## 4. The constant — construction and its measured counterfactual

No published or vendor rule exists for a background-job wall-clock ceiling, so per
"source-rule before design" the constant is fixed **by construction** and frozen, with its
derivation and reproducing query recorded beside it.

**Construction rule.** A ceiling is a *safety backstop, not a health alarm* — the repo's
own framing for a bounding constant ("a `LIMIT` on a maintenance branch is a safety
ceiling, not a rationing device", review-prevention-log). Its obligations are to sit above
every legitimate completion and to be finite; it is set an order of magnitude above the
measured population rather than fitted to it.

**`RUNTIME_CEILING_S = 86_400` (24 hours).**

Reproducing query — ⚠ over **every** status, not successes only. A success-only query
cannot establish the counterfactual "would this have fired", because the population it
would fire on is precisely the runs that never succeeded:

```sql
-- what a 24h ceiling would have fired on, registered jobs, 180 days
SELECT job_name, status, count(*),
       round(max(extract(epoch FROM coalesce(finished_at, now()) - started_at))) max_s
  FROM job_runs
 WHERE started_at > now() - interval '180 days'
   AND extract(epoch FROM coalesce(finished_at, now()) - started_at) > 86400
 GROUP BY 1, 2 ORDER BY 4 DESC;
```

Measured 2026-09-15 on the dev corpus: **9 runs across 6 registered jobs** exceed 24 h in
180 days, and **every one of them is a `failure`** — each an `orphaned: reaped at boot` row
that sat `running` until a restart happened to clear it. They include `jobs_retry_sweeper`
(a five-minute job) at 3.8 days and `monitor_positions` at 4.4 days. **Zero successful runs
of any registered job exceed 24 h**; the longest is `sec_filing_documents_ingest` at
9,662 s (2.7 h), so the ceiling sits 8.9× above the longest legitimate run.

So the false-positive count is not inferred from a margin — it is directly measured at
zero, on the whole 180-day population, and the true-positive set is nine rows that were
independently confirmed wedged by the reaper that eventually caught them. That is what the
ticket's constraint demands: *"a watchdog that fires on legitimately-long corpus jobs is
worse than none."*

⚠ Two honest limits on that evidence. The duration of a reaped row is **right-censored** —
it records when a restart happened, not when the work would have finished — so the
distribution understates long runs in the direction that matters. And the corpus is this
dev box; a slower environment or a larger population could move a legitimate run upward.
Both argue for the large margin rather than against the constant, and neither is a reason
to fit the number more tightly.

No existing threshold is touched. `stale_thresholds.DEFAULT_THRESHOLD_S` and `_OVERRIDES`
are unchanged — retuning those mutes an alarm rather than producing a signal.

No exemption list ships. `strategy_backtest_run` is the one job whose legitimate runtime
(21.2 h observed) is the same order as the ceiling, and it is **not in `SCHEDULED_JOBS`**,
so it renders no `scheduled_job` ProcessRow and rule 5 cannot reach it. An empty exemption
set would be a what-if abstraction; the fact is recorded here instead, and in §6.

## 5. Tests

Pure-logic (`tests/test_stale_detection.py`, no DB):

- a `running` row older than the ceiling fires `runtime_ceiling`;
- a `running` row under it does not (revert-probe: dropping the age comparison fails this);
- **a fresh `last_progress_at` does NOT mute it** — the single most important test, because
  it is the property that licenses the later heartbeat;
- **it still fires when the row reads `disabled`** (kill switch on, run still in flight) —
  the ckpt-2 finding above, and the test that keeps `_WEDGE_STALE` membership meaningful;
- it does not fire without an active run, nor on `bootstrap` / `ingest_sweep`;
- exact-boundary behaviour is pinned (strictly older than, consistent with rules 1/2/4).

`tests/test_health_verdict.py`:

- `runtime_ceiling` reads `attention` with the ceiling headline, and outranks a
  simultaneous `mid_flight_stuck`;
- it stays `attention` under the kill switch (wedge-set membership), where
  `schedule_missed` demotes to `paused`.

Frontend: the existing admin fixture/label parity tests extend to the new member; the union
in `types.ts` is what makes a missed label a typecheck failure rather than a blank chip.
Plus `ProcessRow.test.tsx` — a row with BOTH reasons renders exactly *"running past its
runtime ceiling"* and matches no `\d+m`.

## 6. What this does NOT do — so the next session does not inherit it as done

- **It does not stop the work, and it does not terminalise the row.** A wedged run still
  holds its `job_runs` row, its lane and its thread until a restart. That is deliberate
  (§2) and it means the ticket's literal "transitions to a terminal status" wording is
  **not** implemented; doing so safely needs a liveness oracle for the owning thread
  (`pg_stat_activity` + the `application_name` that `connect_job` already stamps is the
  obvious candidate) and that is a separate piece of work.
- **It does not reach the sync-orchestrator layers.** `scheduled_adapter` enumerates
  `SCHEDULED_JOBS`, and 22 job names ran in the last 90 days that are not in it —
  including `daily_candle_refresh`, `daily_research_refresh` and `strategy_backtest_run`.
  Those surface on the layers registry, not as ProcessRows, so they get no ceiling verdict.
  This is the same blind spot `ScheduledJob.statement_timeout_ms` has, from the same cause.
- **It does not catch the 2026-09-13 incident.** `daily_candle_refresh` reaped every ~50
  minutes by respawns is item 2 (`dev_reload`'s reap), untouched here — and it is one of
  the unreached jobs above besides.
- **It does not detect a running job making no progress.** That is the no-progress
  watchdog, and it needs the heartbeat to exist first.
- Incidental, recorded not fixed: `job_liveness.fetch_active_runs` filters
  `status='running' AND finished_at IS NULL`, so a malformed row with a terminal
  `finished_at` but a `running` status is invisible to it. No such row exists in the
  corpus; the filter is simply narrower than its docstring's "every still-running row".
