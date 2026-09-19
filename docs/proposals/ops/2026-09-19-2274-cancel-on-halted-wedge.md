# #2274 — `can_cancel` on a halted wedge

Scope: the `can_cancel` flag on `mechanism="scheduled_job"` rows, plus the two documents
that record its absence as deliberate. Not the watchdog's acting half, not
`daily_candle_refresh`'s `_OVERRIDES` entry, not the cancel endpoint (which already
permits this and is unchanged), not which jobs cooperate with a stop signal (§5).

## 1. The defect

`app/services/processes/scheduled_adapter.py:1257`:

```python
can_cancel = (
    active_run is not None and active_run.run_id is not None and process_status == "running"
)
```

`_status_for` (same file, :234) returns `disabled` **first** when the kill switch is on,
*before* it consults `has_running_row`. Halting the schedule does not end a run that
started before the halt, so that run keeps its `job_runs` row, keeps producing
`active_run` — `_build_row` builds it from `active_row` with no status term — and loses
its cancel affordance.

The implication that matters is the narrow one, and it is exhaustive over `_status_for`'s
branches: **an active row implies `status ∈ {running, disabled}`**. The converse does not
hold — `running` is also reached with `active_row is None` via the auto-hide branch (a
failed terminal with a retry in flight), which the first two terms already exclude. So
dropping the status term widens the flag on exactly one input: `_kill_switch_active`
returning True.

⚠ That includes the **fail-closed** read. `_kill_switch_active` (:296) returns True when
the singleton row is missing, deliberately ("Mirrors `ops_monitor.get_kill_switch_status`
semantics so the adapter is honest under configuration corruption"). Under corruption the
row will now offer cancel. That is the right direction — a stop action is the one control
that should survive a state you cannot read — but it is a second newly-reachable input,
not a side effect of the first, and it gets its own test.

### It is the finding rule 5 already took, in the same file

`docs/proposals/ops/2026-09-15-2274-job-runtime-ceiling.md` §3, Codex checkpoint 2:

> Gated on the **active run**, not on `status`. … `_status_for` returns `disabled` FIRST
> when the kill switch is on, *before* it consults `has_running_row` … Halting the
> schedule does not end a run that started before the halt, which is exactly when a
> ceiling breach matters.

The verdict was moved off `status`. The flag beside it was not.

### The contradiction is rendered on one row

Run, not reasoned (`stale_detection.compute` + `health_verdict.compute_verdict`, pure,
`mechanism="scheduled_job"`, `status="disabled"`, `active_run_started_at = now − 30 h`):

```
stale_reasons -> ('runtime_ceiling',)
verdict       -> ('attention', False, 'past runtime ceiling')
```

and `ProcessRow.tsx:209` renders on that same row:

```
cancelTooltip = row.can_cancel ? "Cooperative cancel — …" : "No active run to cancel."
```

So the operator reads **`attention · past runtime ceiling`** beside a greyed Cancel
saying **there is no active run**. (No ticking suffix accompanies it: `hasHeartbeatSuffix`
suppresses the elapsed-since-heartbeat text precisely when `runtime_ceiling` is present.
The contradiction is the tooltip against the headline, and nothing else.)

### Nothing in the cancel path consults the kill switch

`app/api/processes.py::cancel_process` (:1759) resolves the latest `status='running'`
`job_runs` row under `SELECT … FOR UPDATE`, calls
`app/services/process_stop.py::request_stop`, sets `cancel_requested_at`. Neither reads
the switch. **The API accepts precisely the cancel the button refuses to offer.**

The hub spec's preconditions matrix (`docs/proposals/ui/admin-control-hub-rewrite.md`)
lists `kill_switch OFF` for the **trigger** modes at :647-650 — `iterate` / `full_wash` —
and cancel's own row (:639) is `409 no_active_run` / `409 stop_already_pending`, naming no
switch. ⚠ Narrow claim only: this says the switch gates those two API preconditions and
not this one. It is **not** a claim that the switch universally gates job start — the
worker dispatch and retry paths carry their own gating and are out of scope here.

Checked by effect rather than by topic: `docs/settled-decisions.md` §"Cancel UX (#1064,
settled 2026-05-09)" fixes cancel as cooperative-with-checkpoints and names the wedged-
worker escape hatch; it says nothing about the kill switch. §"Execution guard semantics /
Kill switch" governs the trade path. Neither is reversed. The two places that DO record
this behaviour record it as a **deferral, not a decision**, and both are amended by this
change (§3).

## 2. Occurrence — measured, not bounded

⚠ An earlier draft of this section said the kill switch has "no history table, so the
overlap is not measurable". **That is false** and Codex checkpoint 1 caught it:
`ops_monitor.activate_kill_switch` / `deactivate_kill_switch` call
`runtime_config.write_kill_switch_audit` in the same transaction as the `kill_switch`
UPDATE, so `runtime_config_audit WHERE field = 'kill_switch'` is the history. Measured on
the dev corpus, observed at **2026-09-19 02:55:55Z**:

```sql
SELECT changed_at, changed_by, old_value, new_value, reason
  FROM runtime_config_audit WHERE field = 'kill_switch' ORDER BY changed_at;
```

Four rows, two halt intervals:

| from | to | duration | reason |
| --- | --- | ---: | --- |
| 2026-06-23 23:20:02.585Z | 2026-06-23 23:20:02.613Z | 28 ms | `dev-verify #1231` |
| **2026-06-28 01:32:30.918Z** | **2026-09-18 15:36:16.200Z** | **82.6 days** | `autonomy loop unattended — block any order path (monitor boot)` |

**The switch was on for 82 days**, ending 2026-09-18 with the attended demo session. For
all of it every one of the 65 `SCHEDULED_JOBS` rows read `disabled`, so no scheduled job
had a cancel affordance at all — the halted **wedge** is the sharpest case, not the only
one.

Inside that window, restricted to the 65 `SCHEDULED_JOBS` names (the only population that
renders these buttons), intervals clipped to the window and merged before summing, so
this is occupancy and not a count of touched time-buckets. ⚠ Reproducing query, because a
figure written by hand goes stale silently — `%(names)s` is
`sorted({j.name for j in SCHEDULED_JOBS})`, and `HALT_START` / `HALT_END` come from the
audit query above:

```sql
WITH r AS (
  SELECT greatest(started_at, %(start)s::timestamptz)                     AS s,
         least(coalesce(finished_at, now()), %(end)s::timestamptz)        AS f
    FROM job_runs
   WHERE job_name = any(%(names)s)
     AND started_at                       < %(end)s::timestamptz
     AND coalesce(finished_at, now())     > %(start)s::timestamptz
), grouped AS (
  SELECT s, f, sum(CASE WHEN s > maxf THEN 1 ELSE 0 END) OVER (ORDER BY s) AS grp
    FROM (SELECT s, f,
                 max(f) OVER (ORDER BY s ROWS BETWEEN unbounded preceding AND 1 preceding) AS maxf
            FROM r) t
)
SELECT round(sum(extract(epoch FROM mx - mn)))                        AS busy_s,
       round(extract(epoch FROM %(end)s::timestamptz - %(start)s::timestamptz)) AS window_s
  FROM (SELECT grp, min(s) AS mn, max(f) AS mx FROM grouped GROUP BY grp) u;
```

Results at the observation timestamp above:

- **99,960 runs across all 65 jobs** were in flight at some instant inside it;
- ≥1 run in flight for **3,338,277 s of 7,135,425 s = 46.8%** of the window;
- **8 runs** (5 jobs) spent time **above** the 24 h `RUNTIME_CEILING_S` inside it —
  `started_at + 24 h` before the halt ended and `finished_at` after it began:
  `monitor_positions` ×2 (105.0 h, 104.0 h), `jobs_retry_sweeper` ×2 (91.0 h, 78.6 h),
  `expected_filings_poller` ×2 (53.6 h, 44.6 h), `sec_per_cik_poll` (40.8 h),
  `thesis_refresh` (24.4 h).

⚠ Three honest limits, none of which the design rests on:

1. Those 8 are **counterfactual** for the verdict, not historical: rule 5 shipped
   2026-09-15 (`7f8d2b99`), after the window. They are what the ceiling *would* have
   painted; the affordance loss is what actually happened.
2. Every one of the 8 is a `failure` — an `orphaned: reaped at boot` row. **Row lifetime
   is not worker lifetime**: some of that time the owning thread was already dead, and a
   cooperative cancel cannot stop a dead worker. What the operator lost is the ability to
   ask, and the durable record of having asked.
3. The switch is **off** as of 2026-09-18 15:36Z, so the defect is dormant right now. It
   returns on the next halt, and the recorded reason for the long one — *block any order
   path while unattended* — is a posture this loop runs under routinely.

## 3. What ships

```python
can_cancel = active_run is not None and active_run.run_id is not None
```

One term dropped. Consequences, stated as the reason this shape was chosen:

- **It cannot widen anything but the two inputs named in §1.** Every other status reaches
  `_build_row` with `active_row is None`, so the expression is unchanged elsewhere by
  construction rather than by measurement.
- **It mints no new power.** The endpoint already accepts the request; this stops the UI
  claiming otherwise.
- **The false tooltip becomes true by construction.** After the change `can_cancel` is
  false **iff** there is no active run — exactly what "No active run to cancel." asserts.
- **No FE change.** Both call sites (`ProcessRow.tsx:209`, `ProcessDetailPage.tsx:538`)
  key on the flag alone; neither gates on `status`.
- **`bootstrap` and `ingest_sweep` are untouched.** `bootstrap_adapter.py:425` reads
  `state_status == "running"` with no kill-switch term and is already correct;
  `ingest_sweep_adapter.py:610` is `False` by design, and the API returns
  `cancel_not_supported`.
- **Starting work stays blocked.** `can_iterate` and `can_full_wash` keep their
  `kill_switch_active` terms, and the trigger endpoint keeps its precondition. A test
  asserts both on the same newly-cancellable row so this cannot be read as "the switch no
  longer gates the row".

Two documents currently record the absence as deliberate, and both are amended in the
same commit — leaving either would make the runbook contradict the code:

- `docs/wiki/runbooks/runbook-stuck-process-triage.md:87` — *"Steps 2 and 3 are
  unavailable while the kill switch is on … Making Cancel reachable while halted is a
  behaviour change on an operator control path and has deliberately NOT been made."*
- `docs/proposals/ops/2026-09-15-2274-mid-flight-stuck-halt-mask.md` §8 — the same
  deferral, from the session that wrote the runbook line.

Neither is a settled decision; both name this exact slice as deferred work, and the
ticket carries it as open. The runbook's action ladder is rewritten to state what the
operator actually gets, including §5's limits.

## 4. Tests

`tests/test_scheduled_adapter.py` (db tier — the file's existing harness; the change
removes a term rather than adding logic, so there is no pure decision to extract):

1. **kill switch ON + a `running` `job_runs` row → `status == "disabled"`,
   `can_cancel is True`, and `can_iterate` / `can_full_wash` still False.** The revert
   probe: restoring `process_status == "running"` reds the `can_cancel` assertion and
   nothing else. The existing `test_kill_switch_active_disables_row` cannot cover this —
   its row is a finished `success`.
2. **kill switch ON + no running row → `can_cancel is False`.** A halt does not mint an
   affordance where there is no run.
3. **missing `kill_switch` singleton + a `running` row → `disabled` and
   `can_cancel is True`** — the fail-closed input from §1, pinned deliberately rather
   than acquired silently.
4. **auto-hide: failed terminal + in-flight retry request, no running row → `running`
   with `can_cancel is False`** — the branch that makes the dropped term safe, pinned so
   a future change to `_status_for` cannot quietly re-couple them.

`tests/test_processes_endpoints.py`:

5. **kill switch ON + a running row → `POST /system/processes/{id}/cancel` returns 202,
   inserts one `process_stop_requests` row against that `run_id`, and sets
   `cancel_requested_at`** — proving the enabled button reaches a path that accepts it,
   rather than trading a grey button for a 409.

## 5. What this does NOT do — so the next session does not inherit it as done

- **It does not make a generic scheduled job stop.** Grepped, not assumed: the only
  callers of `process_stop.is_stop_requested` are `bootstrap_orchestrator`,
  `processes/bootstrap_cancel_signal` and `sync_orchestrator/executor`. **No generic
  scheduled job polls it**, and `job_runs.cancel_requested_at` has exactly one reader —
  `scheduled_adapter:949`, which turns it into the `is_cancelling` display flag. So for
  most scheduled rows Cancel records operator intent and nothing observes it. ⚠ That is
  **pre-existing and status-independent** — equally true with the switch off — and
  fixing it is the hub spec's unimplemented per-job checkpoint catalogue (:452), a
  different decision with a different input. This change makes the halted row behave
  exactly like the unhalted one; it does not improve what that behaviour is.
- **A jobs-process restart is not "completing" the cancel.** `reap_orphaned_job_runs`
  does not consult `cancel_requested_at`: it writes `failure` and the normal retry path
  may re-enqueue the work, under a new `run_id` the existing stop row does not target.
  The runbook amendment says this rather than implying the restart honours the request.
- **Neither FE surface acknowledges the request.** `active_run.is_cancelling` is served
  but rendered by neither `ProcessRow` nor `ProcessDetailPage`, so a second click returns
  `409 stop_already_pending` instead of showing a pending state — and `is_cancelling`
  means *requested*, not *observed*. Pre-existing; recorded, not fixed here.
- **The request does not pin the displayed run.** The endpoint re-resolves the latest
  running row by `process_id`, so a rollover between render and click targets a different
  run. Pre-existing endpoint behaviour, unchanged.
- ⚠ **Incidental, found while verifying and recorded on #2274 rather than fixed here:**
  the two orchestrator wrappers re-home their *terminal* row to `sync_runs`
  (`_ORCHESTRATOR_SYNC_SCOPE`) but their *active* row still comes from
  `_read_running_run` on `job_runs`. A live `orchestrator_full_sync` therefore renders no
  `active_run` and reads "No active run to cancel" **with the switch off**, while
  `_cancel_orchestrator_full_sync` has a working `sync_runs` cancel path behind it. Same
  false sentence, different cause, out of this slice's scope.
- It does not touch the watchdog's acting half, which still needs the thread-liveness
  oracle `2026-09-15-2274-job-runtime-ceiling.md` §6 describes, nor the sync-layer jobs,
  which render no `scheduled_job` ProcessRow at all.
