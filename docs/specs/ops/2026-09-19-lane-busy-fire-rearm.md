# Re-arm a low-cadence fire lost to `lane_busy` (#2603 follow-up)

Extends `docs/specs/ops/2026-06-07-job-retry-backoff.md`. No schema change.

> **Revision 2** after Codex ckpt-1 (24 findings). Three were verified against the code
> and changed the design; they are recorded inline at the decision they moved, because
> each one is a trap the next reader would otherwise walk into. Revision 1's
> cadence-derived admit rule is **withdrawn as unsafe** — see §Admission.

## Problem

`8f2a5b81` removed the *cause* of `core_rebalance_observation`'s starvation (two own
source lanes). It did not remove the *class*: a scheduled fire that loses its lane is
recorded as `status='skipped'` and is then **gone until the next cadence**. Both #2603
movers are `catch_up_on_boot=False`, so nothing re-fires them.

For a daily job that is a whole day. Measured over 30 days of `job_runs`:

| job | cadence | `lane_busy` skips | cost of each |
| --- | --- | ---: | --- |
| `core_eligibility_refresh` | hourly | 18 | 1 h |
| `strategy_intraday_harvest` | every 5 min | 16 | 5 min |
| `thesis_refresh` | hourly (parked) | 5 | 1 h |
| `orchestrator_high_frequency_sync` | every 5 min | 5 | 5 min |
| **`core_rebalance_observation`** | **daily 22:45Z** | **4** | **24 h** |

`core_rebalance_observation` is the core sleeve's only scheduled producer of
`strategy_core_rebalance_intents`, so each lost fire is a day with no rebalance
observation on a live sleeve.

## The mechanism already exists — this is a classification gap

`jobs_retry_sweeper` already re-fires due rows through the audited manual queue
(`app/services/job_retry.py`). It cannot see a lane-busy skip because it filters
`status = 'failure'` in **both** `_select_due` and `_refire_one`, and nothing ever sets
`next_retry_at` on a `skipped` row.

⚠ The partial index `job_runs_due_retry_idx` (`sql/183`) is `ON job_runs (next_retry_at)
WHERE next_retry_at IS NOT NULL` — **no status predicate**. Widening the accepted status
set needs no migration and does not change the access path.

## Admission — per-job opt-in, NOT derived from cadence

⛔⛔ **Revision 1 proposed admitting any job whose cadence gap exceeds
`_RETRY_BASE_SECONDS`. That rule is unsafe and is withdrawn.** It admits
`execute_approved_orders` — `Cadence.daily(hour=6, minute=30)`,
`catch_up_on_boot=False` — whose registry entry states the opposite contract:

> Do not fire on cold boot — order execution must only happen at the scheduled time, not
> as a surprise catch-up hours later.

A re-arm rule keyed on cadence would have reversed that settled decision silently, and
the thing it would have re-fired late is **order submission**. Cadence describes how
often a job runs; it says nothing about whether the job tolerates running late.

The codebase already states the correct predicate, on `ScheduledJob.misfire_grace_seconds`:

> Only a job that is idempotent, side-effect-bounded and indifferent to its own fire time
> may set this.

So admission is an explicit per-job flag carrying that same predicate:

```python
rearm_on_lost_fire: bool = False
```

Default `False`. Set ONLY where the per-job argument is written down. This change sets it
on two jobs:

- **`core_rebalance_observation`** — observes the sleeve and appends one rebalance
  verdict. It sizes nothing, quotes nothing and submits nothing (its own docstring), and
  the submission gate reads the latest intent. ⚠ A late run is **not a reconstruction of
  the missed 22:45 observation** — it observes the sleeve at the time it runs. It is
  admitted because one observation an hour late is strictly better than none for a
  producer that would otherwise contribute nothing that day, not because the two are
  equivalent. Stated because "re-arm" implies a replay it does not perform.
- **`core_eligibility_refresh`** — re-proves eligibility for the same candidate set; its
  own skip reason when there is nothing to do is `prereq_missing: all N proved
  instrument(s) are fresh`, i.e. it already no-ops when late work is unnecessary.

`execute_approved_orders`, every SEC ingest job, and everything else stay `False`.

**Second guard, retained.** Even for an opted-in job, arm only when the job's own next
fire lands later than the first retry could. `min_cadence_gap_seconds(cadence)` returns a
**lower bound** on the gap between consecutive fires, keyed on `Cadence.kind`
(`every_n_minutes` → `interval_minutes × 60`; `hourly` → 3,600; `daily` → 86,400;
`weekly` → 604,800; `monthly` → 28 × 86,400; `yearly` → 365 × 86,400). A lower bound is
sufficient and cannot overstate — it is only ever compared `> _RETRY_BASE_SECONDS`.

⚠ ckpt-1 correctly noted this comparison is **nominal**: `jobs_retry_sweeper` itself runs
on a cadence, so real dispatch is later than `next_retry_at`. The guard is therefore
stated as what it is — a cheap dominance check that stops an obviously pointless arm —
and **not** as a recovery-latency guarantee. There is no bounded recovery latency here
and the spec does not claim one.

## Scope — `lane_busy` ONLY

`scheduled_adapter._NON_ANCHORING_SKIP_PREFIXES` groups `lane_busy` and `misfire` as
"work was due, couldn't start". This spec deliberately takes **only the first**.

- ⛔ **`misfire` excluded — its recovery is already a settled decision.**
  `MISFIRE_SKIP_PREFIX`'s own comment: *"This is telemetry, not recovery. … Recovery,
  where a job can tolerate it, is `ScheduledJob.misfire_grace_seconds`."* Adding a second
  recovery path would contradict a declared one. ⚠ ckpt-1 is right that this leaves a
  real hole — `core_rebalance_observation` has no grace override, so the default 1 s
  still discards a slightly-late fire. That hole is **named and left open here**, not
  closed by silence: it is a `misfire_grace_seconds` decision on that job, which is a
  different change with a different argument.
- ⛔ **`max_instances_active` excluded on meaning.** It says a previous instance of *this*
  job is still running, so the work is in flight rather than lost. ⚠ ckpt-1 notes
  "active" can also mean queued or wedged, so this is an exclusion on the common case and
  not a proof of coverage; a wedged instance is the liveness watchdog's surface, not this
  one.
- ⛔ `prereq_missing`, session-window guards and "no pending X" are legitimate no-ops.
  They are excluded **structurally**: arming happens only at the lane-busy skip site, so
  no other reason can reach it.

## Classifier check (required before writing any matcher)

`job_runs.error_category` exists and is **NULL on all 6,693 skipped rows** in the 30-day
window — the structured column is present but unpopulated for skips. Rather than mint a
parallel vocabulary in it, arming happens at the **emission site**, which knows the reason
without matching anything. Nothing downstream re-parses prose.

## Change

1. **`app/workers/scheduler.py`** — `ScheduledJob.rearm_on_lost_fire: bool = False`,
   documented with the predicate above. Set `True` on `core_rebalance_observation` and
   `core_eligibility_refresh`, each with its argument in a comment.
2. **`app/jobs/runtime.py`** — `min_cadence_gap_seconds(cadence)` and
   `lane_busy_rearm_delay_seconds(job)`, pure and table-tested.
   `_record_lane_busy_skip` arms `next_retry_at` and `attempt = 1` on the row it just
   wrote. ⚠ The arming UPDATE runs **inside the existing `try/except`** so a failure to
   arm can never suppress the telemetry INSERT or raise into the scheduler — the
   function's existing "never raises" posture is preserved, and arming is explicitly
   best-effort rather than durable (ckpt-1 finding: the INSERT commits first, so a crash
   between the two leaves an unarmed skip; that is an accepted, stated loss).
3. **`app/services/job_retry.py`** —
   - `_select_due` / `_refire_one` accept `status IN ('failure','skipped')`.
   - ⚠⚠ **Bound the re-dispatch.** `_refire_one` today uses `attempt` only in the audit
     string: it never checks `_RETRY_MAX_ATTEMPTS` and never increments. Bounding comes
     entirely from `record_job_finish`, which runs only on a new terminal **failure** — so
     a row whose dispatch is rejected asynchronously (gate, prerequisite, fence) re-fires
     every `_DISPATCH_RECHECK_SECONDS` **forever**. That is a pre-existing defect this
     change would make newly reachable, so it is fixed here: `_refire_one` increments
     `attempt` on dispatch and clears the row once `attempt > _RETRY_MAX_ATTEMPTS`.
   - The audit sentence stops asserting "after a transient failure" for a skip-armed row.
4. **`app/services/processes/health_verdict.py`** — ⚠⚠ **false-green fix, required by
   this change.** `retry_in_flight = row.next_retry_at is not None` (line 348) strips
   `schedule_missed` from the actionable set, but the self-healing branch only fires for
   `status in ("failed", "pending_retry")`. A skip-armed row is `idle`, so it would read
   **Current** while overdue — an armed retry would *hide* the very staleness it is
   recovering. Fixed by mirroring the existing `kick_is_recovering` branch, which solves
   this identical shape three lines earlier and says so:

   > Placed before the status-only branches because a kick does NOT flip the adapter
   > status to `running` … so a stalled `ok`/`idle` row would otherwise fall through to
   > Current and hide the recovery.

   `retry_is_recovering = retry_in_flight and "schedule_missed" in stale_reasons` →
   `("self_healing", True, …)`.

## Invariants that must survive

- A job without `rearm_on_lost_fire` arms nothing, whatever its cadence —
  `execute_approved_orders` in particular.
- A lane-busy skip on a sub-5-minute job arms nothing.
- A `prereq_missing` skip arms nothing, on any cadence.
- Re-dispatch is bounded by `_RETRY_MAX_ATTEMPTS`.
- An armed row never reads Current while its job is overdue.
- A re-armed row is superseded and cleared by the next genuine terminal run.
- A live run or queued request defers the re-fire without clearing it.
- Legacy rows (no `next_retry_at`) are unaffected.

## Known limitations, stated rather than claimed away

Carried from ckpt-1 and deliberately NOT fixed here:

- **No bounded recovery latency.** The sweeper is itself a scheduled job and can queue,
  misfire or be gated.
- **Duplicate work is possible.** `JobLock` serialises bodies, not sequential
  invocations; a cadence fire and a re-fire can both run. Admitted only for jobs whose
  flag argument says a duplicate is harmless.
- **`_is_latest_terminal` orders by `started_at`**, so a run that started before a later
  lane-busy skip does not supersede it even if it succeeds afterwards.
- **`mode='iterate'` carries no params**; safe for the two zero-argument jobs admitted
  here, and not proven for any other.

## Acceptance

Deterministic, not wall-clock:

- Table test of `min_cadence_gap_seconds` over every `Cadence.kind`, including
  `every_n_minutes` at `interval_minutes` 5 (reject) and 6 (admit).
- Table test that `execute_approved_orders`' registry entry has
  `rearm_on_lost_fire is False`, so the settled decision above is enforced by a test and
  not by memory.
- DB test: a lane-busy skip on an opted-in daily job arms `next_retry_at`; the sweeper
  selects and dispatches it; a `prereq_missing` skip on the same job does not.
- DB test: `_refire_one` clears the row once `attempt` exceeds `_RETRY_MAX_ATTEMPTS`.
- Pure test: an armed skipped row with `schedule_missed` reads `self_healing`, not
  `current`.
- Revert probe: reverting the `job_retry.py` status widening must red the sweeper test
  and nothing else.

## Security

The helper performs no broker I/O. ⚠ But the honest statement is broader than revision
1's: this change authorises *executions* that would not otherwise have happened, so the
admitted set is the security surface. That is why admission is an explicit per-job flag
defaulting to `False`, why `execute_approved_orders` is excluded by name and by test, and
why both admitted jobs are informational producers that submit nothing. The re-fire
itself goes through the existing audited manual queue with `requested_by =
"system:retry_backoff"`.
