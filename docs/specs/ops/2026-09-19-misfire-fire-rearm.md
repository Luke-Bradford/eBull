# A fire lost to a misfire is re-armed, on the pool that is not jammed

#2603, the `misfire` slice of the lost-fire class. `8f2a5b81` removed the CAUSE of the core
sleeve's four lost days (a lane held 3.4 h across its fire); `ff6a6afa` covered `lane_busy`;
this covers the skip class that fix deliberately left out.

## ⚠⚠ This REVERSES a scope decision in the merged `ff6a6afa` spec

Not a gap closing — a reversal, stated as one. `docs/specs/ops/2026-09-19-lane-busy-fire-rearm.md:101`
reads:

> ⛔ **`misfire` excluded — its recovery is already a settled decision.** `MISFIRE_SKIP_PREFIX`'s
> own comment: *"This is telemetry, not recovery. … Recovery, where a job can tolerate it, is
> `ScheduledJob.misfire_grace_seconds`."* Adding a second [route] …

That was written yesterday, by a prior session, on a ground it did not measure. It is evidence,
not a finding (working-order 3c), and the ground does not survive measurement — see below. It is
**not** in `docs/settled-decisions.md` (grepped: no entry mentions misfire or grace), so this is a
spec-scope reversal inside one ticket, not a register reversal. Recorded here explicitly rather
than reframed, because a reversal presented as a gap is how a decision gets undone by accident.

Two things the prior spec got right and this one keeps: admission stays the per-job
`rearm_on_lost_fire` flag, and `max_instances_active` stays excluded.

## What the measurement says, and what it does not

Every `misfire` row ever written (`_on_job_missed` landed 2026-08-24; 76 rows to 2026-09-18),
lateness parsed from the tail of `error_msg` — prevention-log L1783: the lateness is stored at
the END of the message, and a truncating triage query destroys exactly it.

```sql
select job_name, started_at, error_msg
  from job_runs where status='skipped' and error_msg like 'misfire%';
```

| lateness | rows |
| --- | --- |
| < 5 s | 12 |
| 5–60 s | 2 |
| 60–600 s | 23 |
| 600–3600 s | 24 |
| > 3600 s | 15 |

min 1.1 s · median 959.6 s · max 10,946.3 s.

⚠ **Three limits on that table, because it is easy to over-read and the first draft of this spec
did.**

1. It is the population of **recorded misfires**, not of lateness. It is conditional on exceeding
   the current 1 s grace *and* on the listener's telemetry write succeeding, so it says nothing
   about fires dispatched slightly late and run normally.
2. It pools 26 jobs. It characterises the **mechanism** — which is shared, see below — not the
   right setting for any one job.
3. "A grace of size N would have saved these rows" is a counterfactual that holds congestion
   fixed, and admitting more work changes congestion. It is not quoted as a saving here.

What it does establish is that lateness at this scale is **pool queueing, not dispatch jitter**,
which is already written down (#2985 / prevention-log L1780): a job parked on another lane's
semaphore still OWNS its APScheduler worker thread, so with 50 of the 63 registered jobs sharing
one general permit inside a 10-thread pool, a fire queues behind them and is discarded before it
reaches the wrapper.

### Per job, which is the population that decides this ticket

| job | misfires | lateness |
| --- | --- | --- |
| `core_rebalance_observation` | **0** | — |
| `core_eligibility_refresh` | **2** | 180.8 s, 189.3 s |

⚠ The zero is weak evidence, not proof of immunity: `core_rebalance_observation` has 18 lifetime
`job_runs` rows and those are not 18 independent scheduled opportunities (8 `prereq_missing`, 6
`orphaned`, 4 `lane_busy`). The honest reading is "this job has not yet been observed to misfire",
and the handoff that named it as the job with the hole named the one with no instances.

## Why re-arm rather than a grace override

The first draft of this spec argued grace was indefensible. That was too strong, and the
correction matters: **a ~190 s grace would have admitted both observed `core_eligibility_refresh`
misfires at 5.3% of its cadence.** The case for re-arm is not that grace fails — it is that grace
is the worse instrument *for these two jobs*, for one evidenced reason and two structural ones.

### 1. Grace holds the jammed pool longer; re-arm runs on a different pool

This is the load-bearing argument and it is the repo's own documented mechanism, not a
preference.

A misfire on the general lane means the APScheduler pool was saturated. Raising the grace admits
that fire — and `wrapped()`'s outermost statement is `_job_execution_slot`, whose fallback is an
**unbounded blocking** acquire. So the admitted job now owns a worker thread and parks on the
general permit, which is precisely the pathology prevention-log L1780 describes as exhausting the
pool and queueing every other lane's fires behind it. Grace converts this job's lost fire into
pool occupancy that costs other jobs theirs.

The re-dispatch does not touch that pool. `_refire_one` publishes a manual job request, and the
manual path runs on `JobRuntime._manual_executor` — a separate `ThreadPoolExecutor` sized one slot
per wired invoker, explicitly so distinct jobs do not queue behind each other
(`runtime.py:1996`, `:2335`). Prevention-log L1783 uses exactly this property as a diagnostic:
*"`_catch_up` submits to `_manual_executor`, so a catch-up succeeding while scheduled fires queue
localises the jam to the APScheduler pool rather than to the process."*

**Recovery that routes around the jam beats recovery that waits inside it.**

### 2. It needs no constant, and for the daily job none is derivable

`portfolio_eod_snapshot` is the only daily job carrying a grace. Its anchor is *the next scheduled
event that changes what the job reads*: the fire is due 22:30 and `orchestrator_full_sync` at
03:00 advances the price frontier. ⚠ Stated precisely, since the first draft quoted it as exactly
maximal: the interval is 4½ h and the value is 4 h, so the construction is "derived from that
anchor, with a margin", not "the largest possible window".

`core_rebalance_observation` has no such anchor. It reads one live `get_account_risk_snapshot`
plus the mandate; broker state changes continuously, no sweep advances a frontier for it, and the
one discrete boundary available — the next US open — is ruled out by the job's own registry
comment: *"Gating on `us_market_status` would be wrong — settled-decisions permits a non-US core
instrument whose venue we have no calendar for."*

⚠ **On the objection that the 24 h cadence is itself an anchor:** it bounds *whether to arm*
(a dominance test — is the natural fire nearer than the retry?) but it cannot size *how long a
stale fire may sit before executing*, which is what a grace is. Using the cadence for the first
and refusing it for the second is not inconsistent; they are different questions. A grace of
"just under 24 h" would admit a fire 23 h late, which is indistinguishable from skipping the day.

### 3. Re-dispatch of a misfire is the safest of the three skip classes

`EVENT_JOB_MISSED` is raised in `executors/base.py::run_job` **before** `job.func` is called
(verified against the installed APScheduler 3.11.2; an exception after invocation produces
`EVENT_JOB_ERROR` instead). So the body provably never ran for that slot. `max_instances_active`
stays excluded for the opposite reason — a prior instance may still be running.

⚠ That guarantee is about **that slot only**. It does not say no other invocation of the job is
in flight; see the collision limits below.

### Honest comparison

| | grace | re-arm |
| --- | --- | --- |
| pool it occupies during congestion | the jammed APScheduler pool | `_manual_executor`, a separate pool |
| requires a chosen constant | yes; none derivable for the daily job | no |
| works at the observed median lateness | needs ~16 min of grace | indifferent — dispatches fresh |
| dispatch attempts bounded | no expiry | `RETRY_MAX_ATTEMPTS` = 4, counted in `decision_audit` |
| audited | no — a normal run row | one `decision_audit` row per dispatch |
| recovery **latency** bounded | yes, by the window | **no** — see limitations |
| runs adjacent to a natural fire | no | possible, serialised by the source lane |

Neither column is free. Re-arm reuses `RETRY_BASE_SECONDS` (300) and `_DISPATCH_RECHECK_SECONDS`
(900) — chosen constants, just not *new* ones.

## Design

**One behavioural change.** `_on_job_missed` stamps `next_retry_at` on the skip row it already
writes, for a job that opted in — what `_record_lane_busy_skip` already does. The existing
`jobs_retry_sweeper` re-dispatches it.

No registry row changes: `rearm_on_lost_fire` is already set on exactly the two core producers,
and `execute_approved_orders` does not set it, so the settled decision `ff6a6afa` nearly reversed
stays pinned by `tests/test_job_lane_rearm.py`. This change adds no second admission route.

No migration: `sql/183`'s partial index has no status predicate and `_REARMABLE_STATUSES` already
contains `skipped`. `job_retry.py` needs no code change — the sweeper selects on `next_retry_at`.

### The one new piece of arithmetic — corrected, it is MODULO not subtraction

`lane_busy_rearm_delay_seconds` refuses to arm when the job's own next fire is nearer than the
first retry. A dominance test, not a threshold: below that the natural fire always wins and arming
only adds load.

For `lane_busy` the skip row is written at about the fire time, so "cadence gap" and "time to the
next fire" coincide. **For a misfire they do not** — the row can be written hours after the slot
it represents (max observed 10,946.3 s), and intermediate slots have gone by.

⚠ The first draft debited lateness by subtraction. That is wrong, and ckpt-1 proved it
arithmetically: at 10,946.3 s late on an hourly job it yields −7,346.3 s, while the real next fire
is **3,453.7 s** away. Subtraction measures distance from an abandoned slot; the remaining gap is
periodic:

```
remaining = gap - (lateness_seconds % gap)      # gap = min_cadence_gap_seconds(cadence)
arm iff   remaining > RETRY_BASE_SECONDS
```

10,946.3 % 3600 = 146.3 → remaining 3,453.7 s. ✅ With `lateness_seconds = 0`, `0 % gap == 0` and
`remaining == gap`, so the expression is exactly today's and the `lane_busy` path does not change
behaviour.

⚠⚠ **Only for an exactly-periodic cadence.** `min_cadence_gap_seconds` is documented as a LOWER
BOUND, and its constant's own comment relies on that being safe: *"understating a gap can only
make the re-arm guard stricter, never looser."* Modulo breaks that invariant — `lateness % G_min`
lands anywhere inside the real period, so it can OVERstate the remaining gap. Codex ckpt-3
measured it on a monthly cadence: real next fire 100 s away, the guard arming for 300 s.
`monthly` (28 d) and `yearly` (365 d) are lower bounds; `every_n_minutes` / `hourly` / `daily` /
`weekly` are exact in UTC. Those two therefore fall back to clamped subtraction,
`max(0, G_min − lateness)`, which UNDERstates the remaining gap and is safe in the direction the
constant assumes. Latent today — both opted-in jobs are hourly/daily — and fixed rather than left
for whoever opts a monthly job in.

### The audit caption is read off the row, not hard-coded

`_write_retry_audit`'s cause was `"a transient failure" if status == "failure" else "a lost fire
(lane busy)"`. Admitting misfires would have captioned every one of them as a lane collision it
had nothing to do with — the same defect shape as the sentence #2603 already replaced there, a
fixed string that was true for the only case existing when it was written. The reason prefix now
decides it, and an unrecognised prefix degrades to a vague "a lost fire" rather than guessing.

The function is renamed `lost_fire_rearm_delay_seconds` to match the field it reads.

⚠ **This narrows a collision window; it does not close one.** Near the boundary the test still
passes with little room: an hourly `:20` slot detected 3,299 s late leaves 301 s, so the retry
becomes due at `:19:59` and the 5-minute sweeper can reach it alongside the successor fire. That
is the inherited "condition 2 is NOMINAL" caveat, not a new hole, and the source lane serialises
the two bodies. Named, not argued away.

### Clock anchoring

`next_retry_at` is anchored to **listener-time `now`**, not to the lost slot — arming a 3-h-late
misfire against its slot would stamp a time already in the past. One `datetime.now(UTC)` is taken
and used for both the lateness and the stamp, so the dominance test and the arm cannot disagree.

## Files

| file | change |
| --- | --- |
| `app/jobs/runtime.py` | rename `lane_busy_rearm_delay_seconds` → `lost_fire_rearm_delay_seconds`, add `lateness_seconds` (modulo); stamp `next_retry_at` in `_on_job_missed` |
| `app/services/ops_monitor.py` | `MISFIRE_SKIP_PREFIX`'s recovery sentence now names two routes, not one |
| `app/services/job_retry.py` | `_select_due` + `_REARMABLE_STATUSES` docstrings name both armed skip classes |
| `app/workers/scheduler.py` | `rearm_on_lost_fire`'s docstring says "any lost fire", not "a busy lane" |
| `tests/test_job_lane_rearm.py` | cases below |

## Tests

1. An opted-in job's misfire arms `next_retry_at`; the anchor is now, not the lost slot.
2. `execute_approved_orders` misfiring does NOT arm — asserted against the real registry entry.
3. Modulo arithmetic: hourly job 3,500 s late does not arm; 10,946.3 s late DOES (remaining
   3,453.7 s) — the case subtraction got backwards.
4. `lateness_seconds=0` reproduces today's `lane_busy` answers exactly.
5. The listener stays fail-safe: a stamping failure still leaves the committed skip row and does
   not raise into APScheduler's event dispatch.
6. A missing `scheduled_run_time` on the event still writes telemetry and simply does not arm.

## Limitations, named rather than argued away

- **Recovery latency is not bounded.** `jobs_retry_sweeper` is itself a scheduled job on the
  general lane, so a re-fire lands later than `next_retry_at` — during the same congestion. And
  `RETRY_MAX_ATTEMPTS` bounds *dispatches*, not time: if the sweeper never runs, an armed row can
  read self-healing indefinitely without spending an attempt. Inherited from `ff6a6afa`.
- **A backdated armed row can be cleared before it dispatches.** The misfire row's `started_at` is
  the lost slot (#2880's deliberate choice — the fact worth keeping is *which* slot was lost), and
  `_is_latest_terminal` orders by `started_at`. `TERMINAL_STATUS_SQL` includes `skipped`, so any
  newer terminal row clears the arm. For a newer **success** that is correct — the slot has been
  superseded. For a newer **non-work skip** (`prereq_missing`, `max_instances_active`) it is a
  silent drop. It degrades to exactly today's behaviour (no recovery), so it is a safe floor, not
  a regression, and it is not fixed here: narrowing `_is_latest_terminal` would change the
  `lane_busy` and failure paths too.
- **`retry_in_flight` is derived from the latest run's `next_retry_at`**, so an armed misfire that
  is not the latest row is invisible to the health verdict. Same root as the point above.
- **Collision is serialised, not impossible.** `_refire_one` checks for a running row and an
  active request, then publishes — a natural fire can start between those actions, and a scheduled
  invocation waiting before `record_job_start` is invisible to the check. The source lane
  serialises the bodies; it does not prevent two sequential runs.
- **"Idempotent" means bounded duplication, not no-op.** Each `core_rebalance_observation` run
  appends an intent and the submission gate reads the latest, so a duplicate changes which row is
  latest. That is the position `ff6a6afa` already merged ("a duplicate is an extra append, not a
  double-spend") and the gate's supersession rule is part of this safety argument, not incidental.
- **Re-arm is not replay.** A re-fire observes the sleeve at dispatch time. Admitted because one
  verdict late beats the zero a dropped fire produces, never because it reconstructs the lost one.
- **This does not complete the lost-fire class.** A slot coalesced away emits no `MISSED` event,
  `max_instances` suppression emits a different one, and a memory-jobstore restart replays nothing.
- **This does not fix the cause.** The cause is executor starvation on the general lane (#2985).
  ⚠ The fix for that is a reserved *executor*, and the first draft wrongly claimed the connection
  budget blocks it — that conflates threads with permits. `build_scheduler_executors` sizes pools
  while `EXECUTION_LANE_PERMITS` bounds concurrency, so an executor added without raising permits
  need not add DB demand; #3118's budget argument was about adding a LANE. Whether that is the
  right fix is a separate ticket, not this one — but the budget does not rule it out and this spec
  must not say it does.
