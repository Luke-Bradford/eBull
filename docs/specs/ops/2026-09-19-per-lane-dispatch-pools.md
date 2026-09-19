# Per-lane dispatch pools — a parked fire must not cost another lane its fire (#3220)

Status: spec · 2026-09-19 · Refs #3220, #2985, #3118, #3159, #2603.

## Problem

`_scheduler_executor_alias` sends a job to its own APScheduler executor only when its
lane is in `_RESERVED_EXECUTOR_LANES` — `paper_lifecycle_reserved` and
`quote_observation_reserved`. Everything else dispatches on `default`, which is
`_DEFAULT_EXECUTOR_MAX_WORKERS = 10`.

| lane | members | permits | dispatches on |
| --- | --- | --- | --- |
| `general_non_sec` | 50 | 1 | `default` (10) |
| `sec_rate` | 11 | 4 | `default` (10) |
| `quote_observation_reserved` | 3 | 1 | own pool, `max(1,3) = 3` |
| `paper_lifecycle_reserved` | 1 | 1 | own pool, `max(1,1) = 1` |

**61 of 65 registered jobs share 10 dispatch threads.** A fire that loses its lane
semaphore parks inside `_job_execution_slot`'s fallback acquire — the outermost
statement of `wrapped()` — so it owns one of those 10 threads for the whole wait while
holding **no database connection** (`execution_slot_wait_snapshot`: *"slot admission
deliberately precedes every database connection"*).

Once all 10 are parked, later fires sit in the pool's **queue**. APScheduler's
`run_job` tests `misfire_grace_time = 1 s` at the moment a worker picks the job up, so
each queued fire is declared missed on dequeue.

## Evidence — the daemon log, not the table

⚠ The `job_runs` rows alone do **not** establish this, and an earlier draft of this
spec wrongly claimed they did: `_on_job_missed` stamps its row at
`event.scheduled_run_time`, so several misfires sharing a timestamp means they shared a
*due slot*, not a rejection instant. The log does establish it
(`var/autonomy-logs/launchd.jobs-daemon.err.log`, stamps in BST = UTC+1):

```
04:23:00,791 Job "orchestrator_full_sync ..." executed successfully
04:23:00,792 Run time of job "monitor_positions ..."               was missed by 0:08:00.792096
04:23:00,794 Run time of job "orphan_test_db_reap ..."             was missed by 0:08:00.794110
04:23:00,795 Run time of job "sec_atom_fast_lane ..."              was missed by 0:08:00.795448
04:23:00,797 Run time of job "sec_insider_transactions_ingest ..." was missed by 0:08:00.797071
04:23:00,799 Run time of job "core_eligibility_refresh ..."        was missed by 0:03:00.799038
04:23:00,792 job 'orchestrator_high_frequency_sync' acquired general_non_sec execution capacity after 1380.646s
04:23:06,151 job 'sec_manifest_worker'               acquired general_non_sec execution capacity after 1386.005s
04:23:06,308 job 'sec_per_cik_poll'                  acquired general_non_sec execution capacity after 1386.162s
04:23:30,437 job 'strategy_intraday_harvest'         acquired general_non_sec execution capacity after 1410.291s
04:23:30,440 job 'jobs_retry_sweeper'                acquired general_non_sec execution capacity after 1110.313s
04:23:30,470 job 'recommendation_order_reconcile'    acquired general_non_sec execution capacity after  990.379s
```

Five fires are declared missed in the **millisecond a `default` worker frees**, and the
lines immediately after show six jobs coming off the single general permit after
16–23 minutes each. Three things follow:

1. **`sec_atom_fast_lane` is on `sec_rate`, which had 4 free permits throughout.** It
   lost its fire to a *pool*, not to a semaphore. No per-lane mechanism can do that.
2. **The identical lateness identifies the mechanism.** A semaphore releases one waiter
   at a time; a freed pool worker drains the queue at once. Codex ckpt-1 was right that
   correlated misfires also fit a scheduler stall or host suspension — the log excludes
   both: the scheduler thread fired on time at `04:10:00,14`, `04:15:00,14` and
   `04:20:00,12`, and jobs kept completing throughout.
3. **`core_eligibility_refresh` — a core-sleeve job — lost its 03:20 fire the same
   way.**

Recurrent, not a one-off. Minutes with ≥3 simultaneous misfires: 2026-08-24 03:15Z
(5 jobs), 09-13 04:15Z (5), 09-13 03:15Z (4), 09-18 03:15Z (4), 08-25 03:15Z (4),
09-16 03:15Z (4), 09-13 03:30Z (3), 08-25 03:30Z (3).

⚠ The 08-24 burst includes `strategy_paper_cycle`, which has had its own pool since
#2985 — the log shows it on `apscheduler.executors.default` that day, i.e. before the
reservation landed. History, not a counterexample.

⚠ Not used as evidence: reconstructing occupancy from
`[started_at − execution_slot_wait_seconds, started_at]`. `started_at` is stamped after
admission *and* the prelude, the column is populated by manual and reserved-lane waits
too, and it has only existed since 2026-09-18 04:50Z. The log lines above measure the
same thing directly.

## Source rule

No published formulation exists for a thread-pool size, so the rule is fixed **by
construction**, and the construction is already in this repo twice:

1. `build_scheduler_executors` sizes a reserved lane
   `max(EXECUTION_LANE_PERMITS[lane], member_count(lane))` — permits bound how many
   bodies may RUN, members bound how many fires may be waiting to DISPATCH (#3118).
2. `JobRuntime._manual_executor` is `max(1, len(self._invokers))` — *"one slot per
   wired invoker means every wired job can be in flight simultaneously without
   head-of-line blocking"*.

`max_instances = 1` (a `job_defaults` entry) makes `members` an **exact** ceiling on
parked bodies rather than an estimate: a job can have at most one fire in flight, so a
lane can have at most `members` fires parked, and a pool of that size cannot be
exhausted by its own membership. No constant is introduced.

## Change

Dispatch **every** lane on its own executor, sized by that rule.

1. `_RESERVED_EXECUTOR_LANES` splits in two, because it was carrying two meanings:
   - `_RESTRICTED_LANES = (paper, quote)` — lanes whose **membership** is a reviewed
     allow-list (`test_reserved_lane_membership_is_an_explicit_allow_list` keeps its
     current force). `sec_rate` and `general_non_sec` are where `execution_lane_for`
     sends everything it does not recognise, so membership is not scarce on them.
   - `_DISPATCH_POOL_LANES = tuple(EXECUTION_LANE_PERMITS)` — lanes that get a pool.
2. `build_scheduler_executors` and `_scheduler_executor_alias` read
   `_DISPATCH_POOL_LANES`. `reserved_lane_member_count` → `lane_member_count`.
3. `default` stays at `_DEFAULT_EXECUTOR_MAX_WORKERS`. ⚠ Not as a routing fallback —
   `execution_lane_for` is total, so no registered name can reach it. It stays because
   APScheduler requires it: `add_job` defaults `executor='default'`.

Resulting pool sizes: general 50, sec 11, quote 3, paper 1, default 10.

### Second change — refuse a fire the SLOT admitted too late

Codex ckpt-2 returned one P1 and it is on the order path, so it is fixed here rather
than recorded. `execute_approved_orders` sits on `general_non_sec` and its registration
says order execution *"must only happen at the scheduled time, not as a surprise
catch-up hours later"*. `misfire_grace_time` does not enforce that: APScheduler tests
the grace inside `run_job`, **before** `wrapped()` runs, and the unbounded
`_job_execution_slot` acquire is `wrapped()`'s first statement. Nothing re-checked the
fire's age afterwards. The measured general-lane admissions above are 990–1410 s.

⚠ This is a pre-existing hole, not one this change opens — a parked fire runs late
today too, whenever a `default` thread happens to be free. Giving general its own
50-thread pool makes admission *more* likely, which is why the check belongs in the
same commit.

- New `ScheduledJob.refuse_late_admission: bool = False`. Opt-in, with the same
  predicate as `misfire_grace_seconds` and `rearm_on_lost_fire` stated from the other
  side. Two members:
  - `execute_approved_orders` — the no-catch-up contract above.
  - `portfolio_eod_snapshot` (ckpt-2 round 2, P2) — its 4 h grace is a **ceiling**
    chosen by construction: due 22:30, next frontier-advancing sweep 03:00, so 02:30 is
    the latest admission that cannot leap onto the following session. Enforced only at
    dispatch, that ceiling bounds nothing, because the wait comes after it — the job
    would resolve a later `snapshot_date` and miss the session permanently, which is
    the 2026-08-12 / 08-20 gap the grace was added to close, and #2844's last
    acceptance clause needs five CONSECUTIVE reconciled days.
  - ⚠ Deliberately NOT every job with a `misfire_grace_seconds`. `quotes_refresh` and
    `core_candidate_quote_refresh` set one so a fire is not admitted while its
    successor is due, and they sit on the quote lane where the longest admission
    measured is 13.1 s. Refusing them would trade a bounded wait for the lost
    observation bucket #2934 exists to prevent.
- `late_admission_seconds(job_name)` returns the wait when the job opted in, a slot of
  its own is held, and that wait exceeds the job's own effective grace. `wrapped()`
  then records a `lane_busy` skip and returns.
- **The bound is not a new constant**: it is the job's own
  `misfire_grace_seconds`, or `SCHEDULER_DEFAULT_MISFIRE_GRACE_SECONDS` (the named
  `job_defaults` value, previously the bare literal `1`) — the grace APScheduler
  already applied, applied a second time to the delay the slot added.
- Strictly greater-than, so the only admission this job has ever recorded (`0.000 s`,
  twice) can never be refused.
- `LANE_BUSY_SKIP_PREFIX` is reused deliberately: the cause *is* a busy lane, and that
  prefix is what keeps the row out of `scheduled_adapter`'s `expected_fire_at` anchor.
  It arms nothing, because arming needs `rearm_on_lost_fire`, which this job must never
  set — the two fields are asserted mutually exclusive.

## Costs, measured

- **Configured scheduler threads 14 → 75.** (The earlier draft said 22; that was
  arithmetic error, caught at ckpt-1.) Of the 75, 65 are reachable by recurring
  routing. The separate `_manual_executor` is 113 workers and is unchanged.
- **RSS.** Measured on this box: parking 65 tasks across pools of 50/11/3/1 moved peak
  RSS from **21.3 MiB to 24.2 MiB (+2.9 MiB)**. Reproduce with a
  `concurrent.futures.ThreadPoolExecutor` per size and `resource.getrusage`. Codex's
  ~976 MiB figure is stack *address-space reservation*, which macOS commits lazily and
  which `ru_maxrss` does not count. `ThreadPoolExecutor` also creates workers on
  demand, so the steady state is far below 75.
- **Connections: zero change.** `EXECUTION_LANE_PERMITS` untouched, so
  `_dev_profile_connection_demand()` is unchanged. Threads are the affordable half of
  the pair precisely because a parked fire holds no connection — the correction Codex
  ckpt-3 made on PR #3219 (`build_scheduler_executors` sizes pools;
  `EXECUTION_LANE_PERMITS` bounds concurrency; #3118's budget argument was about adding
  a LANE).

## What this does not buy — stated, not hidden

- **It does not make the general lane faster.** One permit still serialises 50 jobs;
  the log above shows 16–23 minute admissions and those remain. It stops the wait being
  charged to other lanes.
- **⚠ It converts some lost general fires into very late ones.** A fire that today is
  discarded at dispatch will instead be admitted, park, and run when the permit frees.
  For most jobs that is the better failure — see the second change below for the one
  job where it is not.
- **No fairness or throughput guarantee.** `threading.Semaphore` does not document
  FIFO wakeup, so a newly arriving fire can overtake a waiter, and nothing here shows
  the general lane can drain faster than it fills.
- **`jobs_retry_sweeper`'s recovery latency is unchanged**, as `9b8eb995` recorded it.
- **Shutdown exposure grows** with the number of parked threads. `wrapped()` already
  re-checks `process_is_stopping()` after admission, and that is the whole of the
  existing mitigation.

## Residual in the sizing rule

`BaseExecutor._run_job_success` decrements `_instances` **before** dispatching the
job's events, and that dispatch runs on the worker thread. A worker can therefore still
be occupied by a finishing callback while a new fire for the same job is accepted and
queued behind it (Codex ckpt-1 finding 6, reproduced). `members` bounds parked *bodies*
exactly and worker occupancy only approximately. Not padded with a `+1`: the right size
for that case is not derivable, and our two listeners are bounded short writes.

## Tests

1. `test_a_sec_fire_survives_a_fully_parked_general_pool` — fill the general pool, fire
   on `sec_rate`. The 04:23:00,792 line above, as a regression.
2. `test_the_general_pool_jam_is_real` — control arm; the same jam DOES block another
   general fire, so arm 1 cannot pass by failing to reproduce.
3. `test_no_registered_job_dispatches_on_the_shared_default_pool` — inverts #2985's
   `test_non_reserved_lanes_stay_on_the_default_executor`, which pinned the old routing
   and said so (*"this PR does not re-shape them"*).
4. `test_every_lane_with_a_permit_has_a_dispatch_pool` — the two constants are derived
   from each other and must not drift apart.
5. Unchanged and still green: the `max(permits, members)` sizing tests, the
   alias-registration test, the `default`-size upstream pin, and the restricted-lane
   membership allow-list.
6. `tests/test_etoro_core_lane_starvation.py::test_the_split_adds_no_execution_permit`
   keeps its claim and updates its assertion from `"default"` to the general lane.
7. `TestLateAdmissionRefusal` (10 cases) — not refused when the job did not opt in, at
   `0.0`, at exactly the grace, or when the wait is unknown; refused past the grace;
   another job's enclosing slot does not supply the wait (#3189 finding 13);
   `refuse_late_admission` is an explicit allow-list; it and `rearm_on_lost_fire` are
   never both set; the named default equals the one the scheduler configures; and the
   EOD snapshot's 4 h ceiling holds at the boundary and refuses one second past it.

## Security

No security surface. In-process scheduling admission only — no new external input, no
credential path, no broker call, no SQL.
