# #2052 — nightly thesis audit scans starved on the db lane: own lanes + lane-skip telemetry

Status: spec (pre-implementation)
Issue: #2052. Related: #2014 (dq audit), #2012 (break scan), #1707/#1710 (silent daily skip class), #1594 (single-job lane precedent), #1538 (lane-acquire retry), #1526/#1527 (shared-lane starvation lessons).

## Problem

`thesis_dq_audit` (daily 05:12, lane `db`) and `thesis_break_scan` (daily 05:22, lane `db`)
share the `db` lane with `fundamentals_sync` (daily 02:30), whose run has ended
`orphaned: reaped at boot` 4/4 nights (07-13→07-16, 6.1h/10.9h/10.0h/8.6h holds — thesis
cascade LLM calls dominate; each "duration" is start→daemon-restart-reap). The db lane is
held from 02:30 until the next operator daemon restart, so both 05:1x/05:2x audit slots are
starved every night.

Fire path: `app/jobs/runtime.py::_wrap_invoker` → `_fire_scheduled_with_lane_retry`
(`runtime.py:657`) retries the `JobLock` acquire (`app/jobs/locks.py::JobLock.__enter__`,
`pg_try_advisory_lock(hashtext('job_source:db'))`) under the daily "patient" backoff —
`_LANE_BACKOFF_DAILY_PATIENT ≈ 11.5s` (`runtime.py:635`) — then **returns with only a log
line** (`runtime.py:700-718`). No `job_runs` row. The three earlier gates in the same path
(param validation, bootstrap gate, prereq) all write `status='skipped'` rows; the lane-busy
exits are the only silent ones.

Full-population verification (dev DB, re-runnable):

```sql
SELECT job_name, status, started_at, finished_at, row_count, left(error_msg, 80)
  FROM job_runs
 WHERE job_name IN ('thesis_dq_audit', 'thesis_break_scan')
 ORDER BY started_at;          -- ALL-TIME: 1 row total (break_scan manual 07-16 11:10)

SELECT status, started_at, finished_at,
       round(extract(epoch FROM (finished_at - started_at)) / 3600, 1) AS hours
  FROM job_runs
 WHERE job_name = 'fundamentals_sync' AND started_at > now() - interval '4 days'
 ORDER BY started_at;          -- 4/4 nights failure, 6.1/10.9/10.0/8.6h, orphaned-reap
```

`thesis_dq_audit`: zero scheduled fires ever (2 registered nights, both silently skipped).
`thesis_break_scan`: zero scheduled attempts yet (registered 07-16 11:08, first slot
tonight); its one success row 07-16 11:10 is the operator's manual verification run.
Evidence tables also on #2052.

## Decision (option 1 + 3 from the issue, plus a verdict-integrity guard)

### 1. Two single-job lanes (option 1)

- `db_thesis_dq` — `thesis_dq_audit` only.
- `db_thesis_break` — `thesis_break_scan` only.

SEPARATE lanes, not one shared audit lane — the #1526 lesson (pinned in
`app/jobs/sources.py` at the `db_liveness`/`db_retry` entry): a shared lane recreates
starvation between its members when they co-fire, and boot catch-up / manual triggers DO
co-fire them despite the 05:12/05:22 stagger.

Write-disjointness (a lane is a job-overlap bucket, not a rate limiter):

- `thesis_dq_audit` is read-only (`compute_thesis_dq_report`) — writes only `job_runs`
  via `_tracked_job`. MVCC-safe vs every thesis writer.
- `thesis_break_scan` writes `thesis_break_predicates` + `thesis_break_events` ONLY
  (+ `job_runs`). Full-population writer census (2026-07-16):
  `grep -rn "thesis_break_predicates|thesis_break_events" app/ | grep -iE "insert|update|delete"`
  → 3 write sites, all in `app/services/thesis_break_scan.py`. The `break_fired`
  stale-mark is NOT a write — `app/services/thesis.py:298` derives it read-side via
  `EXISTS (thesis_break_events…)`. Source rule: #2012 design
  (`docs/proposals/thesis/2026-07-16-thesis-break-predicates.md`, Design 1) +
  `sql/230_thesis_break_predicates.sql` — `UNIQUE (thesis_id, predicate_index)` on
  predicates and the events table's composite FK onto it make concurrent same-key writes
  impossible to corrupt; the only other invocation path is the manual trigger of the SAME
  job_name, which serialises on the same lane.

Scheduled-only jobs → lanes NOT added to the `bootstrap_stages.lane` CHECK (precedent:
`db_liveness` / `db_retry` / `db_positions` / `db_size_sample`).

Settled-decisions check: #1064 (same-source serialise via one JobLock) preserved — this
changes lane membership, the mechanism is untouched. #1594 single-job-lane precedent
followed. No EXIT/execution-guard surface.

### 2. Lane-busy skip telemetry (option 3)

`_fire_scheduled_with_lane_retry` gains a best-effort skip-row write at BOTH silent exits
(no-free-slot immediate skip; backoff-window exhausted). Row shape: existing
`record_job_skip` (`app/services/ops_monitor.py:573`) — `status='skipped'`,
`error_msg = "lane_busy: <detail>"`, `params_snapshot` threaded from the wrapped fire.
Machine-checkable, delimiter-stable prefix constant `LANE_BUSY_SKIP_PREFIX = "lane_busy: "`
(colon+space included, so a future `lane_busyX` reason can never be misclassified)
exported from `app/services/ops_monitor.py` (single source of truth; consumed by the
writer and the adapter below). Write failures are logged, never raised (same posture as the existing
param-validation skip writer, `runtime.py:1953`). Prevention-log L848 honoured: the row is
written only when the body never ran, outside any `_tracked_job`.

### 3. Verdict-integrity guard (required by 2, else it green-washes)

`app/services/processes/scheduled_adapter.py` anchors `expected_fire_at` on the latest
TERMINAL run (`max(started_at, finished_at)`, line ~859) and `schedule_missed` fires only
when that anchor is >1 full cadence overdue (`stale_detection.py:134-148`). A `skipped`
row is terminal — so a nightly `lane_busy` skip row would reset the schedule-missed clock
every night and render a permanently-starved job GREEN ("idle"/current). That inverts the
telemetry's purpose.

Fix (shape per Codex ckpt-1): a SECOND resolver, `_read_latest_anchor_terminal_run` —
the latest terminal run that is NOT (`status='skipped'` AND `error_msg` starting with
`LANE_BUSY_SKIP_PREFIX`). `_read_latest_terminal_run` is UNTOUCHED — it keeps driving
`last_run`, the status pill, and the retry/cancel look-throughs. Only the
`expected_fire_at` computation switches to the anchor resolver. Explicit fallback: when
the anchor resolver returns no row while `terminal_row` is non-null (entire history is
lane-busy skips — the existing `terminal_row is None` / never-started path at
`scheduled_adapter.py:915` cannot arm because skip rows make `terminal_row` non-null),
anchor on the persisted `job_first_seen` row (#1508 C6) so `schedule_missed` still arms.

Rationale for class-based split: prereq/gate skips mean "decided the work needn't/mustn't
run" (a completed decision — legitimate clock reset; e.g. `retry_deferred_recommendations`
skips every cadence benignly). Lane-busy skips mean "work was due, couldn't start" — not a
completed cycle, must not reset the miss clock.

## Non-goals

- Fixing `fundamentals_sync`'s multi-hour LLM-cascade hold (upstream disease; issue notes
  it; the audit lanes remove its blast radius on these two jobs).
- Reslotting (option 2) — fragile against a holder whose release time is "next daemon
  restart"; superseded by option 1.
- Touching `catch_up_on_boot=False` on the audit jobs — "re-covered next night" becomes
  true once the lane is private.

## Changes

| file | change |
| --- | --- |
| `app/jobs/sources.py` | `Lane` literal + docstring entries for `db_thesis_dq`, `db_thesis_break` |
| `app/workers/scheduler.py` | the two `ScheduledJob.source` values + slot-comment updates |
| `app/jobs/runtime.py` | `_fire_scheduled_with_lane_retry`: optional `params` kwarg + `_record_lane_busy_skip` best-effort writer at both skip exits; `_wrap_invoker` threads `params` |
| `app/services/ops_monitor.py` | export `LANE_BUSY_SKIP_PREFIX` |
| `app/services/processes/scheduled_adapter.py` | second resolver `_read_latest_anchor_terminal_run` + `job_first_seen` fallback; `_read_latest_terminal_run` untouched |
| `tests/test_job_registry.py` | add both lanes to the `_ALLOWED_SOURCES` mirror (line 37) |
| tests | see below |

## Tests (fast tier, pure-logic where possible)

- `tests/test_lane_busy_retry.py` — extend the existing fake-JobLock harness: skip row
  recorded (monkeypatched writer) on window-exhausted AND no-slot exits, with the
  `lane_busy` prefix + params passthrough; NOT recorded when the body runs or when the
  body itself raises `JobAlreadyRunning`.
- Lane registry: `source_for('thesis_dq_audit') == 'db_thesis_dq'`,
  `source_for('thesis_break_scan') == 'db_thesis_break'` (pattern of existing registry
  tests), and the two lanes are disjoint from the bootstrap lane set.
- `scheduled_adapter` anchor: a lane-busy skip as latest terminal does NOT advance
  `expected_fire_at` (anchors on prior success); all-lane-busy history falls back to
  `job_first_seen`; a prereq skip STILL advances it (regression pin for today's benign
  behaviour). One DB-backed test only if the anchor is SQL-side (per test-quality skill:
  one integration test per new SQL mechanism).

## Operator note

Scheduler/lane changes are inert until the jobs daemon restarts (VS Code `stack: jobs`
task shadows launchd — check `ps -o ppid`). After merge: restart the jobs task, then
confirm tonight's 05:12/05:22 rows exist (success OR a visible `lane_busy` skip).
