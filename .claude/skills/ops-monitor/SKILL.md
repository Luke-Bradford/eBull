---
name: ops-monitor
description: eBull ops/health monitoring — layer-staleness + job-health + kill switch in app/services/ops_monitor.py, the contradiction-free admin verdict in app/services/processes/health_verdict.py, the #719 jobs-daemon process topology, and the sync-orchestrator "real failure vs expected noise" triage.
---

# ops-monitor

## When to use

Any change to `app/services/ops_monitor.py` (layer staleness, `job_runs`
tracking, kill switch), `app/services/processes/health_verdict.py` (admin
Processes verdict), the `/system/*` endpoints (`app/api/system.py`),
`/system/processes` (`app/api/processes.py`), or `/jobs/*` (`app/api/jobs.py`).
Also before touching the jobs daemon (`app/jobs/__main__.py`, `supervisor.py`,
`heartbeat.py`), the sync-orchestrator reaper/executor, the `job_runs` /
`job_runtime_heartbeat` / `sync_runs` tables — and before "fixing" an alarming
Postgres log line.

## What it is

Three read-only reporting layers (they observe; never mutate ingest, execute, or
bypass a gate).

**Layer + job staleness (`ops_monitor.py`).** `check_all_layers(conn)` grades
each monitored data layer (universe, prices, quotes, fundamentals, filings,
news, theses, scores) against an expected max age; `check_job_health(conn,
name)` reads the latest `job_runs` row per scheduled job. `record_job_start` /
`record_job_finish` / `record_job_skip` write `job_runs`
(`sql/014_ops_monitor.sql`: `job_name`, `started_at`, `finished_at`, `status`,
`row_count`, `error_msg`). The kill switch also lives here
(`activate_kill_switch` / `get_kill_switch_status`).

⚠⚠ **`success` means "it finished", NOT "it did anything" — until a job opts into
progress reporting (#2218).** Completion and progress are different facts, and
deriving health from the first is how OpenFIGI resolution ran dark for seven weeks
behind an unbroken run of `success / row_count 0` (#2213). A job that binds a
whole-batch failure to a counter instead of raising is invisible to every check
this app has.

The opt-in is `tracker.progress = JobProgress(candidates_seen=…, outcomes={…},
errors={…})` in the invoker; `_finish_tracked` runs
`app/services/job_progress.py::degradation_reason` over it and writes
`status='degraded'` + `progress_json` instead of `success`. Two rules, and
⚠ **"zero rows written" is deliberately NOT one of them** — most jobs legitimately
have nothing to do, and a blanket zero-rows alarm is noise that trains the operator
to ignore the signal. The discriminator is `candidates_seen`: saw nothing / did
nothing is healthy, saw work / produced no terminal outcome is stalled.

⚠ **`progress is None` is judged exactly as before**, so the ~50 unwired jobs are
inert. When investigating a suspect job, `progress_json IS NULL` means "this job
does not report progress" — NOT "it reported zero". They are different claims and
only one of them is evidence.

⚠ **The terminal-status vocabulary is `ops_monitor.TERMINAL_STATUS_SQL`, and it is
the single source.** It was hand-spelled in five SQL `IN (...)` literals across four
modules plus two Python `Literal`s, and pyright cannot see inside a SQL string —
adding a status to the CHECK constraint without touching all of them writes rows
nothing reads. `tests/test_job_terminal_status_contract.py` derives the set from
sql/254's constraint text and greps `app/` for hand-spelled lists.
⚠ `sync_orchestrator/dispatcher.py::reset_stale_in_flight` uses a deliberately
NARROWER set (`success`/`failure`/`degraded`) — `skipped` and `cancelled` mean the
work was not done, and whether boot recovery should re-fire those is an open
question. Do not "fix" it to the shared constant.

**Admin verdict (`health_verdict.py`).** `verdict_for_row(row, now=...)` /
`compute_verdict(...)` collapse a row's `ProcessStatus` pill + `stale_reasons`
chips into ONE precedence-ordered `HealthVerdict` (`current` / `working` /
`self_healing` / `attention` / `stale_manual` / `paused`) so two cells can never
disagree — contradiction-free by construction (#1512). The single choke point
`app/api/processes.py::_convert_row` feeds `/system/processes`, and
`app/api/system.py::_build_jobs_overview` reuses it for the legacy `/system/jobs`
table so both surfaces render the SAME computed verdict.

**Endpoints.** `GET /system/status` (worst-component `overall_status` +
`engine_down`), `/system/jobs`, `/system/processes`, `/system/job-liveness`;
`POST /jobs/{name}/run` (202, durable trigger), `GET /jobs/runs`,
`GET /jobs/requests`. Operator rebuild path is `POST /jobs/sec_rebuild/run`
(`sec_rebuild` is manual-trigger-only, `app/jobs/sources.py`).

**Process topology.** The jobs daemon `python -m app.jobs` owns APScheduler, the
manual-trigger + orchestrator executors, the reaper, the boot freshness sweep,
and the heartbeat writer (per-subsystem upserts into `job_runtime_heartbeat`,
`sql/087_job_runtime_heartbeat.sql`); the API process serves HTTP only. IPC is
Postgres-only (`pending_job_requests` + `pg_notify`). A session-scoped advisory
lock (`JOBS_PROCESS_LOCK_KEY`, `app/jobs/locks.py`) enforces one jobs process.
⚠ Given that singleton, if a VS Code `stack: jobs` task already holds the lock,
`launchctl kickstart` is a no-op and merged scheduler/parser changes are NOT
picked up until the operator restarts that task — check `ps -o ppid` first.

⚠⚠ **The API's application object does NOT live in either process that
`pgrep -f uvicorn` matches.** Under `uvicorn --reload` (the dev stack's shape)
the app runs in a **multiprocessing-spawned worker**; `pgrep -f uvicorn` returns
the `uv run uvicorn` parent and the uvicorn supervisor, and *neither holds
application sockets*. So `lsof` against those pids reports zero outbound
connections on a completely healthy app — and it fails in the direction that
manufactures a bug, because "zero sockets" reads as a dead subsystem.

Measured 2026-08-04 (#2271), with the eToro WS fully connected the whole time:

```
pid=60224 etoro_sockets=1   <- multiprocessing-spawned app worker
pid=61071 etoro_sockets=0   <- uv run uvicorn (parent)
pid=61124 etoro_sockets=0   <- uvicorn supervisor
```

Both the #2271 report and the first diagnosis pass concluded "the WS subscriber
is not connected" from exactly that measurement. It was wrong, and it pointed
the investigation away from the real defect (`quotes` had no scheduled writer).

**To check whether the app holds a connection, ask the app, not the OS.**
`GET /_debug/etoro-ws` returns `ws_state` (CONNECTING/OPEN/CLOSING/CLOSED) from
inside the process that owns the socket, which no external process inspection
can get wrong. If you must inspect externally, resolve the target host and grep
every pid for its address (`lsof -nP -i TCP | grep <ip>`) — matching on the
command line will miss the worker. General rule: **prefer an in-process liveness
report over external process inspection, and when an external measurement says a
subsystem is dead, confirm it in-process before writing it into a ticket.**

## Invariants (do not break)

- **Process topology (#719, settled 2026-04-30):** no scheduler / executor /
  reaper / boot sweep in the API process; jobs-process singleton via the
  advisory lock; IPC Postgres-only; durable triggers (row written before NOTIFY,
  replayed on boot). Do not re-introduce in-process scheduling in the API or a
  raw `ConnectionPool(...)` — use `open_pool`.
- **Kill switch reads neutral, not red (#1831):** a merely-`disabled` process
  row is `paused` (grey) — the halt is the unattended loop's NORMAL state. Only
  a genuine WEDGE (`queue_stuck` / `mid_flight_stuck`) or a last-terminal-run
  `failure` stays `attention` under the switch (nothing genuine is hidden), while
  expected halt-drift (`schedule_missed` / `watermark_gap`) is not painted a
  problem. BY DESIGN (`health_verdict.py:173`); "N problems" on `disabled` +
  `failure` is that failure surfacing, not a bug.
- **An actionable wedge is never masked by a status** (Codex ckpt-1): the
  stale-reason check runs before every non-disabled status branch.
- **Reporting only:** ops-monitor / health-verdict never close positions, bypass
  a failed check, or mutate ingest data — every trade path stays deterministic +
  auditable (repo non-negotiables; long-only v1, no leverage).
- **Reaper is boot-only:** `reap_orphaned_syncs(reap_all=True)` runs at jobs
  startup (`app/jobs/__main__.py`). No periodic stale-`running` watchdog exists,
  so a genuinely-hung live sync blocks syncs until the next restart — flag that
  as an architecture gap; do NOT add a blind age-based reaper (it would kill
  legitimately-slow catch-ups).

## Failure conditions

Missing critical source data, stale timestamps beyond threshold, or
contradictory evidence must surface as an EXPLICIT signal — never a neutral
default. `check_all_layers` wraps each layer query in try/except and emits
`LayerHealth(status="error", detail=<fixed string>)` per broken layer rather
than 500-ing the whole report or silently reporting "fresh" (prevention-log #70:
never let an infra fault degrade into a silent HTTP 200). Treat silent failure as
failure; prefer noisy ops to false confidence; record enough detail to debug.

## Orchestrator / sync health signals — real failure vs expected noise

When the admin portal shows "red" or the Postgres log looks alarming, triage
against the orchestrator's OWN design (`sql/033_sync_orchestrator.sql`,
`app/services/sync_orchestrator/`) before treating anything as a failure. Verify
live (`docker logs ebull-postgres`, `SELECT … FROM sync_runs`) — trace the
symptom to the row/mechanism.

- **`ERROR: duplicate key value violates unique constraint
  "idx_sync_runs_single_running"` = the EXPECTED concurrency gate firing, NOT a
  failure.** `idx_sync_runs_single_running` (`sql/033`) is a partial-unique index
  allowing one `status='running'` sync_run at a time. The designed gate is "try
  INSERT → on `UniqueViolation` raise `SyncAlreadyRunning`"
  (`executor.py::_start_sync_run`); `app/workers/scheduler.py` + `app/jobs/boot_sweep.py`
  catch it and log a graceful skip. Postgres logs the rejected INSERT at ERROR even
  though the app handles it — that line is the gate working. It ONLY appears while a
  LONG sync holds the gate (e.g. a post-restart `scope='behind'` boot freshness sweep
  doing a heavy `fundamentals` catch-up — observed 52k rows / ~43 min); steady-state
  (syncs < 5-min cadence) is silent. Do NOT "fix" this with a pre-INSERT existence
  check: the prevention-log ("A process-level lock does not buy DB-level isolation",
  line 426) endorses relying on the unique constraint + catching the typed exception.
  Cost is log noise + the high-frequency (price/FX) sync skipped for the catch-up's
  duration — a transient, self-healing gap, not lost data (missed cadence re-fires).

- **`sync_runs.status='failed'` with `error_category='orchestrator_crash'` =
  boot reaper output, usually dev churn.** `reaper.py::reap_orphaned_syncs(reap_all=True)`
  runs at jobs startup and transitions any leftover `running` row (from a dead prior
  process) to `failed/orchestrator_crash`. In dev, `uvicorn --reload` cycles + manual
  jobs restarts are the dominant source. A cluster around a restart time is expected;
  investigate only if they appear WITHOUT a restart (a live process crashing mid-sync).

- **`FATAL: database "ebull_test" does not exist` on the DEV cluster (5432) =
  test-harness noise.** DB-backed tests use the isolated test cluster
  (`ebull-postgres-test`, port 5433) + per-worker DBs via the canonical
  `ebull_test_conn` fixture, never the dev cluster. A test hardcoding
  `5432/ebull_test` produced these FATALs; the fix is the canonical fixture. If
  new ones appear, grep tests for a hardcoded `5432` / `/ebull_test"` literal.
