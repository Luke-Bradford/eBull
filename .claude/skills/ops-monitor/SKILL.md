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
