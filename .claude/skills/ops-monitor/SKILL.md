# ops-monitor

## Purpose

Detect stale data, failed jobs, broken sources, and other operational problems.

## Inputs

- job runs\n- row counts\n- timestamps\n- error logs

## Outputs

- health report\n- alerts\n- stale data flags

## Rules

- Treat silent failure as failure\n- Prefer noisy ops to false confidence\n- Record enough detail for debugging

## Failure conditions

- Missing critical source data
- Stale timestamps beyond allowed threshold
- Contradictory evidence without explicit uncertainty handling

## Deliverable format

Return:
- status
- summary
- structured fields
- confidence / uncertainty note where relevant

## Orchestrator / sync health signals — what's a real failure vs expected noise

When the admin portal shows "red" or the Postgres log looks alarming, triage against
the orchestrator's OWN design (`sql/033_sync_orchestrator.sql`, `app/services/sync_orchestrator/`)
before treating anything as a failure. Verify live (`docker logs ebull-postgres`,
`SELECT … FROM sync_runs`) — the symptom is the surface; trace to the row/mechanism.

- **`ERROR: duplicate key value violates unique constraint "idx_sync_runs_single_running"`
  = the EXPECTED concurrency gate firing, NOT a failure.** `idx_sync_runs_single_running`
  is a partial-unique index allowing one `status='running'` sync_run at a time. The
  designed gate is "try INSERT → on `UniqueViolation` raise `SyncAlreadyRunning`"
  (`executor.py::_start_sync_run`); `scheduler.py` + `boot_sweep.py` catch it and log a
  graceful skip. Postgres logs the rejected INSERT at ERROR even though the app handles
  it — that log line is the gate working. It ONLY appears while a LONG sync holds the gate
  (e.g. a post-restart `scope='behind'` boot freshness sweep doing a heavy `fundamentals`
  catch-up — observed 52k rows / ~43 min). Steady-state (syncs < the 5-min cadence) is
  silent. Do NOT "fix" this by adding a pre-INSERT existence check: the prevention-log
  ("A process-level lock does not buy DB-level isolation") endorses relying on the unique
  constraint + catching the typed exception. The cost is log noise during catch-up + the
  scheduled high-frequency (price/FX) sync being skipped for the catch-up's duration — a
  transient, self-healing availability gap, not lost data (missed cadence re-fires).

- **`sync_runs.status='failed'` with `error_category='orchestrator_crash'` = boot reaper
  output, usually dev churn.** `reaper.py::reap_orphaned_syncs(reap_all=True)` runs at jobs
  startup and transitions any leftover `running` row (from a dead prior process) to
  `failed/orchestrator_crash`. In dev, `uvicorn --reload` cycles + manual jobs restarts are
  the dominant source. A cluster of these around a restart time is expected; investigate
  only if they appear WITHOUT a restart (would mean a live process is crashing mid-sync).
  Note: the reaper is BOOT-ONLY — there is no periodic stale-`running` watchdog, because a
  safe age timeout would need a per-layer progress signal (`sync_runs.last_progress_at`) that
  the boot-sweep layers don't currently populate. A genuinely-hung live sync therefore blocks
  all syncs until the next restart; flag that as an architecture gap, don't add a blind
  age-based reaper (it would kill legitimately-slow catch-ups).

- **`FATAL: database "ebull_test" does not exist` on the DEV cluster (5432) = test-harness
  noise, now fixed.** DB-backed tests use the isolated test cluster (`POSTGRES_TEST_PORT`,
  5433) + per-worker DBs, never the dev cluster. A test hardcoding `5432/ebull_test`
  produced these FATALs; the fix is the canonical `ebull_test_conn` fixture (see prevention-log
  "A DB-backed test must use the canonical isolated test-cluster fixture"). If new ones appear,
  grep tests for a hardcoded `5432` / `/ebull_test"` literal.
