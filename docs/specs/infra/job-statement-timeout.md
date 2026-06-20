# Per-job `statement_timeout` for job-body connections (#1690)

**Status:** spec (2026-06-20). Split from #1689 Decision 4. Umbrella: #1472 (connection discipline).
**Goal:** a scheduled job whose body query wedges on a SQL-level wait (lock wait, runaway scan) self-aborts at the backend, so the existing terminal-write + `_retry_plan` self-heal path takes over — instead of leaving a `job_runs` row `status='running'` forever (only the boot reaper catches it, at next restart).

## Problem

Python cannot force-kill a hung thread. A scheduled-job thread wedged inside a SQL call never returns → `_tracked_job` never reaches `record_job_finish` → the row stays `running` until the next process restart's `reap_all`. #1689 chose (Decision 4) the only safe root-cause fix: **bound the query** so the wedged backend cancels itself.

## Source rule (governing decisions, not first-principles)

1. **#1689 Decision 4** — chosen: timeout-bounded adapters via per-job `statement_timeout`. Rejected: blind/periodic age-based reaper (cannot reclaim the wedged thread; would paint a genuinely-stuck job amber "retrying", violating "red = genuinely-stuck only"; age can't tell hung-from-slow).
2. **`docs/proposals/infra/2026-06-04-db-connection-discipline.md` GAP-A/GAP-B + `app/jobs/runtime.py:629-639` comment** — `statement_timeout` is applied **scoped per-connection**, NEVER a process-global `PGOPTIONS` (which would also kill legitimate long ETL). This spec stays inside that settled constraint: per-connection, not global.
3. **PG `statement_timeout` semantics (verified empirically, dev PG17, 2026-06-20):**
   - `statement_timeout` bounds a **single statement**, including time spent **waiting on a lock**. It does NOT bound whole-job wall-clock. → A 6h sweep that issues many short statements is SAFE under a 30-min cap; only a job running a single >cap statement would false-trip.
   - SQLSTATE `57014` `QueryCanceled` is a subclass of `psycopg.OperationalError` → `classify_exception` → `FailureCategory.source_down` → `_is_transient(...) == True`. So the self-heal (retry-with-backoff, then exhaust→red) path fires with **no classifier change**.
   - The bound is applied via the **libpq `options='-c statement_timeout=<ms>'` connect parameter**, NOT a `SET` statement, because:
     - `options=` is a startup parameter → immune to transaction rollback (verified: stays set after `rollback()`).
     - a plain `SET statement_timeout` (even non-`LOCAL`) **is reverted on `ROLLBACK`** (verified: `8910`→`0`). Most job bodies are non-autocommit and roll back on close → a `SET` would be silently undone mid-job.
     - `options=` opens no implicit transaction (no savepoint/commit interaction).
   - `options=` kwarg merges cleanly onto the URL conninfo (`settings.database_url` carries no `options`; verified `'options' in url == False`).

## Design

### 1. `connect_job()` helper — the bounded-connect drop-in
New module `app/jobs/job_connection.py`:

```python
_job_statement_timeout_ms: ContextVar[int | None] = ContextVar("_job_statement_timeout_ms", default=None)

def connect_job(*, autocommit: bool = False, **kwargs) -> psycopg.Connection:
    """Drop-in for psycopg.connect(settings.database_url, ...) inside a
    scheduled-job body. When the active job has a statement_timeout
    (set by _tracked_job via the ContextVar), applies it as a libpq
    startup option. Outside a tracked job (var=None) behaves identically
    to a raw connect — no regression for non-job / manual / unmigrated sites."""
    ms = _job_statement_timeout_ms.get()
    if ms is not None:
        opt = f"-c statement_timeout={ms}"
        kwargs["options"] = f"{kwargs['options']} {opt}" if kwargs.get("options") else opt
    return psycopg.connect(settings.database_url, autocommit=autocommit, **kwargs)
```

Returns a `Connection` (a context manager) — identical use in both `with connect_job() as conn:` and `with (Provider(...), connect_job() as conn):` tuple forms. Transaction shape of every call site is **unchanged** — the helper only adds a connect option.

### 2. Per-job cap on `ScheduledJob` + registry lookup
- Add field `statement_timeout_ms: int | None = _DEFAULT_JOB_STATEMENT_TIMEOUT_MS` to `ScheduledJob` (`scheduler.py:232`).
- `_DEFAULT_JOB_STATEMENT_TIMEOUT_MS = 30 * 60 * 1000` (30 min). Generous: exceeds the longest legitimate **single** statement for steady-state keepers (per-statement, not per-job — see source rule 3).
- `None` = unbounded (explicit exemption).
- Build `_JOBS_BY_NAME: dict[str, ScheduledJob] = {j.name: j for j in SCHEDULED_JOBS}` (none exists today).
- Exemptions (set `statement_timeout_ms=None` explicitly, with a comment citing why heavy): the 4 `role in {bootstrap,backfill}` scheduled jobs —
  `JOB_SEC_BUSINESS_SUMMARY_BOOTSTRAP`, `JOB_SEC_DEF14A_BOOTSTRAP`,
  `JOB_SEC_INSIDER_TRANSACTIONS_BACKFILL`, `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL`
  (single bulk-load/scan statements may legitimately exceed the cap during install/catch-up).
- The two 6h sweeps (`sec_13f_quarterly_sweep`, `sec_n_port_monthly_sweep`) are retired from `SCHEDULED_JOBS` post-#1155 → no registry row → naturally unbounded (manual-trigger path, see §4). No action needed; documented here so a future re-instatement remembers to tag them.

### 3. Wire into the chokepoint `_tracked_job`
`_tracked_job(job_name)` (`scheduler.py:1738`) wraps every job **body**. On entry:
```python
job = _JOBS_BY_NAME.get(job_name)
ms = job.statement_timeout_ms if job is not None else None
tok = _job_statement_timeout_ms.set(ms)
try:
    ...  # existing prelude / fallback body
finally:
    _job_statement_timeout_ms.reset(tok)
```
Token-based set/reset → nests correctly for the orchestrator's inner-adapter `_tracked_job` re-entry. ContextVar lives only for the duration of the invoker call on its worker thread (same proven pattern as `_prelude_run_id`, `_HELD_SOURCES`).

**Manual-trigger jobs** (`sec_rebuild`, `fx_history_backfill`, bootstrap-stage drains) are invoked via `_tracked_job` but are NOT in `SCHEDULED_JOBS` → `_JOBS_BY_NAME.get()` returns `None` → var stays `None` → **unbounded**. This is the desired exemption for the heavy manual jobs, achieved for free (they are operator-initiated + observed, and `reap_all` catches their orphans at restart). v1 scope; can tighten later.

### 4. Migrate `scheduler.py` job-body connect sites
Swap `psycopg.connect(settings.database_url[, autocommit=...])` → `connect_job([autocommit=...])` for the **job-body** sites in `app/workers/scheduler.py`. Pure drop-in: behavior changes ONLY when the ContextVar is set (i.e. for a `SCHEDULED_JOBS` member with a non-None cap). Manual/exempt job bodies migrated to `connect_job()` resolve var=None → identical to today.

- **MUST NOT migrate `_tracked_job`'s own terminal/bookkeeping writes** (Codex ckpt-1 #1): the `record_job_finish` / `record_job_start` / `record_job_skip` connects at `scheduler.py:1786, 1799, 1821, 1840, 1853, 1901, 1951`. The ContextVar is still set when the finalize block runs (it is reset only after `_tracked_job` fully exits), so wrapping these would bound the **self-heal write itself** — if it timed out, the row would stay `running`, defeating the whole mechanism. These stay raw `psycopg.connect`. A code comment marks them as deliberately-unbounded. (Belt-and-braces: a guard test asserts these line ranges don't use `connect_job`.)
- **Out of scope — service-helper-owned connects** (Codex ckpt-1 #2, ckpt-2): scheduled bodies that delegate to a service helper which opens its own raw connect remain unbounded in v1. Known concrete instances (steady-state jobs whose body wedge would still strand `running`):
  - `financial_facts_retention_sweep` → `sweep_retention_all_instruments()` (`financial_facts_retention.py:145`).
  - `sec_submissions_bulk_refresh` / `sec_companyfacts_bulk_refresh` / `sec_quarterly_datasets_bulk_refresh` → `refresh_archive_set` → bootstrap probe (`sec_bulk_refresh.py:679`).
  NOTE: because the ContextVar is set for the whole call stack on the worker thread, migrating those service connects to `connect_job()` later bounds them **for free** — the eventual sweep is a mechanical swap, no plumbing change. Tracked by **#1693** under #1472 (the PR4c service-helper sweep). Documented + ticketed so the coverage limit is explicit, not silent (prevention-log "no silent caps").
- **Out of scope — prelude DB ops** (Codex ckpt-1 #3): `run_with_prelude` / `_run_prelude` (`runtime.py:761`) open/use the DB BEFORE `_tracked_job` sets the var → unbounded by this design. Low-risk + intentional: the source lock uses **non-blocking `pg_try_advisory_lock`** (returns immediately, cannot lock-wait — `locks.py`), the fence check + `job_runs` INSERT are short fixed-shape statements, and **connect-time** wedges are already bounded by `PGCONNECT_TIMEOUT` (#1475, PR0). The scheduled-fire gate/prereq path is separately bounded at 15s (PR4a-bis). No new bound added here.
- **Out of scope — other files** (umbrella #1472 PR4c): `listener.py` (LISTEN connections must NOT carry a statement_timeout — long-lived waiters), `__main__.py`, `sync_orchestrator/executor.py` (orchestrator layers — own gate-check path already bounded by PR4a-bis), `sec_bulk_orchestrator_jobs.py`.

## Testing
Pure-logic unit (`tests/`):
- `connect_job` with var unset → no `options`; with var=N → `options` contains `statement_timeout=N`; merges onto a pre-existing `options=`.
- `_JOBS_BY_NAME` lookup: steady-state → default ms; the 4 exempt → None; unknown name → None.

DB-backed integration (`-m db`, one test):
- A job body that runs `SELECT pg_sleep(...)` exceeding a tiny injected cap → `QueryCanceled` raised → `_tracked_job` records `failure`/`source_down` → `next_retry_at` set (self-heals). Drive via a fake registry entry with `statement_timeout_ms=50`.

## DoD / dev-verification
1. Local gates: ruff, ruff format --check, pyright, `pytest -m "not db"`, smoke.
2. `pytest -m db` for the integration test (DB-touching).
3. Dev-verify the real path: inject a tiny cap on one steady-state job in dev, confirm `/system/jobs` shows it go amber "retrying" (not red) on first trip, then red after exhaustion — and that a normal run is unaffected. Record the job + observed verdicts in the PR.
4. No migration/backfill/sec_rebuild (pure connection-path change) → **no jobs-daemon restart required for data**; but the running daemon won't pick up `connect_job` until restarted onto the merged main (note in PR; restart is the steady-state activation, same as any scheduler.py change).

## Conscious tradeoffs
- **Manual-trigger + 6h-sweep jobs stay unbounded in v1.** They are the heaviest + operator-observed + reaped at boot. Bounding them needs a registry for non-`SCHEDULED_JOBS` jobs — deferred. The wedge #1689 actually observed was a *scheduled* keeper silently red; that cohort is covered.
- **30-min default is a guess at "longest legit single statement," not profiled.** A false-red would surface in dev-verify (step 3) on the panel; mitigated by generous default + explicit exemptions. If a steady-state job legitimately runs a >30-min single statement, it gets an explicit higher cap.
- **`source_down` is reused for a timeout cancel** (semantically "infra", retriable) rather than a new category. Matches issue's desired self-heal→exhaust→red behavior; no new `FailureCategory` churn.
