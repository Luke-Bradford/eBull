# Bound service-helper-owned DB connects reachable from job bodies (#1693, PR4c)

**Status:** spec (2026-06-20). Split from #1690. Umbrella: #1472 (connection discipline). Depends on #1690 (merged 9dbdd473).

**Goal:** close the coverage gap #1690 documented. #1690 bounded the raw `psycopg.connect` **body** sites in `app/workers/scheduler.py` via `connect_job()` + a per-job `statement_timeout` ContextVar set by `_tracked_job`. Scheduled bodies that delegate to a **service helper** which opens its own raw connect stayed unbounded — a SQL wedge there still strands the `job_runs` row `running` (the #1689 failure mode). Migrate those body-path service connects to `connect_job()`.

## Governing decision (not first-principles)

`docs/specs/infra/job-statement-timeout.md` §4 "Out of scope — service-helper-owned connects" (lines 76-79) — Codex ckpt-1 #2 + ckpt-2 on #1690 — named this exact follow-up and its mechanism: *"because the ContextVar is set for the whole call stack on the worker thread, migrating those service connects to `connect_job()` later bounds them for free — the eventual sweep is a mechanical swap, no plumbing change."* This spec executes that, after a full-population audit to confirm the named pair is complete.

## Full-population verification (the falsification)

The #1690 spec **named two** body-reachable service-helper connects. #1693 must not trust that on faith. Audited all **34 steady-state `SCHEDULED_JOBS`** bodies (of 38 total — the other 4 are `role` bootstrap×2 / backfill×2, already `statement_timeout_ms=None`-exempt in #1690) + reverse-traced every raw `psycopg.connect(` in the suspect-file population (counts + targets verified empirically 2026-06-20, dev tree on main 9dbdd473):

| Site | Reaching steady-state job(s) | Verdict |
|------|------------------------------|---------|
| `financial_facts_retention.py:145` (`sweep_retention_all_instruments`) | `financial_facts_retention_sweep` (scheduler.py:4643) | **MIGRATE** |
| `sec_bulk_refresh.py:679` (per-archive bootstrap probe) | `sec_submissions_bulk_refresh` / `sec_companyfacts_bulk_refresh` / `sec_quarterly_datasets_bulk_refresh` (scheduler.py:4888/4897/4913) | **MIGRATE** |

All four jobs are `role="steady_state"` with the default 30-min cap (no `statement_timeout_ms=`/`role=` override) — so the swap genuinely bounds them.

Everything else classified **out of scope, with reason** (no silent cap):
- `raw_payload_retention.py:185`, `filing_events_cleanup.py:95`, `canonical_instrument_redirects.py:288` — reached only from **manual-trigger** bodies (`raw_payload_retention_sweep`, `filing_events_skip_tier_cleanup`, `populate_canonical_redirects`), which are NOT in `SCHEDULED_JOBS` → `_JOBS_BY_NAME.get()→None` → unbounded by design (#1690 spec line 70).
- `fundamentals/bootstrap.py`, `sec_submissions_files_walk.py`, `sec_bulk_download.py:1501`, `sec_bulk_orchestrator_jobs.py`, `bootstrap_orchestrator.py`, `bootstrap_state.py`, `bootstrap_validation.py` — reached only from bootstrap-stage / manual `_INVOKERS` (runtime.py), never a steady-state body. scheduler.py imports of `sec_bulk_download`/`bootstrap_state` pull only connect-free helpers (`assert_archive_belongs_to_run`, `resolve_progress_context`).
- `sync_orchestrator/{executor,adapters,dispatcher,reaper}.py` — orchestrator layer reached by `orchestrator_full_sync`/`orchestrator_high_frequency_sync`; separately bounded (PR4a-bis gate). Separate sweep.
- `listener.py` (LISTEN waiters — must never carry statement_timeout), `__main__.py` (boot/reaper infra), `heartbeat.py:67` (`Heartbeat.beat()` supervisor writer, own DSN), prelude (`runtime.py`/`locks.py`), and `_tracked_job`'s own finalize/audit writes (#1690 — must stay raw) — all excluded by the #1690 design.

**Result: exactly the 2 named sites. Premise confirmed.**

## Design

### 1. `sec_bulk_refresh.py:679` — direct swap
`psycopg.connect(settings.database_url, autocommit=True)` → `connect_job(autocommit=True)` (import `from app.jobs.job_connection import connect_job`). The probe connects to `settings.database_url` unconditionally, so the swap is exact.

Behavior note (conscious, correct): the probe sits inside `try: … except psycopg.Error`. A `QueryCanceled` (SQLSTATE 57014) is a `psycopg.Error` subclass, so a wedged probe now self-aborts at the cap → caught → returns `RefreshResult(skipped_reason=…)`. This **eliminates the stranded-`running`** mode (the #1693 goal); the job records a clean skip and re-fires next cadence. A fence probe does not need retry/self-heal — skip-and-recadence is the right outcome, and matches the probe's existing "DB sick → skip, don't hammer SEC" intent. The migration strictly improves this site (was: potential strand-forever; now: bounded skip).

### 2. `financial_facts_retention.py:145` — branch on the escape-hatch param
`sweep_retention_all_instruments(database_url: str | None = None)` is called by the job body with **no arg** (settings.database_url) but by `tests/test_financial_facts_retention.py:334` with an explicit isolated-cluster URL. `connect_job()` hardcodes `settings.database_url`, so a blind swap would redirect the test's writes onto the dev cluster (prevention-log "DB test must use the canonical isolated fixture"). So:

```python
# #1693 — the scheduled-job body passes no database_url → connect_job binds the
# active job's statement_timeout (ContextVar set by _tracked_job). An explicit
# database_url (tests, isolated 5433 cluster) takes the raw path — connect_job
# hardcodes settings.database_url and would escape test isolation onto dev.
connect_cm = (
    connect_job(autocommit=True)
    if database_url is None
    else psycopg.connect(database_url, autocommit=True)
)
with connect_cm as conn:
    ...
```

Transaction shape unchanged (autocommit conn + per-instrument `with conn.transaction()`; the per-statement cap never false-trips a many-short-DELETE sweep — see #1690 source rule 3). The leftover `url = database_url or settings.database_url` line is removed (only the else-branch needs the literal, and it uses `database_url` directly). The function's docstring/comment text referencing `psycopg.connect(url, …)` is updated to describe the branch (Codex ckpt-1 #4 — keep docs honest).

**Why not extend `connect_job(dsn=…)` instead** (Codex ckpt-1 #3): cleaner long-term (no sentinel branch; isolated DSNs inherit the ContextVar), but `connect_job(*, autocommit, **kwargs)` forwards `**kwargs` to `psycopg.connect`, whose DSN is the *positional* `conninfo` — so carrying a DSN needs an explicit new param that replaces the hardcoded `settings.database_url`. That is the helper-signature/plumbing change #1693 scopes out ("no plumbing change"). Deferred; the guarded sentinel branch is the scope-respecting choice for PR4c.

### 3. Guard test — reuse #1690's source-inspection pattern
Add to `tests/test_job_connection.py` (mirrors `test_tracked_job_finalize_writes_stay_raw`):
- `test_retention_sweep_body_uses_connect_job` — `inspect.getsource(sweep_retention_all_instruments)` contains `connect_job(` (job path bound) AND still references `database_url` (escape hatch kept).
- **`test_retention_sweep_job_passes_no_database_url`** (Codex ckpt-1 #2 — the load-bearing invariant): `inspect.getsource(scheduler.financial_facts_retention_sweep)` calls `sweep_retention_all_instruments()` with **no `database_url=`** arg. Without this, a future "pass `settings.database_url` explicitly" refactor of the body would silently route the scheduled path through the raw (unbounded) else-branch — the source-only check on the service can't see that. This is the assertion that actually proves the *scheduled* path is bound.
- `test_bulk_refresh_probe_uses_connect_job` — the per-archive refresh fn's source contains `connect_job(` and no raw `psycopg.connect(settings.database_url` for the probe.

**Scope of the guard (conscious):** source-inspection locks the two migrated sites against regression (un-migration). A full static call-graph lint that flags an arbitrary NEW steady-state-reachable raw connect in a NEW file is not built — for a 2-site population it is disproportionate, and the realistic regression (reverting these) is caught. Connection discipline is additionally guarded by the existing `scripts/check_caller_owned_tx.py`. Documented here, not silent.

## Testing / DoD
1. Local gates: ruff, ruff format --check, pyright, `pytest -m "not db"` (incl. the 2 new guards + existing #1690 suite), smoke.
2. No new DB-tier test — the cancel-on-cap mechanism is unchanged from #1690 (already covered by `tests/test_job_statement_timeout_db.py`); #1693 only widens *which connects* read the ContextVar.
3. Dev-verify the real path: trigger `financial_facts_retention_sweep` on dev, confirm it completes cleanly (the bounded `connect_job` path) and `/system/jobs` stays honest. Record in PR.
4. No migration/backfill/sec_rebuild (pure connection-path change). The running daemon picks up `connect_job` for these helpers only after restart onto merged main — restart is the steady-state activation (same as any scheduler/service change). Note in PR.

## Conscious tradeoffs
- **Manual-trigger + bootstrap helpers stay unbounded** (unchanged from #1690 — they resolve `_JOBS_BY_NAME.get()→None`). Bounding them needs a registry for non-`SCHEDULED_JOBS` jobs; out of scope.
- **`financial_facts_retention` keeps a raw-connect branch** for the explicit-DSN test path. Required for test isolation, not an oversight; the guard test asserts the job path (`database_url is None`) is the bounded one.
