# Runbook — First-install bootstrap

When an operator stands up a fresh eBull install, the database is
empty after the setup wizard completes. Scheduled jobs that depend
on a populated universe + filings + filer directories stay quiet
behind the ``_bootstrap_complete`` prerequisite gate until the
operator explicitly runs the first-install bootstrap.

This runbook covers when to run it, what to expect, and how to
recover from per-stage failures.

Spec: ``docs/superpowers/specs/2026-05-07-first-install-bootstrap.md``.

## 1. When to run

Click "Run bootstrap" on the admin page when:

- The operator has just saved their eToro broker credentials on a
  fresh install. The dashboard banner ("First-install bootstrap has
  not been run yet…") nudges to ``/admin``.
- The ``bootstrap_state.status`` is ``pending`` or ``partial_error``
  (the panel surfaces both prominently).
- An operator wants to widen historical depth on demand after a long
  gap (re-running on ``complete`` is allowed and creates a fresh
  run).

Do **not** run it as part of routine ops: scheduled jobs handle
incremental refresh once bootstrap is complete.

## 2. What runs (17 stages)

Phases in order; spec §"Stages and lanes" is the source of truth:

1. **Phase A — init** (sequential, single thread): ``universe_sync``
   (~30s, ~1.5k rows).
2. **Phase B — eToro lane** (parallel with SEC lane):
   ``candle_refresh`` (full universe; minutes).
3. **Phase B — SEC lane** (sequential, shared 11 req/s bucket; 15
   stages):
   - ``cusip_universe_backfill``
   - ``sec_13f_filer_directory_sync``
   - ``sec_nport_filer_directory_sync``
   - ``cik_refresh`` (``daily_cik_refresh``)
   - ``filings_history_seed`` (``bootstrap_filings_history_seed`` —
     2-year window, all form types)
   - ``sec_first_install_drain`` (~60min for ~12k filers)
   - ``sec_def14a_bootstrap`` / ``sec_business_summary_bootstrap`` /
     insider/Form 3/8-K typed parsers
   - ``sec_13f_quarterly_sweep`` / ``sec_n_port_ingest``
   - ``ownership_observations_backfill``
   - ``fundamentals_sync``

Total wall-clock: typically **60–90 minutes**, dominated by the SEC
manifest drain (``sec_first_install_drain``).

## 3. Watching it

The admin panel polls ``GET /system/bootstrap/status`` every 5
seconds while a run is in flight. Each stage row shows:

- Current status (``pending`` / ``running`` / ``success`` /
  ``error`` / ``skipped``).
- Progress where the underlying job exposes ``expected_units`` /
  ``units_done``; ``rows_processed`` otherwise.
- Elapsed wall-clock and a truncated ``last_error`` (click to
  expand).

For deeper forensics on a failing stage, jump to the underlying
``job_runs`` row from the admin Background-tasks panel — the
orchestrator's per-stage dispatch routes through the same
``_tracked_job`` shape every scheduled fire writes.

## 4. Per-stage failure paths

Errors do not abort the run. The lane runner catches the exception,
records ``status='error'`` + ``last_error``, and continues to the
next stage. Phase B's two lane threads are independent — a SEC-lane
error does not stop the eToro lane and vice versa.

After the run finalises with at least one error,
``bootstrap_state.status='partial_error'``. Three operator actions:

- **Retry failed (N)** — reuses the same ``bootstrap_runs.id``.
  Resets failed stages **plus all later-numbered stages in the same
  lane** to ``pending`` (dependency walk: a downstream stage that
  ran on stale upstream data must be re-run with fresh data).
  Re-publishes the orchestrator queue row. Successful prior stages
  stay ``success`` and are skipped.
- **Re-run all** — creates a brand-new ``bootstrap_runs`` row +
  freshly seeds 17 ``bootstrap_stages`` rows. Use when an operator
  wants to widen historical depth or after a config change that
  affects upstream ingest (e.g. CIK universe expansion).
- **Mark complete** — operator escape hatch. Forces
  ``bootstrap_state.status='complete'`` so the scheduler gate
  releases. Use only when the operator has manually verified that
  every still-error stage's underlying problem is resolved (the
  panel itself does not enforce this — auditing is on the operator).

## 5. Boot-recovery for a crashed jobs process

If the jobs process crashes mid-bootstrap, ``bootstrap_state.status``
stays at ``running`` until the next jobs-process boot. On startup
the bootstrap reaper (``app/services/bootstrap_state.py::reap_orphaned_running``,
called at ``app/jobs/__main__.py`` Step 4b) sweeps:

- ``bootstrap_stages`` rows with ``status='running'`` →
  ``error`` with ``last_error='jobs process restarted mid-run'``.
- ``bootstrap_stages`` rows with ``status='pending'`` on the latest
  run → ``error`` with ``last_error='orchestrator did not dispatch
  before restart'``.
- ``bootstrap_runs.status`` → ``partial_error``.
- ``bootstrap_state.status`` → ``partial_error``.

The operator can then click "Retry failed" to drive everything
again. There is no cooperative-cancel button in v1; restarting the
jobs process is the abort path.

## 6. Common stage failures

### `cik_refresh` — 304 Not Modified loop

If the watermark is corrupted, the SEC ``company_tickers.json``
endpoint may return 304 indefinitely. Clear the watermark from
``external_data_watermarks`` and re-run.

### `filings_history_seed` — `no CIK-mapped instruments`

The previous stage (``cik_refresh``) failed to populate
``external_identifiers``. Retry from the failed stage; the
dependency walk re-runs everything from there.

### `sec_first_install_drain` — partial drain

The drain is bounded by the SEC 10 req/s shared bucket. A 60-minute
drain over 12k filers can hit transient SEC 503s that record per-CIK
errors but keep the stage running. ``last_error`` on the stage row
will summarise; per-CIK detail is in the underlying
``sec_first_install_drain`` ``job_runs`` row's log + the
``sec_filing_manifest`` ``last_fetch_error`` column.

### Typed parsers — `instruments=0` after the drain

If S6/S7/S8/etc parsers report 0 instruments processed, check that
``filings_history_seed`` (S5) actually populated ``filing_events`` —
without ``filing_events`` rows for the relevant form type, the
parser candidate selectors find nothing.

## 7. Scheduler gate behaviour

While ``bootstrap_state.status`` is anything other than ``complete``,
14 scheduled jobs skip-and-log instead of firing:

- ``orchestrator_full_sync`` (entire DAG-walk path)
- ``fundamentals_sync`` (also gated by ``_has_any_coverage``)
- 12 SEC ingest / bootstrap / backfill jobs

A skip writes a ``job_runs`` row with ``status='skipped'`` and the
reason ``"first-install bootstrap not complete; visit /admin to
run"``. Manual triggers (``POST /jobs/{name}/run``) bypass the gate
deliberately so an operator override stays available pre-bootstrap.

The bootstrap orchestrator itself dispatches stage jobs by direct
invocation (``_INVOKERS[name]()``), bypassing the scheduler-side
gate but acquiring the per-job ``JobLock`` so manual / scheduled
triggers cannot run twice simultaneously.
