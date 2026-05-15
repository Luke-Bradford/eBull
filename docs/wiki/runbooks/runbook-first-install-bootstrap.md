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

## 2. What runs (26 stages)

Phases in order; the catalogue lives in
``app/services/bootstrap_orchestrator.py::_BOOTSTRAP_STAGE_SPECS`` and
is the source of truth (asserted == 26 at module load). Spec:
``docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md``.

#1174 added S25 ``mf_directory_sync`` (dedicated MF-directory refresh
for N-CSR classId resolution) + S26 ``sec_n_csr_bootstrap_drain``
(fund-scoped manifest enqueue for the #1171 fund-metadata parser).
Both ride the ``sec_rate`` lane.

1. **Phase A — init** (sequential, ``init`` lane, single thread):
   - S1 ``universe_sync`` (``nightly_universe_sync``; ~30s, ~1.5k rows).
1. **Phase B — eToro lane** (parallel with SEC):
   - S2 ``candle_refresh`` (``daily_candle_refresh``; full universe).
1. **Phase B — SEC reference lane** (``sec_rate``; shared 10 req/s bucket):
   - S3 ``cusip_universe_backfill``
   - S4 ``sec_13f_filer_directory_sync``
   - S5 ``sec_nport_filer_directory_sync``
   - S6 ``cik_refresh`` (``daily_cik_refresh``)
1. **Phase A3 — bulk archive download** (``sec_bulk_download`` lane;
   disjoint from ``sec_rate``):
   - S7 ``sec_bulk_download`` (#1020; fixed-URL SEC archives).
1. **Phase C — DB-bound bulk ingesters** (``db`` lane; same-source
   serialisation under one ``JobLock`` per #1064):
   - S8 ``sec_submissions_ingest``
   - S9 ``sec_companyfacts_ingest``
   - S10 ``sec_13f_ingest_from_dataset``
   - S11 ``sec_insider_ingest_from_dataset``
   - S12 ``sec_nport_ingest_from_dataset``
1. **Phase C' — secondary-pages walker** (``sec_rate``):
   - S13 ``sec_submissions_files_walk``
1. **Legacy / fallback chain** (``sec_rate``; idempotent no-ops when
   Phase C populated rows; primary write path on the slow-connection
   bypass — see #1041):
   - S14 ``filings_history_seed`` (``params={days_back: 730,
     filing_types: <three-tier allow-list>}``)
   - S15 ``sec_first_install_drain`` (``params={max_subjects: None}``)
   - S16 ``sec_def14a_bootstrap``
   - S17 ``sec_business_summary_bootstrap``
   - S18 ``sec_insider_transactions_backfill``
   - S19 ``sec_form3_ingest``
   - S20 ``sec_8k_events_ingest``
   - S21 ``sec_13f_recent_sweep`` (``sec_13f_quarterly_sweep`` with
     ``min_period_of_report`` ≈ today − 380d; #1008 bound)
   - S22 ``sec_n_port_ingest``
1. **Phase E — final derivations** (``db`` lane):
   - S23 ``ownership_observations_backfill``
   - S24 ``fundamentals_sync``

Total wall-clock: typically **60–90 minutes** on the bulk path,
dominated by ``sec_bulk_download`` + ``sec_submissions_files_walk``.
The legacy ``sec_first_install_drain`` path (slow-connection fallback)
dominates wall-clock when the bulk archives are skipped.

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
  freshly seeds 24 ``bootstrap_stages`` rows. Use when an operator
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

If S16/S17/S18/S19/S20 typed parsers (def14a / business summary /
insider txns / Form 3 / 8-K) report 0 instruments processed, check
that S14 ``filings_history_seed`` actually populated ``filing_events``
— without ``filing_events`` rows for the relevant form type, the
parser candidate selectors find nothing. On the bulk path (#1020),
S8 ``sec_submissions_ingest`` + S13 ``sec_submissions_files_walk``
seed the same ``filing_events`` rows; verify those completed before
suspecting the legacy chain.

## 7. Scheduler gate behaviour

While ``bootstrap_state.status`` is anything other than ``complete``,
the SCHEDULED_JOBS entries with ``prerequisite=_bootstrap_complete``
skip-and-log instead of firing. Grep
``app/workers/scheduler.py`` for ``_bootstrap_complete`` for the
current list; representative members:

- ``orchestrator_full_sync`` (entire DAG-walk path)
- ``fundamentals_sync`` (also gated by ``_has_any_coverage``)
- SEC ingest / bootstrap / backfill jobs

A skip writes a ``job_runs`` row with ``status='skipped'`` and the
reason ``"first-install bootstrap not complete; visit /admin to
run"``. Manual triggers (``POST /jobs/{name}/run``) bypass the gate
deliberately so an operator override stays available pre-bootstrap.

The bootstrap orchestrator itself dispatches stage jobs by direct
invocation (``_INVOKERS[name](params)``; PR1b #1064 widened the
contract from zero-arg to ``(Mapping) -> None``), bypassing the
scheduler-side gate but acquiring the per-source ``JobLock``
(PR1a #1064) so manual / scheduled triggers cannot run twice
simultaneously.
