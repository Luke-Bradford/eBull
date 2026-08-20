"""Pure-logic tests for the per-job statement_timeout plumbing (#1690).

No real DB — the connect call is faked so we assert only the libpq
``options`` the helper composes from the ContextVar. The end-to-end
"a long statement is cancelled" behaviour is in the db-tier test
``tests/test_job_statement_timeout_db.py``.
"""

from __future__ import annotations

import inspect
from typing import Any

import app.jobs.job_connection as jc
from app.workers import scheduler


def _capture_connect(monkeypatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_connect(_conninfo, **kw):  # noqa: ANN001
        captured["kw"] = kw
        return object()

    monkeypatch.setattr(jc.psycopg, "connect", fake_connect)
    return captured


def test_no_timeout_outside_tracked_job(monkeypatch):
    captured = _capture_connect(monkeypatch)
    jc.connect_job()
    assert "options" not in captured["kw"]


def test_timeout_applied_when_var_set(monkeypatch):
    captured = _capture_connect(monkeypatch)
    token = jc.job_statement_timeout_ms.set(1_800_000)
    try:
        jc.connect_job()
    finally:
        jc.job_statement_timeout_ms.reset(token)
    assert captured["kw"]["options"] == "-c statement_timeout=1800000"


def test_timeout_merges_onto_existing_options(monkeypatch):
    captured = _capture_connect(monkeypatch)
    token = jc.job_statement_timeout_ms.set(120_000)
    try:
        jc.connect_job(options="-c lock_timeout=5000")
    finally:
        jc.job_statement_timeout_ms.reset(token)
    opts = captured["kw"]["options"]
    assert "-c lock_timeout=5000" in opts
    assert "-c statement_timeout=120000" in opts


def test_autocommit_passthrough(monkeypatch):
    captured = _capture_connect(monkeypatch)
    jc.connect_job(autocommit=True)
    assert captured["kw"]["autocommit"] is True


def test_registry_resolves_default_for_steady_state():
    steady = next(j for j in scheduler.SCHEDULED_JOBS if j.role == "steady_state")
    assert scheduler._JOBS_BY_NAME[steady.name].statement_timeout_ms == (scheduler._DEFAULT_JOB_STATEMENT_TIMEOUT_MS)


def test_registry_exempts_heavy_bootstrap_backfill_jobs():
    exempt = [
        "sec_business_summary_bootstrap",
        "sec_def14a_bootstrap",
        "sec_insider_transactions_backfill",
        "ownership_observations_backfill",
    ]
    for name in exempt:
        assert scheduler._JOBS_BY_NAME[name].statement_timeout_ms is None


def test_unknown_job_name_not_in_registry():
    # Manual-trigger jobs absent from SCHEDULED_JOBS resolve to no bound.
    assert scheduler._JOBS_BY_NAME.get("sec_rebuild") is None


def test_tracked_job_finalize_writes_use_bounded_background_pool():
    # The terminal write must not open a new raw client at the exact cadence
    # boundary where the body connections peak. It also must not use
    # connect_job: the body statement timeout must not bound self-healing.
    src = inspect.getsource(inspect.unwrap(scheduler._tracked_job))
    code = "\n".join(ln for ln in src.splitlines() if not ln.strip().startswith("#"))
    assert "connect_job(" not in code
    assert "psycopg.connect(" not in code
    assert "background_write_connection(autocommit=False)" in code
    assert "record_job_finish(" in code


def test_job_body_connection_gets_attributable_application_name(monkeypatch):
    captured = _capture_connect(monkeypatch)
    token = jc.job_application_name.set("strategy_paper_cycle")
    try:
        jc.connect_job()
    finally:
        jc.job_application_name.reset(token)
    assert captured["kw"]["application_name"] == "ebull-job-body:strategy_paper_cycle"


# -- #1693 (PR4c): service-helper-owned connects reached from steady-state job
# bodies are bound via connect_job. Guards mirror the finalize-stay-raw pattern.


def test_retention_sweep_body_uses_connect_job():
    # The financial_facts_retention_sweep service path must bind the job's
    # statement_timeout via connect_job; the explicit-database_url escape hatch
    # (tests / isolated 5433 cluster) stays a raw connect.
    from app.services.financial_facts_retention import sweep_retention_all_instruments

    src = inspect.getsource(sweep_retention_all_instruments)
    assert "connect_job(autocommit=True)" in src  # job path is bounded
    assert "database_url" in src  # escape hatch kept for explicit-DSN callers


def test_retention_sweep_job_passes_no_database_url():
    # Codex ckpt-1 #2 (the load-bearing invariant): the scheduled body must call
    # the sweep with NO database_url= arg, else it silently routes through the
    # raw (unbounded) else-branch. Source-checking the service alone can't prove
    # the *scheduled* path is bound — this asserts the call site does.
    src = inspect.getsource(scheduler.financial_facts_retention_sweep)
    assert "sweep_retention_all_instruments()" in src
    assert "database_url=" not in src


def test_bulk_refresh_probe_uses_connect_job():
    # The per-archive bootstrap-state probe (reached by the three steady-state
    # sec_*_bulk_refresh jobs) must bind the job's statement_timeout — a wedged
    # probe self-aborts (QueryCanceled → skip) instead of stranding 'running'.
    from app.services.sec_bulk_refresh import refresh_bulk_archive_if_stale

    src = inspect.getsource(refresh_bulk_archive_if_stale)
    assert "connect_job(autocommit=True)" in src
    # The probe was the only raw connect in this fn — assert none remain. NB:
    # phrased as a bare ``psycopg.connect(`` check on purpose; the
    # destructive-path smoke gate (#129) greps test files for the raw
    # connect-against-settings-url literal, so we must not embed it here.
    assert "psycopg.connect(" not in src
