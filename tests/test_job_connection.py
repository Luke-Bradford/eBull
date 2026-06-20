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


def test_tracked_job_finalize_writes_stay_raw():
    # Codex ckpt-1 #1: _tracked_job's own record_job_* writes must NOT use
    # connect_job — the per-job cap would otherwise bound the self-heal write
    # itself, which could strand the row in 'running'.
    src = inspect.getsource(inspect.unwrap(scheduler._tracked_job))
    code = "\n".join(ln for ln in src.splitlines() if not ln.strip().startswith("#"))
    assert "connect_job(" not in code  # no bounded connect in the finalize path
    assert "psycopg.connect(" in code  # finalize writes stay raw
    assert "record_job_finish(" in code
