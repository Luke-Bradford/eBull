"""DB-tier integration test for the per-job statement_timeout (#1690).

Proves the wired mechanism end-to-end: with the ContextVar set, a body
statement that exceeds the cap is cancelled by the backend
(``QueryCanceled``), and that exception classifies as a transient
``source_down`` failure — the precondition for ``record_job_finish`` to set
``next_retry_at`` and self-heal (the retry/exhaust→red path itself is covered
by the existing ops_monitor tests).
"""

from __future__ import annotations

import psycopg
import psycopg.errors
import pytest

import app.jobs.job_connection as jc
from app.services.ops_monitor import _is_transient
from app.services.sync_orchestrator.exception_classifier import classify_exception
from app.services.sync_orchestrator.layer_types import FailureCategory
from tests.fixtures.ebull_test_db import test_database_url


def _route_to_test_db(monkeypatch) -> None:
    """Make connect_job open against the isolated worker DB while preserving
    the libpq ``options`` it composes (so the statement_timeout is applied)."""
    real_connect = jc.psycopg.connect
    url = test_database_url()

    def routed(_conninfo, **kw):  # noqa: ANN001
        return real_connect(url, **kw)

    monkeypatch.setattr(jc.psycopg, "connect", routed)


def test_connect_job_statement_timeout_cancels_and_classifies_transient(monkeypatch, ebull_test_conn) -> None:
    _route_to_test_db(monkeypatch)

    token = jc.job_statement_timeout_ms.set(50)  # 50 ms cap
    try:
        with pytest.raises(psycopg.errors.QueryCanceled) as excinfo:
            with jc.connect_job() as conn:
                conn.execute("SELECT pg_sleep(2)")
    finally:
        jc.job_statement_timeout_ms.reset(token)

    # statement_timeout cancellation must drive the self-heal path.
    category = classify_exception(excinfo.value)
    assert category == FailureCategory.SOURCE_DOWN
    assert _is_transient(category) is True


def test_connect_job_unbounded_runs_without_cancel(monkeypatch, ebull_test_conn) -> None:
    # var unset → no statement_timeout → a short sleep completes normally.
    _route_to_test_db(monkeypatch)

    with jc.connect_job() as conn:
        row = conn.execute("SELECT pg_sleep(0.05)").fetchone()
    assert row is not None
