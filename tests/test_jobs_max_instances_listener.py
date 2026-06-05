"""#1472 PR-visibility — the EVENT_JOB_MAX_INSTANCES scheduler listener.

When APScheduler suppresses a scheduled fire because an instance is already
running (``max_instances=1``), the listener records a ``job_runs`` 'skipped'
row so the otherwise-silent suppression (the #1474 RCA's ~21h invisible freeze)
becomes operator-visible.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from apscheduler.events import EVENT_JOB_MAX_INSTANCES, JobSubmissionEvent

from app.jobs import runtime as runtime_mod
from app.jobs.runtime import JobRuntime
from tests.fixtures.ebull_test_db import test_database_url, test_db_available


def _max_instances_event(job_id: str | None) -> JobSubmissionEvent:
    """Build the JobSubmissionEvent APScheduler dispatches for a suppressed
    fire (the only field the handler reads is ``job_id``)."""
    return JobSubmissionEvent(EVENT_JOB_MAX_INSTANCES, job_id, "default", [])


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


def _runtime() -> JobRuntime:
    # No start() — the listener handler is independent of scheduler start.
    return JobRuntime()


def test_listener_registered_for_max_instances_event() -> None:
    rt = _runtime()
    # APScheduler stores (callback, mask) tuples in scheduler._listeners.
    listeners = rt._scheduler._listeners
    assert any(cb == rt._on_job_max_instances and (mask & EVENT_JOB_MAX_INSTANCES) for cb, mask in listeners), (
        "max-instances listener not registered"
    )


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_handler_records_skip_row_stripping_prefix(settings_use_test_db: str) -> None:
    rt = _runtime()
    job_name = "nightly_universe_sync"
    with psycopg.connect(settings_use_test_db, autocommit=True) as setup:
        setup.execute("DELETE FROM job_runs WHERE job_name = %s AND error_msg = %s", (job_name, "max_instances_active"))

    rt._on_job_max_instances(_max_instances_event(f"recurring:{job_name}"))

    with psycopg.connect(settings_use_test_db, autocommit=True) as verify:
        row = verify.execute(
            "SELECT status, error_msg FROM job_runs "
            " WHERE job_name = %s AND error_msg = %s "
            " ORDER BY run_id DESC LIMIT 1",
            (job_name, "max_instances_active"),
        ).fetchone()
    assert row is not None
    assert row[0] == "skipped"
    assert row[1] == "max_instances_active"

    with psycopg.connect(settings_use_test_db, autocommit=True) as cleanup:
        cleanup.execute(
            "DELETE FROM job_runs WHERE job_name = %s AND error_msg = %s", (job_name, "max_instances_active")
        )


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_handler_ignores_non_recurring_job_id(settings_use_test_db: str) -> None:
    rt = _runtime()
    before = _job_runs_count(settings_use_test_db)
    # Manual/other ids without the recurring: prefix must be ignored.
    rt._on_job_max_instances(_max_instances_event("manual:something"))
    rt._on_job_max_instances(_max_instances_event(None))
    assert _job_runs_count(settings_use_test_db) == before  # no rows written, no raise


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_handler_writes_via_registered_background_pool(settings_use_test_db: str) -> None:
    """When the jobs process has registered a bg pool, the skip write must
    borrow from it (the PR4c seam), not open a raw connection (Codex ckpt-2)."""
    from app.db.background_write import set_background_pool
    from app.jobs.background_pool import BackgroundConnectionPool

    bg = BackgroundConnectionPool(max_size=1)
    set_background_pool(bg)
    rt = _runtime()
    job_name = "nightly_universe_sync"
    try:
        rt._on_job_max_instances(_max_instances_event(f"recurring:{job_name}"))
        assert bg.metrics()["checkouts"] >= 1  # borrowed from the registered pool
    finally:
        set_background_pool(None)
        bg.close()
        with psycopg.connect(settings_use_test_db, autocommit=True) as cleanup:
            cleanup.execute(
                "DELETE FROM job_runs WHERE job_name = %s AND error_msg = %s",
                (job_name, "max_instances_active"),
            )


def test_handler_swallows_db_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A listener that raised would break APScheduler event dispatch — the
    handler must swallow every error."""
    rt = _runtime()

    def _boom(*_a: object, **_k: object) -> int:
        raise RuntimeError("synthetic DB failure")

    monkeypatch.setattr(runtime_mod, "record_job_skip", _boom)
    # Must NOT raise even though the write blows up.
    rt._on_job_max_instances(_max_instances_event("recurring:nightly_universe_sync"))


def _job_runs_count(url: str) -> int:
    with psycopg.connect(url, autocommit=True) as conn:
        row = conn.execute("SELECT count(*) FROM job_runs").fetchone()
    assert row is not None
    return int(row[0])
