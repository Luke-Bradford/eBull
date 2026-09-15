"""#2274 — the heartbeat writer against a real ``job_runs``, with a real pool.

⚠⚠ THE REGISTERED POOL IS THE TEST, NOT SCAFFOLDING. Two separate things
depend on it and both are silent when it is absent:

1. ``background_write_connection``'s pooled branch rolls back any non-IDLE
   transaction before returning the connection, while its raw fallback
   commits on clean exit. That is precisely how ``checkpoint_progress``
   shipped broken (fixed in ``3f3c3517``) — a test on the raw path passes
   against the bug.
2. ``job_heartbeat`` GATES on pool presence as its ownership predicate, so
   without a pool it installs nothing and every assertion here would be
   vacuously about a no-op.

Separate module from ``tests/test_2274_job_heartbeat.py`` because
``tests/conftest.py`` applies the ``db`` marker per MODULE.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import psycopg
import pytest

from app.db.background_write import set_background_pool
from app.jobs import runtime as jobs_runtime
from app.services.job_heartbeat import job_heartbeat
from app.services.sync_orchestrator.progress import report_progress
from app.workers.scheduler import _tracked_job
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.skipif(not test_db_available(), reason="ebull_test Postgres not reachable")

_FIXTURE_JOB = "test_2274_heartbeat_fixture"
_CLEANUP = "DELETE FROM job_runs WHERE job_name = %s"


@contextmanager
def _registered_pool(url: str) -> Iterator[None]:
    """A minimal real pool so the seam takes its POOLED branch."""

    class _Pool:
        def __init__(self) -> None:
            self._conn = psycopg.connect(url)

        @contextmanager
        def connection(self) -> Iterator[psycopg.Connection[Any]]:
            yield self._conn

        def close(self) -> None:
            self._conn.close()

    pool = _Pool()
    # cast: the seam only ever calls ``.connection()``; a real
    # BackgroundConnectionPool would need a running jobs process.
    set_background_pool(cast(Any, pool))
    try:
        yield
    finally:
        set_background_pool(None)
        pool.close()


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[psycopg.Connection[Any]]:
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())
    conn: psycopg.Connection[Any] = psycopg.connect(test_database_url(), autocommit=True)
    conn.execute(_CLEANUP, (_FIXTURE_JOB,))
    set_background_pool(None)
    try:
        yield conn
    finally:
        set_background_pool(None)
        try:
            conn.execute(_CLEANUP, (_FIXTURE_JOB,))
        finally:
            conn.close()


def _insert_run(conn: psycopg.Connection[Any], status: str = "running") -> int:
    row = conn.execute(
        """
        INSERT INTO job_runs (job_name, status, started_at, progress_json)
        VALUES (%(job)s, %(status)s, now(), '{"sentinel": "must survive"}'::jsonb)
        RETURNING run_id
        """,
        {"job": _FIXTURE_JOB, "status": status},
    ).fetchone()
    assert row is not None
    return int(row[0])


def _read(conn: psycopg.Connection[Any], run_id: int) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT processed_count, target_count, last_progress_at, progress_json
          FROM job_runs WHERE run_id = %s
        """,
        (run_id,),
    ).fetchone()
    assert row is not None
    return {
        "processed_count": row[0],
        "target_count": row[1],
        "last_progress_at": row[2],
        "progress_json": row[3],
    }


def test_a_tick_lands_through_the_pooled_seam(reader: psycopg.Connection[Any]) -> None:
    """The regression shape from ``checkpoint_progress``: an uncommitted write
    is discarded on the pooled path, which is the only path the jobs process
    ever takes."""
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()), job_heartbeat(run_id):
        report_progress(7, 99, force=True)

    got = _read(reader, run_id)
    assert got["processed_count"] == 7
    assert got["target_count"] == 99
    assert got["last_progress_at"] is not None


def test_progress_json_is_untouched(reader: psycopg.Connection[Any]) -> None:
    """``daily_candle_refresh`` stores its scope/session watermark context
    there and ``watermarks.py::_resolve_candle_offset`` reads it. A
    ``(done, total)`` payload written over it would erase candle population
    provenance."""
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()), job_heartbeat(run_id):
        report_progress(7, 99, force=True)

    assert _read(reader, run_id)["progress_json"] == {"sentinel": "must survive"}


def test_a_terminalised_row_is_not_stamped_with_in_flight_state(reader: psycopg.Connection[Any]) -> None:
    """A tick can land after ``record_job_finish`` — the body's last forced
    tick races the terminal write."""
    run_id = _insert_run(reader, status="success")
    with _registered_pool(test_database_url()), job_heartbeat(run_id):
        report_progress(7, 99, force=True)

    got = _read(reader, run_id)
    assert got["last_progress_at"] is None
    assert got["processed_count"] in (0, None)


def test_an_unbounded_tick_stores_null(reader: psycopg.Connection[Any]) -> None:
    """``sql/140``: NULL ``target_count`` means UNBOUNDED — a value the
    producer asserts, not a gap to back-fill. A ``COALESCE`` here would
    invent a treatment the schema already defines."""
    run_id = _insert_run(reader)
    with _registered_pool(test_database_url()), job_heartbeat(run_id):
        report_progress(7, None, force=True)

    assert _read(reader, run_id)["target_count"] is None


# ---------------------------------------------------------------------------
# The install point — BOTH of ``_tracked_job``'s live branches
# ---------------------------------------------------------------------------


def test_the_prelude_branch_heartbeats(reader: psycopg.Connection[Any]) -> None:
    """⚠⚠ THE ONE THE PREDECESSOR SPEC GOT WRONG. Scheduled fires and
    queue-dispatched manual triggers consume ``pre_allocated_run_id`` and the
    branch RETURNS, so an install placed after ``record_job_start`` misses the
    primary path entirely."""
    run_id = _insert_run(reader)
    token = jobs_runtime._prelude_run_id.set(run_id)
    try:
        with _registered_pool(test_database_url()):
            with _tracked_job(_FIXTURE_JOB) as tracker:
                assert tracker.run_id == run_id
                report_progress(3, 4, force=True)
    finally:
        jobs_runtime._prelude_run_id.reset(token)

    got = _read(reader, run_id)
    assert got["last_progress_at"] is not None
    assert got["processed_count"] == 3


def test_the_fallback_branch_heartbeats(reader: psycopg.Connection[Any]) -> None:
    """The legacy/direct path, where ``record_job_start`` opens the row."""
    with _registered_pool(test_database_url()):
        with _tracked_job(_FIXTURE_JOB) as tracker:
            report_progress(3, 4, force=True)
            run_id = tracker.run_id
    assert run_id > 0

    got = _read(reader, run_id)
    assert got["last_progress_at"] is not None
    assert got["processed_count"] == 3


def test_a_body_that_never_ticks_leaves_the_heartbeat_null(reader: psycopg.Connection[Any]) -> None:
    """~80 of the tracked jobs are this case. A synthetic install-time tick
    would stamp every one of them — a fabricated progress claim, and the one
    thing #2274 forbids."""
    with _registered_pool(test_database_url()):
        with _tracked_job(_FIXTURE_JOB) as tracker:
            run_id = tracker.run_id

    assert _read(reader, run_id)["last_progress_at"] is None
