"""#2274 — the in-flight job checkpoint must COMMIT, and must not stamp a terminal row.

``_JobTracker.checkpoint_progress`` writes through
``background_write_connection(autocommit=False)``. That seam returns a POOLED
connection and rolls back any non-IDLE transaction on exit, so an uncommitted
UPDATE is discarded on the pooled path — the path the jobs process always takes
— while the raw fallback (``psycopg.connect``, which commits on clean exit) made
the same code land in a CLI run.

⚠ THE POOL IS WHAT MAKES THIS VISIBLE. A test that lets the seam fall through to
the raw fallback passes against the BROKEN code, because that path commits for
reasons that have nothing to do with this function. The registered-pool fixture
is the test, not scaffolding around it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest

from app.db.background_write import set_background_pool
from app.services.job_progress import JobProgress
from app.workers.scheduler import _JobTracker
from tests.fixtures.ebull_test_db import test_database_url, test_db_available


@pytest.fixture(autouse=True)
def _clear_background_pool_global() -> Iterator[None]:
    set_background_pool(None)
    yield
    set_background_pool(None)


@contextmanager
def _registered_pool(url: str) -> Iterator[None]:
    """Register a minimal real pool so the seam takes its POOLED branch."""

    class _Pool:
        def __init__(self) -> None:
            self._conn = psycopg.connect(url)

        @contextmanager
        def connection(self) -> Iterator[psycopg.Connection[object]]:
            yield self._conn

        def close(self) -> None:
            self._conn.close()

    pool = _Pool()
    set_background_pool(pool)
    try:
        yield
    finally:
        set_background_pool(None)
        pool.close()


def _insert_run(conn: psycopg.Connection[object], status: str) -> int:
    row = conn.execute(
        """
        INSERT INTO job_runs (job_name, status, started_at)
        VALUES ('test_2274_checkpoint', %(status)s, now())
        RETURNING run_id
        """,
        {"status": status},
    ).fetchone()
    assert row is not None
    conn.commit()
    return int(row[0])


def _progress() -> JobProgress:
    return JobProgress(candidates_seen=17, outcomes={"attempted": 0}, errors={}, context={"probe": "2274"})


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_a_checkpoint_survives_the_pooled_seam() -> None:
    """The regression. Without the commit this reads NULL, because the seam
    rolls the transaction back when it returns the connection to the pool."""
    url = test_database_url()
    with psycopg.connect(url) as reader:
        run_id = _insert_run(reader, "running")
        tracker = _JobTracker("test_2274_checkpoint")
        tracker.run_id = run_id
        tracker.progress = _progress()
        with _registered_pool(url):
            tracker.checkpoint_progress()
        stored = reader.execute(
            "SELECT progress_json FROM job_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        assert stored is not None
        assert stored[0] is not None, "the in-flight checkpoint was rolled back by the pooled seam"
        assert stored[0]["candidates_seen"] == 17


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_a_late_checkpoint_does_not_stamp_a_terminal_row() -> None:
    """⚠ A checkpoint that lands after the run terminalised must not rewrite it
    with in-flight state — the strategy-evidence writer's guard, applied here."""
    url = test_database_url()
    with psycopg.connect(url) as reader:
        run_id = _insert_run(reader, "success")
        tracker = _JobTracker("test_2274_checkpoint")
        tracker.run_id = run_id
        tracker.progress = _progress()
        with _registered_pool(url):
            tracker.checkpoint_progress()
        stored = reader.execute(
            "SELECT progress_json FROM job_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        assert stored is not None
        assert stored[0] is None


@pytest.mark.skipif(not test_db_available(), reason="test DB unavailable")
def test_an_untracked_or_progressless_checkpoint_is_a_no_op() -> None:
    """Both early returns are contract, not defensive noise: a job whose start
    row failed to record has no run to stamp, and one that never set a
    ``JobProgress`` has nothing to say."""
    url = test_database_url()
    with psycopg.connect(url) as reader:
        run_id = _insert_run(reader, "running")
        with _registered_pool(url):
            no_run = _JobTracker("test_2274_checkpoint")
            no_run.progress = _progress()
            no_run.checkpoint_progress()

            no_progress = _JobTracker("test_2274_checkpoint")
            no_progress.run_id = run_id
            no_progress.checkpoint_progress()
        stored = reader.execute(
            "SELECT progress_json FROM job_runs WHERE run_id = %s", (run_id,)
        ).fetchone()
        assert stored is not None
        assert stored[0] is None
