"""#2603 — a failing misfire ARM must not cost the skip row its telemetry.

PR #3219's review raised this as BLOCKING: ``_arm_missed_fire`` swallows an
exception from its ``UPDATE`` while sharing ``conn`` with the ``record_job_skip``
INSERT issued moments earlier in the same ``with background_write_connection()``
block. If that block were transactional, a DB-level failure would abort the
transaction, the outer commit would raise ``InFailedSqlTransaction``, and the
INSERT would be lost — destroying the listener's whole purpose, which is that a
lost fire stops being SILENT.

It cannot. ``background_write_connection`` defaults to ``autocommit=True``
(``app/db/background_write.py:70``), so a failed statement leaves the connection
IDLE rather than aborted.

⚠ The reason is NOT "there is no transaction" — ``record_job_skip`` does open an
explicit ``conn.transaction()``, because autocommit never excludes one. That
transaction COMMITS before the function returns, i.e. before the arm runs at
all, so the row is already durable. That is the stronger guarantee and it is the
accurate one.

⚠ That is a rebuttal resting on a default argument two modules away, which is
exactly the kind of fact that stops being true without anyone noticing. So it is
pinned HERE against a real database instead of argued: flipping that call site to
``autocommit=False`` must fail a test, not silently start losing skip rows.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.db.background_write import background_write_connection
from app.jobs import runtime as runtime_module
from app.services.ops_monitor import MISFIRE_SKIP_PREFIX, RETRY_BASE_SECONDS
from app.workers.scheduler import JOB_CORE_REBALANCE_OBSERVATION
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def background_writes_hit_the_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Point ``background_write_connection``'s raw fallback at the TEST database.

    ⚠⚠ Without this the helper reads ``settings.database_url`` and writes to the
    DEV database, so the read-back on ``ebull_test_conn`` finds nothing and the
    test fails for the wrong reason — while quietly leaving a row behind in dev.
    It did exactly that on this file's first run; the row was deleted.

    ⚠ ``test_database_url()``, not ``ebull_test_conn.info.dsn``: psycopg strips
    the password out of ``info.dsn``, so the reconnect fails ``fe_sendauth: no
    password supplied``.

    ⚠ Redirecting the URL only reaches the RAW FALLBACK. A registered global
    pool short-circuits it (``background_write.py:89``) and would write wherever
    that pool points — so the pool is cleared for the test's duration rather
    than assumed absent (Codex ckpt-3).
    """
    from app.db.background_write import get_background_pool, set_background_pool

    previous = get_background_pool()
    set_background_pool(None)
    monkeypatch.setattr("app.db.background_write.settings.database_url", test_database_url())
    try:
        yield
    finally:
        set_background_pool(previous)


def _skip_rows(conn: psycopg.Connection[Any], job_name: str) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT run_id, status, error_msg, next_retry_at FROM job_runs WHERE job_name=%s ORDER BY run_id",
        (job_name,),
    ).fetchall()


def test_background_write_connection_is_autocommit_so_a_failed_arm_cannot_abort(
    ebull_test_conn: psycopg.Connection[Any],
    background_writes_hit_the_test_db: None,
) -> None:
    """The rebuttal's load-bearing fact, measured rather than asserted."""
    ebull_test_conn.autocommit = True
    with background_write_connection() as conn:
        assert conn.autocommit is True
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        with pytest.raises(psycopg.errors.DivisionByZero):
            conn.execute("UPDATE job_runs SET attempt = 1 WHERE run_id = %(id)s AND 1/0 = 1", {"id": -1})
        # The decisive assertion: a failed statement on an autocommit connection
        # leaves NO open transaction to poison, so there is nothing for the
        # with-block's exit to fail on.
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        assert conn.execute("SELECT 1").fetchone() == (1,)


def _fire_missed(job_name: str, scheduled_for: datetime) -> None:
    """Drive the REAL listener, so the production call site is what is tested.

    Pinning ``background_write_connection``'s default alone would not catch the
    regression this file exists to prevent: someone changing THIS call site to
    ``autocommit=False``. So the listener runs, not a hand-rolled imitation.
    """
    from apscheduler.events import EVENT_JOB_MISSED, JobExecutionEvent

    from app.jobs.runtime import JobRuntime, _adapt_zero_arg

    runtime = JobRuntime(
        database_url=test_database_url(),
        invokers={job_name: _adapt_zero_arg(lambda: None)},
    )
    runtime._on_job_missed(JobExecutionEvent(EVENT_JOB_MISSED, f"recurring:{job_name}", "default", scheduled_for))


def test_the_real_listener_records_and_arms_against_a_live_database(
    ebull_test_conn: psycopg.Connection[Any],
    background_writes_hit_the_test_db: None,
) -> None:
    """The happy path end to end — the row lands committed AND armed.

    Read back on a DIFFERENT connection on purpose: reading it on the writer
    would pass even if the row were merely uncommitted.
    """
    ebull_test_conn.autocommit = True
    ebull_test_conn.execute("DELETE FROM job_runs WHERE job_name=%s", (JOB_CORE_REBALANCE_OBSERVATION,))

    slot = datetime.now(UTC) - timedelta(minutes=3)
    _fire_missed(JOB_CORE_REBALANCE_OBSERVATION, slot)

    rows = _skip_rows(ebull_test_conn, JOB_CORE_REBALANCE_OBSERVATION)
    assert len(rows) == 1
    _run_id, status, error_msg, next_retry_at = rows[0]
    assert status == "skipped"
    assert str(error_msg).startswith(MISFIRE_SKIP_PREFIX)
    assert next_retry_at is not None, "an opted-in job's misfire must be armed"
    # Anchored at observation (~now), not at the lost slot three minutes ago.
    assert next_retry_at > slot + timedelta(seconds=RETRY_BASE_SECONDS)

    ebull_test_conn.execute("DELETE FROM job_runs WHERE job_name=%s", (JOB_CORE_REBALANCE_OBSERVATION,))


def test_a_skip_row_survives_an_arm_that_raises(
    ebull_test_conn: psycopg.Connection[Any],
    background_writes_hit_the_test_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """⚠⚠ The review's BLOCKING scenario, run for real.

    The arm is forced to raise INSIDE ``_arm_missed_fire``'s ``try``, on the
    same connection that just inserted the skip row. If the block were
    transactional the INSERT would be rolled back and this would read 0 rows —
    which is precisely the data loss the review predicted. It reads 1.
    """
    ebull_test_conn.autocommit = True
    ebull_test_conn.execute("DELETE FROM job_runs WHERE job_name=%s", (JOB_CORE_REBALANCE_OBSERVATION,))

    # ⚠ A genuine SQL error, NOT a Python one (Codex ckpt-3): a swallowed Python
    # exception never reaches PostgreSQL, so it could not abort a transaction
    # even if one were open — a test raising RuntimeError would pass under the
    # very semantics it claims to rule out. This makes the arm issue a statement
    # the server rejects, on the same connection that just inserted the row.
    real_delay = runtime_module.lost_fire_rearm_delay_seconds

    def _arm_with_a_broken_statement(*args: Any, **kwargs: Any) -> int | None:
        delay = real_delay(*args, **kwargs)
        assert delay is not None, "fixture assumes this job arms"
        return -(10**30)  # make_interval(secs => ...) overflows: a server-side error

    monkeypatch.setattr(runtime_module, "lost_fire_rearm_delay_seconds", _arm_with_a_broken_statement)

    _fire_missed(JOB_CORE_REBALANCE_OBSERVATION, datetime.now(UTC) - timedelta(minutes=3))

    rows = _skip_rows(ebull_test_conn, JOB_CORE_REBALANCE_OBSERVATION)
    assert len(rows) == 1, "the skip row must survive a failed arm — losing it is the defect being fixed"
    assert rows[0][1] == "skipped"
    assert str(rows[0][2]).startswith(MISFIRE_SKIP_PREFIX)
    # Unarmed, which is the pre-#2603 floor and therefore a safe outcome.
    assert rows[0][3] is None

    ebull_test_conn.execute("DELETE FROM job_runs WHERE job_name=%s", (JOB_CORE_REBALANCE_OBSERVATION,))
