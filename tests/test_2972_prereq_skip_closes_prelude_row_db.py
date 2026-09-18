"""#2972 — a body-level prerequisite skip must CLOSE the prelude's row.

⚠ These assert at the ``record_job_skip`` boundary rather than inside any one
job body, because the defect is shared machinery: 23 call sites of
``_record_prereq_skip`` all had it, and a test pinned to one job would go green
while the others kept manufacturing spurious failures.

⚠ In its own ``_db`` module deliberately: the string ``ebull_test_conn`` in a
test source db-marks the WHOLE module at collection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg

from app.jobs.runtime import _job_execution_slot
from app.services.ops_monitor import reap_orphaned_job_runs, record_job_skip, record_job_start

JOB = "test_2972_prereq_skip"


def _open_prelude_row(conn: psycopg.Connection[Any], *, started_at: datetime) -> int:
    """What ``run_with_prelude`` leaves behind before the body runs."""
    row = conn.execute(
        "INSERT INTO job_runs (job_name, started_at, status) VALUES (%s,%s,'running') RETURNING run_id",
        (JOB, started_at),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _rows(conn: psycopg.Connection[Any]) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT run_id, status, finished_at, row_count, error_msg, started_at "
        "FROM job_runs WHERE job_name=%s ORDER BY run_id",
        (JOB,),
    ).fetchall()


def _slot_wait(conn: psycopg.Connection[Any], run_id: int) -> float | None:
    row = conn.execute("SELECT execution_slot_wait_seconds FROM job_runs WHERE run_id=%s", (run_id,)).fetchone()
    assert row is not None
    return None if row[0] is None else float(row[0])


def test_a_skip_inside_a_slot_records_the_wait_it_incurred(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#3189 finding 13 — a skipped fire waited for admission like any other.

    ``_job_execution_slot`` is the OUTERMOST boundary: parameter validation,
    the bootstrap gate and the per-job prerequisite all run inside it. So a
    skip has already paid the admission wait, and writing NULL made the
    column's own contract false — ``None`` is supposed to mean "not written
    inside a slot" — and biased the telemetry toward fires that RAN.

    ⚠ ``0.0``, not "truthy": an uncontended slot admits immediately, and
    0.0-vs-None is exactly the distinction #3159 clause 2 built the column to
    keep. Asserting `is not None` would pass on a branch that wrote NULL for
    contended waits too, so the assertion is on the VALUE.
    """
    ebull_test_conn.autocommit = True
    with _job_execution_slot(JOB):
        run_id = record_job_skip(ebull_test_conn, JOB, "prerequisite not met")

    assert _slot_wait(ebull_test_conn, run_id) == 0.0


def test_a_start_inside_a_slot_records_the_wait_it_incurred(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The other writer: ``_tracked_job``'s fallback when no prelude row exists,
    which is the ``_PRELUDE_OPT_OUT_JOBS`` path."""
    ebull_test_conn.autocommit = True
    with _job_execution_slot(JOB):
        run_id = record_job_start(ebull_test_conn, JOB)

    assert _slot_wait(ebull_test_conn, run_id) == 0.0


def test_outside_a_slot_the_column_still_means_what_it_says(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """What the change must NOT do: invent a wait for a direct or bootstrap
    invocation, which genuinely holds no slot. ``None`` keeps meaning "not
    written inside a slot" — that is the contract finding 13 restores, not one
    it replaces."""
    ebull_test_conn.autocommit = True
    skip_id = record_job_skip(ebull_test_conn, JOB, "prerequisite not met")
    start_id = record_job_start(ebull_test_conn, JOB)

    assert _slot_wait(ebull_test_conn, skip_id) is None
    assert _slot_wait(ebull_test_conn, start_id) is None


def test_a_prelude_row_keeps_the_admission_it_measured(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """#2972's UPDATE branch must not overwrite a recorded wait.

    The prelude opens the row and writes the admission it measured; a later
    skip re-reads the same contextvar, so the write is COALESCEd onto the
    STORED value. Seeded at 12.5 here — a figure no contextvar read could
    produce inside this test — so an overwrite is visible rather than
    coincidentally equal.
    """
    ebull_test_conn.autocommit = True
    run_id = _open_prelude_row(ebull_test_conn, started_at=datetime.now(UTC))
    ebull_test_conn.execute("UPDATE job_runs SET execution_slot_wait_seconds = 12.5 WHERE run_id = %s", (run_id,))

    with _job_execution_slot(JOB):
        record_job_skip(ebull_test_conn, JOB, "prerequisite not met", run_id=run_id)

    assert _slot_wait(ebull_test_conn, run_id) == 12.5


def test_a_skip_with_a_prelude_id_closes_that_row_and_adds_no_second_one(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The fix. Before it, this produced TWO rows — a `skipped` one and a
    `running` one that the boot reaper later rewrote as a failure."""
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    started = datetime.now(UTC) - timedelta(minutes=3)
    run_id = _open_prelude_row(ebull_test_conn, started_at=started)

    returned = record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do", run_id=run_id)

    rows = _rows(ebull_test_conn)
    assert len(rows) == 1, f"expected the prelude row to be CLOSED, not joined by a second: {rows}"
    assert returned == run_id
    assert rows[0][1] == "skipped"
    assert rows[0][2] is not None
    assert rows[0][3] == 0
    assert "nothing to do" in rows[0][4]


def test_the_prelude_rows_started_at_is_preserved(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The run really did begin when the prelude opened the row.

    Overwriting ``started_at`` with the skip instant would understate how long
    the lane was held — the INSERT path only equates the two because on that
    path there is no earlier start to preserve.
    """
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    started = datetime.now(UTC) - timedelta(minutes=7)
    run_id = _open_prelude_row(ebull_test_conn, started_at=started)

    record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do", run_id=run_id)

    rows = _rows(ebull_test_conn)
    assert abs((rows[0][5] - started).total_seconds()) < 1
    assert rows[0][2] > rows[0][5]


def test_nothing_survives_for_the_boot_reaper_to_convert(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The defect's actual consequence, asserted end to end.

    A stranded ``running`` row is only the intermediate state — what the
    operator eventually sees is ``reap_orphaned_job_runs`` rewriting it
    ``status='failure'`` with "orphaned: reaped at boot", which is false in
    every particular. After the fix the reaper finds nothing to convert.
    """
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    run_id = _open_prelude_row(ebull_test_conn, started_at=datetime.now(UTC) - timedelta(hours=2))
    record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do", run_id=run_id)

    reap_orphaned_job_runs(ebull_test_conn, reap_all=True)

    rows = _rows(ebull_test_conn)
    assert len(rows) == 1
    assert rows[0][1] == "skipped"
    assert "orphaned" not in (rows[0][4] or "")


def test_an_already_terminal_run_id_is_not_clobbered(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """The UPDATE is guarded on ``status='running'``.

    A caller mistake must cost an extra row, never a destroyed outcome — so a
    finished run keeps its status and the skip falls back to inserting.
    """
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    row = ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, finished_at, status, row_count) "
        "VALUES (%s, now(), now(), 'success', 42) RETURNING run_id",
        (JOB,),
    ).fetchone()
    assert row is not None
    finished_run_id = int(row[0])

    returned = record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do", run_id=finished_run_id)

    rows = _rows(ebull_test_conn)
    assert returned != finished_run_id
    assert len(rows) == 2
    by_id = {r[0]: r for r in rows}
    assert by_id[finished_run_id][1] == "success"
    assert by_id[finished_run_id][3] == 42
    assert by_id[returned][1] == "skipped"


def test_a_run_id_belonging_to_another_job_is_not_closed(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """``job_name`` is in the WHERE clause, so a leaked contextvar from a
    different invoker cannot close someone else's open run."""
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True
    other = ebull_test_conn.execute(
        "INSERT INTO job_runs (job_name, started_at, status) VALUES ('test_2972_other', now(), 'running') "
        "RETURNING run_id",
        (),
    ).fetchone()
    assert other is not None
    other_run_id = int(other[0])

    record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do", run_id=other_run_id)

    still_running = ebull_test_conn.execute("SELECT status FROM job_runs WHERE run_id=%s", (other_run_id,)).fetchone()
    assert still_running is not None
    assert still_running[0] == "running"
    assert len(_rows(ebull_test_conn)) == 1


def test_without_a_prelude_id_the_historical_insert_is_unchanged(
    ebull_test_conn: psycopg.Connection[Any],
) -> None:
    """Legacy direct calls and tests pass no ``run_id``; that path must keep
    inserting a self-contained row with ``started_at = finished_at``."""
    ebull_test_conn.commit()
    ebull_test_conn.autocommit = True

    record_job_skip(ebull_test_conn, JOB, "prereq_missing: nothing to do")

    rows = _rows(ebull_test_conn)
    assert len(rows) == 1
    assert rows[0][1] == "skipped"
    assert rows[0][5] == rows[0][2]
