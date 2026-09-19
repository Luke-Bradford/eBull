"""#2274 — orphan-reap recurrence counts on the admin Processes row.

DB-backed against the worker ``ebull_test`` template. ONE integration test per
genuinely-new SQL mechanism, per the repo's test-tiering rule — the mechanism
here is the event-vs-row collapse and the ``finished_at`` window, neither of
which is expressible without real rows.

Spec: ``docs/proposals/ops/2026-09-19-2274-repeat-reap-visibility.md``.
"""

from __future__ import annotations

import psycopg

from app.services.ops_monitor import ORPHAN_REAP_ERROR_MSG, reap_orphaned_job_runs
from app.services.processes import scheduled_adapter
from app.workers.scheduler import JOB_RETRY_DEFERRED


def _make_reap(
    conn: psycopg.Connection[tuple],
    *,
    job_name: str,
    started_days_ago: float,
    finished_days_ago: float,
    error_msg: str = ORPHAN_REAP_ERROR_MSG,
    error_category: str = "internal_error",
    status: str = "failure",
) -> None:
    """Insert one already-reaped ``job_runs`` row at an exact instant.

    ``finished_at`` is quantised to the second on purpose — that is the grain
    the event collapse groups on, so two rows given the same
    ``finished_days_ago`` land in the same event.
    """
    conn.execute(
        """
        INSERT INTO job_runs
               (job_name, started_at, finished_at, status, error_msg, error_category)
        VALUES (%(job)s,
                date_trunc('second', now() - %(started)s * interval '1 day'),
                date_trunc('second', now() - %(finished)s * interval '1 day'),
                %(status)s, %(msg)s, %(category)s)
        """,
        {
            "job": job_name,
            "started": started_days_ago,
            "finished": finished_days_ago,
            "status": status,
            "msg": error_msg,
            "category": error_category,
        },
    )


def test_one_boot_reaping_many_rows_is_ONE_event(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The unit is the reap EVENT, not the row.

    ``reap_orphaned_job_runs`` rewrites every orphaned row in a single UPDATE,
    so one boot can write many rows at the same instant — ``thesis_refresh`` has
    25 rows sharing one ``finished_at`` second on dev. Counting rows would
    report that boot as 25 separate failures.
    """
    for _ in range(3):
        _make_reap(
            ebull_test_conn,
            job_name=JOB_RETRY_DEFERRED,
            started_days_ago=1.0,
            finished_days_ago=0.5,
        )
    # A second, genuinely distinct incident.
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=2.0,
        finished_days_ago=1.5,
    )
    ebull_test_conn.commit()

    counts = scheduled_adapter._recent_reap_counts(ebull_test_conn)

    assert counts[JOB_RETRY_DEFERRED] == (2, 4), "3 rows in one instant plus 1 separate reap is 2 EVENTS / 4 runs lost"


def test_window_is_on_finished_at_not_started_at(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A long-running job reaped today counts; an old reap does not.

    The reap EVENT happens when the reaper runs. The lag from ``started_at`` is
    large in the real corpus (p99 1.86 d, max 4.375 d on dev), so a
    ``started_at`` window would miss the first row here and would keep the
    second one alive for days after the incident.
    """
    # Started 30 days ago, reaped today — INSIDE the window on finished_at,
    # outside it on started_at.
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=30.0,
        finished_days_ago=0.25,
    )
    # Started 9 days ago, reaped 8 days ago — OUTSIDE the window on both, and
    # the one a started_at window would still be showing tomorrow.
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=9.0,
        finished_days_ago=8.0,
    )
    ebull_test_conn.commit()

    counts = scheduled_adapter._recent_reap_counts(ebull_test_conn)

    assert counts[JOB_RETRY_DEFERRED] == (1, 1)


def test_predicate_requires_all_three_columns(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The marker is a convention, so the read is as narrow as it can be.

    ``error_category`` is the shared ``internal_error`` and ``status`` is the
    shared ``failure``; neither identifies a reap alone. Matching the message
    alone would widen the count to any row that happens to carry that text.
    """
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=1.0,
        finished_days_ago=0.5,
        error_category="upstream_error",
    )
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=1.0,
        finished_days_ago=0.4,
        status="cancelled",
    )
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=1.0,
        finished_days_ago=0.3,
        error_msg="some other internal failure",
    )
    ebull_test_conn.commit()

    # Scoped to this job, not a global empty map: the worker DB is shared, so a
    # qualifying reap left by any other test would fail a `== {}` assertion
    # while every row under test was correctly excluded.
    assert JOB_RETRY_DEFERRED not in scheduled_adapter._recent_reap_counts(ebull_test_conn)


def test_reaper_output_matches_the_readers_predicate(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Round-trip: what the reaper WRITES is what the counter FINDS.

    This is the test that matters for a convention-based marker. The writer and
    the reader share ``ORPHAN_REAP_ERROR_MSG``, but they do not share the
    ``status`` / ``error_category`` halves of the predicate — so a change to
    either side of the reaper's UPDATE would silently zero this count while
    every unit test that builds its own rows kept passing.
    """
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, status)
        VALUES (%s, now() - interval '2 hours', 'running')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()
    # Scoped, for the shared-DB reason above.
    assert JOB_RETRY_DEFERRED not in scheduled_adapter._recent_reap_counts(ebull_test_conn)

    reaped = reap_orphaned_job_runs(ebull_test_conn, reap_all=True)
    ebull_test_conn.commit()
    assert reaped >= 1

    counts = scheduled_adapter._recent_reap_counts(ebull_test_conn)
    assert counts.get(JOB_RETRY_DEFERRED) == (1, 1)


def test_counts_reach_the_row_and_list_agrees_with_detail(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``get_row`` must not disagree with ``list_rows`` about the window."""
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=1.0,
        finished_days_ago=0.5,
    )
    _make_reap(
        ebull_test_conn,
        job_name=JOB_RETRY_DEFERRED,
        started_days_ago=1.0,
        finished_days_ago=0.5,
    )
    ebull_test_conn.commit()

    detail = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert detail is not None
    assert (detail.recent_reap_events, detail.recent_reap_runs) == (1, 2)

    listed = {row.process_id: row for row in scheduled_adapter.list_rows(ebull_test_conn)}
    assert (
        listed[JOB_RETRY_DEFERRED].recent_reap_events,
        listed[JOB_RETRY_DEFERRED].recent_reap_runs,
    ) == (1, 2)

    # Every other job is 0, not absent — the honest value for "no reap in the
    # window". A KeyError here would mean the map leaked into the row.
    others = {
        (row.recent_reap_events, row.recent_reap_runs) for pid, row in listed.items() if pid != JOB_RETRY_DEFERRED
    }
    assert others == {(0, 0)}


def test_a_reaped_job_that_later_succeeds_still_reads_current(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The defect this slice exists for, pinned.

    ``compute_verdict`` is a function of the latest terminal run plus current
    staleness; a recurrence is a property of a WINDOW of runs. So a job reaped
    repeatedly and then succeeding once reads green — and the ONLY thing
    carrying the history is the count. If a future change makes the verdict
    notice reaps, this test should be updated deliberately, not deleted.
    """
    for day in (3.0, 2.0, 1.0):
        _make_reap(
            ebull_test_conn,
            job_name=JOB_RETRY_DEFERRED,
            started_days_ago=day + 0.5,
            finished_days_ago=day,
        )
    ebull_test_conn.execute(
        """
        INSERT INTO job_runs (job_name, started_at, finished_at, status)
        VALUES (%s, now() - interval '10 minutes', now() - interval '9 minutes', 'success')
        """,
        (JOB_RETRY_DEFERRED,),
    )
    ebull_test_conn.commit()

    row = scheduled_adapter.get_row(ebull_test_conn, process_id=JOB_RETRY_DEFERRED)
    assert row is not None
    assert row.status == "ok", "the later success is what hides the reaps"
    assert row.recent_reap_events == 3
    assert row.stale_reasons == (), "no stale reason carries a recurrence"
