"""Bootstrap adapter round-trip tests (#1071, umbrella #1064 PR3).

DB-backed against the worker ``ebull_test`` template. Mocking psycopg
cursors loses the SQL-shape guarantees the adapter relies on (lateral
JSONB unnest, partial-unique fence index, COUNT FILTER aggregates).
"""

from __future__ import annotations

import psycopg
from psycopg.types.json import Jsonb

from app.services.processes import bootstrap_adapter


def _seed_state(conn: psycopg.Connection[tuple], *, status: str, last_run_id: int | None = None) -> None:
    conn.execute(
        """
        UPDATE bootstrap_state
           SET status = %s,
               last_run_id = %s,
               last_completed_at = CASE
                                       WHEN %s IN ('complete', 'partial_error', 'cancelled')
                                            THEN now()
                                       ELSE NULL
                                   END
         WHERE id = 1
        """,
        (status, last_run_id, status),
    )


def _create_run(
    conn: psycopg.Connection[tuple],
    *,
    status: str,
    cancel_requested: bool = False,
    completed: bool = False,
) -> int:
    row = conn.execute(
        """
        INSERT INTO bootstrap_runs (status, completed_at, cancel_requested_at)
        VALUES (%s,
                CASE WHEN %s THEN now() ELSE NULL END,
                CASE WHEN %s THEN now() ELSE NULL END)
        RETURNING id
        """,
        (status, completed, cancel_requested),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _create_stage(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
    stage_order: int,
    lane: str,
    status: str,
    rows_processed: int | None = None,
    last_error: str | None = None,
    completed: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO bootstrap_stages
               (bootstrap_run_id, stage_key, stage_order, lane, job_name,
                status, started_at, completed_at, rows_processed, last_error)
        VALUES (%s, %s, %s, %s, 'job_x',
                %s,
                CASE WHEN %s != 'pending' THEN now() ELSE NULL END,
                CASE WHEN %s THEN now() ELSE NULL END,
                %s, %s)
        """,
        (run_id, stage_key, stage_order, lane, status, status, completed, rows_processed, last_error),
    )


def test_pending_state_yields_pending_first_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_state(ebull_test_conn, status="pending")
    ebull_test_conn.commit()
    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.status == "pending_first_run"
    assert row.last_run is None
    assert row.active_run is None
    assert row.can_full_wash is True
    assert row.can_iterate is False
    assert row.can_cancel is False
    assert row.last_n_errors == ()


def test_complete_state_yields_ok_with_last_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _create_run(ebull_test_conn, status="complete", completed=True)
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="init",
        stage_order=0,
        lane="init",
        status="success",
        rows_processed=10,
        completed=True,
    )
    _seed_state(ebull_test_conn, status="complete", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.status == "ok"
    assert row.last_run is not None
    assert row.last_run.status == "success"
    assert row.last_run.rows_processed == 10
    assert row.last_n_errors == ()


def test_partial_error_surfaces_failed_with_per_stage_errors(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _create_run(ebull_test_conn, status="partial_error", completed=True)
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="init",
        stage_order=0,
        lane="init",
        status="success",
        rows_processed=5,
        completed=True,
    )
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_form4",
        stage_order=5,
        lane="sec",
        status="error",
        last_error="EDGAR returned 503 for accession 0000320193-...",
        completed=True,
    )
    _seed_state(ebull_test_conn, status="partial_error", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.status == "failed"
    # one error per failed stage; subject = lane
    assert len(row.last_n_errors) == 1
    err = row.last_n_errors[0]
    assert err.error_class == "sec_form4"
    assert err.sample_subject == "sec"
    assert "EDGAR returned 503" in err.sample_message
    assert err.count == 1
    # last_run carries the rows_errored count = number of failed stages
    assert row.last_run is not None
    assert row.last_run.rows_errored == 1
    assert row.can_iterate is True  # retry-failed legal from partial_error
    assert row.can_full_wash is True


def test_running_state_with_active_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _create_run(ebull_test_conn, status="running")
    # Mix of running + pending stages so the aggregate counts are non-trivial
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="init",
        stage_order=0,
        lane="init",
        status="success",
        rows_processed=3,
        completed=True,
    )
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="etoro_meta",
        stage_order=1,
        lane="etoro",
        status="running",
    )
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_form4",
        stage_order=5,
        lane="sec",
        status="pending",
    )
    _seed_state(ebull_test_conn, status="running", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.status == "running"
    assert row.active_run is not None
    assert row.active_run.run_id == run_id
    # 3 stages total, 1 finished
    assert row.active_run.progress_units_done == 1
    assert row.active_run.progress_units_total == 3
    assert row.active_run.is_cancelling is False
    assert row.can_cancel is True
    assert row.can_iterate is False  # mid-run iterate is meaningless
    assert row.can_full_wash is False  # mid-run wipe blocked


def test_cancel_requested_surfaces_is_cancelling(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _create_run(ebull_test_conn, status="running", cancel_requested=True)
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="init",
        stage_order=0,
        lane="init",
        status="running",
    )
    _seed_state(ebull_test_conn, status="running", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.active_run is not None
    assert row.active_run.is_cancelling is True


def test_cancelled_state_yields_cancelled(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    run_id = _create_run(ebull_test_conn, status="cancelled", completed=True)
    _seed_state(ebull_test_conn, status="cancelled", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.status == "cancelled"
    assert row.last_run is not None
    assert row.last_run.status == "cancelled"
    assert row.can_iterate is True  # resume from where the cancel landed
    assert row.can_full_wash is True
    assert row.can_cancel is False


def test_aggregates_skip_reasons_across_archives(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """bootstrap_archive_results.rows_skipped is per-archive; the adapter
    sums per-key across the run for ProcessRunSummary.rows_skipped_by_reason."""
    run_id = _create_run(ebull_test_conn, status="complete", completed=True)
    _create_stage(
        ebull_test_conn,
        run_id=run_id,
        stage_key="sec_form4",
        stage_order=5,
        lane="sec",
        status="success",
        completed=True,
    )
    for archive_name, skips in [
        ("form4_2024Q1.zip", {"unresolved_cusip": 12, "unresolved_cik": 3}),
        ("form4_2024Q2.zip", {"unresolved_cusip": 5, "rate_limited": 2}),
    ]:
        ebull_test_conn.execute(
            """
            INSERT INTO bootstrap_archive_results
                (bootstrap_run_id, stage_key, archive_name, rows_written, rows_skipped)
            VALUES (%s, 'sec_form4', %s, 0, %s)
            """,
            (run_id, archive_name, Jsonb(skips)),
        )
    _seed_state(ebull_test_conn, status="complete", last_run_id=run_id)
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.last_run is not None
    assert row.last_run.rows_skipped_by_reason == {
        "unresolved_cusip": 17,
        "unresolved_cik": 3,
        "rate_limited": 2,
    }


def test_full_wash_fence_disables_buttons(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A pending fence row blocks both Iterate and Full-wash on the
    bootstrap row, even when the state itself would permit one."""
    _seed_state(ebull_test_conn, status="partial_error", last_run_id=None)
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', 'bootstrap_orchestrator', 'bootstrap',
                'full_wash', 'pending')
        """
    )
    ebull_test_conn.commit()

    row = bootstrap_adapter.get_row(ebull_test_conn)
    assert row is not None
    assert row.can_iterate is False
    assert row.can_full_wash is False
