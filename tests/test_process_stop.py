"""Tests for app.services.process_stop.

Real-DB tests against the worker ``ebull_test`` database. Mocking
psycopg cursors loses the partial-unique-index guarantees this module
relies on for cancel-already-pending detection.

Coverage:

* request_stop happy path inserts a row and returns its id.
* Concurrent active stop requests against the same target run hit the
  partial-unique index and raise ``StopAlreadyPendingError``.
* After ``mark_completed`` frees the slot, a fresh request_stop
  succeeds (operator can re-cancel after a worker abandons).
* is_stop_requested pins on EXACT (target_run_kind, target_run_id) and
  ignores stop rows for other runs.
* mark_observed / mark_completed are idempotent.
* acquire_prelude_lock takes a tx-scoped advisory lock that survives
  through the rest of the transaction and releases at COMMIT.
* boot_recovery_sweep frees orphaned cancel rows + stuck full-wash
  fences past the 6h threshold.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import psycopg
import pytest

from app.services.process_stop import (
    StopAlreadyPendingError,
    acquire_prelude_lock,
    boot_recovery_sweep,
    is_stop_requested,
    mark_completed,
    mark_observed,
    reap_orphaned_stop_requests,
    reap_stuck_full_wash_fences,
    request_stop,
)


def _make_operator(conn: psycopg.Connection[tuple]) -> UUID:
    """Insert a synthetic operator row and return its UUID.

    process_stop_requests.requested_by_operator_id has an FK to
    operators(operator_id); test rows need a real referent.
    operators.username has a CHECK lower(username) and UNIQUE — so a
    randomised lower-cased username keeps tests independent.
    """
    operator_id = uuid4()
    username = f"test-{operator_id.hex[:12]}"
    conn.execute(
        """
        INSERT INTO operators (operator_id, username, password_hash)
        VALUES (%s, %s, 'x')
        """,
        (operator_id, username),
    )
    conn.commit()
    return operator_id


def test_request_stop_inserts_row(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    operator_id = _make_operator(ebull_test_conn)

    stop_id = request_stop(
        ebull_test_conn,
        process_id="bootstrap",
        mechanism="bootstrap",
        target_run_kind="bootstrap_run",
        target_run_id=42,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()

    assert isinstance(stop_id, int)
    assert stop_id > 0

    row = is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=42)
    assert row is not None
    assert row.id == stop_id
    assert row.process_id == "bootstrap"
    assert row.mode == "cooperative"
    assert row.observed_at is None
    assert row.completed_at is None


def test_request_stop_partial_unique_blocks_duplicate(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)

    request_stop(
        ebull_test_conn,
        process_id="bootstrap",
        mechanism="bootstrap",
        target_run_kind="bootstrap_run",
        target_run_id=99,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()

    with pytest.raises(StopAlreadyPendingError):
        request_stop(
            ebull_test_conn,
            process_id="bootstrap",
            mechanism="bootstrap",
            target_run_kind="bootstrap_run",
            target_run_id=99,
            mode="cooperative",
            requested_by_operator_id=operator_id,
        )


def test_mark_completed_frees_slot_for_fresh_cancel(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)

    first_id = request_stop(
        ebull_test_conn,
        process_id="sec_form4_ingest",
        mechanism="scheduled_job",
        target_run_kind="job_run",
        target_run_id=1234,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()

    mark_completed(ebull_test_conn, first_id)
    ebull_test_conn.commit()

    # Freed slot — a second cancel should succeed.
    second_id = request_stop(
        ebull_test_conn,
        process_id="sec_form4_ingest",
        mechanism="scheduled_job",
        target_run_kind="job_run",
        target_run_id=1234,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()
    assert second_id != first_id


def test_is_stop_requested_pins_to_exact_run(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)

    request_stop(
        ebull_test_conn,
        process_id="bootstrap",
        mechanism="bootstrap",
        target_run_kind="bootstrap_run",
        target_run_id=10,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()

    # A worker owning run id 11 must not see the stop row for run 10.
    assert is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=11) is None
    # Different kind, same id: also no.
    assert is_stop_requested(ebull_test_conn, target_run_kind="job_run", target_run_id=10) is None
    # Exact match: yes.
    assert is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=10) is not None


def test_mark_observed_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)
    stop_id = request_stop(
        ebull_test_conn,
        process_id="bootstrap",
        mechanism="bootstrap",
        target_run_kind="bootstrap_run",
        target_run_id=7,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )
    ebull_test_conn.commit()

    mark_observed(ebull_test_conn, stop_id)
    ebull_test_conn.commit()

    row = is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=7)
    assert row is not None
    first_observed = row.observed_at
    assert first_observed is not None

    mark_observed(ebull_test_conn, stop_id)
    ebull_test_conn.commit()
    row = is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=7)
    assert row is not None
    # COALESCE-style update — second call must not advance the timestamp.
    assert row.observed_at == first_observed


def test_acquire_prelude_lock_serialises_concurrent_callers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Two transactions racing on the same process_id must serialise.

    The first to acquire holds the advisory lock for the rest of its
    transaction; the second blocks on its own attempt. We exercise the
    blocking shape via ``pg_try_advisory_xact_lock`` (non-blocking
    variant) on a second connection, expecting it to return False
    while the first holds.
    """
    from tests.fixtures.ebull_test_db import test_database_url

    process_id = "test_process_serialise"
    # info.dsn redacts the password; use the helper that produced the
    # original URL so a second connection can authenticate.
    second_url = test_database_url()

    # Hold the lock in the test connection's tx.
    acquire_prelude_lock(ebull_test_conn, process_id)

    # Open a SECOND connection to the same DB and try the lock.
    with psycopg.connect(second_url) as other:
        with other.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)::bigint)",
                (process_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is False  # blocked by first connection's lock
        other.rollback()

    # Releasing the first connection's lock by COMMIT/ROLLBACK should
    # let a fresh acquire succeed.
    ebull_test_conn.rollback()  # releases tx-scoped lock
    with psycopg.connect(second_url) as other:
        with other.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s)::bigint)",
                (process_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is True
        other.rollback()


def test_reap_orphaned_stop_requests_sweeps_only_old_rows(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)

    # Recent row — must NOT be swept.
    fresh_id = request_stop(
        ebull_test_conn,
        process_id="bootstrap",
        mechanism="bootstrap",
        target_run_kind="bootstrap_run",
        target_run_id=100,
        mode="cooperative",
        requested_by_operator_id=operator_id,
    )

    # Backdated row >6h old — must be swept.
    ebull_test_conn.execute(
        """
        INSERT INTO process_stop_requests
            (process_id, mechanism, target_run_kind, target_run_id, mode,
             requested_by_operator_id, requested_at)
        VALUES (%s, %s, %s, %s, %s, %s, now() - interval '7 hours')
        """,
        ("bootstrap", "bootstrap", "bootstrap_run", 200, "cooperative", operator_id),
    )
    ebull_test_conn.commit()

    swept = reap_orphaned_stop_requests(ebull_test_conn, max_age_hours=6)
    ebull_test_conn.commit()
    assert swept == 1

    # Fresh row still active.
    fresh_row = is_stop_requested(ebull_test_conn, target_run_kind="bootstrap_run", target_run_id=100)
    assert fresh_row is not None
    assert fresh_row.id == fresh_id
    assert fresh_row.completed_at is None

    # Old row swept — observed_at remained NULL (sentinel "abandoned, never observed").
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT observed_at, completed_at FROM process_stop_requests WHERE target_run_id = 200",
        )
        row = cur.fetchone()
        assert row is not None
        assert row[0] is None  # abandoned sentinel
        assert row[1] is not None  # completed_at set


def test_reap_stuck_full_wash_fences_targets_dispatched_only(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    # A stuck dispatched fence row >6h old.
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status, requested_at, requested_by, payload, process_id, mode)
        VALUES ('manual_job', 'test_job', 'dispatched',
                now() - interval '7 hours', 'tester', '{}'::jsonb,
                'test_process_stuck', 'full_wash')
        """,
    )
    # A pending row of the same age — must NOT be swept (still legitimately queued).
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status, requested_at, requested_by, payload, process_id, mode)
        VALUES ('manual_job', 'test_job', 'pending',
                now() - interval '7 hours', 'tester', '{}'::jsonb,
                'test_process_pending', 'full_wash')
        """,
    )
    ebull_test_conn.commit()

    swept = reap_stuck_full_wash_fences(ebull_test_conn, max_age_hours=6)
    ebull_test_conn.commit()
    assert swept == 1

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT process_id, status FROM pending_job_requests "
            "WHERE process_id IN ('test_process_stuck', 'test_process_pending') "
            "ORDER BY process_id"
        )
        rows = cur.fetchall()
        assert len(rows) == 2
        # Pending row untouched.
        assert rows[0] == ("test_process_pending", "pending")
        # Stuck dispatched row swept to rejected.
        assert rows[1] == ("test_process_stuck", "rejected")


def test_boot_recovery_sweep_runs_both_reapers(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    operator_id = _make_operator(ebull_test_conn)

    # Stale stop row.
    ebull_test_conn.execute(
        """
        INSERT INTO process_stop_requests
            (process_id, mechanism, target_run_kind, target_run_id, mode,
             requested_by_operator_id, requested_at)
        VALUES ('bootstrap', 'bootstrap', 'bootstrap_run', 999, 'cooperative',
                %s, now() - interval '7 hours')
        """,
        (operator_id,),
    )
    # Stale fence row.
    ebull_test_conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, status, requested_at, requested_by, payload, process_id, mode)
        VALUES ('manual_job', 'job', 'dispatched',
                now() - interval '7 hours', 'tester', '{}'::jsonb,
                'test_process_combined', 'full_wash')
        """,
    )
    ebull_test_conn.commit()

    orphaned, stuck = boot_recovery_sweep(ebull_test_conn)
    assert orphaned == 1
    assert stuck == 1
