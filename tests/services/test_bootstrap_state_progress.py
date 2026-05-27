"""Direct exercise of the #1273 PR1 stage-progress helpers.

Plan: ``docs/proposals/etl/1273-pr1-cohort-shapes.md`` §5.

Three helpers under test:

* :func:`app.services.bootstrap_state.set_stage_target` —
  absolute UPDATE of ``bootstrap_stages.target_count``;
  rowcount 0 when not ``status='running'``.
* :func:`app.services.bootstrap_state.set_stage_processed` —
  absolute UPDATE of ``bootstrap_stages.processed_count``; same
  predicate; no monotonicity guard.
* :func:`app.services.bootstrap_state._current_running_stage_key` —
  resolves the running stage_key for a given job_name; handles the
  S25 stage_key/job_name divergence.

All helpers open their own ``psycopg.connect`` against
``settings.database_url`` so the writes survive caller rollback.
The autouse monkeypatch fixture below pins ``settings.database_url``
to the worker's private test DB; without it the helpers would write
to the operator's dev DB and trip the test-DB-isolation guard.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.bootstrap_state import (
    _current_running_stage_key,
    set_stage_processed,
    set_stage_target,
)
from tests.fixtures.ebull_test_db import (
    ebull_test_conn,  # noqa: F401 — re-exported fixture
    test_database_url,
)


@pytest.fixture(autouse=True)
def _pin_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect helpers' ``settings.database_url`` reads at the test DB.

    The helpers open ``psycopg.connect(settings.database_url)``;
    without this patch they would write to the dev DB. Mirrors the
    pattern used by ``tests/test_jobs_queue_recovery.py:45`` +
    ``tests/test_bootstrap_orchestrator.py:78``.
    """
    monkeypatch.setattr("app.config.settings.database_url", test_database_url())
    # The helpers also import the ``settings`` symbol at module
    # import time via ``from app.config import settings``. Pin the
    # module-local attribute too so the same monkeypatch reaches
    # the helpers' bound reference.
    monkeypatch.setattr("app.services.bootstrap_state.settings.database_url", test_database_url())


def _seed_run_with_stage(
    conn: psycopg.Connection[tuple],
    *,
    stage_key: str = "fundamentals_sync",
    job_name: str = "fundamentals_sync_bootstrap",
    stage_order: int = 25,
    lane: str = "db",
    run_status: str = "running",
    stage_status: str = "running",
) -> int:
    """Factory: COMMIT one bootstrap_runs + one bootstrap_stages row.

    Returns the new ``bootstrap_runs.id``. Caller-supplied defaults
    give a S25-shaped row so test #6 (stage_key/job_name divergence)
    works without extra plumbing.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bootstrap_runs (triggered_by_operator_id, status)
            VALUES (NULL, %s)
            RETURNING id
            """,
            (run_status,),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT INTO bootstrap_runs returned no row"
        run_id: int = int(row[0])
        cur.execute(
            """
            INSERT INTO bootstrap_stages
                   (bootstrap_run_id, stage_key, stage_order, lane, job_name, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, stage_key, stage_order, lane, job_name, stage_status),
        )
    conn.commit()
    return run_id


def _read_stage_counts(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
) -> tuple[int | None, int | None]:
    """Read ``(target_count, processed_count)`` for a stage row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT target_count, processed_count
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %s
               AND stage_key        = %s
            """,
            (run_id, stage_key),
        )
        row = cur.fetchone()
    assert row is not None, "stage row vanished"
    return (None if row[0] is None else int(row[0]), None if row[1] is None else int(row[1]))


def _read_last_progress_at(
    conn: psycopg.Connection[tuple],
    *,
    run_id: int,
    stage_key: str,
) -> object | None:
    """Read ``last_progress_at`` for a stage row (object since psycopg
    returns ``datetime``; tests assert via IS NOT NULL semantics)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_progress_at
              FROM bootstrap_stages
             WHERE bootstrap_run_id = %s
               AND stage_key        = %s
            """,
            (run_id, stage_key),
        )
        row = cur.fetchone()
    assert row is not None, "stage row vanished"
    return row[0]


def test_set_stage_target_happy_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    run_id = _seed_run_with_stage(ebull_test_conn, stage_key="S22", job_name="job_S22")

    rowcount = set_stage_target(run_id=run_id, stage_key="S22", target_count=42)

    assert rowcount == 1
    target, _processed = _read_stage_counts(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert target == 42


def test_set_stage_target_no_op_on_terminal_stage(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    run_id = _seed_run_with_stage(
        ebull_test_conn,
        stage_key="S22",
        job_name="job_S22",
        stage_status="success",
    )

    rowcount = set_stage_target(run_id=run_id, stage_key="S22", target_count=42)

    assert rowcount == 0
    target, _processed = _read_stage_counts(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert target is None


def test_set_stage_processed_happy_path(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    run_id = _seed_run_with_stage(ebull_test_conn, stage_key="S22", job_name="job_S22")

    rowcount = set_stage_processed(run_id=run_id, stage_key="S22", processed_count=17)

    assert rowcount == 1
    _target, processed = _read_stage_counts(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert processed == 17


def test_set_stage_processed_absolute_write_no_monotonicity_guard(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    """Contract test: the helper is an ABSOLUTE write, not an
    increment. Calling it with 10 then 5 leaves processed_count at 5.
    Caller is responsible for enforcing monotonicity if needed."""
    run_id = _seed_run_with_stage(ebull_test_conn, stage_key="S22", job_name="job_S22")

    assert set_stage_processed(run_id=run_id, stage_key="S22", processed_count=10) == 1
    assert set_stage_processed(run_id=run_id, stage_key="S22", processed_count=5) == 1

    _target, processed = _read_stage_counts(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert processed == 5


def test_set_stage_processed_no_op_on_terminal_stage(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    run_id = _seed_run_with_stage(
        ebull_test_conn,
        stage_key="S22",
        job_name="job_S22",
        stage_status="error",
    )

    rowcount = set_stage_processed(run_id=run_id, stage_key="S22", processed_count=42)

    assert rowcount == 0
    _target, processed = _read_stage_counts(ebull_test_conn, run_id=run_id, stage_key="S22")
    # processed_count is INTEGER NOT NULL DEFAULT 0 per sql/140; the
    # seed leaves it at 0 and the no-op write does not change it.
    assert processed == 0


def test_current_running_stage_key_resolves_s25_divergence(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    """S25 has stage_key='fundamentals_sync' but job_name=
    'fundamentals_sync_bootstrap'. The helper must resolve
    stage_key from job_name."""
    _seed_run_with_stage(
        ebull_test_conn,
        stage_key="fundamentals_sync",
        job_name="fundamentals_sync_bootstrap",
    )

    resolved = _current_running_stage_key("fundamentals_sync_bootstrap")

    assert resolved == "fundamentals_sync"


def test_current_running_stage_key_none_for_unknown_job(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    _seed_run_with_stage(
        ebull_test_conn,
        stage_key="fundamentals_sync",
        job_name="fundamentals_sync_bootstrap",
    )

    resolved = _current_running_stage_key("not_a_real_job_name")

    assert resolved is None


def test_current_running_stage_key_none_when_no_running_run(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    _seed_run_with_stage(
        ebull_test_conn,
        stage_key="fundamentals_sync",
        job_name="fundamentals_sync_bootstrap",
        run_status="complete",
        stage_status="success",
    )

    resolved = _current_running_stage_key("fundamentals_sync_bootstrap")

    assert resolved is None


def test_current_running_stage_key_none_when_stage_pending(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    """Run is running but stage has not yet transitioned out of
    pending. Helper returns None because the predicate requires
    stage status='running'."""
    _seed_run_with_stage(
        ebull_test_conn,
        stage_key="fundamentals_sync",
        job_name="fundamentals_sync_bootstrap",
        stage_status="pending",
    )

    resolved = _current_running_stage_key("fundamentals_sync_bootstrap")

    assert resolved is None


def test_set_stage_target_survives_caller_rollback(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    """Prove the helper's fresh-connection commit is independent of
    the caller's open transaction.

    Sequence (per spec §5 test #10):
      (a) seed factory COMMITS run + stage (already done above
          via ebull_test_conn);
      (b) open a CALLER conn in non-autocommit mode, BEGIN, INSERT
          a row into bootstrap_archive_results (FK to bootstrap_runs
          is satisfied because step (a) committed the parent);
      (c) WHILE the caller tx is still open, call set_stage_target;
      (d) ROLLBACK the caller tx;
      (e) assert via a THIRD fresh connection that target_count
          persisted AND the side INSERT did not.
    """
    # Step (a)
    run_id = _seed_run_with_stage(ebull_test_conn, stage_key="S22", job_name="job_S22")

    url = test_database_url()
    caller_conn = psycopg.connect(url)
    caller_conn.autocommit = False
    try:
        # Step (b) — INSERT a side row inside the caller tx
        with caller_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bootstrap_archive_results
                       (bootstrap_run_id, stage_key, archive_name, rows_written)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, "S22", "rollback-canary.zip", 7),
            )
        # Step (c) — helper invocation against its own fresh conn
        rowcount = set_stage_target(run_id=run_id, stage_key="S22", target_count=99)
        assert rowcount == 1
        # Step (d) — caller tx rolls back
        caller_conn.rollback()
    finally:
        caller_conn.close()

    # Step (e) — third fresh connection observes durable state
    with psycopg.connect(url) as verifier:
        with verifier.cursor() as cur:
            cur.execute(
                "SELECT target_count FROM bootstrap_stages WHERE bootstrap_run_id=%s AND stage_key=%s",
                (run_id, "S22"),
            )
            target_row = cur.fetchone()
            assert target_row is not None
            assert int(target_row[0]) == 99, "helper write must survive caller rollback"

            cur.execute(
                """
                SELECT COUNT(*) FROM bootstrap_archive_results
                 WHERE bootstrap_run_id=%s AND archive_name='rollback-canary.zip'
                """,
                (run_id,),
            )
            count_row = cur.fetchone()
            assert count_row is not None
            assert int(count_row[0]) == 0, "side INSERT must NOT persist after caller rollback"


def test_helpers_bump_last_progress_at_heartbeat(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811 — re-exported fixture
) -> None:
    """Codex 2 P2 fold contract: BOTH helpers must write
    ``last_progress_at = now()`` so the heartbeat (read by
    ``bootstrap_adapter.MAX(last_progress_at)`` +
    ``stale_detection.mid_flight_stuck``) advances every time a
    progress signal lands. Without this a long-running stage would
    advance ``processed_count`` while the process panel marked the
    whole run stuck."""
    run_id = _seed_run_with_stage(ebull_test_conn, stage_key="S22", job_name="job_S22")

    # Seed leaves heartbeat NULL.
    assert _read_last_progress_at(ebull_test_conn, run_id=run_id, stage_key="S22") is None

    set_stage_target(run_id=run_id, stage_key="S22", target_count=42)
    after_target = _read_last_progress_at(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert after_target is not None, "set_stage_target must bump heartbeat"

    set_stage_processed(run_id=run_id, stage_key="S22", processed_count=17)
    after_processed = _read_last_progress_at(ebull_test_conn, run_id=run_id, stage_key="S22")
    assert after_processed is not None, "set_stage_processed must bump heartbeat"
    # Second call advances strictly past the first (now() is wall-clock).
    assert after_processed >= after_target  # type: ignore[operator]
