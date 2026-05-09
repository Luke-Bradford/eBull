"""Cooperative-cancel + finalize tests for the sync orchestrator (#1078, PR6).

Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §"Cancel — cooperative" / §"sync_runs analogue" /
      §"Finalizer-preserves-cancelled invariant" (Codex round 5 R5-W4).

Pinned invariants:
* Cancel checkpoint inside ``_run_layers_loop`` transitions
  ``sync_runs.status='cancelled'`` and unfinished
  ``sync_layer_progress`` rows to ``status='cancelled'`` with
  ``skip_reason='cancelled by operator'`` (NOT the crash text).
* Cancel-branch finalizer updates ``layers_*`` counts but does NOT
  touch ``status`` (already set by the checkpoint).
* Crash-branch ``_finalize_sync_run`` carries the
  ``WHERE status='running'`` guard so a cancel-then-finalize race
  preserves cancelled.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from app.services import process_stop
from app.services.sync_orchestrator import executor
from app.services.sync_orchestrator.types import LayerOutcome
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Point ``settings.database_url`` at the test DB.

    The orchestrator opens fresh autocommit connections via
    ``psycopg.connect(settings.database_url)`` for cancel checkpoints,
    layer-progress writes, and finalize. Without this monkeypatch they
    hit the operator's dev DB. ``test_database_url()`` returns the
    template-derived per-worker DB URL.
    """
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


def _wipe_sync_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute("DELETE FROM sync_layer_progress")
    conn.execute("DELETE FROM sync_runs")
    conn.execute("DELETE FROM process_stop_requests WHERE target_run_kind = 'sync_run'")


def _seed_running_sync_run(
    conn: psycopg.Connection[tuple],
    *,
    layers: list[str],
) -> int:
    """Insert a running sync_runs row + N pending sync_layer_progress rows."""
    row = conn.execute(
        """
        INSERT INTO sync_runs (scope, scope_detail, trigger, layers_planned, status)
        VALUES ('full', NULL, 'manual', %s, 'running')
        RETURNING sync_run_id
        """,
        (len(layers),),
    ).fetchone()
    assert row is not None
    sync_run_id = int(row[0])
    for layer_name in layers:
        conn.execute(
            """
            INSERT INTO sync_layer_progress (sync_run_id, layer_name, status)
            VALUES (%s, %s, 'pending')
            """,
            (sync_run_id, layer_name),
        )
    return sync_run_id


def _insert_stop_request(
    conn: psycopg.Connection[tuple],
    *,
    sync_run_id: int,
    mode: str = "cooperative",
) -> int:
    row = conn.execute(
        """
        INSERT INTO process_stop_requests (
            process_id, mechanism, target_run_kind, target_run_id, mode
        ) VALUES (
            'orchestrator_full_sync', 'scheduled_job', 'sync_run', %s, %s
        ) RETURNING id
        """,
        (sync_run_id, mode),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_check_cancel_signal_transitions_status_and_raises(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """``_check_cancel_signal`` observes the stop request, flips status,
    and raises ``SyncCancelled`` so the loop bails."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe", "candles"])
    stop_id = _insert_stop_request(ebull_test_conn, sync_run_id=sync_run_id)
    ebull_test_conn.commit()

    with pytest.raises(executor.SyncCancelled) as exc_info:
        executor._check_cancel_signal(sync_run_id)
    assert exc_info.value.sync_run_id == sync_run_id

    # sync_runs.status → cancelled. Read on a fresh connection so
    # we observe the autocommit write.
    row = ebull_test_conn.execute(
        "SELECT status, finished_at FROM sync_runs WHERE sync_run_id = %s",
        (sync_run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "cancelled"
    assert row[1] is not None  # finished_at populated

    # process_stop_requests row terminalised: observed_at + completed_at.
    stop_row = ebull_test_conn.execute(
        "SELECT observed_at, completed_at FROM process_stop_requests WHERE id = %s",
        (stop_id,),
    ).fetchone()
    assert stop_row is not None
    assert stop_row[0] is not None
    assert stop_row[1] is not None


def test_check_cancel_signal_no_stop_request_no_op(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """No stop request → checkpoint returns silently; status untouched."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    ebull_test_conn.commit()

    # Should not raise.
    executor._check_cancel_signal(sync_run_id)

    row = ebull_test_conn.execute("SELECT status FROM sync_runs WHERE sync_run_id = %s", (sync_run_id,)).fetchone()
    assert row is not None
    assert row[0] == "running"


def test_finalize_cancelled_sync_run_marks_unfinished_layers_and_updates_counts(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Cancel-branch finalizer:
    * unfinished layers → cancelled with operator skip_reason,
    * sync_runs counts updated WITHOUT touching status."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(
        ebull_test_conn,
        layers=["universe", "candles", "fundamentals"],
    )
    # Simulate one layer already complete, one running, one pending.
    ebull_test_conn.execute(
        """
        UPDATE sync_layer_progress
           SET status = 'complete', finished_at = now()
         WHERE sync_run_id = %s AND layer_name = 'universe'
        """,
        (sync_run_id,),
    )
    ebull_test_conn.execute(
        """
        UPDATE sync_layer_progress
           SET status = 'running', started_at = now()
         WHERE sync_run_id = %s AND layer_name = 'candles'
        """,
        (sync_run_id,),
    )
    # Cancel checkpoint set status='cancelled' before finalizer runs.
    ebull_test_conn.execute(
        "UPDATE sync_runs SET status='cancelled', finished_at=now() WHERE sync_run_id=%s",
        (sync_run_id,),
    )
    ebull_test_conn.commit()

    executor._finalize_cancelled_sync_run(sync_run_id)

    # Layer rows: complete preserved, running + pending → cancelled with operator skip_reason.
    layer_rows = ebull_test_conn.execute(
        """
        SELECT layer_name, status, skip_reason
          FROM sync_layer_progress
         WHERE sync_run_id = %s
         ORDER BY layer_name
        """,
        (sync_run_id,),
    ).fetchall()
    by_name = {r[0]: (r[1], r[2]) for r in layer_rows}
    assert by_name["universe"] == ("complete", None)
    assert by_name["candles"] == ("cancelled", "cancelled by operator")
    assert by_name["fundamentals"] == ("cancelled", "cancelled by operator")

    # sync_runs status preserved as 'cancelled'; counts updated.
    row = ebull_test_conn.execute(
        """
        SELECT status, layers_done, layers_failed, layers_skipped
          FROM sync_runs
         WHERE sync_run_id = %s
        """,
        (sync_run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "cancelled"
    assert row[1] == 1  # done = universe
    assert row[2] == 0  # failed
    assert row[3] == 2  # skipped (candles + fundamentals)


def test_crash_branch_finalize_preserves_cancelled_status(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Codex round 5 R5-W4: ``_finalize_sync_run`` carries the
    ``WHERE status='running'`` guard so a cancel-then-crash sequence
    does not overwrite the cancelled terminal status."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    # Cancel checkpoint already set status='cancelled'.
    ebull_test_conn.execute(
        "UPDATE sync_runs SET status='cancelled', finished_at=now() WHERE sync_run_id=%s",
        (sync_run_id,),
    )
    ebull_test_conn.commit()

    # Crash-branch finalize would normally set 'failed' here; with the
    # guard the UPDATE no-ops.
    outcomes = {"universe": LayerOutcome.FAILED}
    executor._finalize_sync_run(sync_run_id, outcomes)

    row = ebull_test_conn.execute("SELECT status FROM sync_runs WHERE sync_run_id = %s", (sync_run_id,)).fetchone()
    assert row is not None
    assert row[0] == "cancelled"  # guard preserved the cancel terminal status


def test_check_cancel_signal_rowcount_zero_raises(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """If sync_runs.status is already terminal when the checkpoint
    runs, the UPDATE rowcount is 0 — Codex M-r2-1 says this is
    impossible by design (the loop runs strictly before the finalizer)
    so we raise rather than silently no-op."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    # Pre-flip the row to a terminal state to simulate the impossible
    # race the producer-bug guard catches.
    ebull_test_conn.execute(
        "UPDATE sync_runs SET status='cancelled', finished_at=now() WHERE sync_run_id=%s",
        (sync_run_id,),
    )
    _insert_stop_request(ebull_test_conn, sync_run_id=sync_run_id)
    ebull_test_conn.commit()

    with pytest.raises(RuntimeError, match="cancel checkpoint"):
        executor._check_cancel_signal(sync_run_id)


def test_resolver_active_sync_run_returns_running_id(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The cancel API path uses ``_resolve_active_sync_run`` to FOR UPDATE
    the running row. Sanity that it returns the freshest running row."""
    from app.api.processes import _resolve_active_sync_run

    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    ebull_test_conn.commit()

    with ebull_test_conn.transaction():
        resolved = _resolve_active_sync_run(ebull_test_conn)
    assert resolved == sync_run_id


def test_resolver_active_sync_run_returns_none_when_no_running(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    from app.api.processes import _resolve_active_sync_run

    _wipe_sync_state(ebull_test_conn)
    ebull_test_conn.commit()

    with ebull_test_conn.transaction():
        resolved = _resolve_active_sync_run(ebull_test_conn)
    assert resolved is None


def test_finalize_sync_run_observes_in_tx_stop_signal(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Codex round 2: cancel arriving in the narrow window between the
    post-loop checkpoint and ``_finalize_sync_run`` would otherwise be
    overwritten. The in-tx ``FOR UPDATE`` + stop-probe in
    ``_finalize_sync_run`` observes the late signal and raises
    ``SyncCancelled`` so the cancel-branch finalizer runs."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    # Mark the layer complete so the normal finalize would write 'complete'.
    ebull_test_conn.execute(
        """
        UPDATE sync_layer_progress
           SET status = 'complete', finished_at = now()
         WHERE sync_run_id = %s AND layer_name = 'universe'
        """,
        (sync_run_id,),
    )
    # Late cancel inserts AFTER the post-loop checkpoint has run but
    # BEFORE finalize executes — simulate by inserting now.
    stop_id = _insert_stop_request(ebull_test_conn, sync_run_id=sync_run_id)
    ebull_test_conn.commit()

    with pytest.raises(executor.SyncCancelled):
        executor._finalize_sync_run(sync_run_id, {"universe": LayerOutcome.SUCCESS})

    row = ebull_test_conn.execute(
        "SELECT status FROM sync_runs WHERE sync_run_id = %s",
        (sync_run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "cancelled"
    stop_row = ebull_test_conn.execute(
        "SELECT observed_at, completed_at FROM process_stop_requests WHERE id = %s",
        (stop_id,),
    ).fetchone()
    assert stop_row is not None
    assert stop_row[0] is not None
    assert stop_row[1] is not None


def test_post_loop_cancel_checkpoint_observes_late_signal(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Codex pre-push review #1: cancel arriving during the final layer
    was previously dropped because ``_check_cancel_signal`` only runs
    at the TOP of each layer iteration. The post-loop call ensures a
    late cancel still observes + transitions the run to ``cancelled``.

    This test exercises ``_run_layers_loop`` directly with a tiny plan
    where the cancel row is inserted BEFORE the loop starts — but the
    intent of the test is to pin the post-loop checkpoint, so we use a
    plan with zero layers (`build_execution_plan` would not produce
    such a plan in production, but exercising the post-loop path
    directly is the cheapest way to pin the invariant).
    """
    from app.services.sync_orchestrator.types import ExecutionPlan

    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=[])
    _insert_stop_request(ebull_test_conn, sync_run_id=sync_run_id)
    ebull_test_conn.commit()

    empty_plan = ExecutionPlan(
        layers_to_refresh=(),
        layers_skipped=(),
        estimated_duration=None,
    )
    outcomes: dict[str, LayerOutcome] = {}

    with pytest.raises(executor.SyncCancelled):
        executor._run_layers_loop(sync_run_id, empty_plan, outcomes)

    row = ebull_test_conn.execute(
        "SELECT status FROM sync_runs WHERE sync_run_id = %s",
        (sync_run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "cancelled"


def test_process_stop_helpers_round_trip_for_sync_run_kind(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Spec §"Cancel — cooperative" — ``target_run_kind='sync_run'`` is
    already in the sql/135 CHECK; round-trip through the helpers."""
    _wipe_sync_state(ebull_test_conn)
    sync_run_id = _seed_running_sync_run(ebull_test_conn, layers=["universe"])
    ebull_test_conn.commit()

    stop_id = process_stop.request_stop(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        mechanism="scheduled_job",
        target_run_kind="sync_run",
        target_run_id=sync_run_id,
        mode="cooperative",
        requested_by_operator_id=None,
    )
    ebull_test_conn.commit()

    found = process_stop.is_stop_requested(
        ebull_test_conn,
        target_run_kind="sync_run",
        target_run_id=sync_run_id,
    )
    assert found is not None
    assert found.id == stop_id
    assert found.mode == "cooperative"
    assert found.target_run_kind == "sync_run"
