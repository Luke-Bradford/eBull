"""Orchestrator fence-prelude tests (#1078, umbrella #1064 PR6).

Spec: ``docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md``
      §"sync_runs analogue" / §"Full-wash execution fence".

Pinned invariants:
* ``_start_sync_run`` raises ``OrchestratorFenceHeld`` when a full-wash
  fence row is held on ``pending_job_requests`` for any orchestrator
  process_id.
* Sibling fence (``orchestrator_high_frequency_sync`` full-wash) blocks
  ``orchestrator_full_sync`` because they share the orchestrator's
  scheduler state (``sync_runs`` single-running unique index).
* ``bypass_fence_check=True`` bypasses self-fence — listener-dispatched
  ``mode='full_wash'`` runs ARE the fence holder.
* ``linked_request_id`` is excluded from the fence query as
  defence-in-depth.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from app.services.sync_orchestrator import executor
from app.services.sync_orchestrator.types import OrchestratorFenceHeld, SyncScope
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def settings_use_test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    from app.config import settings

    url = test_database_url()
    monkeypatch.setattr(settings, "database_url", url)
    yield url


def _wipe_state(conn: psycopg.Connection[tuple]) -> None:
    conn.execute(
        "DELETE FROM pending_job_requests WHERE process_id IN ("
        "'orchestrator_full_sync', 'orchestrator_high_frequency_sync')"
    )
    conn.execute("DELETE FROM sync_layer_progress")
    conn.execute("DELETE FROM sync_runs")


def _insert_full_wash_fence(
    conn: psycopg.Connection[tuple],
    *,
    process_id: str,
    job_name: str,
    status: str = "pending",
) -> int:
    row = conn.execute(
        """
        INSERT INTO pending_job_requests
            (request_kind, job_name, process_id, mode, status)
        VALUES ('manual_job', %s, %s, 'full_wash', %s)
        RETURNING request_id
        """,
        (job_name, process_id, status),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_read_orchestrator_fence_holder_returns_none_when_no_fence(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    _wipe_state(ebull_test_conn)
    ebull_test_conn.commit()
    with psycopg.connect(settings_use_test_db) as conn:
        with conn.transaction():
            holder = executor._read_orchestrator_fence_holder(conn, exclude_request_id=None)
    assert holder is None


def test_read_orchestrator_fence_holder_returns_self(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    _wipe_state(ebull_test_conn)
    _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        job_name="orchestrator_full_sync",
    )
    ebull_test_conn.commit()
    with psycopg.connect(settings_use_test_db) as conn:
        with conn.transaction():
            holder = executor._read_orchestrator_fence_holder(conn, exclude_request_id=None)
    assert holder == "orchestrator_full_sync"


def test_read_orchestrator_fence_holder_returns_sibling(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """High-frequency-sync full-wash blocks full-sync (shared scheduler state)."""
    _wipe_state(ebull_test_conn)
    _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_high_frequency_sync",
        job_name="orchestrator_high_frequency_sync",
    )
    ebull_test_conn.commit()
    with psycopg.connect(settings_use_test_db) as conn:
        with conn.transaction():
            holder = executor._read_orchestrator_fence_holder(conn, exclude_request_id=None)
    assert holder == "orchestrator_high_frequency_sync"


def test_read_orchestrator_fence_holder_excludes_request_id(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Defence-in-depth: ``linked_request_id`` excluded so the listener-
    dispatched run doesn't self-fence even without the bypass flag."""
    _wipe_state(ebull_test_conn)
    request_id = _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        job_name="orchestrator_full_sync",
    )
    ebull_test_conn.commit()
    with psycopg.connect(settings_use_test_db) as conn:
        with conn.transaction():
            holder = executor._read_orchestrator_fence_holder(conn, exclude_request_id=request_id)
    assert holder is None


def test_start_sync_run_raises_orchestrator_fence_held_when_self_fence(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    _wipe_state(ebull_test_conn)
    _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        job_name="orchestrator_full_sync",
    )
    ebull_test_conn.commit()
    with pytest.raises(OrchestratorFenceHeld) as exc_info:
        executor._start_sync_run(SyncScope.full(), "manual")
    assert exc_info.value.holder_process_id == "orchestrator_full_sync"


def test_start_sync_run_raises_when_sibling_fence_held(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    _wipe_state(ebull_test_conn)
    _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_high_frequency_sync",
        job_name="orchestrator_high_frequency_sync",
    )
    ebull_test_conn.commit()
    with pytest.raises(OrchestratorFenceHeld) as exc_info:
        executor._start_sync_run(SyncScope.full(), "manual")
    assert exc_info.value.holder_process_id == "orchestrator_high_frequency_sync"


def test_start_sync_run_bypass_fence_check_skips_self_fence(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Listener-dispatched ``mode='full_wash'`` runs ARE the fence
    holder; ``bypass_fence_check=True`` lets the run proceed."""
    _wipe_state(ebull_test_conn)
    request_id = _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        job_name="orchestrator_full_sync",
    )
    ebull_test_conn.commit()

    sync_run_id, _plan = executor._start_sync_run(
        SyncScope.full(),
        "manual",
        linked_request_id=request_id,
        bypass_fence_check=True,
    )
    # Run actually started — sync_runs row written.
    row = ebull_test_conn.execute("SELECT status FROM sync_runs WHERE sync_run_id = %s", (sync_run_id,)).fetchone()
    assert row is not None
    assert row[0] == "running"

    # Tidy up (the test would otherwise leave a running row that
    # would block the next test's fence probe via SyncAlreadyRunning).
    ebull_test_conn.execute(
        "UPDATE sync_runs SET status='complete', finished_at=now() WHERE sync_run_id=%s",
        (sync_run_id,),
    )
    ebull_test_conn.commit()


def test_start_sync_run_bypass_still_blocks_sibling_fence(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Codex pre-push review #2: ``bypass_fence_check=True`` only
    excludes the run's OWN linked_request_id; a sibling orchestrator
    full-wash row must still block.

    Concretely: ``orchestrator_high_frequency_sync`` worker dispatched
    with ``mode='full_wash'`` (its own queue row excluded by
    ``linked_request_id``) must still see + raise on the
    ``orchestrator_full_sync`` queue row.
    """
    _wipe_state(ebull_test_conn)
    # Self queue row (HF worker's own) — to be excluded.
    self_request_id = _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_high_frequency_sync",
        job_name="orchestrator_high_frequency_sync",
    )
    # Sibling full-sync queue row — must STILL block.
    _insert_full_wash_fence(
        ebull_test_conn,
        process_id="orchestrator_full_sync",
        job_name="orchestrator_full_sync",
    )
    ebull_test_conn.commit()

    with pytest.raises(OrchestratorFenceHeld) as exc_info:
        executor._start_sync_run(
            SyncScope.high_frequency(),
            "manual",
            linked_request_id=self_request_id,
            bypass_fence_check=True,
        )
    assert exc_info.value.holder_process_id == "orchestrator_full_sync"


def test_start_sync_run_no_fence_proceeds_normally(
    ebull_test_conn: psycopg.Connection[tuple],
    settings_use_test_db: str,
) -> None:
    """Sanity: with no fence held, _start_sync_run goes through normally."""
    _wipe_state(ebull_test_conn)
    ebull_test_conn.commit()

    sync_run_id, _plan = executor._start_sync_run(SyncScope.full(), "manual")
    row = ebull_test_conn.execute(
        "SELECT status, scope, trigger FROM sync_runs WHERE sync_run_id = %s",
        (sync_run_id,),
    ).fetchone()
    assert row is not None
    assert row[0] == "running"
    assert row[1] == "full"
    assert row[2] == "manual"

    ebull_test_conn.execute(
        "UPDATE sync_runs SET status='complete', finished_at=now() WHERE sync_run_id=%s",
        (sync_run_id,),
    )
    ebull_test_conn.commit()
