"""HeartbeatWriter unit coverage (#719).

Drives ``HeartbeatWriter.beat`` against the real dev DB so the SQL
upsert (ON CONFLICT (subsystem) DO UPDATE) is exercised. The
``heartbeat_loop`` driver is exercised structurally via a stop_event
that fires after one tick — a longer integration runs lives in the
smoke gate against the running jobs process.
"""

from __future__ import annotations

import threading
from collections.abc import Generator
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.config import settings
from app.jobs.heartbeat import HeartbeatWriter, heartbeat_loop


def _db_reachable() -> bool:
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; heartbeat tests require the real DB",
)


@pytest.fixture()
def _cleanup_subsystems() -> Generator[list[str]]:
    """Track subsystem rows the test wrote so teardown can delete them."""
    written: list[str] = []
    yield written
    if written:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM job_runtime_heartbeat WHERE subsystem = ANY(%s)",
                    (written,),
                )


def test_beat_inserts_then_updates_row(_cleanup_subsystems: list[str]) -> None:
    subsystem = "test_beat_subsystem"
    _cleanup_subsystems.append(subsystem)
    writer = HeartbeatWriter(settings.database_url, pid=99999, process_started_at=datetime.now(UTC))

    writer.beat(subsystem, notes={"first": True})
    writer.beat(subsystem, notes={"first": False, "second": True})

    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pid, notes FROM job_runtime_heartbeat WHERE subsystem=%s",
                (subsystem,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == 99999
    # Latest beat overwrites — second notes payload wins.
    assert row[1]["second"] is True


def test_beat_swallows_db_errors() -> None:
    """A heartbeat write that fails must not raise — the next tick resyncs."""
    writer = HeartbeatWriter(
        "postgresql://postgres:wrongpw@127.0.0.1:5432/ebull",
        pid=99999,
        process_started_at=datetime.now(UTC),
    )
    # Should NOT raise — error is logged and swallowed.
    writer.beat("test_swallow")


def test_heartbeat_loop_stops_on_event(_cleanup_subsystems: list[str]) -> None:
    """The loop honours stop_event and exits cleanly."""
    subsystem = "test_loop_stop"
    _cleanup_subsystems.append(subsystem)
    writer = HeartbeatWriter(settings.database_url, pid=99999, process_started_at=datetime.now(UTC))
    stop_event = threading.Event()

    def _provider() -> dict[str, Any]:
        # Set the stop_event after the first beat so the loop exits quickly.
        stop_event.set()
        return {"tick": 1}

    heartbeat_loop(
        writer,
        subsystem,
        stop_event,
        tick_seconds=0.01,
        notes_provider=_provider,
    )

    # The first beat ran (notes_provider was called); after that the
    # stop_event interrupted the wait.
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT notes FROM job_runtime_heartbeat WHERE subsystem=%s",
                (subsystem,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == {"tick": 1}
