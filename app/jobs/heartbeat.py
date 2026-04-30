"""Multi-subsystem heartbeat for the jobs process (#719).

Each supervised subsystem (`scheduler`, `manual_listener`, `queue_drainer`,
`main`) calls ``HeartbeatWriter.beat(subsystem)`` from its own thread.
The writer upserts a row into ``job_runtime_heartbeat`` per subsystem
every 10s. The API ``/system/jobs`` endpoint reads all rows; a stale
subsystem (>60s since last beat) downgrades the aggregate state to
``degraded`` even when other threads are still beating.

Running this in a dedicated thread (rather than coupling each
subsystem's own loop to the writer) keeps the writer simple but means
a stalled subsystem can't sneak through — the calling subsystem must
explicitly re-beat each tick.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


# Subsystems we expect to see in the heartbeat table. The supervisor
# also feeds 'main' so an absent jobs process is unambiguously
# distinguishable from a partial subsystem failure.
EXPECTED_SUBSYSTEMS: frozenset[str] = frozenset({"scheduler", "manual_listener", "queue_drainer", "main"})


class HeartbeatWriter:
    """Per-subsystem upserts into ``job_runtime_heartbeat``.

    One instance is shared across the jobs process; subsystems call
    ``beat(name, notes=...)`` to record liveness. The writer opens
    its own short-lived connection per beat (no pool dependency, no
    long-held conn) so a wedged main pool cannot also wedge the
    heartbeat path.

    The class is thread-safe: each ``beat`` call is independent and
    serialised at the DB layer by the primary key.
    """

    def __init__(
        self,
        database_url: str,
        *,
        pid: int,
        process_started_at: datetime,
    ) -> None:
        self._database_url = database_url
        self._pid = pid
        self._process_started_at = process_started_at

    def beat(self, subsystem: str, *, notes: dict[str, Any] | None = None) -> None:
        """Upsert a heartbeat row for ``subsystem``.

        Errors are logged but never raised — a heartbeat write that
        fails must not take down the calling subsystem. The next
        successful tick will resync the row.
        """
        try:
            with psycopg.connect(self._database_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO job_runtime_heartbeat
                            (subsystem, last_beat_at, pid, process_started_at, notes)
                        VALUES
                            (%(subsystem)s, NOW(), %(pid)s, %(started_at)s, %(notes)s)
                        ON CONFLICT (subsystem) DO UPDATE
                        SET last_beat_at = NOW(),
                            pid = EXCLUDED.pid,
                            process_started_at = EXCLUDED.process_started_at,
                            notes = EXCLUDED.notes
                        """,
                        {
                            "subsystem": subsystem,
                            "pid": self._pid,
                            "started_at": self._process_started_at,
                            "notes": Jsonb(notes) if notes is not None else None,
                        },
                    )
        except Exception:
            logger.warning("heartbeat write failed for subsystem=%r", subsystem, exc_info=True)


def heartbeat_loop(
    writer: HeartbeatWriter,
    subsystem: str,
    stop_event: threading.Event,
    *,
    tick_seconds: float = 10.0,
    notes_provider: Any = None,
) -> None:
    """Drive a single subsystem's heartbeat tick at ``tick_seconds`` interval.

    Runs until ``stop_event`` is set. ``notes_provider`` is an optional
    zero-arg callable that returns a dict of subsystem-specific
    metadata to record alongside the timestamp (restart counts, last
    claim ts, etc.) — called once per tick.

    The loop's first action is a beat, so a freshly-started subsystem
    is visible in the heartbeat table immediately rather than after
    one tick of dead air.
    """
    while not stop_event.is_set():
        notes = None
        if notes_provider is not None:
            try:
                notes = notes_provider()
            except Exception:
                logger.warning(
                    "heartbeat notes_provider raised for subsystem=%r",
                    subsystem,
                    exc_info=True,
                )
        writer.beat(subsystem, notes=notes)
        # `wait` returns True if stop_event is set, letting shutdown
        # interrupt the sleep without waiting out the full tick.
        if stop_event.wait(timeout=tick_seconds):
            break
    logger.info("heartbeat loop stopped: subsystem=%r", subsystem)


# A tiny helper for callers building note dicts that include a current
# timestamp — saves duplicating the same `datetime.now(UTC).isoformat()`
# boilerplate across subsystems.
def now_iso() -> str:
    return datetime.now(UTC).isoformat()
