"""Pure-logic tests for the sync-run liveness verdict (#2274).

No DB. ``assess_run`` is a wall-clock comparison and nothing else; the point of
these tests is to pin the two properties that make it a BACKSTOP rather than a
health alarm — the boundary is inclusive-below, and a fresh heartbeat cannot
mute it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.services.sync_orchestrator.run_liveness import RUNTIME_CEILING_S, assess_run

NOW = datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC)


def _ago(seconds: float) -> datetime:
    return NOW - timedelta(seconds=seconds)


def test_fresh_run_is_live() -> None:
    assert assess_run(started_at=_ago(60), last_progress_at=_ago(5), now=NOW) == "live"


def test_run_older_than_ceiling_is_over_ceiling() -> None:
    verdict = assess_run(started_at=_ago(RUNTIME_CEILING_S + 1), last_progress_at=None, now=NOW)
    assert verdict == "over_ceiling"


def test_boundary_is_inclusive_below() -> None:
    """Exactly at the ceiling reads ``live``.

    Matches ``stale_detection`` rule 5, which fires on
    ``active_run_started_at < now - RUNTIME_CEILING_S`` — strictly older. Two
    rules describing the same runs must agree at the edge or an operator sees a
    row that is over the ceiling on one surface and not the other.
    """
    assert assess_run(started_at=_ago(RUNTIME_CEILING_S), last_progress_at=None, now=NOW) == "live"
    assert assess_run(started_at=_ago(RUNTIME_CEILING_S - 1), last_progress_at=None, now=NOW) == "live"


def test_a_fresh_heartbeat_does_not_mute_the_ceiling() -> None:
    """The property the whole design rests on.

    Rule 4 (``mid_flight_stuck``) is muted the instant a producer ticks. If the
    ceiling could be muted the same way, a job that ticks forever would read
    healthy forever — which is precisely the state #2274 was filed about. The
    heartbeat is accepted and ignored.
    """
    verdict = assess_run(
        started_at=_ago(RUNTIME_CEILING_S + 3600),
        last_progress_at=_ago(1),
        now=NOW,
    )
    assert verdict == "over_ceiling"


def test_null_heartbeat_does_not_by_itself_condemn_a_young_run() -> None:
    """A run with no heartbeat yet is not stalled — it has not reached a layer.

    Every sync starts this way: ``sync_runs`` is inserted before the first
    ``_record_layer_started``, so ``last_progress_at`` is NULL for the whole
    prelude. Treating NULL as a fault would fire on every run.
    """
    assert assess_run(started_at=_ago(30), last_progress_at=None, now=NOW) == "live"


def test_heartbeat_in_the_future_does_not_flip_the_verdict() -> None:
    """Clock skew between the API process and Postgres must not create a state.

    ``started_at`` and ``last_progress_at`` are DB-written while ``now`` is the
    caller's clock. The verdict reads only ``started_at``, so a heartbeat ahead
    of ``now`` is inert rather than a third outcome.
    """
    assert assess_run(started_at=_ago(60), last_progress_at=NOW + timedelta(seconds=30), now=NOW) == "live"
