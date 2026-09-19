"""Real-lock proof for the #2603 core-lane split (2026-09-19).

The registry assertions live in ``tests/test_etoro_core_lane_starvation.py``
(fast tier). This module holds the arms that need Postgres, and is separate
because the ``db`` marker is applied per MODULE — merging the two would evict
the registry guard from every push.

What it proves, and the trap each arm avoids:

* **The split works** — with a sibling holding ``job_source:etoro`` (the lane
  ``daily_candle_refresh`` holds for 3.2-3.8h a day), each mover still acquires
  its own lane.
* **The control** — a job that is STILL on ``etoro`` collides with that same
  holder. Without this arm a green result could come from a holder that never
  engaged, which is the failure mode where a broken probe runner reads as a
  caught bug.

⚠ Every acquire targets ``test_database_url()`` — the per-worker private DB on
the separate ``postgres-test`` cluster — NEVER ``settings.database_url``.
Postgres advisory locks are PER-DATABASE (#2224 residual 3, measured on PG
17.9), and the operator's dev DB has a running jobs daemon holding these very
keys. The ``xdist_group`` is shared with the other modules that lock real
source keys.

⚠⚠ The holder runs on its own THREAD. ``JobLock`` has a #1184 same-context
re-entrancy bypass keyed on a contextvar of held lanes, so a same-thread nested
acquire of the SAME lane returns without touching Postgres — a control arm
written that way would pass while proving nothing.
"""

from __future__ import annotations

import queue
import threading

import pytest

from app.jobs.locks import JobAlreadyRunning, JobLock
from app.jobs.sources import source_for
from app.workers.scheduler import (
    JOB_CORE_ELIGIBILITY_REFRESH,
    JOB_CORE_REBALANCE_OBSERVATION,
)
from tests.fixtures.ebull_test_db import test_database_url

pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")

_HOLDER = "daily_candle_refresh"  # the job that holds job_source:etoro for hours
_STILL_ON_ETORO = "execute_approved_orders"  # control: unmoved lanemate


def _acquire_while_held(holder_job: str, contender_job: str) -> BaseException | str:
    """Hold ``holder_job``'s lane on a worker thread; try ``contender_job`` on
    another. Returns ``"acquired"`` or the exception the contender saw.
    """
    # Resolve both names BEFORE spawning: a registry-absent name would KeyError
    # inside ``JobLock.__init__`` on a worker thread and surface from the main
    # thread as a misleading TimeoutError.
    assert source_for(holder_job)
    assert source_for(contender_job)

    holding = threading.Event()
    contender_done = threading.Event()
    holder_errors: queue.Queue[BaseException] = queue.Queue()
    result: queue.Queue[BaseException | str] = queue.Queue()

    def hold() -> None:
        try:
            with JobLock(test_database_url(), holder_job):
                holding.set()
                if not contender_done.wait(timeout=10.0):
                    raise TimeoutError("contender thread did not complete within 10s")
        except BaseException as exc:  # noqa: BLE001
            holder_errors.put(exc)

    def contend() -> None:
        try:
            if not holding.wait(timeout=10.0):
                raise TimeoutError("holder thread did not acquire within 10s")
            try:
                with JobLock(test_database_url(), contender_job):
                    result.put("acquired")
            except JobAlreadyRunning as exc:
                result.put(exc)
        finally:
            contender_done.set()

    t_hold = threading.Thread(target=hold, daemon=True)
    t_contend = threading.Thread(target=contend, daemon=True)
    t_hold.start()
    t_contend.start()
    t_hold.join(timeout=15.0)
    t_contend.join(timeout=15.0)
    assert not t_hold.is_alive() and not t_contend.is_alive(), "test threads hung"
    assert holder_errors.empty(), f"holder thread failed: {holder_errors.get()!r}"
    assert not result.empty(), "contender thread recorded no outcome"
    return result.get()


@pytest.mark.parametrize(
    "contender",
    [JOB_CORE_REBALANCE_OBSERVATION, JOB_CORE_ELIGIBILITY_REFRESH],
)
def test_a_core_job_acquires_while_the_candle_lane_is_held(contender: str) -> None:
    """The fix: neither mover is blocked by the lane that starved it."""
    assert _acquire_while_held(_HOLDER, contender) == "acquired"


def test_control_an_unmoved_lanemate_still_collides_with_the_same_holder() -> None:
    """Control arm — proves the holder genuinely engages.

    Without it, the arms above could pass because the holder's acquire silently
    failed, and a lock that never engaged looks exactly like a lane that no
    longer contends.
    """
    assert source_for(_STILL_ON_ETORO) == source_for(_HOLDER) == "etoro"
    outcome = _acquire_while_held(_HOLDER, _STILL_ON_ETORO)
    assert isinstance(outcome, JobAlreadyRunning), f"expected contention, got {outcome!r}"


def test_the_two_core_lanes_do_not_contend_with_each_other() -> None:
    """The eligibility batch can still be running at the observation's :45 fire;
    separate lanes are what stop that from re-creating the starvation."""
    outcome = _acquire_while_held(JOB_CORE_ELIGIBILITY_REFRESH, JOB_CORE_REBALANCE_OBSERVATION)
    assert outcome == "acquired"
