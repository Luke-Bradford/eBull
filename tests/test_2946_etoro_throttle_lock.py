"""eToro trading-throttle coordination (#2946).

Pure: no DB, no network, no broker request. The httpx clients here are pointed at an
unroutable port and never used — only ``ResilientClient._throttle_and_stamp`` runs.

Spec: ``docs/proposals/execution/2026-09-13-etoro-trading-throttle-coordination.md``
"""

from __future__ import annotations

import threading
import time

import httpx
import pytest

from app.providers.implementations.etoro_broker import (
    _ETORO_READ_INTERVAL_S,
    _ETORO_WRITE_INTERVAL_S,
    EtoroBrokerProvider,
)
from app.providers.resilient_client import ResilientClient

#: Large enough that a scheduler hiccup cannot fake a violation, small enough to keep
#: the test fast. Both lanes use the SAME floor here so the gaps are directly readable
#: against one number — the production floors differ and are asserted separately.
_FLOOR_S = 0.3

#: Slack on the floor comparison. A gap is only a violation if it is materially below
#: the floor; `time.sleep` may return marginally early.
_TOLERANCE_S = 0.02


def _acquire_concurrently(*, share_lock: bool, threads_per_lane: int = 2) -> list[float]:
    """Fire a read/write pair concurrently over one shared clock; return fire offsets.

    A ``threading.Barrier`` releases every thread at once. Without it the threads start
    staggered by however long each takes to spawn, and a staggered start can satisfy the
    floor by accident — which would let the test pass while the race is live.
    """
    client = httpx.Client(base_url="http://127.0.0.1:1")
    clock: list[float] = [0.0]
    lock = threading.Lock()
    shared = {"shared_throttle_lock": lock} if share_lock else {}
    lanes = [
        ResilientClient(
            client,
            min_request_interval_s=_FLOOR_S,
            shared_last_request=clock,
            **shared,  # type: ignore[arg-type]
        )
        for _ in range(2)
    ]

    fired: list[float] = []
    fired_lock = threading.Lock()
    barrier = threading.Barrier(len(lanes) * threads_per_lane)
    start = time.monotonic()

    def fire(lane: ResilientClient) -> None:
        barrier.wait()
        lane._throttle_and_stamp()
        with fired_lock:
            fired.append(time.monotonic() - start)

    workers = [threading.Thread(target=fire, args=(lane,)) for lane in lanes for _ in range(threads_per_lane)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    client.close()
    return sorted(fired)


def _violations(offsets: list[float]) -> list[float]:
    return [
        round(later - earlier, 4)
        for earlier, later in zip(offsets, offsets[1:], strict=False)
        if later - earlier < _FLOOR_S - _TOLERANCE_S
    ]


def test_a_shared_clock_with_a_shared_lock_holds_the_floor() -> None:
    """The fix. Every gap is at or above the floor."""
    offsets = _acquire_concurrently(share_lock=True)
    assert _violations(offsets) == [], f"offsets={offsets}"


def test_a_shared_clock_with_independent_locks_bursts_past_the_floor() -> None:
    """The defect the fix closes — asserted, so the test above cannot pass for an
    unrelated reason.

    Two ``ResilientClient``s sharing ``shared_last_request`` but holding their own locks
    perform an unsynchronised read-modify-write on that list, which is the #726
    check-and-write race. This pins that the LOCK is what closes it: if a future change
    made the throttle safe some other way, this test fails and says so rather than
    leaving a redundant guard in place.
    """
    offsets = _acquire_concurrently(share_lock=False)
    assert _violations(offsets), f"expected at least one gap below the floor with independent locks; offsets={offsets}"


def test_the_broker_provider_shares_one_lock_and_one_clock_across_both_lanes() -> None:
    """Identity, not behaviour: a refactor must not silently return to per-lane locks."""
    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
        read, write = broker._http_read, broker._http_write
        assert read._throttle_lock is write._throttle_lock
        assert read._last_request_at is write._last_request_at
        # The two lanes keep their DIFFERENT floors — sharing the clock must not
        # collapse the read and write rates into one.
        assert read._min_interval == _ETORO_READ_INTERVAL_S
        assert write._min_interval == _ETORO_WRITE_INTERVAL_S
        assert read._min_interval != write._min_interval


def test_two_broker_instances_do_not_share_a_budget() -> None:
    """#2946 item 1, still OPEN — documented by a test rather than by its absence.

    Two providers on the same user key hold independent clocks and locks, so nothing
    bounds their combined rate against the per-user-key quota. Not fixed here: eligibility
    and what-if costs ride the write lane (`etoro_broker.py:857`, `:895`) on dedicated
    portal quotas, so keying one pool per credential would create cross-instance
    contention the source rule says should not exist. Splitting the lanes by quota family
    is the prerequisite.
    """
    with (
        EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as first,
        EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as second,
    ):
        assert first._http_read._throttle_lock is not second._http_read._throttle_lock
        assert first._http_read._last_request_at is not second._http_read._last_request_at


@pytest.mark.parametrize(
    ("floor", "expected"),
    [(_ETORO_READ_INTERVAL_S, 1.1), (_ETORO_WRITE_INTERVAL_S, 3.5)],
)
def test_the_production_floors_are_the_documented_ones(floor: float, expected: float) -> None:
    """Pins the constants the head-of-line-blocking trade-off is argued from: the
    accepted worst case is 3.5 - 1.1 = 2.4s, and a silent change to either number
    changes that argument without changing the spec."""
    assert floor == expected
