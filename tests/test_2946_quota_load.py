"""#2946 step 2 -- what actually paces an eToro caller, and what two of them cost.

Pure: no DB, no network, no broker request.  Every request goes through the SHIPPED
``ResilientClient._request`` path into an ``httpx.MockTransport``; only the transport and
the clock are fake.

Sibling file: ``tests/test_2946_etoro_throttle_lock.py`` owns item 2's LOCK fix and the
identity assertions (one clock across both lanes; two instances do not share one).  This
file owns step 2's LOAD questions, which that file does not reach:

* a provider built INSIDE a request loop never reaches its own floor (finding 1);
* two instances on one user key stamp at the same instant -- asserted with a fake clock
  AND real concurrency, which #2946 item 2 requires in those words;
* the constant that actually paces lane B is LOOSER than the floor it substitutes for.

Census + verdict: ``docs/proposals/execution/2026-09-13-etoro-quota-load-census.md``.

⚠ Prevention log §3665 governs the assertions: capture the quantity the code WROTE from
inside its own critical section, never a reading taken after the call returns.  Stamps
are recorded by a ``list`` subclass whose ``__setitem__`` fires while
``_throttle_and_stamp`` holds the lock.  Transport ARRIVAL order is a different
invariant and is deliberately not asserted here -- a pacing lock is released before the
HTTP call, so arrival says nothing about stamping.
"""

from __future__ import annotations

import threading
from typing import Any

import httpx
import pytest

from app.providers.implementations.etoro_broker import _ETORO_WRITE_INTERVAL_S, EtoroBrokerProvider
from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_REQUEST_INTERVAL_S
from app.providers import resilient_client as rc_module
from app.providers.implementations import etoro_quota_lanes as lanes

#: Virtual monotonic never starts at 0: ``_last_request_at`` is initialised to 0.0, and a
#: clock starting there would make the very first elapsed reading 0 and force a spurious
#: sleep. Production has the same shape (a real monotonic is far from 0), so starting
#: high is faithful rather than convenient.
_T0 = 10_000.0


class VirtualClock:
    """A monotonic clock that only advances when something sleeps.

    Deliberately NOT a wall clock: with real time, a 3.5 s floor makes the concurrent
    test take 3.5 s and the assertion becomes a race against the scheduler. Here the
    only thing that can move time is a throttle deciding to wait, so "did it wait?" is
    answered exactly rather than statistically.
    """

    def __init__(self) -> None:
        self._t = _T0
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._t

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self._t += seconds


class RecordingClock(list):  # type: ignore[type-arg]
    """``_last_request_at`` that records every stamp as the throttle writes it.

    ⚠ Substituting this must not CHANGE the sharing under test (Codex ckpt-1): two
    clients that shared one list must still share one recorder, and two that did not
    must still not. ``attach_recorders`` enforces that by keying on object identity.
    """

    def __init__(self, seed: list[float]) -> None:
        super().__init__(seed)
        self.stamps: list[float] = []

    def __setitem__(self, index: Any, value: Any) -> None:  # noqa: ANN401 - list protocol
        super().__setitem__(index, value)
        self.stamps.append(float(value))


def attach_recorders(*clients: rc_module.ResilientClient) -> dict[int, RecordingClock]:
    """Swap each client's clock for a recorder, PRESERVING the sharing structure."""
    mapping: dict[int, RecordingClock] = {}
    for client in clients:
        original = client._last_request_at
        recorder = mapping.get(id(original))
        if recorder is None:
            recorder = RecordingClock(list(original))
            mapping[id(original)] = recorder
        client._last_request_at = recorder
    return mapping


def _mock_client() -> httpx.Client:
    return httpx.Client(
        base_url="https://mock.invalid",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )


@pytest.fixture()
def virtual_time(monkeypatch: pytest.MonkeyPatch) -> VirtualClock:
    """Patch the module attribute, never the global ``time`` module."""
    clock = VirtualClock()
    monkeypatch.setattr(rc_module, "time", clock)
    return clock


# ---------------------------------------------------------------------------


def test_a_provider_built_per_request_never_reaches_its_own_floor(virtual_time: VirtualClock) -> None:
    """FINDING 1. ``scheduler.py:6290`` constructs an ``EtoroBrokerProvider`` INSIDE the
    per-instrument loop, and both arms of ``scripts/prove_2603_core_eligibility.py``
    (``:119``, ``:191``) do the same. Every request is therefore the first on a virgin
    clock, so ``_ETORO_WRITE_INTERVAL_S`` paces nothing on the entire lane-B path.

    The contrast arm is the point: the identical request sequence through ONE reused
    provider is floor-spaced. Nothing about the requests changed -- only where the
    ``with`` sits.
    """
    rounds = 4
    mock = _mock_client()

    per_request_stamps: list[float] = []
    for _ in range(rounds):
        with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as broker:
            broker._http_write._client = mock
            recorders = attach_recorders(broker._http_write)
            broker._http_write.post("/api/v2/trading/info/demo/eligibility", json={})
            per_request_stamps += next(iter(recorders.values())).stamps

    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as reused:
        reused._http_write._client = mock
        recorder = next(iter(attach_recorders(reused._http_write).values()))
        for _ in range(rounds):
            reused._http_write.post("/api/v2/trading/info/demo/eligibility", json={})
        reused_stamps = recorder.stamps
    mock.close()

    assert len(per_request_stamps) == rounds
    assert len(reused_stamps) == rounds

    # A fresh provider each time: the clock never advances, because nothing ever waits.
    assert max(per_request_stamps) - min(per_request_stamps) == pytest.approx(0.0), (
        f"a per-request provider must never wait; stamps={per_request_stamps}"
    )
    # The same sequence on one provider pays the floor between every pair.
    reused_gaps = [b - a for a, b in zip(reused_stamps, reused_stamps[1:], strict=False)]
    assert reused_gaps and all(g == pytest.approx(_ETORO_WRITE_INTERVAL_S) for g in reused_gaps), (
        f"a reused provider must pay the floor; gaps={reused_gaps}"
    )


def test_two_instances_on_one_user_key_stamp_at_the_same_virtual_instant(
    virtual_time: VirtualClock,
) -> None:
    """Fake-clock AND concurrent, which is what #2946 item 2 asks for in those words.

    Two ``EtoroBrokerProvider`` instances on the SAME api/user key -- the quota is per
    user key, so different keys would not contend and the test would prove nothing.
    A ``threading.Barrier`` releases both threads together (prevention log §4318: a
    rendezvous, not a stopwatch), and the assertion reads the stamps the throttle wrote
    while holding its lock.

    The second arm is the revert probe, in-file: force the two instances onto one clock
    and one lock, and the same two requests become floor-separated. So this test cannot
    pass for a reason unrelated to the sharing.
    """
    mock = _mock_client()

    def fire_pair(*, share: bool) -> list[float]:
        with (
            EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as first,
            EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as second,
        ):
            # Production identity, asserted BEFORE any instrumentation touches it.
            assert first._http_write._last_request_at is not second._http_write._last_request_at
            if share:
                second._http_write._last_request_at = first._http_write._last_request_at
                second._http_write._throttle_lock = first._http_write._throttle_lock
            for broker in (first, second):
                broker._http_write._client = mock
            recorders = attach_recorders(first._http_write, second._http_write)

            barrier = threading.Barrier(2)
            errors: list[BaseException] = []

            def worker(broker: EtoroBrokerProvider) -> None:
                try:
                    barrier.wait(timeout=10)
                    broker._http_write.post("/api/v2/trading/info/demo/eligibility", json={})
                except BaseException as exc:  # noqa: BLE001 - re-raised in the main thread
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(b,)) for b in (first, second)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
                assert not t.is_alive(), "worker did not finish"
            if errors:
                raise errors[0]
            return sorted(s for r in recorders.values() for s in r.stamps)

    independent = fire_pair(share=False)
    shared = fire_pair(share=True)
    mock.close()

    assert len(independent) == 2
    assert independent[1] - independent[0] == pytest.approx(0.0), (
        "two instances on one user key are not coordinated, so both stamp at the same "
        f"instant and their rates ADD; stamps={independent}"
    )
    assert len(shared) == 2
    assert shared[1] - shared[0] == pytest.approx(_ETORO_WRITE_INTERVAL_S), (
        f"the control arm must pay the floor, or the assertion above proves nothing; stamps={shared}"
    )


def test_the_lane_b_pacing_constant_is_looser_than_the_floor_it_substitutes_for() -> None:
    """The arithmetic behind finding 1, pinned so a constant change cannot go unnoticed.

    Because every lane-B caller builds its provider per request, the only thing spacing
    those requests is ``CORE_ELIGIBILITY_REQUEST_INTERVAL_S`` -- a constant in a
    different module from the throttle it stands in for, and a smaller one.

    ⚠ The comparison is ``floor(60 / interval) + 1``, not ``60 / interval``. A caller
    spaced at ``i`` fires at 0, i, 2i, ... so a 60 s window holds one more request than
    the sustained-throughput figure. At 3.2 s that is 19 against a documented 20, which
    is the difference between "comfortable" and "one caller from the limit".
    """
    site = next(c for c in lanes.CALL_SITES if c.method == "check_instrument_eligibility")
    budget = site.conservative_per_minute

    def in_one_minute(interval_s: float) -> int:
        return int(60.0 // interval_s) + 1

    assert CORE_ELIGIBILITY_REQUEST_INTERVAL_S < _ETORO_WRITE_INTERVAL_S, (
        "the hand-written pacing is expected to be LOOSER than the floor it replaces; "
        "if this ever inverts, finding 1's arithmetic changes and the census is stale"
    )
    assert in_one_minute(CORE_ELIGIBILITY_REQUEST_INTERVAL_S) == 19
    assert in_one_minute(_ETORO_WRITE_INTERVAL_S) == 18
    # One caller already sits inside the budget with a single request of headroom.
    assert in_one_minute(CORE_ELIGIBILITY_REQUEST_INTERVAL_S) < budget
    # Two do not, and nothing serialises the hourly job against a research run.
    assert 2 * in_one_minute(CORE_ELIGIBILITY_REQUEST_INTERVAL_S) > budget
