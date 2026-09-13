"""#2946 steps 2+3 -- what actually paces an eToro caller, and what two of them cost.

Pure: no DB, no network, no broker request.  Every request goes through the SHIPPED
``ResilientClient._request`` path into an ``httpx.MockTransport``; only the transport and
the clock are fake.

Sibling file: ``tests/test_2946_etoro_throttle_lock.py`` owns item 2's LOCK fix and the
identity assertions (one clock across both lanes; two instances do not share one).  This
file owns step 2's LOAD questions, which that file does not reach:

* a provider built INSIDE a request loop never reaches its own floor (finding 1);
* two instances on one user key stamp at the same instant -- asserted with a fake clock
  AND real concurrency, which #2946 item 2 requires in those words;
* lane B's pacing fits lane B's documented budget, and is DERIVED from it.

⚠⚠ **Finding 1's VERDICT changed at step 3 and the mechanism did not.** The first two
tests still describe exactly what the code does; what moved is what that means. A
provider built per request never reaching the shared write floor is DELIBERATE, because
lane B is a dedicated quota and that floor also paces order submission -- see
``test_lane_b_is_deliberately_not_paced_by_the_shared_write_floor``. Do not read these
two tests as pinning a defect awaiting repair; the repair was specced, reviewed and
rejected as a regression on the execution path.

Census + verdict: ``docs/proposals/execution/2026-09-13-etoro-quota-load-census.md``.
Step 3's change + the rejected alternatives:
``docs/proposals/execution/2026-09-13-lane-b-dead-throttle-fix.md``.

⚠ Prevention log §3665 governs the assertions: capture the quantity the code WROTE from
inside its own critical section, never a reading taken after the call returns.  Stamps
are recorded by a ``list`` subclass whose ``__setitem__`` fires while
``_throttle_and_stamp`` holds the lock.  Transport ARRIVAL order is a different
invariant and is deliberately not asserted here -- a pacing lock is released before the
HTTP call, so arrival says nothing about stamping.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import ExitStack
from typing import Any

import httpx
import pytest

from app.providers import resilient_client as rc_module
from app.providers.implementations import etoro_quota_lanes as lanes
from app.providers.implementations.etoro_broker import _ETORO_WRITE_INTERVAL_S, EtoroBrokerProvider
from app.services.strategy_core_eligibility import CORE_ELIGIBILITY_REQUEST_INTERVAL_S

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


@pytest.fixture()
def mock() -> Iterator[httpx.Client]:
    """A transport that answers 200 to anything, closed by pytest.

    A fixture rather than a helper so teardown survives an assertion failing between
    the two arms of a test -- review round 1 caught that leak.
    """
    client = httpx.Client(
        base_url="https://mock.invalid",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )
    try:
        yield client
    finally:
        client.close()


@pytest.fixture()
def virtual_time(monkeypatch: pytest.MonkeyPatch) -> VirtualClock:
    """Patch the module attribute, never the global ``time`` module."""
    clock = VirtualClock()
    monkeypatch.setattr(rc_module, "time", clock)
    return clock


# ---------------------------------------------------------------------------


def test_a_provider_built_per_request_never_reaches_its_own_floor(
    virtual_time: VirtualClock, mock: httpx.Client
) -> None:
    """FINDING 1's MECHANISM. ``scheduler.py:6292`` constructs an ``EtoroBrokerProvider``
    INSIDE the per-instrument loop, and both arms of
    ``scripts/prove_2603_core_eligibility.py`` (``:119``, ``:191``) do the same. Every
    request is therefore the first on a virgin clock, so ``_ETORO_WRITE_INTERVAL_S``
    paces nothing on the lane-B path.

    The contrast arm is the point: the identical request sequence through ONE reused
    provider is floor-spaced. Nothing about the requests changed -- only where the
    ``with`` sits.

    ⚠⚠ This is CHARACTERISATION, not a defect awaiting repair. Step 3 established that
    lane B must NOT be paced by that floor (dedicated quota; the floor also carries order
    submission), so what actually paces this path is
    ``CORE_ELIGIBILITY_REQUEST_INTERVAL_S``, derived from lane B's own budget. See
    ``test_lane_b_is_deliberately_not_paced_by_the_shared_write_floor``.
    """
    rounds = 4

    # ⚠ Every provider is built, and the recorders attached, BEFORE any request fires.
    # Attaching per iteration would give each provider its own recorder even when they
    # shared a clock object, which silently manufactures the independence under test --
    # a revert probe caught exactly that and this loop is the fix. One
    # ``attach_recorders`` call over all of them keys on identity, so a shared clock
    # yields one recorder and independent clocks yield several.
    with ExitStack() as stack:
        per_request_brokers = [
            stack.enter_context(EtoroBrokerProvider(api_key="k", user_key="u", env="demo")) for _ in range(rounds)
        ]
        for broker in per_request_brokers:
            broker._http_write._client = mock
        per_request_recorders = attach_recorders(*(b._http_write for b in per_request_brokers))
        for broker in per_request_brokers:
            broker._http_write.post("/api/v2/trading/info/demo/eligibility", json={})
        per_request_stamps = sorted(s for r in per_request_recorders.values() for s in r.stamps)

    with EtoroBrokerProvider(api_key="k", user_key="u", env="demo") as reused:
        reused._http_write._client = mock
        recorder = next(iter(attach_recorders(reused._http_write).values()))
        for _ in range(rounds):
            reused._http_write.post("/api/v2/trading/info/demo/eligibility", json={})
        reused_stamps = recorder.stamps

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
    virtual_time: VirtualClock, mock: httpx.Client
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

    assert len(independent) == 2
    assert independent[1] - independent[0] == pytest.approx(0.0), (
        "two instances on one user key are not coordinated, so both stamp at the same "
        f"instant and their rates ADD; stamps={independent}"
    )
    assert len(shared) == 2
    assert shared[1] - shared[0] == pytest.approx(_ETORO_WRITE_INTERVAL_S), (
        f"the control arm must pay the floor, or the assertion above proves nothing; stamps={shared}"
    )


def _in_one_minute(interval_s: float) -> int:
    """Requests a caller spaced at ``interval_s`` places in a ROLLING minute.

    ⚠ ``floor(60 / i) + 1``, not ``60 / i``. A caller fires at 0, i, 2i, ... so the
    window holds one more request than the sustained-throughput figure. At lane B that
    is the difference between "comfortable" and "at the limit".
    """
    return int(60.0 // interval_s) + 1


def test_single_lane_b_caller_fits_its_documented_budget() -> None:
    """#2946 step 2's acceptance clause 1, read from the lane map rather than a literal.

    ``floor(60 / pacing) + 1 <= CallSite.conservative_per_minute`` for the lane-B call
    site. Both sides come from ``etoro_quota_lanes`` -- the budget from the portal-cited
    ``QuotaLane``, the pacing from ``CORE_ELIGIBILITY_REQUEST_INTERVAL_S``, which #2946
    step 3 made a DERIVATION of that same budget rather than a number chosen beside it.
    """
    site = next(c for c in lanes.CALL_SITES if c.method == "check_instrument_eligibility")
    placed = _in_one_minute(CORE_ELIGIBILITY_REQUEST_INTERVAL_S)

    assert placed <= site.conservative_per_minute, (
        f"one lane-B caller places {placed} requests against a documented {site.conservative_per_minute}/min"
    )
    # The constructed choice is one request of HEADROOM -- the portal publishes a budget,
    # not a recommended utilisation, so the margin is fixed here and pinned rather than
    # left implicit.
    assert placed <= site.conservative_per_minute - 1


def test_two_concurrent_lane_b_callers_still_exceed_the_budget() -> None:
    """#2946 step 2's acceptance clause 2 is OPEN, and this records that it is.

    Two lane-B callers on one user key -- the hourly ``core_eligibility_refresh`` job and
    an operator running ``scripts/prove_2603_core_eligibility.py`` -- are separate
    PROCESSES. No pacing constant and no process-local clock can reach across them, so
    step 3's change does not close this and does not claim to. Closing it needs a
    cross-process gate (``app/providers/postgres_rate_gate.py`` exists but is wired for
    SEC only, via ``set_sec_rate_gate``, and fails OPEN) or an advisory lock serialising
    the two callers. Both are the coordinator, whose step-2 verdict is
    ``insufficient_evidence``: zero eToro 429s over 76 days from two independent sources.

    ⚠ This test asserts the SHORTFALL. If it ever fails, the gap has been closed and this
    test should be replaced by the assertion that it stays closed -- not deleted.
    """
    site = next(c for c in lanes.CALL_SITES if c.method == "check_instrument_eligibility")
    assert 2 * _in_one_minute(CORE_ELIGIBILITY_REQUEST_INTERVAL_S) > site.conservative_per_minute


def test_lane_b_is_deliberately_not_paced_by_the_shared_write_floor() -> None:
    """⚠⚠ The invariant that stops the next session shipping the obvious regression.

    ``_ETORO_WRITE_INTERVAL_S`` paces ``_http_write``, which also carries lane A (ORDER
    SUBMISSION) and lane C. Lane B is documented DEDICATED -- "not shared (pooled across)
    any other endpoint" -- so pacing eligibility from that floor, whether by hoisting the
    provider out of the request loop or by promoting the clock to module scope, makes a
    research eligibility sweep delay ORDER WRITES for no quota reason. #2946 step 2
    recorded the same coupling as item 1's blocker, and
    ``docs/proposals/execution/2026-09-13-etoro-trading-throttle-coordination.md``
    rejected a per-user-key registry on that ground.

    So the two constants must stay independent, and lane B's must be derived from lane
    B's own budget. Asserting only ``!=`` would pass on any coincidence, so this asserts
    the DERIVATION: the interval is what the lane map produces for lane B's budget.
    """
    site = next(c for c in lanes.CALL_SITES if c.method == "check_instrument_eligibility")
    expected = lanes.min_interval_for_stamps(lanes.LANES[site.lane].window_s, site.conservative_per_minute - 1)

    assert CORE_ELIGIBILITY_REQUEST_INTERVAL_S == pytest.approx(expected), (
        "lane B's pacing must be derived from lane B's documented budget, not chosen"
    )
    assert CORE_ELIGIBILITY_REQUEST_INTERVAL_S != _ETORO_WRITE_INTERVAL_S, (
        "lane B is a DEDICATED quota; pacing it from the shared write floor couples a "
        "research sweep to order submission -- see this test's docstring"
    )
    assert lanes.LANES[site.lane].scope == "dedicated", (
        "this invariant's whole justification is lane B's dedicated scope; if the portal "
        "ever repools it, re-derive rather than keeping the test green"
    )
