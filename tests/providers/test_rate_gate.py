# tests/providers/test_rate_gate.py
import asyncio

import pytest

from app.providers.rate_gate import InProcessFloorGate, compute_wait


def test_compute_wait_idle_returns_zero():
    assert compute_wait(now=100.0, next_free_at=99.0, floor=0.11) == 0.0


def test_compute_wait_backlogged_returns_remaining():
    assert compute_wait(now=100.0, next_free_at=100.05, floor=0.11) == pytest.approx(0.05)


def test_inprocess_floor_spaces_sync_calls():
    clock = [0.0]
    sleeps: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s

    gate = InProcessFloorGate(floor=0.11, _monotonic=lambda: clock[0], _sleep=fake_sleep)
    gate.acquire()
    gate.acquire()
    assert sleeps[0] == 0.0
    assert sleeps[1] == pytest.approx(0.11, abs=1e-9)


def test_inprocess_floor_async_shares_clock_with_sync():
    clock = [0.0]
    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s

    gate = InProcessFloorGate(
        floor=0.11,
        _monotonic=lambda: clock[0],
        _sleep=lambda s: clock.__setitem__(0, clock[0] + s),
        _async_sleep=fake_sleep,
    )
    gate.acquire()
    asyncio.run(gate.acquire_async())
    assert sleeps[-1] == pytest.approx(0.11, abs=1e-9)


def test_resilient_client_delegates_to_gate():
    import httpx

    from app.providers.resilient_client import ResilientClient

    calls = {"n": 0}

    class StubGate:
        def acquire(self):
            calls["n"] += 1

        async def acquire_async(self): ...

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text="ok"))
    client = httpx.Client(transport=transport)
    rc = ResilientClient(client, gate=StubGate())
    rc.get("https://example.test/x")
    assert calls["n"] == 1


def test_on_429_callback_fires_only_when_wired():
    import httpx

    from app.providers import sec_throttle_metrics as m
    from app.providers.resilient_client import ResilientClient

    before = m.sec_throttle_429_total()

    def make_client(on_429):
        seq = iter([httpx.Response(429, headers={"retry-after": "0.01"}), httpx.Response(200, text="ok")])
        return ResilientClient(
            httpx.Client(transport=httpx.MockTransport(lambda req: next(seq))),
            max_retries=1,
            backoff_schedule=(0.0,),
            on_429=on_429,
        )

    make_client(m.incr_sec_429).get("https://example.test/x")  # SEC-wired
    assert m.sec_throttle_429_total() == before + 1

    make_client(None).get("https://example.test/y")  # non-SEC, no callback
    assert m.sec_throttle_429_total() == before + 1  # unchanged
