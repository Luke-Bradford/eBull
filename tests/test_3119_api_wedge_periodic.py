"""#3119 — the periodic API wedge probe's decision, loop and request deadline.

Pure: ``observe`` and ``dump_threads`` are replaced, so nothing here touches
git, a real worker or the DB; the one socket test uses a local listener.
"""

from __future__ import annotations

import logging
import socket
import threading
import time

import pytest

from app.system import api_wedge_probe as probe_mod
from app.system.api_wedge_probe import Observation, ProbeResult, periodic_decision


def _obs(
    *,
    live: int | None = 200,
    health: int | None = 200,
    verdict: str = "fresh",
    served: str | None = "aaa",
    ondisk: str | None = "aaa",
) -> Observation:
    return Observation(
        live=ProbeResult("/health/live", live, 0.01, None if live else "timeout"),
        health=ProbeResult("/health", health, 0.1, None if health else "timeout"),
        sidecar={"pid": 1},
        verdict=verdict,
        served=served,
        ondisk=ondisk,
    )


@pytest.mark.parametrize(
    ("obs", "previous", "fails", "remembered"),
    [
        (_obs(), None, False, None),
        (_obs(), ("aaa", "bbb"), False, None),
        # A 503 from /health is an answer, not a wedge.
        (_obs(health=503), None, False, None),
        (_obs(live=None), None, True, None),
        (_obs(health=None), None, True, None),
        (_obs(live=None, health=None), ("aaa", "bbb"), True, None),
        # First STALE sighting is remembered, not raised: it may be a reload in flight.
        (_obs(verdict="STALE", ondisk="bbb"), None, False, ("aaa", "bbb")),
        # Same pair twice in a row is not a reload.
        (_obs(verdict="STALE", ondisk="bbb"), ("aaa", "bbb"), True, ("aaa", "bbb")),
        # A different pair restarts the count (the checkout moved again).
        (_obs(verdict="STALE", ondisk="ccc"), ("aaa", "bbb"), False, ("aaa", "ccc")),
        # 404 on /health/live means the build predates #3119 — stale even if git says fresh.
        (_obs(live=404), None, False, ("aaa", "aaa")),
        (_obs(live=404), ("aaa", "aaa"), True, ("aaa", "aaa")),
        # Unknown is never fresh.
        (_obs(verdict="unknown", served=None), None, True, None),
    ],
)
def test_periodic_decision(obs, previous, fails, remembered) -> None:
    failure, pair = periodic_decision(obs, previous)
    assert (failure is not None) is fails
    assert pair == remembered


class _Ticks:
    """A stop_event stand-in: ``wait`` reports "not stopped" ``n`` times."""

    def __init__(self, n: int) -> None:
        self.remaining = n

    def wait(self, _timeout: float) -> bool:
        self.remaining -= 1
        return self.remaining < 0


@pytest.fixture
def loop(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture):
    """Run ``run_periodic_probe`` over a scripted sequence of observations."""
    dumps: list[object] = []
    monkeypatch.setattr(probe_mod.settings, "app_env", "dev")
    monkeypatch.setattr(probe_mod, "dump_threads", lambda sidecar: dumps.append(sidecar) or "sent")

    def run(*observations: Observation) -> list[str]:
        queue = list(observations)
        monkeypatch.setattr(probe_mod, "observe", lambda: queue.pop(0))
        caplog.clear()
        with caplog.at_level(logging.ERROR, logger=probe_mod.__name__):
            probe_mod.run_periodic_probe(_Ticks(len(observations)), interval_s=0)  # type: ignore[arg-type]
        return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]

    run.dumps = dumps  # type: ignore[attr-defined]
    return run


def test_loop_dumps_threads_and_alarms_on_a_wedge(loop) -> None:
    obs = _obs(live=None, health=None)
    errors = loop(obs)
    assert loop.dumps == [obs.sidecar]
    assert any("wedged" in e for e in errors)


def test_loop_needs_two_consecutive_stale_passes(loop) -> None:
    stale = _obs(verdict="STALE", ondisk="bbb")
    assert loop(stale) == []
    assert any("stale" in e for e in loop(stale, stale))
    assert loop.dumps == []


def test_loop_healthy_pass_clears_the_stale_memory(loop) -> None:
    stale = _obs(verdict="STALE", ondisk="bbb")
    assert loop(stale, _obs(), stale) == []


def test_loop_survives_a_raising_pass(loop, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = iter([RuntimeError("boom"), _obs(live=None, health=None)])

    def observe() -> Observation:
        item = next(calls)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(probe_mod, "observe", observe)
    probe_mod.run_periodic_probe(_Ticks(2), interval_s=0)  # type: ignore[arg-type]
    assert len(loop.dumps) == 1


def test_loop_is_a_no_op_outside_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe_mod.settings, "app_env", "prod")

    def boom() -> Observation:
        raise AssertionError("probe must not run outside dev")

    monkeypatch.setattr(probe_mod, "observe", boom)
    probe_mod.run_periodic_probe(_Ticks(3), interval_s=0)  # type: ignore[arg-type]


def test_probe_releases_a_dribbling_peer_at_the_deadline() -> None:
    """A peer that sends a byte every 0.1s never trips the per-op socket timeout.

    The deadline must end the request thread, not abandon it: the jobs process
    runs this every cadence period and would otherwise accumulate sockets.
    """
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    stop = threading.Event()

    def dribble() -> None:
        conn, _ = listener.accept()
        with conn:
            conn.recv(4096)
            try:
                conn.sendall(b"HTTP/1.1 200 OK\r\nX-Slow: ")
                while not stop.is_set():
                    conn.sendall(b"a")
                    time.sleep(0.1)
            except OSError:
                pass

    server = threading.Thread(target=dribble, daemon=True)
    server.start()
    before = {t.ident for t in threading.enumerate() if t.name == "api-wedge-probe-request"}
    try:
        result = probe_mod.probe(f"http://127.0.0.1:{port}", "/health/live", 0.5)
        leaked = [t for t in threading.enumerate() if t.name == "api-wedge-probe-request" and t.ident not in before]
    finally:
        stop.set()
        listener.close()

    assert not result.answered
    assert leaked == []
