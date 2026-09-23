"""#3119 — the periodic ``api_wedge_probe`` job's decision and wrapper.

Pure: ``observe`` and ``dump_threads`` are replaced, so nothing here touches
the network, git, a real worker or the DB.
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from app.system import api_wedge_probe as probe_mod
from app.system.api_wedge_probe import Observation, ProbeResult, periodic_decision
from app.workers import scheduler


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


@pytest.fixture
def job(monkeypatch: pytest.MonkeyPatch):
    """Run the wrapper with the tracker, env and probe all replaced."""

    @contextmanager
    def fake_tracked(_name: str):
        yield object()

    dumps: list[object] = []
    monkeypatch.setattr(scheduler, "_tracked_job", fake_tracked)
    monkeypatch.setattr(scheduler, "_api_wedge_previous_stale", None)
    monkeypatch.setattr(scheduler.settings, "app_env", "dev")
    monkeypatch.setattr(probe_mod, "dump_threads", lambda sidecar: dumps.append(sidecar) or "sent")

    def run(obs: Observation) -> None:
        monkeypatch.setattr(probe_mod, "observe", lambda: obs)
        scheduler.api_wedge_probe()

    run.dumps = dumps  # type: ignore[attr-defined]
    return run


def test_job_dumps_threads_before_failing_on_a_wedge(job) -> None:
    obs = _obs(live=None, health=None)
    with pytest.raises(RuntimeError, match="wedged"):
        job(obs)
    assert job.dumps == [obs.sidecar]


def test_job_needs_two_consecutive_stale_runs(job) -> None:
    stale = _obs(verdict="STALE", ondisk="bbb")
    job(stale)
    with pytest.raises(RuntimeError, match="stale"):
        job(stale)
    assert job.dumps == []


def test_job_healthy_run_clears_the_stale_memory(job) -> None:
    stale = _obs(verdict="STALE", ondisk="bbb")
    job(stale)
    job(_obs())
    job(stale)  # first sighting again, so no failure


def test_job_is_a_no_op_outside_dev(job, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scheduler.settings, "app_env", "prod")

    def boom() -> Observation:
        raise AssertionError("probe must not run outside dev")

    monkeypatch.setattr(probe_mod, "observe", boom)
    scheduler.api_wedge_probe()
