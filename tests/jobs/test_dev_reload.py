"""#2144 / #2666 / #2274 — dev jobs-daemon auto-reload.

Stale-batch suppression, drain visibility, and (#2274) deferring a
reload past a job that is provably alive.

Pure-logic only (no DB, no subprocess). The one non-obvious decision in
the supervisor is whether a change batch delivered *after* a restart
should trigger another restart: the rust watcher keeps buffering while
we block on a drain, so the batch that lands right after a reload
usually replays the very edits that caused it, and acting on it costs a
second full drain for nothing.

⚠ Keep this module DB-free. ``tests/conftest.py::pytest_collection_modifyitems``
auto-applies the ``db`` marker per MODULE (``_module_source_touches_db``),
so one DB-backed test here would deselect every pure test in this file
from the ``-m "not db"`` pre-push gate — silently, since the file still
passes when run directly. ``live_job``'s SQL is exercised in
``tests/jobs/test_dev_reload_live_job_sql.py`` for exactly that reason.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any, cast

import pytest

from app.jobs import dev_reload
from app.jobs.dev_reload import changes_are_stale


def _touch(path: Path, mtime: float) -> str:
    path.write_text("x")
    os.utime(path, (mtime, mtime))
    return str(path)


def test_empty_batch_is_not_stale() -> None:
    # The supervisor uses an empty batch as the watcher's idle tick. If it
    # read as "stale" the caller could confuse a suppressed reload with a
    # liveness poll.
    assert changes_are_stale([], spawn_time=1000.0) is False


def test_batch_older_than_the_running_child_is_stale(tmp_path: Path) -> None:
    paths = [
        _touch(tmp_path / "a.py", 900.0),
        _touch(tmp_path / "b.py", 950.0),
    ]
    assert changes_are_stale(paths, spawn_time=1000.0) is True


def test_any_path_newer_than_the_child_forces_a_reload(tmp_path: Path) -> None:
    paths = [
        _touch(tmp_path / "a.py", 900.0),
        _touch(tmp_path / "b.py", 1001.0),
    ]
    assert changes_are_stale(paths, spawn_time=1000.0) is False


def test_deleted_path_alone_forces_a_reload(tmp_path: Path) -> None:
    # Codex pre-push review: a deleted/renamed module has no mtime to
    # compare against, so it must NOT be suppressed — otherwise deleting a
    # module leaves the daemon running the old code until an unrelated edit
    # lands. One redundant restart beats silently-stale code.
    assert changes_are_stale([str(tmp_path / "gone.py")], spawn_time=1000.0) is False


def test_deleted_path_does_not_mask_a_newer_sibling(tmp_path: Path) -> None:
    paths = [
        str(tmp_path / "gone.py"),
        _touch(tmp_path / "fresh.py", 1001.0),
    ]
    assert changes_are_stale(paths, spawn_time=1000.0) is False


# --- #2666: a slow drain must not look like a reload that never fired ---------


class _FakeProc:
    """A child that ignores SIGTERM for `hangs_for` wait() calls."""

    def __init__(self, hangs_for: int) -> None:
        self.pid = 4242
        self._hangs_left = hangs_for
        self.signals: list[int] = []
        self.killed = False
        self._alive = True

    def poll(self) -> int | None:
        return None if self._alive else 0

    def send_signal(self, signum: int) -> None:
        self.signals.append(signum)

    def wait(self, timeout: float | None = None) -> int:
        if self._hangs_left > 0:
            self._hangs_left -= 1
            # Sleep the slice before raising, as a real Popen.wait does. Without
            # it the supervisor's deadline loop spins in microseconds and the
            # budget under test is never actually consumed.
            time.sleep(timeout or 0.0)
            raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout or 0.0)
        self._alive = False
        return 0

    def kill(self) -> None:
        self.killed = True
        self._hangs_left = 0


def test_a_slow_drain_reports_progress_instead_of_going_silent(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """#2666's real gap: three minutes of silence between SIGTERM and the
    respawn reads, to a PID-polling observer, as 'the reload never fired'."""
    monkeypatch.setattr(dev_reload, "_DRAIN_TIMEOUT_S", 1.0)
    monkeypatch.setattr(dev_reload, "_DRAIN_PROGRESS_S", 0.01)
    proc = _FakeProc(hangs_for=3)
    with caplog.at_level(logging.INFO, logger=dev_reload.__name__):
        dev_reload._stop_child(cast(Any, proc))
    progress = [r for r in caplog.records if "still draining," in r.message]
    assert progress, "a slow drain emitted no progress line"
    assert proc.signals == [signal.SIGTERM]
    assert not proc.killed, "drained before the deadline, so no escalation"


def test_slicing_the_wait_does_not_extend_the_drain_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """Waiting in slices must not turn one 180s budget into N x 180s."""
    monkeypatch.setattr(dev_reload, "_DRAIN_TIMEOUT_S", 0.2)
    monkeypatch.setattr(dev_reload, "_DRAIN_PROGRESS_S", 0.02)
    monkeypatch.setattr(dev_reload, "_POST_KILL_SETTLE_S", 0.0)
    proc = _FakeProc(hangs_for=10_000)  # never drains
    started = time.monotonic()
    dev_reload._stop_child(cast(Any, proc))
    elapsed = time.monotonic() - started
    assert proc.killed, "a child past the deadline must be escalated"
    # Generous ceiling: the assertion is that the budget is BOUNDED, not a
    # wall-clock ratio (a timing ratio is a host measurement, not a property).
    assert elapsed < 5.0, f"drain budget was not bounded: {elapsed:.2f}s"


def test_running_commit_ignores_an_inherited_git_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """A git hook exports GIT_DIR into everything it runs, so an unscrubbed call
    would resolve against the hook's repo rather than ours (#2658's trap)."""
    monkeypatch.setenv("GIT_DIR", "/nonexistent/hook.git")
    monkeypatch.setenv("GIT_WORK_TREE", "/nonexistent")
    assert dev_reload.running_commit() != "unknown"


def test_running_commit_degrades_rather_than_raising(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(dev_reload, "_REPO_ROOT", tmp_path)  # not a git repo
    assert dev_reload.running_commit() == "unknown"


# --- #2274: a live job defers the reload instead of being SIGKILLed ------------


class _FakeCursorConn:
    """Minimal stand-in for the connection `live_job` opens."""

    def __init__(self, row: tuple[Any, ...] | None) -> None:
        self._row = row

    def __enter__(self) -> _FakeCursorConn:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, _sql: str, _params: object = None) -> _FakeCursorConn:
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


def test_live_job_fails_open_when_the_database_is_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    """The probe is an optimisation on top of correct-but-destructive
    behaviour. If it cannot answer, the reload must proceed exactly as it
    did before #2274 — a DB blip must never freeze the daemon on stale code."""
    import psycopg

    def _boom(*_a: object, **_kw: object) -> object:
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", _boom)
    assert dev_reload.live_job() is None


def test_live_job_names_the_blocking_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """'reload deferred' with no subject is a message an operator learns to
    ignore, so the blocker identifies itself."""
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_a, **_kw: _FakeCursorConn((111447, "strategy_backtest_run", 12625, 17290, 8.0)),
    )
    described = dev_reload.live_job()
    assert described is not None
    assert "strategy_backtest_run" in described
    assert "111447" in described
    assert "12625/17290" in described


def test_live_job_handles_an_unbounded_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """`target_count` is NULL for unbounded sweeps (sql/140), so the
    description must not render 'None' at the operator."""
    import psycopg

    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *_a, **_kw: _FakeCursorConn((99, "sec_manifest_worker", 312, None, 2.0)),
    )
    described = dev_reload.live_job()
    assert described is not None
    assert "312 done" in described
    assert "None" not in described


def test_the_threshold_registry_travels_into_the_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """The per-job cut is applied by Postgres, so the only thing provable here
    is that the registry is actually what gets sent. Whether the SQL honours it
    is a property of the statement, tested in `test_dev_reload_live_job_sql.py`.
    """
    import json

    from app.services.processes.stale_thresholds import get_threshold, overridden_process_ids

    sent = json.loads(dev_reload._THRESHOLD_OVERRIDES_JSON)
    assert sent, "no overrides reached the query — every job would fall back to the default"
    assert set(sent) == set(overridden_process_ids()), "the query and the registry disagree on which jobs are slow-tick"
    for process_id, seconds in sent.items():
        assert seconds == get_threshold(process_id)


class _LiveProc:
    """A child that never exits on its own."""

    def __init__(self) -> None:
        self.pid = 4242

    def poll(self) -> int | None:
        return None


class _DyingProc:
    """A child that reports itself crashed from the Nth poll onwards."""

    def __init__(self, alive_polls: int) -> None:
        self.pid = 4243
        self._alive_polls = alive_polls

    def poll(self) -> int | None:
        if self._alive_polls > 0:
            self._alive_polls -= 1
            return None
        return -9


def _drive_supervisor(
    monkeypatch: pytest.MonkeyPatch,
    *,
    batches: list[list[tuple[int, str]]],
    live_job_results: list[str | None],
    procs: list[Any] | None = None,
) -> tuple[list[str], int]:
    """Run `_supervise` against a scripted watcher, returning
    (stop_child call reasons, spawn count).

    `watchfiles` is imported inside `_supervise`, so a stub module in
    `sys.modules` is enough to script the change batches without any real
    filesystem watching or subprocess.
    """
    import sys
    import types

    stub = types.ModuleType("watchfiles")
    stub.PythonFilter = object  # type: ignore[attr-defined]
    stub.watch = lambda *_a, **_kw: iter(batches)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "watchfiles", stub)

    spawns = 0

    def _fake_spawn() -> tuple[Any, float]:
        nonlocal spawns
        proc = procs[spawns] if procs is not None and spawns < len(procs) else _LiveProc()
        spawns += 1
        # spawn_time 0.0 so any real file counts as newer (not stale).
        return cast(Any, proc), 0.0

    stops: list[str] = []
    monkeypatch.setattr(dev_reload, "_spawn_child", _fake_spawn)
    monkeypatch.setattr(dev_reload, "_stop_child", lambda proc: stops.append(str(proc.pid)))
    monkeypatch.setattr(dev_reload, "_LIVE_JOB_PROBE_PERIOD_S", 0.0)

    results = iter(live_job_results)
    monkeypatch.setattr(dev_reload, "live_job", lambda: next(results, live_job_results[-1]))

    dev_reload._supervise()
    return stops, spawns


def test_a_live_job_defers_the_reload_rather_than_killing_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """#2274's regression. Three `strategy_backtest_run` attempts died inside
    one hour because an ordinary merge changed an `app/**` mtime; the last was
    12,625/17,290 through and had heartbeat 8s earlier. The reload must wait."""
    changed = _touch(tmp_path / "svc.py", time.time())
    with caplog.at_level(logging.INFO, logger=dev_reload.__name__):
        stops, spawns = _drive_supervisor(
            monkeypatch,
            batches=[[(1, changed)], [], [], []],
            live_job_results=["strategy_backtest_run run 111447 (12625/17290 done, heartbeat 8s ago)"],
        )
    # One spawn (the initial child) and one stop (the `finally` on exit) — the
    # change never triggered a reload.
    assert spawns == 1, "a live job must not be preempted by a file change"
    assert stops == ["4242"], "the only _stop_child is the supervisor's own shutdown"
    assert any("DEFERRING reload" in r.message for r in caplog.records)


def test_a_deferred_reload_is_applied_once_the_job_finishes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Deferral must not be a silent drop: the held change has to land as soon
    as the job stops being live, without waiting for another edit."""
    changed = _touch(tmp_path / "svc.py", time.time())
    stops, spawns = _drive_supervisor(
        monkeypatch,
        batches=[[(1, changed)], [], [], []],
        live_job_results=["strategy_backtest_run run 111447 (1/2 done, heartbeat 1s ago)", None],
    )
    assert spawns == 2, "the held reload never landed"
    assert len(stops) == 2, "expected the deferred reload's stop plus the shutdown stop"


def test_a_child_that_dies_holding_changes_respawns_on_them(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Codex checkpoint 2, P1. If the child crashes while a reload is deferred,
    the held edits were never loaded by anything — clearing them would leave the
    daemon dead until some unrelated file happened to change.

    Not hypothetical: the jobs child took a bare SIGKILL (`child exited -9`,
    no preceding SIGTERM) mid-backtest on 2026-08-21.

    The respawn must also NOT consult `live_job` first — the dead child's own
    `running` rows stay inside the staleness window, so probing would make a
    crash wait out a heartbeat that can never advance again.
    """
    changed = _touch(tmp_path / "svc.py", time.time())
    probes = 0

    def _counting_live_job() -> str | None:
        nonlocal probes
        probes += 1
        return "strategy_backtest_run run 111447 (1/2 done, heartbeat 1s ago)"

    import sys
    import types

    stub = types.ModuleType("watchfiles")
    stub.PythonFilter = object  # type: ignore[attr-defined]
    # Change arrives, deferred behind a live job, then the child crashes.
    batches: list[list[tuple[int, str]]] = [[(1, changed)], [], []]
    stub.watch = lambda *_a, **_kw: iter(batches)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "watchfiles", stub)

    spawns = 0
    procs: list[Any] = [_DyingProc(alive_polls=0), _LiveProc()]

    def _fake_spawn() -> tuple[Any, float]:
        nonlocal spawns
        proc = procs[spawns] if spawns < len(procs) else _LiveProc()
        spawns += 1
        return cast(Any, proc), 0.0

    monkeypatch.setattr(dev_reload, "_spawn_child", _fake_spawn)
    monkeypatch.setattr(dev_reload, "_stop_child", lambda _proc: None)
    monkeypatch.setattr(dev_reload, "_LIVE_JOB_PROBE_PERIOD_S", 0.0)
    monkeypatch.setattr(dev_reload, "live_job", _counting_live_job)

    with caplog.at_level(logging.INFO, logger=dev_reload.__name__):
        dev_reload._supervise()

    assert spawns == 2, "a crash holding pending changes must respawn on them"
    assert any("died with" in r.message for r in caplog.records)
    # Exactly one probe: the initial deferral. The crash path must not add one.
    assert probes == 1, f"the crash path probed live_job {probes} times; it must not probe at all"


def test_no_live_job_reloads_immediately(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The pre-#2274 path is unchanged for the 111,401 of 111,409 `job_runs`
    rows that never heartbeat: nothing to defer behind, so reload at once."""
    changed = _touch(tmp_path / "svc.py", time.time())
    _stops, spawns = _drive_supervisor(
        monkeypatch,
        batches=[[(1, changed)], []],
        live_job_results=[None],
    )
    assert spawns == 2
