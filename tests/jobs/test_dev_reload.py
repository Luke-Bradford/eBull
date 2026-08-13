"""#2144 / #2666 — dev jobs-daemon auto-reload: stale-batch suppression and drain visibility.

Pure-logic only (no DB, no subprocess). The one non-obvious decision in
the supervisor is whether a change batch delivered *after* a restart
should trigger another restart: the rust watcher keeps buffering while
we block on a drain, so the batch that lands right after a reload
usually replays the very edits that caused it, and acting on it costs a
second full drain for nothing.
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
