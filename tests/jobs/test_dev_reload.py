"""#2144 — dev jobs-daemon auto-reload: stale-batch suppression.

Pure-logic only (no DB, no subprocess). The one non-obvious decision in
the supervisor is whether a change batch delivered *after* a restart
should trigger another restart: the rust watcher keeps buffering while
we block on a drain, so the batch that lands right after a reload
usually replays the very edits that caused it, and acting on it costs a
second full drain for nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

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
