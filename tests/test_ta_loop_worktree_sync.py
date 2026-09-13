"""The loop driver must re-sync the CODE it works from, not just its prompt (#2607).

#2658 fixed the prompt: a copy nothing compared to anything. The code had no check
at all. On 2026-08-12 the loop resumed on a worktree detached ~160 commits behind
`origin/main`, so every branch it cut would have been based on stale code — the shape
of the `git reset --soft origin/main` incident in the gotchas archive. Nothing failed,
because nothing looked.

⚠ The load-bearing half of this contract is the REFUSALS, not the fast-forward. A
re-sync that discarded a dirty tree or moved a feature branch would cost exactly what
the 2026-07-16 clobber race cost, and it would do it unattended. Each refusal below is
a test, and each asserts that the tree did not move.

These drive the real `scripts/autonomy/ta_loop.sh` for one iteration against a
throwaway git repo and a stub `claude`, reusing the #2658 harness — one driver, one
fixture, so a change to the invocation cannot be right in one file and stale in the
other.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.test_ta_loop_prompt_sync import _env, _git
from tests.test_ta_loop_prompt_sync import loop as _loop  # noqa: F401  (fixture, re-exported below)


@pytest.fixture
def loop(_loop):  # noqa: F811
    """The #2658 harness, plus the one thing the real worktree has and it did not.

    ⚠⚠ `.gitignore` is load-bearing here, and its absence is not a fixture detail:
    the driver writes its log, status and installed prompt into `var/autonomy/`
    INSIDE the worktree it re-syncs. `eBull/.gitignore` carries `/var/*`, so that
    state is invisible to `git status --porcelain` and cannot block a re-sync.
    Without the rule every single case below reads as "working tree is dirty" —
    which is exactly what the first run of these tests reported, and it would be
    the live behaviour on any branch that lost the rule. Fail-safe (the loop warns
    and runs on stale code rather than discarding anything), but it is a real
    coupling and `test_the_loops_own_state_directory_does_not_block_a_re_sync`
    is here to keep it asserted rather than assumed.
    """
    (_loop.root / ".gitignore").write_text("/var/*\n")
    _git(_loop.root, "add", "-A")
    _git(_loop.root, "commit", "-q", "-m", "ignore the loop's own state dir")
    _git(_loop.root, "push", "-q", "origin", "main")
    return _loop


def _head(repo: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
        env=_env(),
    ).stdout.strip()


def _advance_origin(loop: Any) -> str:
    """Land a commit on origin/main that the worktree does not have.

    Pushed from a SECOND clone rather than by committing here: the worktree under
    test must be genuinely behind its remote, which is the state the 2026-08-12
    resume was in. Committing locally and pushing would leave it up to date.
    """
    other = loop.root.parent / "other-clone"
    subprocess.run(
        ["git", "clone", "-q", str(loop.origin_url), str(other)],
        check=True,
        capture_output=True,
        env=_env(),
    )
    (other / "landed_elsewhere.txt").write_text("merged while the loop was down\n")
    _git(other, "add", "-A")
    _git(other, "commit", "-q", "-m", "a merge the worktree has not seen")
    _git(other, "push", "-q", "origin", "main")
    return _head(other)


def test_a_worktree_behind_origin_is_fast_forwarded_before_the_agent_runs(loop) -> None:  # noqa: F811
    """The whole point: an iteration must not start from stale code."""
    behind = _head(loop.root)
    ahead = _advance_origin(loop)
    assert behind != ahead

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == ahead
    assert (loop.root / "landed_elsewhere.txt").exists()
    assert "worktree RESYNCED" in loop.log.read_text()
    assert f"- worktree: RESYNCED {behind[:12]} -> origin/main {ahead[:12]}" in loop.status.read_text()


def test_a_detached_worktree_is_re_detached_rather_than_put_on_a_branch(loop) -> None:  # noqa: F811
    """Detached is the loop's resting state and must stay detached after a re-sync."""
    _git(loop.root, "checkout", "--quiet", "--detach", "HEAD")
    ahead = _advance_origin(loop)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == ahead
    branch = subprocess.run(
        ["git", "-C", str(loop.root), "symbolic-ref", "--quiet", "--short", "HEAD"],
        capture_output=True,
        text=True,
        env=_env(),
    )
    assert branch.returncode != 0, f"HEAD should still be detached, got {branch.stdout!r}"
    assert "(detached)" in loop.log.read_text()


def test_an_up_to_date_worktree_is_reported_and_left_alone(loop) -> None:  # noqa: F811
    before = _head(loop.root)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert f"worktree at origin/main ({before[:12]})" in loop.log.read_text()


def test_a_dirty_worktree_is_never_re_synced(loop) -> None:  # noqa: F811
    """Uncommitted work is unfinished work — announce, do not discard."""
    before = _head(loop.root)
    _advance_origin(loop)
    tracked = loop.root / ".autonomy" / "loop_prompt.md"
    tracked.write_text("# an edit nobody committed\n")

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert tracked.read_text() == "# an edit nobody committed\n"
    assert "WARN worktree STALE" in loop.log.read_text()
    assert "working tree is dirty" in loop.log.read_text()


def test_an_untracked_file_also_blocks_the_re_sync(loop) -> None:  # noqa: F811
    """`--porcelain` covers untracked files, and it must — a re-sync can clobber one."""
    before = _head(loop.root)
    _advance_origin(loop)
    (loop.root / "half_written_spec.md").write_text("draft\n")

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert (loop.root / "half_written_spec.md").exists()
    assert "working tree is dirty" in loop.log.read_text()


def test_a_named_feature_branch_is_never_moved(loop) -> None:  # noqa: F811
    """A branch is a ticket in flight, not a stale checkout."""
    _git(loop.root, "checkout", "--quiet", "-b", "fix/1234-in-flight")
    before = _head(loop.root)
    _advance_origin(loop)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    log = loop.log.read_text()
    assert "NOT re-synced, on branch fix/1234-in-flight" in log
    assert "a named branch is a ticket in flight" in log


def test_a_main_that_diverged_is_reported_not_rewritten(loop) -> None:  # noqa: F811
    """Local commits on main stop the re-sync; nothing here rewrites them."""
    _advance_origin(loop)
    (loop.root / "local_only.txt").write_text("committed here, never pushed\n")
    _git(loop.root, "add", "-A")
    _git(loop.root, "commit", "-q", "-m", "diverged locally")
    before = _head(loop.root)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert (loop.root / "local_only.txt").exists()
    assert "HEAD carries commits origin/main does not" in loop.log.read_text()


def test_a_committed_but_unpushed_detached_head_is_never_checked_away(loop) -> None:  # noqa: F811
    """The clean-tree check is not enough, and this is the case that proves it.

    A crashed iteration can leave COMMITTED work on a detached HEAD. The tree is
    clean, so the dirty guard says nothing — and a plain `checkout --detach` then
    deletes those files and leaves the commits reachable only through the reflog.
    """
    _git(loop.root, "checkout", "--quiet", "--detach", "HEAD")
    _advance_origin(loop)
    orphanable = loop.root / "unpublished.txt"
    orphanable.write_text("an interrupted iteration's work\n")
    _git(loop.root, "add", "-A")
    _git(loop.root, "commit", "-q", "-m", "committed, never pushed, no branch")
    before = _head(loop.root)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert orphanable.exists(), "the re-sync deleted committed work that exists nowhere else"
    log = loop.log.read_text()
    assert "HEAD carries commits origin/main does not" in log
    assert "they exist ONLY here" in log


def test_a_main_ahead_of_the_remote_is_not_reported_as_re_synced(loop) -> None:  # noqa: F811
    """`git merge --ff-only` exits 0 WITHOUT moving HEAD when local main is ahead.

    So a status line derived from that exit status announces a re-sync that did
    not happen, while the agent runs on local-only commits. Origin is deliberately
    NOT advanced here: ahead-only is the case a divergence test does not reach.
    """
    (loop.root / "local_only.txt").write_text("committed here, never pushed\n")
    _git(loop.root, "add", "-A")
    _git(loop.root, "commit", "-q", "-m", "ahead of origin, not diverged")
    before = _head(loop.root)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    log = loop.log.read_text()
    # ⚠ Prefixed: the prompt sync logs its own `prompt RESYNCED` on a first run,
    # so a bare "RESYNCED" assertion here passes or fails on the wrong subsystem.
    assert "worktree RESYNCED" not in log
    assert "HEAD carries commits origin/main does not" in log


def test_the_loops_own_state_directory_does_not_block_a_re_sync(loop) -> None:  # noqa: F811
    """The driver writes inside the tree it re-syncs; the ignore rule is what saves it.

    Asserted rather than assumed because the coupling is invisible: lose `/var/*`
    from `.gitignore` and every re-sync silently degrades to "dirty" forever.
    """
    ahead = _advance_origin(loop)
    assert loop.log.exists() is False  # written by the run below, inside the worktree

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert loop.log.parent.is_relative_to(loop.root)
    assert _head(loop.root) == ahead
    assert "worktree RESYNCED" in loop.log.read_text()


def test_an_unknown_ref_is_announced_and_the_iteration_still_runs(loop) -> None:  # noqa: F811
    """A broken reference point must not become a self-inflicted outage."""
    before = _head(loop.root)
    # The prompt cannot be derived from a ref that does not exist, and an absent
    # installed copy is fatal by design — so install one. This test is about the
    # WORKTREE check surviving a broken reference point, not about the prompt.
    loop.prompt.parent.mkdir(parents=True, exist_ok=True)
    loop.prompt.write_text("# installed prompt\n")

    result = loop.run(TA_LOOP_PROMPT_REF="refs/remotes/origin/does-not-exist")

    assert result.returncode == 0, result.stderr
    assert _head(loop.root) == before
    assert "worktree UNVERIFIED" in loop.log.read_text()
    # The iteration is the thing that must survive: the agent still ran.
    assert loop.handed_to_agent.exists()
