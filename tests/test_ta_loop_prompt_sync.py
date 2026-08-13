"""The loop driver must derive its prompt, not inherit a hand-installed copy (#2658).

For seven days `var/autonomy/bin/ta_loop_prompt.md` was a copy nothing compared
to anything: #2604 re-aimed `.autonomy/loop_prompt.md`, the driver read a
different file, and every iteration reported `OK` while working from a task list
that had shipped a week earlier. Nothing failed, because nothing checked.

These tests drive the real `scripts/autonomy/ta_loop.sh` for one iteration
against a throwaway git repo and a stub `claude`, and assert on the TEXT THE
AGENT WAS HANDED rather than on the file the driver left behind — the file being
right is not the property that failed, the prompt reaching the agent is.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DRIVER = REPO_ROOT / "scripts" / "autonomy" / "ta_loop.sh"
CANONICAL_PATH = ".autonomy/loop_prompt.md"

CANONICAL_TEXT = "# canonical prompt\n\nActive milestone first.\n"
STALE_TEXT = "# stale prompt\n\nPhase 3c — shipped a week ago.\n"

_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@example.com",
}


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        env={**os.environ, **_GIT_ENV},
    )


@pytest.fixture
def loop(tmp_path: Path):
    """A worktree with the driver installed, the canonical prompt on origin/main.

    `origin/main` is minted with `update-ref` rather than by cloning: the driver
    only ever reads it through `git show`, so a remote-tracking ref pointing at a
    local commit exercises exactly the same path.
    """
    worktree = Path(os.path.realpath(tmp_path)) / "wt"
    (worktree / ".autonomy").mkdir(parents=True)
    (worktree / "scripts" / "autonomy").mkdir(parents=True)
    (worktree / CANONICAL_PATH).write_text(CANONICAL_TEXT)
    shutil.copy(DRIVER, worktree / "scripts" / "autonomy" / "ta_loop.sh")

    _git(worktree, "init", "-q", "-b", "main")
    _git(worktree, "add", "-A")
    _git(worktree, "commit", "-q", "-m", "base")
    _git(worktree, "update-ref", "refs/remotes/origin/main", "HEAD")

    installed_dir = worktree / "var" / "autonomy" / "bin"
    installed_dir.mkdir(parents=True)
    shutil.copy(DRIVER, installed_dir / "ta_loop.sh")

    # Stub `claude`: records the prompt text it was handed, then emits the one
    # result event the driver parses for its verdict.
    stub_dir = worktree / "stub"
    stub_dir.mkdir()
    capture = worktree / "handed-to-agent.txt"
    stub = stub_dir / "claude"
    stub.write_text(
        "#!/bin/sh\n"
        f'printf "%s" "$2" > "{capture}"\n'
        'printf \'%s\\n\' \'{"type":"result","is_error":false}\'\n'
    )
    stub.chmod(0o755)

    class Loop:
        root = worktree
        prompt = installed_dir / "ta_loop_prompt.md"
        driver = installed_dir / "ta_loop.sh"
        handed_to_agent = capture
        log = worktree / "var" / "autonomy" / "loop.log"
        status = worktree / "var" / "autonomy" / "status.md"

        @staticmethod
        def run(**extra_env: str) -> subprocess.CompletedProcess[str]:
            env = {
                **os.environ,
                **_GIT_ENV,
                "PATH": f"{stub_dir}:{os.environ['PATH']}",
                "TA_LOOP_WORKTREE": str(worktree),
                "TA_LOOP_MAX": "1",
                "TA_LOOP_COOLDOWN": "0",
                **extra_env,
            }
            return subprocess.run(
                ["bash", str(installed_dir / "ta_loop.sh")],
                capture_output=True,
                text=True,
                timeout=120,
                env=env,
            )

    return Loop


def test_a_stale_installed_prompt_is_replaced_before_the_agent_sees_it(loop):
    loop.prompt.write_text(STALE_TEXT)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    # The load-bearing assertion: the agent ran the canonical text. Asserting
    # only on the file would pass even if the sync landed after the invocation.
    # `.strip()` because the driver passes `"$(cat "$PROMPT")"` and command
    # substitution eats trailing newlines — a property of the caller, not of
    # the sync, so pinning it here would be pinning the wrong thing.
    assert loop.handed_to_agent.read_text().strip() == CANONICAL_TEXT.strip()
    assert loop.prompt.read_text() == CANONICAL_TEXT
    assert "prompt RESYNCED" in loop.log.read_text()
    # status.md carries the CURRENT verdict, which by the end of the iteration is
    # "in sync" — the resync happened at startup and the per-iteration check then
    # found nothing to do. The hash is what a human needs there: it answers
    # "which prompt ran?", the question nobody could answer for seven days.
    canonical_sha = subprocess.run(
        ["shasum", "-a", "256", str(loop.prompt)], capture_output=True, text=True
    ).stdout.split()[0]
    assert f"- prompt: in sync with origin/main:{CANONICAL_PATH} ({canonical_sha[:12]})" in (
        loop.status.read_text()
    )


def test_an_absent_installed_prompt_is_bootstrapped_rather_than_fatal(loop):
    assert not loop.prompt.exists()

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert loop.handed_to_agent.read_text().strip() == CANONICAL_TEXT.strip()


def test_an_unreadable_canonical_runs_the_installed_copy_and_says_so(loop):
    """A missing ref is an environment fault, not a reason to halt the loop.

    But it must not be silent — running an unverified prompt is the state this
    whole mechanism exists to make visible.
    """
    loop.prompt.write_text(STALE_TEXT)

    result = loop.run(TA_LOOP_PROMPT_REF="refs/remotes/origin/does-not-exist")

    assert result.returncode == 0, result.stderr
    assert loop.handed_to_agent.read_text().strip() == STALE_TEXT.strip()
    assert "WARN prompt UNVERIFIED" in loop.log.read_text()
    assert "UNVERIFIED" in loop.status.read_text()


def test_an_in_sync_prompt_is_reported_with_its_hash(loop):
    loop.prompt.write_text(CANONICAL_TEXT)

    result = loop.run()

    assert result.returncode == 0, result.stderr
    log = loop.log.read_text()
    assert "prompt in sync" in log
    assert "RESYNCED" not in log


def test_a_stale_driver_warns_and_is_never_replaced(loop):
    """The prompt is auto-corrected; the driver deliberately is not.

    This process is already running the installed bytes, so replacing the file
    would change what a reader sees without changing what ran.
    """
    installed_before = loop.driver.read_bytes()
    tracked = loop.root / "scripts" / "autonomy" / "ta_loop.sh"
    tracked.write_text(installed_before.decode() + "\n# a later commit\n")
    _git(loop.root, "add", "-A")
    _git(loop.root, "commit", "-q", "-m", "driver moves on")
    _git(loop.root, "update-ref", "refs/remotes/origin/main", "HEAD")

    result = loop.run()

    assert result.returncode == 0, result.stderr
    assert "WARN driver STALE" in loop.log.read_text()
    assert loop.driver.read_bytes() == installed_before
