"""Bounded, hook-safe reads of the checkout's build identity (#3119).

Modelled on ``app.jobs.dev_reload.running_commit`` (#2666), which solved the
same problem for the jobs child and carries the trap this module must not
re-learn: a git hook exports ``GIT_DIR`` (and friends) into everything it runs,
so an un-scrubbed ``git rev-parse`` resolves against the HOOK's repository
rather than the intended checkout (#2658). Every call here scrubs ``GIT_*``.

``running_commit`` is deliberately NOT refactored to delegate here yet — it is
imported by the jobs supervisor, which cannot reload itself, so changing it
costs a ``launchctl kickstart`` that #2833's in-flight hourly observation
buckets do not need. Two-line follow-up once that verdict lands.

Nothing in this module raises. A missing git, a non-repo checkout, a slow disk
or a hook-poisoned environment all degrade to ``None``. Build identity is an
observability signal; it must never be able to fail a boot.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

# ``app/system/git_identity.py`` -> ``app/system`` -> ``app`` -> repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]

# The directory uvicorn is told to watch (``--reload-dir app``). The staleness
# comparison is scoped to this tree and not to HEAD, because a docs-only or
# test-only commit moves HEAD while correctly producing no reload.
WATCHED_SUBTREE = "app"

_GIT_TIMEOUT_S = 5.0

# py3.14 (PEP 758) accepts an unparenthesised tuple here, but ``ruff format``
# strips the parens and the bare form is easy to misread — bind it instead.
_GIT_FAILURES = (OSError, subprocess.SubprocessError)


def _git(*args: str) -> str | None:
    """Run ``git *args`` in the repo root. ``None`` means the command FAILED.

    ``GIT_*`` is scrubbed from the child environment for the #2658 reason
    above.

    ⚠ Success with empty output returns ``""``, not ``None``. The distinction
    is load-bearing for ``git status --porcelain``, where empty output IS the
    answer ("clean") — collapsing the two made a timed-out status read report a
    dirty checkout as clean, which is the failure direction that matters.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_S,
            env=env,
            check=False,
        )
    except _GIT_FAILURES:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def head_commit() -> str | None:
    """The full ``HEAD`` sha of the checkout this module was loaded from."""
    return _git("rev-parse", "HEAD") or None


def app_tree_hash() -> str | None:
    """The tree hash of the watched subtree, or ``None``.

    This — not ``HEAD`` — is the staleness comparand. It changes exactly when
    the tree uvicorn watches changes, so it does not false-positive on the
    docs/spec/test commits that make up most of this repo's history.
    """
    return _git("rev-parse", f"HEAD:{WATCHED_SUBTREE}") or None


def is_dirty() -> bool | None:
    """Whether the checkout has uncommitted changes, or ``None`` if unknown.

    Carried so that a worker running a dev-verify branch with local edits is
    visibly "identity is a proxy here" rather than silently reported as fresh.

    ⚠ ``None`` is NOT ``False``. A ``git status`` that times out tells you
    nothing about the working tree, and reporting that as clean would hide the
    one case this field exists to expose.
    """
    out = _git("status", "--porcelain")
    return None if out is None else bool(out)
