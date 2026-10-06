"""#2645 — refuse broker state mutation from an unattended linked worktree.

The unattended autonomy loop runs in a `git worktree`, not the operator's
checkout, and is forbidden by prompt from executing trades. Its prompt used to
claim a mechanical layer beneath that prohibition — "the loop is run with NO
broker credentials configured, so the order client fails closed". That was
false: `.env` is absent from the worktree, but the database-URL setting defaults
to the shared dev cluster that holds `broker_credentials`, and the decryption
root secret resolves through `platformdirs.user_data_dir("eBull")` to a
machine-wide OS directory. Neither half of the credential path is repo-local, so
a worktree reaches both.

This module is the layer that claim described. It is deliberately an
**execution-time** refusal raised before any network I/O, not a lint or a test:
the near miss it exists for (#2644's preflight probe) ran long before anything
was pushed, so a push-time gate could not have stopped it.

Detection is the git worktree layout itself, which needs no cooperation from the
separate autonomy-engine repo and no environment variable anyone has to
remember to set. A linked worktree's `.git` is a FILE containing a `gitdir:`
pointer; the main checkout's `.git` is a DIRECTORY. Verified 2026-08-13:
the operator's main checkout has a `.git` DIRECTORY and is listed first by
`git worktree list`, so the operator's trading checkout cannot trip this.

⚠ ONLY POSITIVE DETECTION REFUSES. A checkout with no `.git` at all (an
installed package, a container image) is NOT treated as unattended. The guard
exists to stop one specific environment from trading; making it also fail in
environments it cannot recognise would add a new way for the operator's own
order path to break, in exactly the places this can never be exercised.

⚠ THIS IS AN ACCIDENT CONTROL, NOT AN ADVERSARIAL ONE. It lives in the repo the
loop can edit, so it constrains a confused run, not a determined one. The
prohibition in `.autonomy/hard_rules.md` remains the first layer.

#3614 — ENTRIES ALSO REFUSE UNMERGED CODE. The jobs daemon's `dev_reload` restarts its
child on any `app/**` mtime from whatever the main checkout holds, so a branch checked out
there runs unreviewed code against the broker. `refuse_entry_if_unmerged_code` refuses an
order that OPENS exposure unless the checkout's tracked files are unmodified and HEAD is
contained in `origin/main`. Containment, not equality: a fetch advances `origin/main` before
anyone re-detaches the checkout, and merged-but-older code is reviewed code. Untracked files
are ignored because tracked code cannot import one without a tracked edit, which is refused.
Exits, closes and SL/TP edits never call it: "Exits and protective actions are never blocked"
(`.claude/CLAUDE.md`). As above, only a positively detected git checkout is checked; one whose
git state cannot be read refuses.

⚠⚠ WHAT THIS DELIBERATELY DOES NOT DO: distinguish the autonomy loop from any
other linked worktree. There were 22 on this box on 2026-08-13, and an attended
operator running a broker mutation from one of them is refused too. Raised as a
P1 at Codex checkpoint 2 and kept on purpose, for two reasons.

First, the repo already routes attended broker work away from worktrees:
`.claude/CLAUDE.md` line 151 — "work that must be exercised against the running
dev stack, which serves `~/Dev/eBull` only. Branch in the main checkout for the
dev-verify step". The dev API and vite serve the main checkout, so the scenario
the finding describes is one the working rules already send elsewhere.

Second, the error costs are asymmetric. Over-refusing costs an attended operator
one loud, actionable message naming the fix ("run it from the main checkout").
Under-refusing costs an unattended trade, which is the entire subject of this
module. A narrower signal — a TTY probe, a process-name match, a path allow-list
— buys precision in the direction that does not matter and adds a way to be
wrong in the direction that does. Widen this only with a signal that is strictly
more specific AND cannot be absent when the loop runs.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

#: Repo root: this file is `<root>/app/security/unattended_guard.py`.
_REPO_ROOT = Path(__file__).resolve().parents[2]


class BrokerRefusedBeforeIo(RuntimeError):
    """A broker mutation refused before any network I/O: the request provably never left."""


class UnattendedExecutionRefused(BrokerRefusedBeforeIo):
    """A broker state mutation was attempted from an unattended worktree."""


class UnmergedCodeRefused(BrokerRefusedBeforeIo):
    """A broker entry was attempted from a checkout running code not merged to origin/main (#3614)."""


def is_linked_worktree(repo_root: Path | None = None) -> bool:
    """True when `repo_root` is a linked `git worktree`, not the main checkout.

    Takes the root as an argument purely so the two branches are testable
    without a real second checkout. There is deliberately NO environment
    override: a documented escape hatch is how a guard becomes decorative.
    """
    root = _REPO_ROOT if repo_root is None else repo_root
    return (root / ".git").is_file()


def refuse_broker_mutation_if_unattended(operation: str) -> None:
    """Raise if a broker state mutation is being attempted from a worktree.

    Called at the top of every `BrokerProvider` method that changes order or
    position state at the broker, BEFORE credentials are used or a request is
    built. `tests/test_unattended_broker_mutation_guard.py` asserts that
    every such method calls this, so a sixth mutating method cannot be added
    without either the call or a deliberate exemption.
    """
    if not is_linked_worktree():
        return
    raise UnattendedExecutionRefused(
        f"refusing {operation!r}: this checkout is a linked git worktree, which is where the "
        "unattended autonomy loop runs, and unattended runs must never execute, close or amend "
        "a position — demo fills are still persisted writes (#2645). Run it from the operator's "
        "main checkout."
    )


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, timeout=10, check=False)


def unmerged_code_reason(repo_root: Path | None = None) -> str | None:
    """Why the checkout is not running merged code, or None when it is (or is not a git checkout)."""
    root = _REPO_ROOT if repo_root is None else repo_root
    if not (root / ".git").exists():
        return None
    try:
        status = _git(root, "status", "--porcelain", "--untracked-files=no")
        if status.returncode != 0:
            return f"git status failed: {status.stderr.strip()}"
        if status.stdout.strip():
            return f"tracked files are modified: {[line[3:] for line in status.stdout.splitlines()][:3]}"
        contained = _git(root, "merge-base", "--is-ancestor", "HEAD", "origin/main")
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"git state unreadable: {exc}"
    if contained.returncode == 1:
        head = _git(root, "rev-parse", "--short", "HEAD").stdout.strip()
        return f"HEAD {head} is not on origin/main"
    if contained.returncode != 0:
        return f"git merge-base failed: {contained.stderr.strip()}"
    return None


def refuse_entry_if_unmerged_code(operation: str) -> None:
    """Raise if an order that opens exposure would run from unmerged or modified code (#3614).

    Called at the top of every `EtoroBrokerProvider` entry method, after
    `refuse_broker_mutation_if_unattended`, and by the strategy executors BEFORE they commit
    their entered-marker, so a refusal there is the provably-pre-broker case rather than an
    authority that "may have reached the broker". Never from an exit, close or SL/TP edit.
    """
    reason = unmerged_code_reason()
    if reason is None:
        return
    raise UnmergedCodeRefused(
        f"refusing {operation!r}: {reason}. Entries run only from reviewed code — detach the checkout "
        "at origin/main with a clean tracked tree (#3614). Exits and SL/TP edits are not affected."
    )
