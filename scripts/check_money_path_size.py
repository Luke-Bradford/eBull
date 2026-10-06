"""Money-path size cap (#3613 item 5).

Fails when a change adds plus deletes more than ``MAX_CHURN`` lines across
the ``MONEY_PATH`` files. The remedy is splitting the change into PRs that
are each reviewed on their own; there is deliberately no override label or
body marker, because the loop that opens the PR would be the one applying it.

What this is, and is not: a churn cap on a named path inventory, under the
diff policy below. It does not bound total review volume (tests, SQL and
other app code are not counted) and it cannot see order-path behaviour moved
into a module outside the list. Protecting this file from a PR that edits it
is #3613 item 3 (CODEOWNERS), not this check.

Neither the cap nor the inventory has a published source rule; both are fixed
by construction:

- ``MONEY_PATH``: the four direct callers of the six broker-mutating methods
  (``_MUTATING`` in ``tests/test_unattended_broker_mutation_guard.py``), the
  executors that submit through them, and the guards, sandbox and allocator
  on the order path. A new order-path module must be added here.
- ``MAX_CHURN``: the ticket's own example. Its historical impact (how many
  past merges would have had to split) is reproduced by::

      for h in $(git log origin/main --first-parent --since=2026-08-06 --format=%H); do
        git diff --no-renames --numstat "$h^" "$h" -- <MONEY_PATH patterns> \
          | awk -v h="$h" '{s+=$1+$2} END {if (s) print s, h}'
      done | sort -n

Diff policy, pinned rather than inherited from local git config:
``--no-renames`` (a move is charged as delete plus add), the myers algorithm
(``diff.algorithm`` can change counts), no external diff or
textconv, ``-z`` numstat. A binary row has no line count and fails closed.
Each pattern must match at least one file at ``head``, so a typo or a deleted
file cannot silently drop coverage.

Usage: ``check_money_path_size.py [BASE HEAD]``. With no arguments, BASE is
``git merge-base origin/main HEAD`` and HEAD is ``HEAD`` (the pre-push hook).
CI passes the PR merge commit's first parent and the merge commit itself.
"""

from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

MAX_CHURN = 400

# ``:(top,glob)``: anchored at the repo root, and ``*`` never crosses ``/``.
MONEY_PATH: tuple[str, ...] = tuple(
    f":(top,glob){p}"
    for p in (
        "app/providers/implementations/etoro_broker.py",
        "app/providers/broker.py",
        "app/security/unattended_guard.py",
        "app/services/order_client.py",
        "app/services/execution_guard.py",
        "app/services/*executor*.py",
        "app/services/*window_b_release*.py",
        "app/services/strategy_position_manager.py",
        "app/services/strategy_capital_sandbox.py",
        "app/services/strategy_core_allocator.py",
        "app/services/strategy_engine_capital.py",
        "app/services/strategy_order_reconciliation.py",
        "app/services/strategy_control_plane.py",
        "app/services/strategy_core_broker_preflight.py",
        "app/services/engine_book_risk.py",
    )
)

_DIFF_POLICY = ("--no-renames", "--diff-algorithm=myers", "--no-ext-diff", "--no-textconv", "--numstat", "-z")


class CheckError(Exception):
    """The measurement itself failed; never read as zero churn."""


@dataclass(frozen=True)
class FileChurn:
    path: str
    added: int | None  # None = binary row
    deleted: int | None


def _git(args: list[str], cwd: Path, stdin: str | None = None) -> str:
    # A pre-push hook exports GIT_DIR; scrub it so ``cwd`` decides the repo.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    proc = subprocess.run(["git", *args], cwd=cwd, env=env, input=stdin, capture_output=True, text=True)
    if proc.returncode != 0:
        raise CheckError(f"git {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


def resolve_commit(ref: str, cwd: Path) -> str:
    try:
        return _git(["rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd).strip()
    except CheckError:
        # ``--quiet`` leaves stderr empty, so name the ref ourselves.
        raise CheckError(f"{ref!r} does not resolve to a commit") from None


def parse_numstat_z(raw: str) -> list[FileChurn]:
    """Parse ``git diff --numstat -z --no-renames`` output.

    Each record is ``added<TAB>deleted<TAB>path<NUL>``; a binary file has
    ``-`` for both counts.
    """
    rows: list[FileChurn] = []
    for record in raw.split("\0"):
        if not record:
            continue
        parts = record.split("\t", 2)
        if len(parts) != 3:
            raise CheckError(f"unparseable numstat record: {record!r}")
        added, deleted, path = parts
        if added == "-" and deleted == "-":
            rows.append(FileChurn(path, None, None))
        elif added.isascii() and added.isdigit() and deleted.isascii() and deleted.isdigit():
            rows.append(FileChurn(path, int(added), int(deleted)))
        else:
            raise CheckError(f"unparseable numstat counts: {record!r}")
    return rows


def unmatched_patterns(head: str, cwd: Path) -> list[str]:
    """Patterns that match no file at ``head``, using git's own pathspec engine."""
    # Empty stdin rather than /dev/null, which native Windows git cannot open.
    empty_tree = _git(["hash-object", "-t", "tree", "--stdin"], cwd, stdin="").strip()
    return [
        p
        for p in MONEY_PATH
        if not _git(["diff", "--no-renames", "--name-only", empty_tree, head, "--", p], cwd).strip()
    ]


def measure(base: str, head: str, cwd: Path) -> list[FileChurn]:
    return parse_numstat_z(_git(["diff", *_DIFF_POLICY, base, head, "--", *MONEY_PATH], cwd))


def evaluate(rows: list[FileChurn]) -> tuple[int, list[str]]:
    """Return (total churn, failure reasons). Empty reasons = pass."""
    reasons = [f"binary change has no line count: {r.path}" for r in rows if r.added is None]
    total = sum((r.added or 0) + (r.deleted or 0) for r in rows)
    if total > MAX_CHURN:
        reasons.append(f"{total} lines changed on the money path; the cap is {MAX_CHURN}")
    return total, reasons


def main(argv: list[str]) -> int:
    cwd = Path(_git(["rev-parse", "--show-toplevel"], Path.cwd()).strip())
    if len(argv) == 2:
        base_ref, head_ref = argv
    elif not argv:
        head_ref = "HEAD"
        base_ref = _git(["merge-base", resolve_commit("origin/main", cwd), "HEAD"], cwd).strip()
    else:
        print("usage: check_money_path_size.py [BASE HEAD]", file=sys.stderr)
        return 2
    base = resolve_commit(base_ref, cwd)
    head = resolve_commit(head_ref, cwd)

    missing = unmatched_patterns(head, cwd)
    if missing:
        print("ERROR: money-path patterns match no file at head:", file=sys.stderr)
        for p in missing:
            print(f"  - {p}", file=sys.stderr)
        return 1

    rows = measure(base, head, cwd)
    total, reasons = evaluate(rows)
    print(f"==> money-path size cap: {base[:12]}..{head[:12]}, added+deleted, --no-renames")
    for r in rows:
        counts = "binary" if r.added is None else f"+{r.added} -{r.deleted}"
        print(f"  {counts:>14}  {r.path}")
    print(f"  total {total} / cap {MAX_CHURN}")
    if reasons:
        for reason in reasons:
            print(f"ERROR: {reason}", file=sys.stderr)
        print("Split the change into separately reviewed PRs (#3613 item 5).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except CheckError as exc:
        print(f"ERROR: money-path size cap could not measure: {exc}", file=sys.stderr)
        sys.exit(1)
