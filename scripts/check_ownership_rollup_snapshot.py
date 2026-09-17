#!/usr/bin/env python3
# CAVEMAN: rollup read many times. Reads must share one snapshot.
# CAVEMAN: Walk AST. Find call to get_ownership_rollup. Look up at
#          ancestors. No `with snapshot_read(...)` above it? Yell.
#
# Run: uv run python scripts/check_ownership_rollup_snapshot.py [--self-test]
#
# Exit 0 = clean. Exit 1 = at least one unsnapshotted render. Each
# violation prints as `path:line`.
#
# Rule (#2789): ``app.services.ownership_rollup.get_ownership_rollup``
# documents its own precondition —
#
#     The caller MUST already be inside :func:`app.db.snapshot.snapshot_read`
#     … The function does NOT open its own transaction.
#
# It issues many separate reads (the denominator, each ``ownership_*_current``
# slice, notices, families). Under the connection's default READ COMMITTED a
# concurrent ownership refresh can hand ONE render a denominator from one commit
# and a wedge from another. **Nothing raises.** The row is simply wrong, which is
# the failure mode that looks like a clean run — and the harnesses that decided
# #2230, #2385, #2386 and #2785 all rendered this way while the jobs daemon was
# writing ownership continuously on dev.
#
# The contract lived only in a docstring, so nothing enforced it and 8 of the 9
# scripts that render the rollup drifted off it. This is that enforcement.
#
# ⚠ Why AST and not "does the file import snapshot_read": the import proves
# nothing about the CALL. A file can import it, use it somewhere else, and still
# render outside a snapshot — which is exactly the shape a grep-based gate would
# wave through. The check here is lexical containment: the call node must have a
# ``with snapshot_read(...)`` ancestor.
#
# ⚠ A legitimate caller-owned case (the snapshot is opened by a caller one frame
# up, so it cannot be seen lexically) is allowed with an explicit reviewed
# marker on or just above the call:
#
#     # ownership-rollup-snapshot: caller-owned — <why>
#
# There are ZERO such sites today. The escape exists so a future legitimate case
# is a reviewed one-line comment rather than a reason to delete the gate.

from __future__ import annotations

import ast
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCOPES = ("app", "scripts")
TARGET = "get_ownership_rollup"
SNAPSHOT = "snapshot_read"
ESCAPE = "ownership-rollup-snapshot: caller-owned"


def _is_target_call(node: ast.AST) -> bool:
    """``get_ownership_rollup(...)`` or ``<mod>.get_ownership_rollup(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == TARGET
    if isinstance(func, ast.Attribute):
        return func.attr == TARGET
    return False


def _opens_snapshot(node: ast.With | ast.AsyncWith) -> bool:
    """True when any ``with`` item is a ``snapshot_read(...)`` call.

    ⚠ ``ast.AsyncWith`` is a DISTINCT node type, not a flavour of ``ast.With``:
    matching only the latter would flag an ``async with snapshot_read(...)``
    render as unsnapshotted and fail the build on correct code. (A plain
    ``with`` inside an ``async def`` — which is what the three production call
    sites use, ``snapshot_read`` being a sync context manager — is an
    ``ast.With`` and was always handled.) Caught by the review bot on PR #3151.
    """
    for item in node.items:
        expr = item.context_expr
        if not isinstance(expr, ast.Call):
            continue
        func = expr.func
        if isinstance(func, ast.Name) and func.id == SNAPSHOT:
            return True
        if isinstance(func, ast.Attribute) and func.attr == SNAPSHOT:
            return True
    return False


def _violations(source: str, path: str) -> list[str]:
    """``["path:line", ...]`` for every render not lexically inside a snapshot."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [f"{path}:{exc.lineno or 0}  (unparseable: {exc.msg})"]

    lines = source.splitlines()
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    out: list[str] = []
    for node in ast.walk(tree):
        if not _is_target_call(node):
            continue
        # The definition site itself is not a call; a call inside the defining
        # module would still need the snapshot, so nothing is excluded here.
        line = getattr(node, "lineno", 0)
        window = lines[max(0, line - 3) : line]
        if any(ESCAPE in text for text in window):
            continue
        cursor: ast.AST | None = node
        snapshotted = False
        while cursor is not None:
            if isinstance(cursor, ast.With | ast.AsyncWith) and _opens_snapshot(cursor):
                snapshotted = True
                break
            cursor = parents.get(cursor)
        if not snapshotted:
            out.append(f"{path}:{line}")
    return out


_GOOD = """
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import get_ownership_rollup

def render(conn, symbol, iid):
    with snapshot_read(conn):
        return get_ownership_rollup(conn, symbol, iid)
"""

_BAD = """
from app.services import ownership_rollup as orl

def render(conn, symbol, iid):
    return orl.get_ownership_rollup(conn, symbol, iid)
"""

_BAD_IMPORTS_BUT_DOES_NOT_USE = """
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import get_ownership_rollup

def other(conn):
    with snapshot_read(conn):
        pass

def render(conn, symbol, iid):
    return get_ownership_rollup(conn, symbol, iid)
"""

_ASYNC_GOOD = """
from app.db.snapshot import snapshot_read
from app.services.ownership_rollup import get_ownership_rollup

async def render(conn, symbol, iid):
    async with snapshot_read(conn):
        return get_ownership_rollup(conn, symbol, iid)
"""

_ASYNC_BAD = """
from app.services.ownership_rollup import get_ownership_rollup

async def render(conn, symbol, iid):
    return get_ownership_rollup(conn, symbol, iid)
"""

_SYNC_WITH_IN_ASYNC_DEF = """
from app.db.snapshot import snapshot_read
from app.services import ownership_rollup

async def render(conn, symbol, iid):
    with snapshot_read(conn):
        return ownership_rollup.get_ownership_rollup(conn, symbol, iid)
"""

_ESCAPED = """
from app.services.ownership_rollup import get_ownership_rollup

def render(conn, symbol, iid):
    # ownership-rollup-snapshot: caller-owned — the job opens it one frame up
    return get_ownership_rollup(conn, symbol, iid)
"""


def _self_test() -> int:
    """Name the input that would fail this check, then prove it does.

    A gate that has never been shown to fail is not evidence it passed.
    ``_BAD_IMPORTS_BUT_DOES_NOT_USE`` is the case an import-grep gate accepts
    and this one must not.
    """
    cases = [
        ("good", _GOOD, 0),
        ("bad", _BAD, 1),
        ("bad_imports_but_does_not_use", _BAD_IMPORTS_BUT_DOES_NOT_USE, 1),
        ("async_good", _ASYNC_GOOD, 0),
        ("async_bad", _ASYNC_BAD, 1),
        ("sync_with_in_async_def", _SYNC_WITH_IN_ASYNC_DEF, 0),
        ("escaped", _ESCAPED, 0),
    ]
    failed = 0
    for name, src, expected in cases:
        found = len(_violations(src, f"<{name}>"))
        status = "ok" if found == expected else "FAIL"
        if found != expected:
            failed += 1
        print(f"self-test {name:30s} expected={expected} found={found} {status}")
    return 1 if failed else 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return _self_test()

    violations: list[str] = []
    for scope in SCOPES:
        for path in sorted((REPO_ROOT / scope).rglob("*.py")):
            rel = path.relative_to(REPO_ROOT).as_posix()
            violations.extend(_violations(path.read_text(encoding="utf-8"), rel))

    if violations:
        print("get_ownership_rollup rendered OUTSIDE snapshot_read (#2789):", file=sys.stderr)
        for entry in violations:
            print(f"  {entry}", file=sys.stderr)
        print(
            "\nWrap the call: `with snapshot_read(conn): ...`. The reader issues many\n"
            "separate reads and does NOT open its own transaction, so without one\n"
            "REPEATABLE READ snapshot a concurrent refresh yields a torn render that\n"
            "raises nothing. See the function's docstring and #2789.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
