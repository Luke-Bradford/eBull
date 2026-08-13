#!/usr/bin/env python3
# CAVEMAN: FINRA ingest service own no tx. Tx belong to caller.
# CAVEMAN: Walk AST. Find `with conn.transaction():`. Bad. Yell.
# CAVEMAN: Docstring no bad — docstring is `Expr(Constant(str))`, not `With`.
#          AST naturally ignore.
# CAVEMAN: Manifest parsers (manifest_parsers/) do own tx legitimately.
#          They NOT in scope. We only look at app/services/finra_*_ingest.py.
#
# Run: uv run python scripts/check_caller_owned_tx.py [extra_path ...]
#
# Exit 0 = clean. Exit 1 = at least one bad `with conn.transaction():`
# found. Each violation prints as `path:line`.
#
# Rule (#1233 run-8-readiness-fixes Item 8 / Codex 1 narrowed scope):
# FINRA caller-owned ingest modules (`app/services/finra_*_ingest.py`)
# MUST NOT enter their own `with conn.transaction():` block — the
# manifest worker drives transaction lifecycle on their behalf. A second
# nested SAVEPOINT here breaks atomicity reasoning across the
# observations + manifest UPSERT pair.
#
# AST shape we flag (Codex 1 corrected from malformed spec draft):
#
#     with conn.transaction():        # NAME receiver
#     with self.conn.transaction():   # ATTRIBUTE receiver
#
# i.e. ast.With.items[*].context_expr ==
#     ast.Call(func=ast.Attribute(attr='transaction',
#                                 value=Name('conn')
#                                       | Attribute(value=Name('self'),
#                                                   attr='conn')))
#
# Anything else (e.g. `with other.transaction():`) is out of scope and
# silently allowed; we are not the general-purpose tx-shape linter.

from __future__ import annotations

import ast
import glob
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
FINRA_GLOB = "app/services/finra_*_ingest.py"


def _is_conn_receiver(node: ast.expr) -> bool:
    """Return True if `node` is ``conn`` or ``self.conn``."""
    # CAVEMAN: receiver `conn` — plain Name.
    if isinstance(node, ast.Name) and node.id == "conn":
        return True
    # CAVEMAN: receiver `self.conn` — Attribute on Name('self').
    if (
        isinstance(node, ast.Attribute)
        and node.attr == "conn"
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    ):
        return True
    return False


def _is_violating_call(ctx: ast.expr) -> bool:
    """Return True if `ctx` is ``conn.transaction()`` or ``self.conn.transaction()``."""
    if not isinstance(ctx, ast.Call):
        return False
    func = ctx.func
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr != "transaction":
        return False
    return _is_conn_receiver(func.value)


def _scan_file(path: Path) -> list[tuple[Path, int]]:
    """Return list of (path, line_number) violations within `path`."""
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        # CAVEMAN: file no parse → loud fail. Better noise than silence.
        print(f"{path}:{exc.lineno or 0}: SyntaxError: {exc.msg}", file=sys.stderr)
        return [(path, exc.lineno or 0)]

    violations: list[tuple[Path, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.With, ast.AsyncWith)):
            continue
        for item in node.items:
            if _is_violating_call(item.context_expr):
                violations.append((path, item.context_expr.lineno))
    return violations


def _resolve_targets(extra_paths: list[str]) -> list[Path]:
    """Glob the default FINRA scope + any extra explicit paths."""
    targets: list[Path] = []
    # CAVEMAN: default glob from REPO_ROOT. Operator can pass extra files
    # (used by tests).
    for match in sorted(glob.glob(str(REPO_ROOT / FINRA_GLOB))):
        targets.append(Path(match))
    for extra in extra_paths:
        p = Path(extra)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        targets.append(p)
    return targets


def main(argv: list[str]) -> int:
    targets = _resolve_targets(argv)
    if not targets:
        # CAVEMAN: no files? Spec promise was at least 2 FINRA files.
        # Empty scope is a regression — yell.
        print(
            f"::error::check_caller_owned_tx: no target files matched glob {FINRA_GLOB!r} under {REPO_ROOT}",
            file=sys.stderr,
        )
        return 1

    all_violations: list[tuple[Path, int]] = []
    for target in targets:
        if not target.exists():
            print(f"::error::check_caller_owned_tx: missing file {target}", file=sys.stderr)
            return 1
        all_violations.extend(_scan_file(target))

    if all_violations:
        for path, line in all_violations:
            # CAVEMAN: path printed relative to REPO_ROOT when possible —
            # easier to grep in CI logs.
            try:
                rel = path.relative_to(REPO_ROOT)
                display = str(rel)
            except ValueError:
                display = str(path)
            print(f"{display}:{line}: forbidden `with conn.transaction():` in caller-owned FINRA ingest module")
        print(
            f"\nFAIL: {len(all_violations)} caller-owned-tx violation(s). "
            "FINRA ingest services must NOT enter their own "
            "`with conn.transaction():` — the manifest worker drives the "
            "transaction lifecycle. See docs/proposals/etl/"
            "run-8-readiness-fixes.md §Item 8.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
