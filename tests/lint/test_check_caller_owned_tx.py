"""Acceptance tests for ``scripts/check_caller_owned_tx.py``.

Spec: docs/proposals/etl/run-8-readiness-fixes.md §Item 8.

The lint guard enforces that caller-owned FINRA ingest modules under
``app/services/finra_*_ingest.py`` do NOT enter their own
``with conn.transaction():`` block. The manifest worker owns the
transaction lifecycle for those modules; a nested SAVEPOINT here breaks
atomicity reasoning across the observations + manifest UPSERT pair.

Codex 1 narrowed the original scope: manifest parsers under
``app/services/manifest_parsers/`` LEGITIMATELY use
``with conn.transaction():`` (different transaction-ownership contract)
and MUST NOT be touched by this guard. The FINRA ingest module
docstrings themselves describe the rule — so the guard MUST be
AST-aware (string literals are ``Expr(Constant)`` nodes, not ``With``
nodes; a grep would false-positive on the docstring).

These tests pin all four properties:

1. Positive — current FINRA ingest code is clean (exit 0).
2. Negative — synthetic violator fixture trips the guard (exit 1 +
   path:line printed on stdout).
3. Docstring-only — a file with the forbidden phrase ONLY inside
   docstrings is silent (exit 0). Pins the AST-aware property.
4. Manifest parsers are NOT in scope — the default glob never matches
   ``app/services/manifest_parsers/`` even though those files
   legitimately use ``with conn.transaction():``.
"""

from __future__ import annotations

import ast
import glob
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT_PY = REPO_ROOT / "scripts" / "check_caller_owned_tx.py"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "lint"
VIOLATOR_FIXTURE = FIXTURE_DIR / "violating_finra_ingest.py"
DOCSTRING_FIXTURE = FIXTURE_DIR / "docstring_only.py"


def _run(*extra_paths: str | Path) -> subprocess.CompletedProcess[str]:
    """Invoke the lint script as a subprocess; return CompletedProcess."""
    cmd = [sys.executable, str(SCRIPT_PY)] + [str(p) for p in extra_paths]
    return subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )


# ---------------------------------------------------------------------
# 1. Positive — current FINRA ingest code is clean
# ---------------------------------------------------------------------
def test_current_finra_ingest_clean() -> None:
    """Running the script against the real codebase must exit 0.

    No extra paths — the script's default glob covers
    ``app/services/finra_*_ingest.py`` from REPO_ROOT.
    """
    result = _run()
    assert result.returncode == 0, (
        f"Expected exit 0 on clean current code, got {result.returncode}.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    # Empty stdout: no violation lines printed.
    assert result.stdout == "", f"Unexpected stdout: {result.stdout!r}"


# ---------------------------------------------------------------------
# 2. Negative — synthetic violator fixture trips the guard
# ---------------------------------------------------------------------
def test_violating_fixture_trips_guard() -> None:
    """A file containing a real ``with conn.transaction():`` block must
    exit 1 and print path:line on stdout."""
    # Defensive: confirm the fixture parses to an ast.With with the
    # expected shape before relying on it. If the fixture is ever
    # accidentally rewritten, this test would otherwise silently degrade.
    source = VIOLATOR_FIXTURE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    with_nodes = [n for n in ast.walk(tree) if isinstance(n, ast.With)]
    assert with_nodes, "Fixture lost its `with conn.transaction():` block — restore it before running this test."

    result = _run(VIOLATOR_FIXTURE)
    assert result.returncode == 1, (
        f"Expected exit 1 on violator fixture, got {result.returncode}.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    # Violation line printed on stdout in `path:line:` form. The script
    # prints relative path when possible; the fixture is inside REPO_ROOT
    # so the path renders as ``tests/fixtures/lint/violating_finra_ingest.py``.
    assert "violating_finra_ingest.py" in result.stdout, f"Expected violator filename in stdout, got: {result.stdout!r}"
    assert "forbidden" in result.stdout, f"Expected 'forbidden' marker in stdout, got: {result.stdout!r}"


# ---------------------------------------------------------------------
# 3. Docstring-only mention must be silent (AST-aware property)
# ---------------------------------------------------------------------
def test_docstring_only_mention_ignored() -> None:
    """A file mentioning ``with conn.transaction():`` only inside
    docstrings must NOT trip the guard.

    This is the property the spec demands: grep can't distinguish a
    docstring from a real ``With`` node, but AST naturally treats
    docstrings as ``Expr(Constant(str))`` and never visits them as
    ``With`` nodes.
    """
    # Defensive: confirm the fixture has zero With nodes.
    source = DOCSTRING_FIXTURE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    with_nodes = [n for n in ast.walk(tree) if isinstance(n, ast.With)]
    assert not with_nodes, "Fixture grew an ast.With node — restore docstring-only shape before running this test."

    result = _run(DOCSTRING_FIXTURE)
    assert result.returncode == 0, (
        f"Expected exit 0 on docstring-only fixture, got {result.returncode}.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert result.stdout == "", f"Unexpected stdout: {result.stdout!r}"


# ---------------------------------------------------------------------
# 4. Manifest parsers are NOT in scope
# ---------------------------------------------------------------------
def test_manifest_parsers_not_in_default_scope() -> None:
    """The default glob ``app/services/finra_*_ingest.py`` MUST NOT
    match anything under ``app/services/manifest_parsers/``.

    This is the Codex 1 scope correction: manifest parsers legitimately
    use ``with conn.transaction():`` (different transaction-ownership
    contract) and would emit dozens of false positives if the guard
    ever expanded its default glob.
    """
    # Probe the same glob the script uses.
    matches = sorted(glob.glob(str(REPO_ROOT / "app/services/finra_*_ingest.py")))
    # Confirm at least the two known FINRA ingest files are present —
    # otherwise the positive test in (1) would have caught nothing.
    assert any("finra_short_interest_ingest.py" in m for m in matches), matches
    assert any("finra_regsho_ingest.py" in m for m in matches), matches
    # None of the matches may be under manifest_parsers/.
    for m in matches:
        assert "manifest_parsers" not in m, f"Default scope must not include manifest parsers, got: {m}"

    # Sanity-check: manifest parsers DO contain `with conn.transaction():`
    # (the legitimate use we are protecting from this guard). If this
    # assertion ever breaks, the property the guard guards has shifted
    # and the scope rule should be re-examined.
    parser_glob = sorted(glob.glob(str(REPO_ROOT / "app/services/manifest_parsers/*.py")))
    legitimate_uses_found = False
    for parser_path in parser_glob:
        src = Path(parser_path).read_text(encoding="utf-8")
        try:
            t = ast.parse(src)
        except SyntaxError:
            continue
        for node in ast.walk(t):
            if not isinstance(node, ast.With):
                continue
            for item in node.items:
                ctx = item.context_expr
                if isinstance(ctx, ast.Call) and isinstance(ctx.func, ast.Attribute) and ctx.func.attr == "transaction":
                    legitimate_uses_found = True
                    break
            if legitimate_uses_found:
                break
        if legitimate_uses_found:
            break
    assert legitimate_uses_found, (
        "Expected at least one legitimate `with ...transaction():` in "
        "app/services/manifest_parsers/ — the scope-exclusion premise "
        "no longer holds. Re-examine whether this guard still needs to "
        "exclude that directory."
    )
