#!/usr/bin/env python3
# CAVEMAN: pooled conn held whole request = bad if route also call out to
#          eToro/SEC over HTTP. Small pool + conn pinned across slow I/O =
#          block-then-PoolTimeout for next request. Walk AST. Yell.
#
# Run: uv run python scripts/check_pooled_conn_across_http.py [extra_path ...]
#
# Exit 0 = clean. Exit 1 = at least one route holds a pooled connection
# (via ``Depends(get_conn)``) AND references an external-provider HTTP
# entrypoint in its body. Each violation prints as ``path:line``.
#
# Rule (#1472 PR2 — connection-lifetime audit):
# ``app/db/__init__.py::get_conn`` is a FastAPI *generator* dependency: a
# route that declares ``conn = Depends(get_conn)`` holds that pooled
# connection for its ENTIRE handler body, including any outbound HTTP to
# eToro / SEC EDGAR / GLEIF. ``psycopg_pool`` is configured
# ``timeout=15, max_waiting=0`` — under the (shrunk) API pool a conn held
# across slow external I/O becomes a queue-stall (block-then-PoolTimeout)
# for other requests. So: a ``Depends(get_conn)`` route MUST NOT reach an
# external-provider HTTP client in its call graph while the conn is held.
#
# The sanctioned fix (prevention-log #267) is to DROP ``Depends(get_conn)``
# and drive get_conn by hand inside a bounded scope —
#     gen = get_conn(request); conn = next(gen)
#     try: ...reads... finally: gen.close()   # release BEFORE the call
# — which this guard does NOT flag (no ``Depends(get_conn)`` parameter).
#
# Detection: a FunctionDef/AsyncFunctionDef under ``app/api/`` whose
# signature has a parameter defaulting to ``Depends(get_conn)`` (positional
# or ``dependency=`` keyword) AND whose body DIRECTLY references any name in
# ``EXTERNAL_MARKERS`` (an external-provider HTTP client constructor /
# probe). Allowlisted (route, fn) pairs are the KNOWN-DEFERRED violations
# tracked in a follow-up issue — remove them when fixed, re-arming the guard.
#
# SCOPE / LIMITATION (Codex ckpt-2): this is a regression TRIPWIRE for the
# COMMON, DIRECT shape — it intentionally does NOT do transitive call-graph
# analysis (AST can't reliably resolve imports across modules). A
# ``Depends(get_conn)`` route that reaches an external client only through a
# helper it CALLS will NOT be flagged. The full request-call-graph audit
# that found V1-V4 was a one-time MANUAL trace (recorded in the #1472 PR2
# plan doc); this guard catches the direct regression that would otherwise
# silently reintroduce the pattern. Adding a marker for any new
# route-body-visible external client keeps the common case covered.

from __future__ import annotations

import ast
import glob
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
API_GLOB = "app/api/*.py"

# External-provider HTTP entrypoints reachable from a route body. A
# ``Depends(get_conn)`` route that references any of these constructs an
# external client (or a probe that does) while holding the pooled conn.
EXTERNAL_MARKERS: frozenset[str] = frozenset(
    {
        "EtoroMarketDataProvider",  # eToro REST market-data client
        "SecFilingsProvider",  # SEC EDGAR document fetcher
        "_probe_etoro",  # broker_credentials helper → httpx.Client
        "httpx",  # any direct httpx use in a route body
    }
)

# Per-route waivers, keyed by (relpath, function name). Empty: the two
# former entries (V3/V4 — get_instrument_8k_filing_body /
# get_instrument_business_sections) were the lazy-fill routes that held a
# session ``pg_advisory_lock`` on the pooled conn across the SEC fetch.
# #1492 reworked them to fetch-first (the service borrows + releases its
# own short-lived pool conns; the routes drive ``get_conn`` by hand and no
# longer take ``Depends(get_conn)``), so the tripwire is re-armed against
# both. Add a route here only with a tracking issue + a removal trigger.
ALLOWLIST: frozenset[tuple[str, str]] = frozenset()


def _is_depends_get_conn(node: ast.expr) -> bool:
    """Return True if `node` is ``Depends(get_conn)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    # Accept both ``Depends(...)`` and ``fastapi.Depends(...)``.
    is_depends = (isinstance(func, ast.Name) and func.id == "Depends") or (
        isinstance(func, ast.Attribute) and func.attr == "Depends"
    )
    if not is_depends:
        return False
    # Positional ``Depends(get_conn)`` and keyword ``Depends(dependency=get_conn)``.
    candidates: list[ast.expr] = list(node.args) + [kw.value for kw in node.keywords]
    for arg in candidates:
        if isinstance(arg, ast.Name) and arg.id == "get_conn":
            return True
        if isinstance(arg, ast.Attribute) and arg.attr == "get_conn":
            return True
    return False


def _holds_pooled_conn(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True if any parameter defaults to ``Depends(get_conn)``."""
    args = fn.args
    defaults: list[ast.expr] = [d for d in args.defaults]
    defaults += [d for d in args.kw_defaults if d is not None]
    return any(_is_depends_get_conn(d) for d in defaults)


def _references_external(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> int | None:
    """Return the line of the first external-marker reference, or None."""
    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and node.id in EXTERNAL_MARKERS:
            return node.lineno
        if isinstance(node, ast.Attribute) and node.attr in EXTERNAL_MARKERS:
            return node.lineno
    return None


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _scan_file(path: Path) -> list[tuple[str, int, str]]:
    """Return (relpath, line, fn_name) violations within `path`."""
    source = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        print(f"{path}:{exc.lineno or 0}: SyntaxError: {exc.msg}", file=sys.stderr)
        return [(_rel(path), exc.lineno or 0, "<syntax-error>")]

    rel = _rel(path)
    violations: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not _holds_pooled_conn(node):
            continue
        marker_line = _references_external(node)
        if marker_line is None:
            continue
        if (rel, node.name) in ALLOWLIST:
            continue
        violations.append((rel, marker_line, node.name))
    return violations


def _resolve_targets(extra_paths: list[str]) -> list[Path]:
    targets: list[Path] = [Path(m) for m in sorted(glob.glob(str(REPO_ROOT / API_GLOB)))]
    for extra in extra_paths:
        p = Path(extra)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        targets.append(p)
    return targets


def main(argv: list[str]) -> int:
    targets = _resolve_targets(argv)
    if not targets:
        print(
            f"::error::check_pooled_conn_across_http: no files matched {API_GLOB!r} under {REPO_ROOT}",
            file=sys.stderr,
        )
        return 1

    all_violations: list[tuple[str, int, str]] = []
    for target in targets:
        if not target.exists():
            print(f"::error::check_pooled_conn_across_http: missing file {target}", file=sys.stderr)
            return 1
        all_violations.extend(_scan_file(target))

    if all_violations:
        for rel, line, fn_name in all_violations:
            print(
                f"{rel}:{line}: route `{fn_name}` holds a pooled conn (Depends(get_conn)) "
                "across an external-provider HTTP call"
            )
        print(
            f"\nFAIL: {len(all_violations)} pooled-conn-across-HTTP violation(s). A route that "
            "takes `Depends(get_conn)` holds the pooled connection for its whole body — it must "
            "NOT call out to eToro/SEC while holding it (block-then-PoolTimeout under a small "
            "pool). Drop `Depends(get_conn)` and drive get_conn by hand in a bounded scope, "
            "releasing via `gen.close()` BEFORE the external call (prevention-log #267). See "
            "#1472 PR2 + docs/proposals/infra/2026-06-04-db-connection-discipline.md.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
