#!/usr/bin/env python3
"""Lint guard for ownership refresh writer MERGE column shape.

Pinned invariants (per archived spec
``docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md`` §5,
strengthened via #1256):

* **I.a (set-equality)** — UPDATE SET cols == diff-tuple LHS cols ∪ {refreshed_at}
* **I.b (LHS-RHS ordered equality)** — diff-tuple LHS cols (after stripping
  ``tgt.``) equals RHS cols (after stripping ``src.``), exact ordered name match
* **I.c (refreshed_at placement)** — ``refreshed_at`` appears exactly once in
  UPDATE SET; never in either diff-tuple span
* **I.d (uniqueness)** — no duplicate column names in any of the three spans
* **I.e (UPDATE assignment LHS==RHS)** — each non-``refreshed_at`` UPDATE SET
  pair must be ``col = src.col`` (LHS bare name equals RHS-after-``src.``-strip)

Invoked from ``scripts/check_ownership_refresh_writer_pattern.sh`` per-function
(7 single-instrument helpers + 7 batch helpers = 14 total).

CLI:
    uv run python scripts/_check_ownership_writer_columns.py \\
        --function refresh_institutions_current app/services/ownership_observations.py
    uv run python scripts/_check_ownership_writer_columns.py \\
        --function refresh_X --source-text "<text>"
    uv run python scripts/_check_ownership_writer_columns.py \\
        --coverage-report app/services/ownership_observations.py

Exit codes:
    0 — all invariants pass
    2 — any invariant fails (diagnostic on stderr)
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Function discovery
# ---------------------------------------------------------------------------

# Helpers in scope. Single-instrument helpers run per-instrument; batch helpers
# run across a list of instrument_ids. Both share the diff-aware MERGE shape.
_SINGLE_HELPERS: tuple[str, ...] = (
    "refresh_insiders_current",
    "refresh_institutions_current",
    "refresh_blockholders_current",
    "refresh_treasury_current",
    "refresh_def14a_current",
    "refresh_funds_current",
    "refresh_esop_current",
)
_BATCH_HELPERS: tuple[str, ...] = (
    "refresh_insiders_current_batch",
    "refresh_institutions_current_batch",
    "refresh_funds_current_batch",
    "refresh_blockholders_current_batch",
    "refresh_treasury_current_batch",
    "refresh_def14a_current_batch",
    "refresh_esop_current_batch",
)
ALL_HELPERS: tuple[str, ...] = _SINGLE_HELPERS + _BATCH_HELPERS


def _extract_function_body(source: str, function_name: str) -> str:
    """Return the body of ``function_name`` from ``source``.

    Searches for ``def <function_name>(`` and returns lines until the next
    top-level ``def`` or end of file. Caller doesn't need full AST; the
    MERGE block is embedded inside a triple-quoted SQL string within the
    function body and regex-extractable.
    """
    lines = source.splitlines()
    start_idx: int | None = None
    for i, line in enumerate(lines):
        if line.startswith(f"def {function_name}("):
            start_idx = i
            break
    if start_idx is None:
        raise SystemExit(
            f"FATAL: function '{function_name}' not found in source. "
            f"Available helpers in scope: {', '.join(ALL_HELPERS)}."
        )
    # Walk forward until next top-level def or EOF
    end_idx = len(lines)
    for j in range(start_idx + 1, len(lines)):
        if lines[j].startswith("def "):
            end_idx = j
            break
    return "\n".join(lines[start_idx:end_idx])


# ---------------------------------------------------------------------------
# Span extraction (MERGE column blocks)
# ---------------------------------------------------------------------------


def _extract_spans(body: str) -> tuple[str, str, str]:
    """Extract (diff_lhs_span, diff_rhs_span, update_set_span) from helper body.

    Boundaries (per the MERGE shape used in app/services/ownership_observations.py):

    * Diff LHS: between ``WHEN MATCHED AND (`` and ``) IS DISTINCT FROM (``
    * Diff RHS: between ``) IS DISTINCT FROM (`` and ``) THEN UPDATE SET``
    * UPDATE SET: between ``THEN UPDATE SET`` and ``WHEN NOT MATCHED BY TARGET``

    Raises SystemExit(2) on missing boundary.
    """

    def _between(start_marker: str, end_marker: str) -> str:
        s = body.find(start_marker)
        if s == -1:
            print(
                f"FAIL: cannot locate '{start_marker}' in function body. MERGE shape changed?",
                file=sys.stderr,
            )
            sys.exit(2)
        s += len(start_marker)
        e = body.find(end_marker, s)
        if e == -1:
            print(
                f"FAIL: cannot locate '{end_marker}' after '{start_marker}'.",
                file=sys.stderr,
            )
            sys.exit(2)
        return body[s:e]

    diff_lhs = _between("WHEN MATCHED AND (", ") IS DISTINCT FROM (")
    diff_rhs = _between(") IS DISTINCT FROM (", ") THEN UPDATE SET")
    update_set = _between("THEN UPDATE SET", "WHEN NOT MATCHED BY TARGET")
    return diff_lhs, diff_rhs, update_set


# ---------------------------------------------------------------------------
# Shape gates (prefix-asymmetric per Codex iter-2 BLOCKING-1)
# ---------------------------------------------------------------------------

# Diff-tuple line: comma-separated <prefix>.<col> tokens, optional trailing
# comma, no inline comments, no expressions. Prefix is ``tgt`` for LHS, ``src``
# for RHS. Empty/whitespace lines allowed.
_DIFF_LHS_LINE = re.compile(r"^\s*tgt\.\w+(\s*,\s*tgt\.\w+)*\s*,?\s*$")
_DIFF_RHS_LINE = re.compile(r"^\s*src\.\w+(\s*,\s*src\.\w+)*\s*,?\s*$")

# UPDATE SET line: either ``col = src.col[,]`` OR terminal ``refreshed_at = now()[,]``
_UPDATE_SET_COL_LINE = re.compile(r"^\s*(\w+)\s*=\s*src\.(\w+)\s*,?\s*$")
_UPDATE_SET_REFRESHED_LINE = re.compile(r"^\s*refreshed_at\s*=\s*now\(\)\s*,?\s*$")


def _shape_gate(span: str, line_re: re.Pattern[str], span_name: str) -> None:
    """Assert every non-blank line in span matches the expected pattern."""
    for lineno, raw in enumerate(span.splitlines(), start=1):
        if not raw.strip():
            continue
        if not line_re.match(raw):
            print(
                f"FAIL: shape violation in {span_name} (line {lineno}): {raw!r}\n  expected pattern: {line_re.pattern}",
                file=sys.stderr,
            )
            sys.exit(2)


def _shape_gate_update_set(span: str) -> None:
    """UPDATE SET allows two line shapes; reject anything else."""
    for lineno, raw in enumerate(span.splitlines(), start=1):
        if not raw.strip():
            continue
        if _UPDATE_SET_REFRESHED_LINE.match(raw):
            continue
        if _UPDATE_SET_COL_LINE.match(raw):
            continue
        print(
            f"FAIL: shape violation in UPDATE SET span (line {lineno}): {raw!r}\n"
            f"  expected either 'col = src.col[,]' or 'refreshed_at = now()[,]'",
            file=sys.stderr,
        )
        sys.exit(2)


# ---------------------------------------------------------------------------
# Tokenization
# ---------------------------------------------------------------------------


def _tokenize_diff_span(span: str, prefix: str) -> list[str]:
    """Return ordered column names from a diff-tuple span, stripping prefix."""
    cols: list[str] = []
    for raw in span.splitlines():
        line = raw.strip().rstrip(",")
        if not line:
            continue
        for tok in line.split(","):
            tok = tok.strip()
            if not tok:
                continue
            if not tok.startswith(f"{prefix}."):
                print(
                    f"FAIL: diff token {tok!r} missing prefix {prefix!r}.",
                    file=sys.stderr,
                )
                sys.exit(2)
            cols.append(tok[len(prefix) + 1 :])
    return cols


def _tokenize_update_set_pairs(span: str) -> list[tuple[str, str]]:
    """Return ordered (lhs_col, rhs_col) pairs from UPDATE SET span.

    ``refreshed_at = now()`` represented as ``("refreshed_at", "__NOW__")`` so
    callers can distinguish it from regular ``col = src.col`` pairs.
    """
    pairs: list[tuple[str, str]] = []
    for raw in span.splitlines():
        if not raw.strip():
            continue
        m_ref = _UPDATE_SET_REFRESHED_LINE.match(raw)
        if m_ref:
            pairs.append(("refreshed_at", "__NOW__"))
            continue
        m_col = _UPDATE_SET_COL_LINE.match(raw)
        if m_col:
            pairs.append((m_col.group(1), m_col.group(2)))
            continue
        # shape-gate should have caught this; defensive
        print(f"FAIL: unparseable UPDATE SET line: {raw!r}", file=sys.stderr)
        sys.exit(2)
    return pairs


# ---------------------------------------------------------------------------
# Invariants
# ---------------------------------------------------------------------------


def check_invariants(function_name: str, source: str) -> None:
    """Run all 5 invariant checks (I.a-I.e); SystemExit(2) on failure."""
    body = _extract_function_body(source, function_name)
    diff_lhs_span, diff_rhs_span, update_set_span = _extract_spans(body)

    # Shape gates first — give actionable diagnostic before semantic checks
    _shape_gate(diff_lhs_span, _DIFF_LHS_LINE, "diff-tuple LHS")
    _shape_gate(diff_rhs_span, _DIFF_RHS_LINE, "diff-tuple RHS")
    _shape_gate_update_set(update_set_span)

    diff_lhs_cols = _tokenize_diff_span(diff_lhs_span, "tgt")
    diff_rhs_cols = _tokenize_diff_span(diff_rhs_span, "src")
    update_set_pairs = _tokenize_update_set_pairs(update_set_span)
    update_set_lhs = [lhs for lhs, _ in update_set_pairs]

    failures: list[str] = []

    # I.a — set-equality (UPDATE SET cols modulo refreshed_at == diff LHS cols)
    update_set_modulo = set(update_set_lhs) - {"refreshed_at"}
    if update_set_modulo != set(diff_lhs_cols):
        only_update = update_set_modulo - set(diff_lhs_cols)
        only_diff = set(diff_lhs_cols) - update_set_modulo
        failures.append(
            f"I.a (set-equality): UPDATE SET \\ {{refreshed_at}} != diff LHS.\n"
            f"  only in UPDATE SET: {sorted(only_update) or 'none'}\n"
            f"  only in diff LHS:   {sorted(only_diff) or 'none'}"
        )

    # I.b — LHS-RHS ordered equality
    if diff_lhs_cols != diff_rhs_cols:
        failures.append(f"I.b (LHS-RHS ordered): diff LHS != diff RHS.\n  LHS: {diff_lhs_cols}\n  RHS: {diff_rhs_cols}")

    # I.c — refreshed_at exactly-once in UPDATE SET, never in diff
    ref_count = update_set_lhs.count("refreshed_at")
    if ref_count != 1:
        failures.append(
            f"I.c (refreshed_at placement): UPDATE SET has refreshed_at {ref_count} times; expected exactly 1"
        )
    if "refreshed_at" in diff_lhs_cols:
        failures.append("I.c: refreshed_at must NOT appear in diff LHS")
    if "refreshed_at" in diff_rhs_cols:
        failures.append("I.c: refreshed_at must NOT appear in diff RHS")

    # I.d — uniqueness
    for name, cols in (
        ("UPDATE SET LHS", update_set_lhs),
        ("diff LHS", diff_lhs_cols),
        ("diff RHS", diff_rhs_cols),
    ):
        if len(cols) != len(set(cols)):
            dupes = sorted({c for c in cols if cols.count(c) > 1})
            failures.append(f"I.d (uniqueness): {name} has duplicate cols: {dupes}")

    # I.e — UPDATE assignment LHS==RHS for every non-refreshed_at pair
    for lhs, rhs in update_set_pairs:
        if lhs == "refreshed_at" and rhs == "__NOW__":
            continue
        if lhs != rhs:
            failures.append(f"I.e (assignment LHS==RHS): UPDATE SET pair {lhs!r} = src.{rhs!r} has mismatched names")

    if failures:
        print(
            f"FAIL: function {function_name} violates invariant I:",
            file=sys.stderr,
        )
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Coverage report
# ---------------------------------------------------------------------------


def coverage_report(source: str) -> None:
    """Print every helper in ``ALL_HELPERS`` found in source; exit 0 if all 14
    present + invariant-checked OK, exit 2 otherwise.

    Shell wrapper greps the final line for ``14 functions covered`` to defend
    against silent double-checking (Codex iter-3 BLOCKING-1).
    """
    found: list[str] = []
    missing: list[str] = []
    failed: list[str] = []
    for fn in ALL_HELPERS:
        if f"def {fn}(" in source:
            found.append(fn)
            try:
                check_invariants(fn, source)
            except SystemExit as exc:
                if exc.code != 0:
                    failed.append(fn)
        else:
            missing.append(fn)

    print("coverage report:")
    for fn in found:
        marker = "FAIL" if fn in failed else "PASS"
        print(f"  [{marker}] {fn}")
    for fn in missing:
        print(f"  [MISS] {fn}")
    print(f"{len(found)} functions covered (expected {len(ALL_HELPERS)})")
    if missing or failed:
        sys.exit(2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_ownership_writer_columns",
        description=(
            "Lint ownership refresh writer MERGE column shape (#1256). "
            "Pins 5-axis invariant I per archived spec "
            "docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md §5."
        ),
    )
    parser.add_argument(
        "--function",
        type=str,
        help=(
            "Exact function name to check (e.g. refresh_institutions_current, "
            "refresh_insiders_current_batch). Required unless --coverage-report."
        ),
    )
    parser.add_argument(
        "--source-text",
        type=str,
        help="Source code text (string). For tests; mutually exclusive with source_file.",
    )
    parser.add_argument(
        "--coverage-report",
        action="store_true",
        help="Print coverage report of all helpers in ALL_HELPERS found in source.",
    )
    parser.add_argument(
        "source_file",
        nargs="?",
        type=str,
        help="Path to source file. Required unless --source-text is given.",
    )
    args = parser.parse_args(argv)

    if args.source_text is not None and args.source_file:
        parser.error("--source-text and source_file are mutually exclusive")
    if args.source_text is None and not args.source_file:
        parser.error("either source_file or --source-text required")

    source = args.source_text if args.source_text is not None else Path(args.source_file).read_text()

    if args.coverage_report:
        coverage_report(source)
        return 0

    if not args.function:
        parser.error("--function required (or use --coverage-report)")

    check_invariants(args.function, source)
    print(f"PASS: function {args.function} satisfies invariant I (axes a-e)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
