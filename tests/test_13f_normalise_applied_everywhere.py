"""Superset guard (#1566 / #1567).

Every per-filing path that consumes a parsed ``ThirteenFHolding`` list
MUST funnel it through ``normalise_13f_holdings`` (PRN + bad-quantity
filter, VALUE cutover, SUM aggregation) — otherwise a path silently
regresses to keep-first / unscaled / PRN-as-shares. Prevention-log
L1190 ("mirror every write-side guard across paths"). The bulk
COPY/SQL path applies the same corrections in SQL and so is verified
separately by ``tests/test_sec_13f_dataset_ingest.py``.

AST-level so a renamed import or a commented-out call still fails.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_APP = Path(__file__).resolve().parent.parent / "app"

# (module path, enclosing function that does the per-filing write)
_PER_FILING_SITES = [
    ("services/institutional_holdings.py", "_ingest_single_accession"),
    ("services/manifest_parsers/sec_13f_hr.py", "_parse_13f_hr"),
    ("services/rewash_filings.py", "_apply_13f_infotable"),
]


def _function_calls(path: Path, func_name: str) -> set[str]:
    """Return the set of called names inside the named function."""
    tree = ast.parse(path.read_text())
    target: ast.FunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            target = node
            break
    if target is None:
        raise AssertionError(f"{path.name}: function {func_name} not found")
    calls: set[str] = set()
    for node in ast.walk(target):
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name):
                calls.add(fn.id)
            elif isinstance(fn, ast.Attribute):
                calls.add(fn.attr)
    return calls


@pytest.mark.parametrize(("module", "func"), _PER_FILING_SITES)
def test_per_filing_path_applies_shared_normaliser(module: str, func: str) -> None:
    calls = _function_calls(_APP / module, func)
    assert "normalise_13f_holdings" in calls, (
        f"{module}::{func} must call normalise_13f_holdings — every 13F per-filing "
        "ingest path applies the same PRN/VALUE/SUM normalisation"
    )
    assert "merge_resolved_by_instrument" in calls, (
        f"{module}::{func} must call merge_resolved_by_instrument so two CUSIPs "
        "resolving to one instrument are summed, matching the bulk GROUP BY"
    )


def test_no_per_filing_path_keeps_first_via_setdefault() -> None:
    """The keep-first ``resolved_by_key.setdefault`` pattern (the #1567 bug)
    must not reappear in any per-filing path."""
    for module, _ in _PER_FILING_SITES:
        src = (_APP / module).read_text()
        assert "resolved_by_key" not in src, (
            f"{module}: keep-first `resolved_by_key.setdefault` undercounts "
            "multi-row 13F positions — use normalise_13f_holdings instead"
        )
