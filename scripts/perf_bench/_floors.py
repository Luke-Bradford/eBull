"""Shared floor loader for the perf-bench harness + lint.

Single source of truth: scripts/perf_bench/floors.yaml. The master plan
table at docs/proposals/etl/bootstrap-sub-1h-plan.md §4 is a doc mirror;
drift is asserted by
tests/scripts/test_perf_bench_lint.py::test_14_floors_yaml_matches_master_plan.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml


def _floors_yaml_path() -> Path:
    override = os.environ.get("PERF_BENCH_FLOORS_YAML_OVERRIDE")
    if override:
        return Path(override)
    return Path(__file__).parent / "floors.yaml"


def load_floors() -> dict[str, int]:
    """Return ``{table_name: min_rows}``. Raises on missing or malformed yaml."""
    path = _floors_yaml_path()
    if not path.exists():
        raise FileNotFoundError(f"missing {path}")
    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: expected top-level mapping, got {type(raw).__name__}")
    out: dict[str, int] = {}
    for table, floor in raw.items():
        if not isinstance(table, str):
            raise ValueError(f"{path}: non-string key {table!r}")
        if not isinstance(floor, int) or isinstance(floor, bool) or floor <= 0:
            raise ValueError(f"{path}: floor for {table!r} must be positive int, got {floor!r}")
        out[table] = floor
    return out
