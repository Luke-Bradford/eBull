from __future__ import annotations

import ast
from dataclasses import asdict
from pathlib import Path

from scripts.benchmark_2772_synthetic_control_scale import BENCHMARK_ID, CASES, run_case


def test_the_scale_curve_is_fixed_and_outcome_free() -> None:
    assert BENCHMARK_ID == "synthetic-control-scale-v1"
    assert CASES == (
        ("wiring", 8, 16, 1),
        ("small", 32, 64, 2),
        ("medium", 64, 256, 3),
        ("scale", 128, 1024, 3),
    )
    records = run_case(*CASES[0])
    assert [record.engine for record in records] == ["reference", "shared_marks"]
    assert records[0].fixture_digest == records[1].fixture_digest
    assert all(record.reference_equivalent for record in records)
    forbidden = {"return", "sharpe", "profit", "passed", "drawdown", "outcome"}
    for record in records:
        assert not (forbidden & set(asdict(record)))


def test_backtest_operational_logs_do_not_publish_outcomes() -> None:
    tree = ast.parse(Path("app/services/backtest_run.py").read_text())
    messages: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute) or not node.args:
            continue
        if node.func.attr not in {"debug", "info", "warning", "error", "exception"}:
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Name) and receiver.id == "logger" and isinstance(node.args[0], ast.Constant):
            messages.append(str(node.args[0].value).lower())
    forbidden = ("sharpe", "return", "profit", "drawdown", "passed=", "refusals=", "position(s)")
    assert messages
    assert not [(message, token) for message in messages for token in forbidden if token in message]
