"""The recent evidence denominator and its anti-window-shopping boundary."""

from __future__ import annotations

from datetime import date

import pytest

from app.services.processes.param_metadata import MANUAL_TRIGGER_JOB_METADATA
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS, recent_evidence_window
from app.services.strategy_result import EVALUATION_WINDOW_END, HOLDOUT_BOUNDARY


def test_all_required_windows_are_named_and_inside_the_holdout_corpus() -> None:
    assert tuple(RECENT_EVIDENCE_WINDOWS) == (
        "primary-2022-plus",
        "rolling-36m",
        "rolling-24m",
        "year-2022",
        "year-2023",
        "year-2024",
        "year-2025",
        "year-2026-ytd",
    )
    assert all(item.required_for_allocation for item in RECENT_EVIDENCE_WINDOWS.values())
    assert all(item.window.start >= HOLDOUT_BOUNDARY for item in RECENT_EVIDENCE_WINDOWS.values())
    assert all(item.window.end <= EVALUATION_WINDOW_END for item in RECENT_EVIDENCE_WINDOWS.values())
    assert RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window.start == date(2022, 1, 1)


def test_raw_or_unknown_window_ids_are_refused() -> None:
    with pytest.raises(ValueError, match="unknown recent evidence window"):
        recent_evidence_window("2025-03-01:2025-04-01")


def test_manual_job_exposes_ids_but_never_raw_dates() -> None:
    metadata = {item.name: item for item in MANUAL_TRIGGER_JOB_METADATA["strategy_backtest_run"]}
    window = metadata["evidence_window"]
    assert window.field_type == "enum"
    assert window.enum_values == tuple(RECENT_EVIDENCE_WINDOWS)
