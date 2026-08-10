"""The recent evidence denominator and its anti-window-shopping boundary."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.api.strategies import _evidence_window_counts
from app.services.processes.param_metadata import MANUAL_TRIGGER_JOB_METADATA
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS, recent_evidence_window
from app.services.strategy_result import EVALUATION_WINDOW_END, HOLDOUT_BOUNDARY
from app.workers import scheduler


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


def test_invalid_window_is_recorded_inside_the_job_tracking_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    @contextmanager
    def tracked(job_name: str) -> Iterator[SimpleNamespace]:
        events.append(f"entered:{job_name}")
        try:
            yield SimpleNamespace(row_count=0)
        finally:
            events.append(f"exited:{job_name}")

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)

    with pytest.raises(ValueError, match="unknown recent evidence window"):
        scheduler.strategy_backtest_run({"evidence_window": "searched-favourable-dates"})

    assert events == ["entered:strategy_backtest_run", "exited:strategy_backtest_run"]


def test_refresh_recent_skips_complete_windows_and_commits_each_missing_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(row_count=None, run_id=0, progress=None, note=None)
    conn = MagicMock()

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    @contextmanager
    def connected() -> Iterator[MagicMock]:
        yield conn

    calls: list[object] = []

    def run_backtest(_conn: object, **kwargs: object) -> SimpleNamespace:
        calls.append(kwargs["evaluation_window"])
        return SimpleNamespace(rows_written=12)

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(scheduler, "_recent_evidence_completion", lambda _conn: ({"primary-2022-plus"}, set()))
    monkeypatch.setattr("app.services.backtest_run.run_backtest", run_backtest)

    scheduler.strategy_backtest_run(
        {
            "refresh_recent": True,
            "holdout_purpose": "declared recent evidence",
            "holdout_accessed_by": "operator",
        }
    )

    assert len(calls) == len(RECENT_EVIDENCE_WINDOWS) - 1
    assert conn.commit.call_count == len(calls)
    assert tracker.row_count == len(calls) * 12
    assert tracker.progress.outcomes == {"already_complete": 1, "completed": len(calls)}


def test_refresh_recent_refuses_partial_immutable_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(row_count=None, run_id=0, progress=None, note=None)

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    @contextmanager
    def connected() -> Iterator[MagicMock]:
        yield MagicMock()

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(scheduler, "_recent_evidence_completion", lambda _conn: (set(), {"rolling-36m"}))

    with pytest.raises(RuntimeError, match="partial immutable windows"):
        scheduler.strategy_backtest_run(
            {
                "refresh_recent": True,
                "holdout_purpose": "declared recent evidence",
                "holdout_accessed_by": "operator",
            }
        )


def test_overview_counts_missing_members_and_empty_runnable_set_without_crashing() -> None:
    missing_member = SimpleNamespace(
        runnable=True,
        evidence_windows=[SimpleNamespace(window_id="primary-2022-plus", status="complete")],
    )
    excluded = SimpleNamespace(runnable=False, evidence_windows=[])

    assert _evidence_window_counts([missing_member]) == (1, 0)  # type: ignore[list-item]
    assert _evidence_window_counts([excluded]) == (0, 0)  # type: ignore[list-item]
