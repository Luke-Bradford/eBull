"""The recent evidence denominator and its anti-window-shopping boundary."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app.api.strategies import _evidence_window_counts
from app.services.backtest_run import BacktestProgressEvent, _ControlProgress
from app.services.position_builder import Window
from app.services.processes.param_metadata import MANUAL_TRIGGER_JOB_METADATA
from app.services.strategy_recent_evidence import (
    RECENT_EVIDENCE_WINDOWS,
    RecentEvidenceWindow,
    recent_evidence_window,
)
from app.services.strategy_result import EVALUATION_WINDOW_END, HOLDOUT_BOUNDARY
from app.services.universe_selection import INTRADER_CAPTURE_DATE
from app.workers import scheduler


def test_all_required_windows_are_named_and_inside_the_holdout_corpus() -> None:
    assert tuple(RECENT_EVIDENCE_WINDOWS) == (
        "primary-2022-plus",
        "rolling-36m",
        "rolling-24m",
        "year-2022",
        "year-2023",
        "year-2024",
    )
    assert all(item.required_for_allocation for item in RECENT_EVIDENCE_WINDOWS.values())
    assert all(item.window.start >= HOLDOUT_BOUNDARY for item in RECENT_EVIDENCE_WINDOWS.values())
    assert all(item.window.end <= EVALUATION_WINDOW_END for item in RECENT_EVIDENCE_WINDOWS.values())
    assert all(item.window.end <= INTRADER_CAPTURE_DATE for item in RECENT_EVIDENCE_WINDOWS.values())
    assert RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window.start == date(2022, 1, 1)
    assert RECENT_EVIDENCE_WINDOWS["primary-2022-plus"].window.end == INTRADER_CAPTURE_DATE


def test_raw_or_unknown_window_ids_are_refused() -> None:
    with pytest.raises(ValueError, match="unknown recent evidence window"):
        recent_evidence_window("2025-03-01:2025-04-01")


def test_a_post_capture_window_cannot_be_declared_as_survivorship_free_evidence() -> None:
    with pytest.raises(ValueError, match="after the survivorship-free archive capture"):
        RecentEvidenceWindow(
            "year-2024",
            "invalid post-capture window",
            Window(date(2024, 1, 1), date(2024, 12, 31)),
        )


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
        calls.append(kwargs["evidence_window_id"])
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


def test_refresh_progress_is_transient_until_evidence_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(row_count=None, run_id=9, progress=None, note=None)
    conn = MagicMock()
    events: list[str] = []
    conn.commit.side_effect = lambda: events.append("evidence_commit")
    missing = next(reversed(RECENT_EVIDENCE_WINDOWS))

    class Writer:
        def start_window(self, window_id: str) -> None:
            events.append(f"start:{window_id}")

        def __call__(self, event: BacktestProgressEvent) -> None:
            events.append(f"progress:{event.phase}")

        def record_window_commit(self, *, rows_written: int, completed: int) -> bool:
            events.append(f"checkpoint:{rows_written}:{completed}")
            return True

        def close(self) -> None:
            events.append("closed")

    writer = Writer()

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    @contextmanager
    def connected() -> Iterator[MagicMock]:
        yield conn

    def run_backtest(_conn: object, **kwargs: object) -> SimpleNamespace:
        progress = kwargs["progress"]
        assert callable(progress)
        progress(BacktestProgressEvent(phase="evaluation", series_seen=25, series_total=100))
        return SimpleNamespace(rows_written=16)

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(
        scheduler,
        "_recent_evidence_completion",
        lambda _conn: (set(RECENT_EVIDENCE_WINDOWS) - {missing}, set()),
    )
    monkeypatch.setattr(scheduler, "_open_backtest_progress_writer", lambda **_kwargs: writer)
    monkeypatch.setattr("app.services.backtest_run.run_backtest", run_backtest)

    scheduler.strategy_backtest_run(
        {
            "refresh_recent": True,
            "holdout_purpose": "declared recent evidence",
            "holdout_accessed_by": "operator",
        }
    )

    assert events == [
        f"start:{missing}",
        "progress:evaluation",
        "evidence_commit",
        "checkpoint:16:1",
        "closed",
    ]
    assert conn.execute.call_count == 0


def test_single_run_reports_progress_and_only_checkpoints_after_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(row_count=None, run_id=9, progress=None, note=None)
    conn = MagicMock()
    events: list[str] = []

    class Writer:
        def start_window(self, window_id: str) -> None:
            events.append(f"start:{window_id}")

        def __call__(self, event: BacktestProgressEvent) -> None:
            events.append(f"progress:{event.phase}:{event.series_seen}/{event.series_total}")

        def record_window_commit(self, *, rows_written: int, completed: int) -> bool:
            events.append(f"checkpoint:{rows_written}:{completed}")
            return True

        def close(self) -> None:
            events.append("closed")

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    @contextmanager
    def connected() -> Iterator[MagicMock]:
        yield conn

    def run_backtest(_conn: object, **kwargs: object) -> SimpleNamespace:
        progress = kwargs["progress"]
        assert callable(progress)
        progress(BacktestProgressEvent(phase="synthetic_control", series_seen=17, series_total=1000))
        events.append("evidence_returned")
        return SimpleNamespace(rows_written=40)

    writer = Writer()
    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(scheduler, "_open_backtest_progress_writer", lambda **_kwargs: writer)
    monkeypatch.setattr("app.services.backtest_run.run_backtest", run_backtest)

    scheduler.strategy_backtest_run({"synthetic_control": True})

    assert events == [
        "start:in_sample",
        "progress:synthetic_control:17/1000",
        "evidence_returned",
        "checkpoint:40:1",
        "closed",
    ]
    assert tracker.row_count == 40


def test_refresh_failure_before_commit_never_publishes_a_window_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = SimpleNamespace(row_count=None, run_id=9, progress=None, note=None)
    conn = MagicMock()
    events: list[str] = []

    class Writer:
        def start_window(self, window_id: str) -> None:
            events.append(f"start:{window_id}")

        def __call__(self, _event: BacktestProgressEvent) -> None:
            events.append("progress")

        def record_window_commit(self, *, rows_written: int, completed: int) -> bool:
            events.append(f"checkpoint:{rows_written}:{completed}")
            return True

        def close(self) -> None:
            events.append("closed")

    @contextmanager
    def tracked(_job_name: str) -> Iterator[SimpleNamespace]:
        yield tracker

    @contextmanager
    def connected() -> Iterator[MagicMock]:
        yield conn

    monkeypatch.setattr(scheduler, "_tracked_job", tracked)
    monkeypatch.setattr(scheduler, "connect_job", connected)
    monkeypatch.setattr(scheduler, "_recent_evidence_completion", lambda _conn: (set(), set()))
    monkeypatch.setattr(scheduler, "_open_backtest_progress_writer", lambda **_kwargs: Writer())

    def fail(_conn: object, **kwargs: object) -> None:
        progress = kwargs["progress"]
        assert callable(progress)
        progress(BacktestProgressEvent(phase="evaluation", series_seen=25, series_total=100))
        raise RuntimeError("cancelled before evidence commit")

    monkeypatch.setattr("app.services.backtest_run.run_backtest", fail)

    with pytest.raises(RuntimeError, match="cancelled before evidence commit"):
        scheduler.strategy_backtest_run(
            {
                "refresh_recent": True,
                "holdout_purpose": "declared recent evidence",
                "holdout_accessed_by": "operator",
            }
        )

    assert events == [f"start:{next(iter(RECENT_EVIDENCE_WINDOWS))}", "progress", "closed"]
    conn.commit.assert_not_called()


def test_progress_writer_failure_is_non_fatal_and_disables_future_writes() -> None:
    conn = MagicMock()
    conn.execute.side_effect = RuntimeError("telemetry unavailable")
    writer = scheduler._BacktestProgressWriter(  # noqa: SLF001 - the failure boundary is the contract under test
        conn,
        run_id=9,
        total_windows=8,
        already_complete=0,
    )
    writer.start_window("primary-2022-plus")
    event = BacktestProgressEvent(phase="evaluation", series_seen=25, series_total=100)

    writer(event)
    writer(event)

    assert conn.execute.call_count == 1
    conn.close.assert_called_once_with()


def test_progress_writer_does_not_claim_window_completion_before_commit() -> None:
    conn = MagicMock()
    writer = scheduler._BacktestProgressWriter(  # noqa: SLF001 - SQL/payload boundary under test
        conn,
        run_id=9,
        total_windows=8,
        already_complete=2,
    )
    writer.start_window("rolling-24m")

    writer(
        BacktestProgressEvent(
            phase="evaluation",
            strategy_id="s4-volatility-compression-breakout",
            quarantine_arm="masked",
            series_seen=50,
            series_total=100,
        )
    )

    sql, params = conn.execute.call_args.args
    assert "row_count" not in sql
    payload = params["progress"].obj
    assert payload["outcomes"] == {"already_complete": 2, "completed": 0}
    assert payload["active"] == {
        "window_id": "rolling-24m",
        "phase": "evaluation",
        "strategy_id": "s4-volatility-compression-breakout",
        "quarantine_arm": "masked",
        "ambiguity_arm": None,
        "series_seen": 50,
        "series_total": 100,
        "work_unit": "series",
        "elapsed_s": 0.0,
        "rate_per_s": None,
        "eta_s": None,
        "control_seen": 0,
        "control_total": None,
    }


def test_progress_writer_publishes_outcome_free_rate_and_eta(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = MagicMock()
    writer = scheduler._BacktestProgressWriter(  # noqa: SLF001 - payload contract under test
        conn,
        run_id=9,
        total_windows=1,
        already_complete=0,
    )
    writer.start_window("in_sample")
    clock = iter((100.0, 106.0))
    monkeypatch.setattr(scheduler.time, "monotonic", lambda: next(clock))

    writer(BacktestProgressEvent(phase="synthetic_control", series_seen=3, series_total=1000))
    writer(BacktestProgressEvent(phase="synthetic_control", series_seen=15, series_total=1000))

    payload = conn.execute.call_args.args[1]["progress"].obj["active"]
    assert payload["work_unit"] == "members"
    assert payload["elapsed_s"] == 6.0
    assert payload["rate_per_s"] == 2.0
    assert payload["eta_s"] == 492.5
    assert not ({"return", "sharpe", "passed", "profit"} & set(payload))


def test_global_control_progress_counts_an_arm_once_across_member_ticks() -> None:
    events: list[BacktestProgressEvent] = []
    progress = _ControlProgress(events.append, total=40)
    for seen in (1, 2, 25):
        progress(
            BacktestProgressEvent(
                phase="synthetic_control",
                strategy_id="s1",
                quarantine_arm="admitted",
                ambiguity_arm="worst_case",
                series_seen=seen,
                series_total=1000,
            )
        )
    progress(
        BacktestProgressEvent(
            phase="synthetic_control",
            strategy_id="s1",
            quarantine_arm="admitted",
            ambiguity_arm="best_case",
            series_seen=1,
            series_total=1000,
        )
    )
    assert [(event.control_seen, event.control_total) for event in events] == [
        (1, 40),
        (1, 40),
        (1, 40),
        (2, 40),
    ]


def test_overview_counts_missing_members_and_empty_runnable_set_without_crashing() -> None:
    missing_member = SimpleNamespace(
        runnable=True,
        evidence_windows=[SimpleNamespace(window_id="primary-2022-plus", status="complete")],
    )
    excluded = SimpleNamespace(runnable=False, evidence_windows=[])

    assert _evidence_window_counts([missing_member]) == (1, 0)  # type: ignore[list-item]
    assert _evidence_window_counts([excluded]) == (0, 0)  # type: ignore[list-item]
