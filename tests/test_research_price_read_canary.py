from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from unittest.mock import patch

import pytest

from app.services.research_price_read_canary import (
    ReadDecodeCanaryConfig,
    ReadDecodeCanaryRefused,
    plan_read_decode_canary,
    run_read_decode_canary,
)
from app.services.research_price_structure_store import MaskedSeries


class _Rows:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _Connection:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.calls = 0

    def execute(self, _query: str, _params: object) -> _Rows:
        self.calls += 1
        return _Rows(self.rows)


def _metadata(count: int = 9) -> list[tuple[object, ...]]:
    first = date(2000, 1, 1)
    return [(index + 1, 100 * (index + 1), first, first + timedelta(days=index), True) for index in range(count)]


def _series(series_id: int, bars: int) -> MaskedSeries:
    return (
        MaskedSeries(
            series_id=series_id,
            bars=tuple(),
            wealth_closes=tuple(),
            range_masked=0,
            return_masked=0,
            range_flagged=0,
            return_flagged=0,
            bars_flagged=0,
        )
        if bars == 0
        else _series_with_bars(series_id, bars)
    )


def _series_with_bars(series_id: int, bars: int) -> MaskedSeries:
    from app.services.price_structure import StructureBar

    values = tuple(
        StructureBar(date(2000, 1, 1) + timedelta(days=index), None, None, None, None, None) for index in range(bars)
    )
    return MaskedSeries(series_id, values, (None,) * bars, 0, 0, 0, 0, 0)


def test_plan_is_stratified_deterministic_and_digest_bound_before_bar_reads() -> None:
    conn = _Connection(_metadata())
    plan = plan_read_decode_canary(conn, config=ReadDecodeCanaryConfig(series_count=5))  # type: ignore[arg-type]

    assert [item.series_id for item in plan.selected] == [1, 3, 5, 7, 9]
    assert plan.census_series == 9
    assert plan.eligible_series == 9
    assert plan.fail_closed_series == 0
    assert plan.declared_bars == 2_500
    assert len(plan.selection_digest) == 64
    assert conn.calls == 1

    with pytest.raises(ReadDecodeCanaryRefused, match="selection digest changed.*no bar rows were read"):
        plan_read_decode_canary(
            conn,  # type: ignore[arg-type]
            config=ReadDecodeCanaryConfig(series_count=5, expected_selection_digest="0" * 64),
        )
    assert conn.calls == 2


def test_canary_reads_each_fixed_series_once_reports_resources_and_no_outcomes() -> None:
    conn = _Connection(_metadata())
    loaded: list[int] = []

    def load(_conn: object, series_id: int, *, through_date: date | None = None) -> dict[str, MaskedSeries]:
        del through_date
        loaded.append(series_id)
        series = _series(series_id, series_id)
        return {"masked": series, "admitted": series}

    with patch("app.services.research_price_read_canary.load_arms", load):
        report = run_read_decode_canary(conn, config=ReadDecodeCanaryConfig(series_count=5))  # type: ignore[arg-type]

    assert loaded == [1, 3, 5, 7, 9]
    assert report.query_count == 6
    assert report.decoded_bars == 25
    assert report.arms_identical_shape
    assert report.wall_s >= 0
    assert report.cpu_s >= 0
    assert report.peak_rss_bytes > 0
    assert report.stopped_after_selected_series
    forbidden = ("sharpe", "return", "drawdown", "profit", "trade", "signal", "cohort")
    assert not any(word in key for key in asdict(report) for word in forbidden)


def test_declared_work_cap_refuses_before_any_bar_read() -> None:
    conn = _Connection(_metadata())
    with (
        patch("app.services.research_price_read_canary.load_arms") as load,
        pytest.raises(ReadDecodeCanaryRefused, match="above canary cap.*no bar rows were read"),
    ):
        run_read_decode_canary(
            conn,  # type: ignore[arg-type]
            config=ReadDecodeCanaryConfig(series_count=5, max_declared_bars=2_499),
        )
    load.assert_not_called()
