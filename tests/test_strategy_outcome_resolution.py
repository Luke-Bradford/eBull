"""Forward outcome resolution: mature once, never persist an immature poll."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import psycopg
import pytest

from app.services.indicator_series import BarSeries
from app.services.outcome_ledger import PendingFill
from app.services.outcome_resolver import ExitLevels
from app.services.strategy_manifest import StrategyEntry
from app.services.strategy_outcome_resolution import _read_cursor, _resolve_fill, _select_round_robin, _write_cursor


def _series(*ranges: tuple[Decimal | None, Decimal | None]) -> BarSeries:
    dates = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(len(ranges)))
    rows = tuple(
        {
            "open": Decimal("100"),
            "high": high,
            "low": low,
            "close": Decimal("100"),
            "volume": Decimal("1000"),
        }
        for high, low in ranges
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def _entry(*, max_hold_bars: int = 2) -> StrategyEntry:
    levels = ExitLevels(
        take_profit=Decimal("110"),
        stop_loss=Decimal("90"),
        max_hold_bars=max_hold_bars,
    )
    return cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda _series, *, signal_index, entry_price, universe: levels,
        ),
    )


def _fill(series: BarSeries) -> PendingFill:
    return PendingFill(
        signal_id=7,
        instrument_id=42,
        signal_bar_date=series.dates[0],
        fill_bar_date=series.dates[1],
        fill_price=Decimal("100"),
        universe="survivor_only",
    )


def test_an_immature_window_is_left_pending_without_a_row() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("105"), Decimal("95")))
    assert _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=()) is None


def test_a_decisive_target_is_stored_as_a_booked_outcome() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("111"), Decimal("95")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.gross_return_pct) == (
        "tp_hit",
        series.dates[1],
        Decimal("0.1"),
    )


def test_same_bar_both_touch_remains_ambiguous_and_unbooked() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("111"), Decimal("89")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.gross_return_pct) == (
        "ambiguous",
        series.dates[1],
        None,
    )


def test_a_scale_break_is_terminal_but_never_priced_across_scales() -> None:
    series = _series(
        (Decimal("105"), Decimal("95")),
        (Decimal("105"), Decimal("95")),
        (Decimal("111"), Decimal("89")),
    )
    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(series.dates[2],),
    )
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "series_break", None)


def test_a_break_between_signal_and_fill_is_terminal_before_levels_are_mixed() -> None:
    series = _series((Decimal("105"), Decimal("95")), (Decimal("150"), Decimal("100")))
    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(series.dates[1],),
    )
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "series_break", None)


def test_a_masked_required_field_is_counted_not_silently_dropped() -> None:
    series = _series((Decimal("105"), Decimal("95")), (None, Decimal("95")))
    row = _resolve_fill(_entry(), _fill(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "quarantined_bar", None)


def test_unorderable_exit_levels_are_terminal_without_aborting_the_batch() -> None:
    series = _series((Decimal("1"), Decimal("0.5")), (Decimal("1"), Decimal("0.5")))
    entry = cast(
        StrategyEntry,
        SimpleNamespace(
            strategy_id="test-level",
            exit_levels=lambda _series, *, signal_index, entry_price, universe: "unorderable_exit_levels",
        ),
    )

    row = _resolve_fill(entry, _fill(series), series=series, unresolved_breaks=())

    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == (
        "unresolved",
        "unorderable_exit_levels",
        None,
    )


def test_precomputed_masked_reasons_are_reused_for_an_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    series = _series((Decimal("105"), Decimal("95")), (None, Decimal("95")))
    monkeypatch.setattr(
        "app.services.strategy_outcome_resolution._masked_reasons",
        lambda _rows: pytest.fail("masked reasons were recomputed per fill"),
    )

    row = _resolve_fill(
        _entry(),
        _fill(series),
        series=series,
        unresolved_breaks=(),
        masked_bar_reasons={1: "quarantined_bar"},
    )

    assert row is not None
    assert (row.outcome, row.reason) == ("unresolved", "quarantined_bar")


def test_round_robin_wraps_after_the_cursor_without_repeating(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[int, int | None, int]] = []

    def select(_conn: object, **kwargs: object) -> list[PendingFill]:
        after = cast(int, kwargs["after_signal_id"])
        ceiling = cast(int | None, kwargs.get("at_or_before_signal_id"))
        limit = cast(int, kwargs["limit"])
        seen.append((after, ceiling, limit))
        ids = [8] if after == 7 else [2, 3]
        return [
            PendingFill(
                signal_id=signal_id,
                instrument_id=42,
                signal_bar_date=date(2026, 8, 1),
                fill_bar_date=date(2026, 8, 2),
                fill_price=Decimal("100"),
                universe="survivor_only",
            )
            for signal_id in ids[:limit]
        ]

    monkeypatch.setattr("app.services.strategy_outcome_resolution.select_pending_fills", select)
    fills = _select_round_robin(
        cast(psycopg.Connection[object], object()),
        strategy_id="test",
        strategy_version="v1",
        cursor=7,
        limit=3,
    )
    assert [fill.signal_id for fill in fills] == [8, 2, 3]
    assert seen == [(7, None, 3), (0, 7, 2)]


def test_cursor_is_one_mutable_row_per_version_pair(ebull_test_conn: psycopg.Connection[object]) -> None:
    assert _read_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1") == 0
    _write_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1", last_signal_id=10)
    _write_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1", last_signal_id=3)
    assert _read_cursor(ebull_test_conn, strategy_id="test", strategy_version="v1") == 3
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_outcome_cursor").fetchone() == (1,)
