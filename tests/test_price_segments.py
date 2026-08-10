"""Pure price-scale segment boundary tests."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries
from app.services.price_segments import segment_end_index, segment_for_index, series_segment_bounds


def _series() -> BarSeries:
    dates = tuple(date(2020, 1, day) for day in (1, 2, 4, 5))
    row = {"open": Decimal("10"), "high": Decimal("11"), "low": Decimal("9"), "close": Decimal("10")}
    return BarSeries(dates=dates, rows=(row, row, row, row))  # type: ignore[arg-type]


def test_a_missing_break_date_still_splits_at_the_first_later_stored_bar() -> None:
    assert series_segment_bounds(_series(), unresolved_breaks=(date(2020, 1, 3),)) == ((0, 2), (2, 4))
    assert segment_end_index(_series(), fill_index=0, unresolved_breaks=(date(2020, 1, 3),)) == 1


def test_a_fill_on_the_break_bar_is_inside_the_new_segment() -> None:
    assert segment_end_index(_series(), fill_index=2, unresolved_breaks=(date(2020, 1, 4),)) is None
    segment, local_index = segment_for_index(_series(), index=2, unresolved_breaks=(date(2020, 1, 4),))
    assert (segment.dates, local_index) == ((date(2020, 1, 4), date(2020, 1, 5)), 0)


def test_breaks_must_be_unique_and_ordered() -> None:
    with pytest.raises(ValueError, match="unique and ordered"):
        series_segment_bounds(
            _series(),
            unresolved_breaks=(date(2020, 1, 4), date(2020, 1, 3)),
        )
