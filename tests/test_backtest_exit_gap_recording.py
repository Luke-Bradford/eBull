"""#3104 slice 9 — how ``_absorb`` gives each realised leg ONE session-gap verdict.

Separate from ``test_strategy_exit_gap.py`` because these exercise the recording
path in ``backtest_run`` rather than the pure arithmetic, and separate from the
existing ``backtest_run`` suites because those are DB-marked — the ``db`` marker
is per MODULE, so one DB test in a file evicts every test there from the fast
tier.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from app.services.backtest_run import (
    _exit_gap_summary,
    _NamespaceBook,
    _record_exit_gap,
)
from app.services.indicator_series import BarSeries

START = date(2020, 1, 1)


def _series(count: int) -> BarSeries:
    dates = tuple(START + timedelta(days=offset) for offset in range(count))
    rows = tuple({"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100} for _ in dates)
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def _all_measured(
    series: BarSeries, *, values: list[float] | None = None
) -> tuple[tuple[float, ...], tuple[None, ...]]:
    """Every boundary measurable, so the test isolates the recording rule."""
    supplied = values if values is not None else [0.0 for _ in series.dates]
    return tuple(supplied), tuple(None for _ in series.dates)


def _record(**overrides: object) -> _NamespaceBook:
    series = overrides.pop("series", None) or _series(5)
    assert isinstance(series, BarSeries)
    gaps, reasons = _all_measured(series)
    book = _NamespaceBook()
    book.returns.append(-1.0)
    kwargs: dict[str, object] = {
        "series": series,
        "fill_date": series.dates[0],
        "exit_date": series.dates[-1],
        "name_key": 42,
        "close_source": "level",
        "policy_known": True,
        "gaps": gaps,
        "reasons": reasons,
    }
    kwargs.update(overrides)
    _record_exit_gap(book, **kwargs)  # type: ignore[arg-type]
    return book


def test_a_measured_leg_records_both_extrema_over_its_held_boundaries() -> None:
    series = _series(5)
    gaps, reasons = _all_measured(series, values=[99.0, -1.0, -7.0, 3.0, -2.0])
    book = _record(series=series, gaps=gaps, reasons=reasons)
    # ⚠ index 0 carries 99.0 and is NOT held — the fill bar's own boundary
    # happened before the position existed.
    assert list(book.exit_gap_min) == [-7.0]
    assert list(book.exit_gap_max) == [3.0]
    assert book.exit_gap_measured_boundaries == 4
    assert book.exit_gap_excluded == {}


def test_a_same_bar_close_is_excluded_rather_than_wrapping() -> None:
    """⚠ At ``fill_index = 0`` an unguarded ``i-1`` wraps to the LAST bar."""
    series = _series(5)
    book = _record(series=series, fill_date=series.dates[0], exit_date=series.dates[0])
    assert book.exit_gap_excluded == {"no_session_boundary": 1}
    assert list(book.exit_gap_min) == []


def test_unresolved_provenance_withholds_the_whole_leg() -> None:
    book = _record(policy_known=False)
    assert book.exit_gap_excluded == {"provenance_unknown": 1}


def test_a_same_bar_close_outranks_unresolved_provenance() -> None:
    """Both are leg-level; the frozen precedence decides, not the code order."""
    series = _series(5)
    book = _record(series=series, exit_date=series.dates[0], policy_known=False)
    assert book.exit_gap_excluded == {"no_session_boundary": 1}


def test_a_wholly_unmeasurable_leg_reports_its_lowest_ranked_reason() -> None:
    series = _series(4)
    reasons = (None, "open_unusable", "off_axis", "open_unusable")
    book = _record(series=series, gaps=(0.0, 0.0, 0.0, 0.0), reasons=reasons)
    assert book.exit_gap_excluded == {"off_axis": 1}
    assert book.exit_gap_unmeasurable_boundaries == 3
    # ⚠ A wholly excluded leg is NOT partially covered — the two counts are
    # distinct, and conflating them lets "0 partial" coexist with "0 measured".
    assert book.exit_gap_partial_legs == 0


def test_a_partly_covered_leg_is_measured_and_counted_as_such() -> None:
    series = _series(4)
    reasons = (None, None, "provisional_bar", None)
    book = _record(series=series, gaps=(0.0, -4.0, 0.0, -1.0), reasons=reasons)
    assert list(book.exit_gap_min) == [-4.0]
    assert book.exit_gap_partial_legs == 1
    assert book.exit_gap_unmeasurable_boundaries == 1
    assert book.exit_gap_measured_boundaries == 2


def test_a_negative_name_key_is_carried_as_a_caveat_not_an_exclusion() -> None:
    book = _record(name_key=-7)
    assert book.exit_gap_unlinked_legs == 1
    assert len(book.exit_gap_min) == 1


def test_the_binding_observation_carries_both_ends_of_its_boundary() -> None:
    series = _series(4)
    gaps, reasons = _all_measured(series, values=[0.0, -1.0, -9.0, -2.0])
    book = _record(series=series, gaps=gaps, reasons=reasons)
    binding = book.exit_gap_binding_min
    assert binding is not None
    assert binding.value == -9.0
    assert binding.bar_date == series.dates[2]
    assert binding.prior_bar_date == series.dates[1]
    assert binding.boundary_calendar_days == 1
    assert binding.close_source == "level"


@pytest.mark.parametrize(
    ("fill_offset", "exit_offset", "match"),
    [(0, 5, "absent from the series"), (3, 1, "before its fill")],
)
def test_structural_corruption_raises_rather_than_excluding(fill_offset: int, exit_offset: int, match: str) -> None:
    series = _series(5)
    # ⚠ Index 5 is a date the series does not hold — a missing endpoint is a
    # plumbing bug, not an ordinary `off_axis` exclusion.
    dates = [*series.dates, START + timedelta(days=900)]
    with pytest.raises(RuntimeError, match=match):
        _record(series=series, fill_date=dates[fill_offset], exit_date=dates[exit_offset])


def test_a_short_verdict_array_raises_rather_than_reporting_a_boundary_reason() -> None:
    series = _series(5)
    with pytest.raises(RuntimeError, match="cannot disagree with it"):
        _record(series=series, gaps=(0.0, 0.0), reasons=(None, None))


# ---------------------------------------------------------------------------
# Summary transport
# ---------------------------------------------------------------------------


def test_the_summary_survives_a_book_that_was_never_instrumented() -> None:
    """⚠ Slice 7a returns ``None`` here and takes the accounting with it."""
    book = _NamespaceBook()
    book.returns.extend([1.0, -2.0, 3.0])
    measurement = _exit_gap_summary(book, liquidity_policy=None, return_basis="total_return")
    assert measurement.realised_leg_count == 3
    assert measurement.measured_leg_count == 0
    assert measurement.excluded == {"not_instrumented": 3}
    assert measurement.rule_version


def test_the_summary_holds_the_accounting_equality_on_a_recorded_book() -> None:
    series = _series(4)
    book = _record(series=series)
    book.returns.append(2.0)
    _record_exit_gap(
        book,
        series=series,
        fill_date=series.dates[0],
        exit_date=series.dates[0],
        name_key=9,
        close_source="max_hold",
        policy_known=True,
        gaps=(0.0, 0.0, 0.0, 0.0),
        reasons=(None, None, None, None),
    )
    measurement = _exit_gap_summary(book, liquidity_policy=None, return_basis="total_return")
    assert measurement.realised_leg_count == 2
    assert measurement.measured_leg_count == 1
    assert measurement.excluded == {"no_session_boundary": 1}
    assert measurement.adjustment_basis is None
