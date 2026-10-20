"""#3903 Amendment 2: the acceptance script's independent evaluation of sql/508's conditions (pure, no DB)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from scripts.measure_3903_dividend_acceptance import _plus_one_year, guard_cause, observed_ttm


def _window(starts_ends: Sequence[tuple[date | None, date]], paid: list[str]) -> list[dict[str, Any]]:
    return [
        {"period_start_date": s, "period_end_date": e, "dividends_paid": Decimal(p)}
        for (s, e), p in zip(starts_ends, paid, strict=True)
    ]


_STANDARD: list[tuple[date | None, date]] = [
    (date(2025, 4, 1), date(2025, 6, 30)),
    (date(2025, 7, 1), date(2025, 9, 30)),
    (date(2025, 10, 1), date(2025, 12, 31)),
    (date(2026, 1, 1), date(2026, 3, 31)),
]
_LONG: list[tuple[date | None, date]] = [(date(2025, 3, 29), _STANDARD[0][1]), *_STANDARD[1:]]


def test_plus_one_year_matches_postgres_interval_arithmetic() -> None:
    assert _plus_one_year(date(2025, 4, 1)) == date(2026, 4, 1)
    assert _plus_one_year(date(2024, 2, 29)) == date(2025, 2, 28)
    assert _plus_one_year(date(2023, 3, 1)) == date(2024, 3, 1)


def test_guard_passes_a_year_and_refuses_a_positive_long_window() -> None:
    assert guard_cause(_window(_STANDARD, ["1"] * 4)) is None
    assert guard_cause(_window(_LONG, ["1"] * 4)) == "window over one calendar year"


def test_a_zero_total_skips_the_span_test() -> None:
    assert guard_cause(_window(_LONG, ["0"] * 4)) is None
    assert observed_ttm(_window(_LONG, ["0"] * 4)) == (Decimal(0), "published")


def test_a_nan_total_is_not_zero_and_takes_the_span_test() -> None:
    # SQL: SUM(...) = 0 is false for NaN, so the date comparison decides, as for a positive total.
    assert guard_cause(_window(_LONG, ["1", "NaN", "1", "1"])) == "window over one calendar year"
    assert observed_ttm(_window(_LONG, ["1", "NaN", "1", "1"])) == (None, "window over one calendar year")
    value, cause = observed_ttm(_window(_STANDARD, ["1", "NaN", "1", "1"]))
    assert value is not None and value.is_nan() and cause == "published"  # NaN sorts above 0: not a negative member


def test_conditions_apply_in_order() -> None:
    inverted = [_LONG[0], (date(2025, 10, 1), date(2025, 9, 30)), *_LONG[2:]]
    assert guard_cause(_window(inverted, ["1"] * 4)) == "invalid member interval"
    overlap = [*_STANDARD[:2], (date(2025, 9, 30), date(2025, 12, 31)), _STANDARD[3]]
    assert guard_cause(_window(overlap, ["1"] * 4)) == "overlapping members"
    assert guard_cause(_window([(None, _STANDARD[0][1]), *_STANDARD[1:]], ["1"] * 4)) == "invalid member interval"
