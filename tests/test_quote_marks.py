"""Tests for app.services.quote_marks.positive_decimal_or_none.

The shared strictly-positive floor for execution prices / marks (#1439).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.quote_marks import positive_decimal_or_none


class TestPositiveDecimalOrNone:
    @pytest.mark.parametrize(
        "value, expected",
        [
            (Decimal("150.5"), Decimal("150.5")),
            (150.5, Decimal("150.5")),
            ("150.5", Decimal("150.5")),
            (1, Decimal("1")),
        ],
    )
    def test_strictly_positive_passes_through(self, value: object, expected: Decimal) -> None:
        assert positive_decimal_or_none(value) == expected

    @pytest.mark.parametrize("value", [None, 0, 0.0, Decimal("0"), -1, -0.01, Decimal("-5")])
    def test_non_positive_and_null_floor_to_none(self, value: object) -> None:
        assert positive_decimal_or_none(value) is None

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), "nan", "inf"])
    def test_non_finite_inputs_floor_to_none(self, value: object) -> None:
        """A mark must be a finite positive number — NaN/inf are missing."""
        assert positive_decimal_or_none(value) is None

    def test_garbage_string_floors_to_none(self) -> None:
        """Unparseable input raises InvalidOperation internally → None."""
        assert positive_decimal_or_none("not-a-number") is None
