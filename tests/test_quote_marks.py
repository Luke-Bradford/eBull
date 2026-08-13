"""Tests for app.services.quote_marks.positive_decimal_or_none.

The shared strictly-positive floor for execution prices / marks (#1439).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.quote_marks import directional_fill_price, positive_decimal_or_none


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


class TestDirectionalFillPrice:
    """The shared BUY-at-ask / EXIT-at-bid rule (#1465)."""

    @pytest.mark.parametrize("action", ["BUY", "ADD"])
    def test_buy_fills_at_ask(self, action: str) -> None:
        assert directional_fill_price(action, 100.0, 99.0, 101.0) == Decimal("101.0")

    def test_exit_fills_at_bid(self) -> None:
        assert directional_fill_price("EXIT", 100.0, 99.0, 101.0) == Decimal("99.0")

    @pytest.mark.parametrize("action", ["BUY", "ADD"])
    def test_buy_falls_back_to_last_when_ask_missing(self, action: str) -> None:
        assert directional_fill_price(action, 100.0, 99.0, None) == Decimal("100.0")

    def test_exit_falls_back_to_last_when_bid_missing(self) -> None:
        assert directional_fill_price("EXIT", 100.0, None, 101.0) == Decimal("100.0")

    def test_buy_zero_ask_does_not_override_valid_last(self) -> None:
        """#1439: a 0.00 ask is missing — must not price the fill at 0."""
        assert directional_fill_price("BUY", 100.0, 0.0, 0.0) == Decimal("100.0")

    def test_buy_fills_at_ask_when_last_zero_but_book_positive(self) -> None:
        """#1465: a 0.00 last with a valid book prices at ask, not None."""
        assert directional_fill_price("BUY", 0.0, 99.0, 101.0) == Decimal("101.0")

    def test_exit_fills_at_bid_when_last_zero_but_book_positive(self) -> None:
        assert directional_fill_price("EXIT", 0.0, 99.0, 101.0) == Decimal("99.0")

    @pytest.mark.parametrize("action", ["BUY", "ADD", "EXIT"])
    def test_no_usable_side_returns_none(self, action: str) -> None:
        assert directional_fill_price(action, 0.0, 0.0, 0.0) is None
        assert directional_fill_price(action, None, None, None) is None
