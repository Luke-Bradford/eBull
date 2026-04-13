"""Tests for app.services.fx — currency conversion logic."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.fx import FxRateNotFound, convert


class TestConvert:
    def test_same_currency_returns_amount(self) -> None:
        rates: dict[tuple[str, str], Decimal] = {}
        assert convert(Decimal("100.00"), "USD", "USD", rates) == Decimal("100.00")

    def test_direct_rate(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.78")}
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        assert result == Decimal("78.00")

    def test_inverse_rate(self) -> None:
        rates = {("GBP", "USD"): Decimal("1.28")}
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        # 100 / 1.28 = 78.125
        assert result == Decimal("100.00") / Decimal("1.28")

    def test_direct_preferred_over_inverse(self) -> None:
        rates = {
            ("USD", "GBP"): Decimal("0.78"),
            ("GBP", "USD"): Decimal("1.28"),
        }
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        assert result == Decimal("78.00")

    def test_missing_rate_raises(self) -> None:
        rates: dict[tuple[str, str], Decimal] = {}
        with pytest.raises(FxRateNotFound, match="USD.*EUR"):
            convert(Decimal("100.00"), "USD", "EUR", rates)

    def test_zero_amount(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.78")}
        assert convert(Decimal("0"), "USD", "GBP", rates) == Decimal("0.00")
