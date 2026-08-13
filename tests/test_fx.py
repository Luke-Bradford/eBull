"""Tests for app.services.fx — currency conversion logic."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.fx import FxRateNotFound, convert, convert_quote_fields


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


class TestConvertQuoteFields:
    """Quote-triple conversion is the SSE hot-path. One direct, one
    inverse, one same-ccy passthrough, one missing-rate, plus
    last=None pass-through cover the surface."""

    def test_same_ccy_passes_through_unchanged(self) -> None:
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("101"),
            Decimal("100.5"),
            native_ccy="USD",
            display_ccy="USD",
            rates={},
        )
        assert result == (Decimal("100"), Decimal("101"), Decimal("100.5"))

    def test_direct_rate(self) -> None:
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("200"),
            Decimal("150"),
            native_ccy="USD",
            display_ccy="GBP",
            rates={("USD", "GBP"): Decimal("0.75")},
        )
        assert result == (Decimal("75.00"), Decimal("150.00"), Decimal("112.50"))

    def test_inverse_rate(self) -> None:
        # USD→GBP unavailable; GBP→USD = 1.25 means USD→GBP = 1/1.25 = 0.8
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("200"),
            None,
            native_ccy="USD",
            display_ccy="GBP",
            rates={("GBP", "USD"): Decimal("1.25")},
        )
        assert result is not None
        bid, ask, last = result
        assert bid == Decimal("80")
        assert ask == Decimal("160")
        assert last is None

    def test_last_none_preserved(self) -> None:
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("101"),
            None,
            native_ccy="USD",
            display_ccy="GBP",
            rates={("USD", "GBP"): Decimal("0.75")},
        )
        assert result is not None
        assert result[2] is None

    def test_inverse_non_terminating_reciprocal_matches_convert(self) -> None:
        """Inverse path uses Decimal(1)/inv internally. A
        non-terminating reciprocal (e.g. 1/3) tests whether the
        helper picks up the same Decimal-context rounding behavior
        as the canonical ``convert`` function — divergence between
        the two would silently break parity for triple-conversion
        callers."""
        rates = {("GBP", "USD"): Decimal("3")}  # 1/3 doesn't terminate
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("100"),
            Decimal("100"),
            native_ccy="USD",
            display_ccy="GBP",
            rates=rates,
        )
        assert result is not None
        bid, ask, last = result
        # Compare against the canonical helper to confirm parity.
        canonical = convert(Decimal("100"), "USD", "GBP", rates)
        assert bid == canonical
        assert ask == canonical
        assert last == canonical

    def test_missing_rate_returns_none(self) -> None:
        result = convert_quote_fields(
            Decimal("100"),
            Decimal("101"),
            None,
            native_ccy="USD",
            display_ccy="JPY",
            rates={},
        )
        assert result is None
