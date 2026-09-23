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


class TestMinorUnitGbx:
    """#3322: LSE pence lines are labelled GBX; 1 GBX = 0.01 GBP."""

    rates = {("USD", "GBP"): Decimal("0.75")}

    def test_gbx_to_gbp_needs_no_rate(self) -> None:
        assert convert(Decimal("12678"), "GBX", "GBP", {}) == Decimal("126.78")

    def test_gbp_to_gbx_multiplies(self) -> None:
        assert convert(Decimal("126.78"), "GBP", "GBX", {}) == Decimal("12678")

    def test_gbx_to_gbx_is_identity(self) -> None:
        assert convert(Decimal("5"), "GBX", "GBX", {}) == Decimal("5")

    def test_gbx_to_usd_divides_by_100_then_converts(self) -> None:
        # 7500 pence = £75 = $100 at USD→GBP 0.75 (inverse path).
        assert convert(Decimal("7500"), "GBX", "USD", self.rates) == pytest.approx(Decimal("100"))

    def test_usd_to_gbx_multiplies_by_100(self) -> None:
        assert convert(Decimal("100"), "USD", "GBX", self.rates) == Decimal("7500")

    def test_gbx_without_a_gbp_pair_raises(self) -> None:
        with pytest.raises(FxRateNotFound):
            convert(Decimal("1"), "GBX", "EUR", self.rates)

    def test_quote_fields_gbx_to_usd(self) -> None:
        out = convert_quote_fields(
            Decimal("7500"), Decimal("7515"), None, native_ccy="GBX", display_ccy="USD", rates=self.rates
        )
        assert out is not None
        assert out[0] == pytest.approx(Decimal("100"))
        assert out[1] == pytest.approx(Decimal("100.2"))
        assert out[2] is None

    def test_quote_fields_gbx_to_gbp(self) -> None:
        out = convert_quote_fields(
            Decimal("100"), Decimal("101"), Decimal("100.5"), native_ccy="GBX", display_ccy="GBP", rates={}
        )
        assert out == (Decimal("1.00"), Decimal("1.01"), Decimal("1.005"))
