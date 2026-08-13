"""Pure-logic tests for #1745 — the as-of freshness of the per-period
total-company cap, and the USD-base FX cross rate. No DB; the DB read path is
covered end-to-end by the dev-verify recorded in the PR."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.fx_history import fx_cross_rate
from app.services.xbrl_derived_stats import _assemble_total_company_cap


def _legs() -> list[tuple[int, Decimal, Decimal | None]]:
    # Two priced classes, each a clean subset of the combined count.
    return [(1, Decimal("60"), Decimal("100")), (2, Decimal("30"), Decimal("90"))]


class TestAssembleAsOf:
    """``as_of`` lets a historical period be judged fresh vs the period it prices,
    not vs today — the #1745 fix (Codex ckpt-1 HIGH-2)."""

    def test_old_period_fails_without_as_of(self) -> None:
        # Class data from 3 years before "today" — stale by the 548-day window.
        old = date(2022, 12, 31)
        cap = _assemble_total_company_cap(
            period_end=old,
            today=date(2026, 6, 27),
            legs_raw=_legs(),
            combined_shares=Decimal("100"),
            combined_as_of=old,
        )
        assert cap is None  # stale vs today

    def test_old_period_passes_with_matching_as_of(self) -> None:
        old = date(2022, 12, 31)
        cap = _assemble_total_company_cap(
            period_end=old,
            today=date(2026, 6, 27),
            as_of=old,  # judged fresh relative to the period it prices
            legs_raw=_legs(),
            combined_shares=Decimal("100"),
            combined_as_of=old,
        )
        assert cap is not None
        # 60×100 + 30×90 + residual 10 × impute(largest leg price=100) = 6000+2700+1000
        assert cap.value == Decimal("9700")
        assert cap.residual_shares == Decimal("10")

    def test_future_period_still_rejected_even_with_as_of(self) -> None:
        future = date(2099, 1, 1)
        cap = _assemble_total_company_cap(
            period_end=future,
            today=date(2026, 6, 27),
            as_of=future,
            legs_raw=_legs(),
            combined_shares=Decimal("100"),
            combined_as_of=future,
        )
        assert cap is None  # future-date guard uses real today, not as_of


class TestFxCrossRate:
    """USD-base cross: ``rates[(USD, X)]`` = X per 1 USD; USD itself = 1."""

    def test_reported_to_trading_cross(self) -> None:
        # GBP 0.80/USD, EUR 0.90/USD. reported=GBP → trading=EUR:
        # (EUR per USD) / (GBP per USD) = 0.90 / 0.80.
        rates = {("USD", "GBP"): Decimal("0.80"), ("USD", "EUR"): Decimal("0.90")}
        assert fx_cross_rate(rates, from_ccy="GBP", to_ccy="EUR") == Decimal("0.90") / Decimal("0.80")

    def test_usd_is_unity_either_side(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.80")}
        assert fx_cross_rate(rates, from_ccy="GBP", to_ccy="USD") == Decimal(1) / Decimal("0.80")
        assert fx_cross_rate(rates, from_ccy="USD", to_ccy="GBP") == Decimal("0.80")
        assert fx_cross_rate(rates, from_ccy="USD", to_ccy="USD") == Decimal(1)

    def test_unsupported_currency_fails_closed(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.80")}
        assert fx_cross_rate(rates, from_ccy="GBP", to_ccy="JPY") is None
        assert fx_cross_rate(rates, from_ccy="ZZZ", to_ccy="GBP") is None

    def test_zero_from_rate_fails_closed(self) -> None:
        rates = {("USD", "GBP"): Decimal("0"), ("USD", "EUR"): Decimal("0.90")}
        assert fx_cross_rate(rates, from_ccy="GBP", to_ccy="EUR") is None
