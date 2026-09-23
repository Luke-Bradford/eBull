"""#3322: classify an instrument's price currency from the broker's conversion rate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.instrument_price_currency import (
    MAX_AGE,
    MAX_FUTURE_SKEW,
    FxReference,
    classify_price_currency,
)
from app.services.tax_ledger import _load_fx_rate

NOW = datetime(2026, 9, 23, 12, tzinfo=UTC)
# Rates from the 2026-09-23 census (live_fx_rates USD→GBP 0.74832, USD→EUR 0.87237).
FX = FxReference(
    usd_per_unit={
        "USD": Decimal(1),
        "EUR": 1 / Decimal("0.87237"),
        "GBP": 1 / Decimal("0.74832"),
        "GBX": 1 / Decimal("0.74832") / 100,
    },
    quoted_at=NOW - timedelta(hours=12),
)


@pytest.mark.parametrize(
    ("conversion_rate", "expected"),
    [
        (Decimal("0.013303"), "GBX"),  # attended IUSA.L close, #3007
        (Decimal("1.0"), "USD"),  # CSPX.L-style USD line on the LSE
        (Decimal("1.3295"), "GBP"),
        (Decimal("1.1466"), "EUR"),
    ],
)
def test_fresh_quote_matches_exactly_one_currency(conversion_rate: Decimal, expected: str) -> None:
    assert classify_price_currency(conversion_rate, NOW - timedelta(hours=1), FX, NOW) == (expected, "derived")


def test_stale_quote_never_reclassifies() -> None:
    # A dead listing keeps the FX rate from when it died (census: 82 GBP lines).
    stale = NOW - MAX_AGE - timedelta(seconds=1)
    assert classify_price_currency(Decimal("0.0133"), stale, FX, NOW) == (None, "stale_quote")


def test_quote_slightly_ahead_of_our_clock_is_accepted() -> None:
    ahead = NOW + MAX_FUTURE_SKEW - timedelta(seconds=1)
    assert classify_price_currency(Decimal("0.0133"), ahead, FX, NOW) == ("GBX", "derived")


def test_quote_far_in_the_future_is_refused() -> None:
    ahead = NOW + MAX_FUTURE_SKEW + timedelta(seconds=1)
    assert classify_price_currency(Decimal("0.0133"), ahead, FX, NOW) == (None, "stale_quote")


def test_stale_fx_reference_refuses_everything() -> None:
    old_fx = FxReference(usd_per_unit=FX.usd_per_unit, quoted_at=NOW - MAX_AGE - timedelta(seconds=1))
    assert classify_price_currency(Decimal("1.0"), NOW, old_fx, NOW) == (None, "stale_fx")


@pytest.mark.parametrize("rate", [None, Decimal(0), Decimal(-1), Decimal("NaN"), Decimal("Infinity")])
def test_unusable_rate_is_refused(rate: Decimal | None) -> None:
    assert classify_price_currency(rate, NOW, FX, NOW) == (None, "bad_rate")


def test_no_candidate_within_tolerance() -> None:
    # SEK ≈ 0.101 USD matches no candidate: the SEK venue is not reclassified.
    assert classify_price_currency(Decimal("0.101"), NOW, FX, NOW) == (None, "no_match")


def test_two_candidates_within_tolerance_is_ambiguous() -> None:
    close = FxReference(usd_per_unit={"USD": Decimal(1), "XXA": Decimal("1.01")}, quoted_at=NOW)
    assert classify_price_currency(Decimal("1.005"), NOW, close, NOW) == (None, "ambiguous")


def test_tax_fx_treats_gbx_as_gbp_without_a_query() -> None:
    # A GBX label must not abort the tax ingest batch on a missing GBX row.
    assert _load_fx_rate(None, NOW.date(), "GBX") == Decimal("1")  # type: ignore[arg-type]


@pytest.mark.parametrize("rate", [Decimal(0), Decimal(-1), Decimal("NaN")])
def test_native_amount_refuses_an_unusable_open_rate(rate: Decimal) -> None:
    from app.services.portfolio_value_history import native_amount

    assert native_amount(Decimal("25"), rate) is None


def test_native_amount_uses_the_broker_open_rate() -> None:
    from app.services.portfolio_value_history import native_amount

    # IUSA.L: $25 at 0.013303 → 1879.28p.
    assert native_amount(Decimal("25"), Decimal("0.013303")) == pytest.approx(Decimal("1879.2753"), rel=Decimal("1e-6"))
