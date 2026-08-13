"""Pure-logic tests for the per-class total-company market cap (#1662).

No DB: the IO is split out (``_build_total_company_cap`` reads; the policy lives in
the pure ``_assemble_total_company_cap`` + ``_sum_class_caps``), so every fail-closed
branch is table-tested here. DB-backed resolver smoke lives in
``tests/test_xbrl_derived_stats.py``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.xbrl_derived_stats import (
    TotalCompanyMarketCap,
    _assemble_total_company_cap,
    _ClassLeg,
    _leg_market_value,
    _sum_class_caps,
)

# A "today" + a fresh FSDS instant ~5 months old (well within the 548-day window),
# and the matching combined instant (delta 0).
_TODAY = date(2026, 6, 17)
_FRESH = date(2024, 12, 31)


def _D(x: object) -> Decimal:
    return Decimal(str(x))


# --- _sum_class_caps (pure arithmetic) ------------------------------------------


def test_sum_class_caps_no_residual() -> None:
    legs = [_ClassLeg(1, _D("100"), _D("10")), _ClassLeg(2, _D("50"), _D("20"))]
    # 100*10 + 50*20 = 2000; residual 0 contributes nothing.
    assert _sum_class_caps(legs, Decimal(0), _D("10")) == _D("2000")


def test_sum_class_caps_with_imputed_residual() -> None:
    legs = [_ClassLeg(1, _D("100"), _D("10")), _ClassLeg(2, _D("50"), _D("20"))]
    # + residual 30 imputed at 10 = 2000 + 300 = 2300.
    assert _sum_class_caps(legs, _D("30"), _D("10")) == _D("2300")


# --- _assemble_total_company_cap (policy + guards) ------------------------------


def _alphabet_legs(*, price_a: str = "369.20", price_c: str = "358.20") -> list[tuple[int, Decimal, Decimal | None]]:
    # Class A (largest) + Class C, GOOGL/GOOG dev shape.
    return [(6434, _D("5835000000"), _D(price_a)), (1002, _D("5515000000"), _D(price_c))]


def test_alphabet_total_with_class_b_residual() -> None:
    got = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=_alphabet_legs(),
        combined_shares=_D("12116000000"),
        combined_as_of=_FRESH,
    )
    assert got is not None
    # 5.835B*369.20 + 5.515B*358.20 + residual(766M)*369.20 (largest leg).
    expected = _D("5835000000") * _D("369.20") + _D("5515000000") * _D("358.20") + _D("766000000") * _D("369.20")
    assert got.value == expected
    assert got.residual_shares == _D("766000000")
    assert got.imputed_residual is True
    assert got.leg_count == 2
    assert isinstance(got, TotalCompanyMarketCap)


def test_identical_regardless_of_which_sibling_drives() -> None:
    # The legs/combined are issuer-level, so the value does not depend on which
    # sibling the endpoint is rendering — GOOGL and GOOG get the same total.
    a = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=_alphabet_legs(),
        combined_shares=_D("12116000000"),
        combined_as_of=_FRESH,
    )
    b = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=list(reversed(_alphabet_legs())),
        combined_shares=_D("12116000000"),
        combined_as_of=_FRESH,
    )
    assert a is not None and b is not None and a.value == b.value


def test_no_residual_when_classes_sum_to_combined() -> None:
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (1, _D("6000000000"), _D("10")),
        (2, _D("6000000000"), _D("10")),
    ]
    got = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=legs,
        combined_shares=_D("12000000000"),
        combined_as_of=_FRESH,
    )
    assert got is not None
    assert got.residual_shares == Decimal(0)
    assert got.imputed_residual is False
    assert got.value == _D("12000000000") * _D("10")


def test_future_period_end_fails_closed() -> None:
    assert (
        _assemble_total_company_cap(
            period_end=date(2027, 1, 1),
            today=_TODAY,
            legs_raw=_alphabet_legs(),
            combined_shares=_D("12116000000"),
            combined_as_of=date(2027, 1, 1),
        )
        is None
    )


def test_single_sibling_fails_closed() -> None:
    legs: list[tuple[int, Decimal, Decimal | None]] = [(6434, _D("5835000000"), _D("369.20"))]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12116000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


def test_far_combined_instant_fails_closed() -> None:
    # combined ~2 years from the FSDS instant → beyond the 400-day delta bound.
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=_alphabet_legs(),
            combined_shares=_D("12116000000"),
            combined_as_of=date(2022, 12, 31),
        )
        is None
    )


def test_unpriced_sibling_fails_closed() -> None:
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (6434, _D("5835000000"), _D("369.20")),
        (1002, _D("5515000000"), None),
    ]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12116000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


def test_nonpositive_price_fails_closed() -> None:
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (6434, _D("5835000000"), _D("369.20")),
        (1002, _D("5515000000"), _D("0")),
    ]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12116000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


def test_stale_class_fails_closed() -> None:
    # period_end > 548 days before today → class_shares_usable freshness rejects.
    stale = date(2024, 1, 1)
    assert (
        _assemble_total_company_cap(
            period_end=stale,
            today=_TODAY,
            legs_raw=[(6434, _D("5835000000"), _D("369.20")), (1002, _D("5515000000"), _D("358.20"))],
            combined_shares=_D("12116000000"),
            combined_as_of=stale,
        )
        is None
    )


def test_class_not_strict_subset_fails_closed() -> None:
    # A class >= combined is structurally implausible (class_shares_usable rejects).
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (6434, _D("12200000000"), _D("369.20")),  # exceeds combined
        (1002, _D("5515000000"), _D("358.20")),
    ]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12116000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


def test_class_sum_overage_fails_closed() -> None:
    # Σ classes materially exceeds combined (> 0.5%) → mismatch, not a residual.
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (6434, _D("6100000000"), _D("369.20")),
        (1002, _D("6100000000"), _D("358.20")),  # Σ = 12.2B vs combined 12.116B (+0.69%)
    ]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12116000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


def test_tiny_overage_within_tolerance_clamps_residual_to_zero() -> None:
    # Σ just over combined (< 0.5%) → treated as rounding noise, residual = 0.
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (6434, _D("6010000000"), _D("10")),
        (1002, _D("6010000000"), _D("10")),  # Σ = 12.02B vs combined 12.0B (+0.17%)
    ]
    got = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=legs,
        combined_shares=_D("12000000000"),
        combined_as_of=_FRESH,
    )
    assert got is not None
    assert got.residual_shares == Decimal(0)
    assert got.value == _D("12020000000") * _D("10")


def test_residual_too_large_fails_closed() -> None:
    # Mapped classes are only ~50% of combined → residual 50% > 25% cap → suppress.
    legs: list[tuple[int, Decimal, Decimal | None]] = [
        (1, _D("3000000000"), _D("10")),
        (2, _D("3000000000"), _D("10")),
    ]
    assert (
        _assemble_total_company_cap(
            period_end=_FRESH,
            today=_TODAY,
            legs_raw=legs,
            combined_shares=_D("12000000000"),
            combined_as_of=_FRESH,
        )
        is None
    )


# --- per-class float legs (#1665) -----------------------------------------------


def _alphabet_total() -> TotalCompanyMarketCap:
    got = _assemble_total_company_cap(
        period_end=_FRESH,
        today=_TODAY,
        legs_raw=_alphabet_legs(),
        combined_shares=_D("12116000000"),
        combined_as_of=_FRESH,
    )
    assert got is not None
    return got


def test_legs_carried_and_residual_excluded() -> None:
    # The two PRICED traded legs survive on the return; the imputed untraded
    # residual (Class B, 766M) is NOT a leg — it has no instrument.
    got = _alphabet_total()
    assert {leg.instrument_id for leg in got.legs} == {6434, 1002}
    assert len(got.legs) == got.leg_count == 2
    by_id = {leg.instrument_id: leg for leg in got.legs}
    assert by_id[6434].shares == _D("5835000000") and by_id[6434].price == _D("369.20")
    assert by_id[1002].shares == _D("5515000000") and by_id[1002].price == _D("358.20")


def test_legs_reconcile_to_value_minus_residual() -> None:
    # Σ(leg shares × price) == total.value - residual valued at the largest leg's
    # price (the imputation). Each leg value is strictly positive and ≤ the total.
    got = _alphabet_total()
    impute_price = max(got.legs, key=lambda leg: leg.shares).price
    sum_legs = sum((leg.shares * leg.price for leg in got.legs), Decimal(0))
    assert sum_legs == got.value - got.residual_shares * impute_price
    for leg in got.legs:
        assert leg.shares * leg.price > 0
        assert leg.shares * leg.price <= got.value


def test_leg_market_value_picks_viewed_instrument() -> None:
    # Per-class float = the VIEWED sibling's own leg, NOT identical across siblings.
    got = _alphabet_total()
    googl = _leg_market_value(got, 6434)
    goog = _leg_market_value(got, 1002)
    assert googl == _D("5835000000") * _D("369.20")  # GOOGL Class A
    assert goog == _D("5515000000") * _D("358.20")  # GOOG Class C
    # …and each is a strict subset of (less than) the whole-company value.
    assert googl is not None and googl < got.value


def test_leg_market_value_absent_instrument_is_none() -> None:
    # A same-CIK sibling that is not itself a priced leg (e.g. a .US listing with
    # no FSDS class row) has no per-class float → honest None.
    assert _leg_market_value(_alphabet_total(), 99999) is None


def test_leg_market_value_duplicate_id_takes_first_no_double_count() -> None:
    # The FSDS PK (instrument_id, period_end) + a single period_end make a
    # duplicate ID impossible in practice; defensively the pick takes the FIRST
    # match and never sums two legs into a doubled value.
    total = TotalCompanyMarketCap(
        value=_D("3000"),
        period_end=_FRESH,
        combined_shares=_D("300"),
        sum_mapped_shares=_D("300"),
        residual_shares=Decimal(0),
        imputed_residual=False,
        leg_count=2,
        legs=(_ClassLeg(7, _D("100"), _D("10")), _ClassLeg(7, _D("50"), _D("20"))),
    )
    assert _leg_market_value(total, 7) == _D("1000")  # first leg only (100×10), not 2000
