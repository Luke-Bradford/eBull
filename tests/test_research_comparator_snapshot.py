"""Pure contract tests for the bounded recent comparator snapshot (#2482)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.research_comparator_snapshot import (
    COMPARATOR_SYMBOLS,
    FROZEN_FRONTIER,
    ComparatorBar,
    ComparatorUnavailable,
    align_exact_sessions,
    build_snapshot,
    fingerprint_series,
    measure_overlap,
)


def _bar(**overrides: object) -> ComparatorBar:
    values: dict[str, object] = {
        "bar_date": FROZEN_FRONTIER,
        "open": Decimal("100.00"),
        "high": Decimal("102.00"),
        "low": Decimal("99.00"),
        "close": Decimal("101.00"),
        "volume": Decimal("1000.00"),
    }
    values.update(overrides)
    return ComparatorBar(**values)  # type: ignore[arg-type]


def _members(*, reverse: bool = False) -> list[tuple[str, int, tuple[ComparatorBar, ...]]]:
    rows = [(symbol, 1000 + index, (_bar(),)) for index, symbol in enumerate(COMPARATOR_SYMBOLS)]
    return list(reversed(rows)) if reverse else rows


def test_snapshot_identity_is_independent_of_input_iteration_order() -> None:
    assert build_snapshot(_members()).sha256 == build_snapshot(_members(reverse=True)).sha256


def test_numeric_scale_does_not_move_the_fingerprint() -> None:
    left = fingerprint_series("SPY", 3000, (_bar(close=Decimal("101.000000")),))
    right = fingerprint_series("SPY", 3000, (_bar(close=Decimal("101")),))
    assert left == right


def test_source_instrument_mapping_is_identity_bearing() -> None:
    bars = (_bar(),)
    assert fingerprint_series("SPY", 3000, bars) != fingerprint_series("SPY", 3001, bars)


def test_missing_member_is_refused_not_silently_smaller() -> None:
    with pytest.raises(ValueError, match="incomplete"):
        build_snapshot(_members()[:-1])


def test_future_bar_is_refused() -> None:
    members = _members()
    symbol, instrument_id, _ = members[0]
    members[0] = (symbol, instrument_id, (_bar(bar_date=FROZEN_FRONTIER + timedelta(days=1)),))
    with pytest.raises(ValueError, match="beyond frozen frontier"):
        build_snapshot(members)


def test_member_must_reach_the_exact_frontier() -> None:
    members = _members()
    symbol, instrument_id, _ = members[0]
    members[0] = (symbol, instrument_id, (_bar(bar_date=FROZEN_FRONTIER - timedelta(days=1)),))
    with pytest.raises(ValueError, match="latest bar"):
        build_snapshot(members)


def test_invalid_ohlc_envelope_is_refused() -> None:
    members = _members()
    symbol, instrument_id, _ = members[0]
    members[0] = (symbol, instrument_id, (_bar(high=Decimal("100"), close=Decimal("101")),))
    with pytest.raises(ValueError, match="invalid OHLC envelope"):
        build_snapshot(members)


def test_negative_volume_is_refused_but_missing_volume_is_allowed() -> None:
    missing = _members()
    symbol, instrument_id, _ = missing[0]
    missing[0] = (symbol, instrument_id, (_bar(volume=None),))
    assert build_snapshot(missing).row_count == len(COMPARATOR_SYMBOLS)

    negative = _members()
    symbol, instrument_id, _ = negative[0]
    negative[0] = (symbol, instrument_id, (_bar(volume=Decimal("-1")),))
    with pytest.raises(ValueError, match="negative or non-finite volume"):
        build_snapshot(negative)

    fractional = _members()
    symbol, instrument_id, _ = fractional[0]
    fractional[0] = (symbol, instrument_id, (_bar(volume=Decimal("1.25")),))
    with pytest.raises(ValueError, match="fractional volume cannot be stored losslessly"):
        build_snapshot(fractional)


def _overlap_series(*, ratio: Decimal, count: int = 501) -> tuple[dict[date, Decimal], dict[date, Decimal]]:
    first = date(2022, 1, 1)
    legacy = {
        first + timedelta(days=index): Decimal(100 + index) + Decimal(index % 3) / Decimal("10")
        for index in range(count)
    }
    recent = {bar_date: close * ratio for bar_date, close in legacy.items()}
    return recent, legacy


def test_overlap_accounts_for_the_official_post_legacy_split() -> None:
    recent, legacy = _overlap_series(ratio=Decimal("0.49925"))
    measurement = measure_overlap("XLK", recent, legacy)
    assert measurement.expected_split_factor == Decimal("0.5")
    assert measurement.median_normalised_level_ratio == pytest.approx(0.9985)
    assert measurement.return_correlation == pytest.approx(1.0)


def test_overlap_refuses_an_unexplained_level_basis_change() -> None:
    recent, legacy = _overlap_series(ratio=Decimal("0.5"))
    with pytest.raises(ValueError, match="normalised median level ratio"):
        measure_overlap("SPY", recent, legacy)


def test_overlap_refuses_a_too_short_comparison() -> None:
    recent, legacy = _overlap_series(ratio=Decimal("0.9985"), count=499)
    with pytest.raises(ValueError, match="only 499 overlapping sessions"):
        measure_overlap("SPY", recent, legacy)


def test_exact_session_alignment_refuses_instead_of_forward_filling() -> None:
    friday = date(2026, 7, 3)
    monday = date(2026, 7, 6)
    closes = {friday: Decimal("100")}
    with pytest.raises(ComparatorUnavailable, match="2026-07-06"):
        align_exact_sessions("SPY", (friday, monday), closes)
