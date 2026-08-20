from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.providers.market_data import BroadMarketSnapshot, MarketSnapshotInstrument
from app.services.strategy_market_snapshot import BREADTH_VERSION, measure_daily_breadth


def _row(instrument_id: int, change: str | None) -> MarketSnapshotInstrument:
    return MarketSnapshotInstrument(
        instrument_id=instrument_id,
        current_rate=None,
        daily_price_change_pct=None if change is None else Decimal(change),
        weekly_price_change_pct=None,
        monthly_price_change_pct=None,
        is_currently_tradable=None,
        is_exchange_open=None,
        is_active_in_platform=None,
        is_buy_enabled=None,
        industry_id=None,
        sector_id=None,
        popularity_uniques_7d=None,
        traders_7d_change=None,
        buy_holding_pct=None,
        sell_holding_pct=None,
    )


def _snapshot(*rows: MarketSnapshotInstrument) -> BroadMarketSnapshot:
    now = datetime(2026, 8, 12, 12, tzinfo=UTC)
    return BroadMarketSnapshot(now, now, len(rows), 0, rows)


def test_complete_cohort_reports_sign_shares() -> None:
    result = measure_daily_breadth(
        _snapshot(_row(1, "1.5"), _row(2, "-0.2"), _row(3, "0")),
        expected_instrument_ids=(1, 2, 3),
        minimum_coverage=Decimal("0.90"),
    )
    assert result.version == BREADTH_VERSION
    assert result.verdict == "usable"
    assert result.coverage == 1
    assert result.advance_share == Decimal(1) / Decimal(3)
    assert result.decline_share == Decimal(1) / Decimal(3)
    assert result.unchanged_share == Decimal(1) / Decimal(3)


def test_partial_cross_section_refuses_with_exact_denominator() -> None:
    result = measure_daily_breadth(
        _snapshot(_row(1, "1"), _row(2, None), _row(999, "2")),
        expected_instrument_ids=(1, 2, 3),
        minimum_coverage=Decimal("0.90"),
    )
    assert result.verdict == "refused"
    assert result.coverage == Decimal(1) / Decimal(3)
    assert result.refusal_reason == "coverage:1/3<0.9"
    assert result.advance_share == 1


def test_impossible_negative_return_counts_as_missing() -> None:
    result = measure_daily_breadth(
        _snapshot(_row(1, "-100.01"), _row(2, "4")),
        expected_instrument_ids=(1, 2),
        minimum_coverage=Decimal("1"),
    )
    assert result.observed_count == 1
    assert result.verdict == "refused"


@pytest.mark.parametrize("threshold", [Decimal("0"), Decimal("-0.1"), Decimal("1.01")])
def test_invalid_coverage_threshold_rejected(threshold: Decimal) -> None:
    with pytest.raises(ValueError, match="minimum_coverage"):
        measure_daily_breadth(_snapshot(_row(1, "1")), expected_instrument_ids=(1,), minimum_coverage=threshold)


def test_duplicate_expected_ids_rejected() -> None:
    with pytest.raises(ValueError, match="unique"):
        measure_daily_breadth(_snapshot(_row(1, "1")), expected_instrument_ids=(1, 1), minimum_coverage=Decimal("1"))


def test_duplicate_snapshot_ids_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate instrument ID"):
        measure_daily_breadth(
            _snapshot(_row(1, "1"), _row(1, "2")),
            expected_instrument_ids=(1,),
            minimum_coverage=Decimal("1"),
        )
