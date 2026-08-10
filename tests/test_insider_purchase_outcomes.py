from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from app.services.insider_purchase_candidate import ClassifiedPurchase, PurchaseObservation
from app.services.insider_purchase_outcomes import (
    MonthlyPortfolioReturn,
    build_firm_month_signals,
    build_matched_control_signals,
    expected_shortfall_5_pct,
    maximum_drawdown_pct,
    profit_factor,
)


def _classified(insider_class: str, *, accession: str, month: int) -> ClassifiedPurchase:
    observation = PurchaseObservation(
        issuer_cik="0000000001",
        issuer_symbol="ABC",
        accession_number=accession,
        filer_cik="0000000002",
        transaction_date=date(2024, month, 2),
        filed_date=date(2024, month, 4),
        disclosed_value=Decimal("1000"),
        accepted_at=datetime(2024, month, 4, 20, tzinfo=UTC),
        instrument_id=7,
    )
    return ClassifiedPurchase(observation=observation, insider_class=insider_class)  # type: ignore[arg-type]


def _month(value: float) -> MonthlyPortfolioReturn:
    return MonthlyPortfolioReturn(
        entry_date=date(2024, 1, 2),
        opportunistic_pct=value,
        routine_pct=0,
        spread_pct=value,
        equal_weight_spread_pct=value,
        market_relative_spread_pct=None,
        sector_relative_spread_pct=None,
        opportunistic_firms=1,
        routine_firms=1,
        unique_firms=2,
        minimum_median_dollar_volume=Decimal("10000000"),
    )


def test_firm_month_signals_deduplicate_accessions_but_preserve_classes() -> None:
    signals = build_firm_month_signals(
        [
            _classified("opportunistic", accession="a", month=1),
            _classified("opportunistic", accession="b", month=1),
            _classified("routine", accession="c", month=1),
        ]
    )
    assert len(signals) == 2
    opportunistic = next(item for item in signals if item.insider_class == "opportunistic")
    assert opportunistic.accession_numbers == ("a", "b")


def test_control_moves_within_same_quarter_and_is_deterministic() -> None:
    signal = build_firm_month_signals([_classified("opportunistic", accession="a", month=2)])[0]
    first, _ = build_matched_control_signals([signal])
    second, _ = build_matched_control_signals([signal])
    assert first == second
    assert first[0].signal_month in {1, 3}
    assert first[0].signal_month != signal.signal_month


def test_portfolio_tail_metrics_use_monthly_spread_distribution() -> None:
    monthly = [_month(10), _month(-5), _month(4), _month(-2)]
    assert profit_factor(monthly) == 14 / 7
    assert expected_shortfall_5_pct(monthly) == -5
    assert maximum_drawdown_pct(monthly) is not None
    assert maximum_drawdown_pct(monthly) < 0  # type: ignore[operator]
