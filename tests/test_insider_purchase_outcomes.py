from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal

from app.services.insider_purchase_candidate import ClassifiedPurchase, InsiderClass, PurchaseObservation
from app.services.insider_purchase_outcomes import (
    _WINDOW_SQL,
    FirmMonthOutcome,
    FirmMonthSignal,
    FirmMonthWindow,
    MonthlyPortfolioReturn,
    _eligible_window,
    _portfolio_months,
    build_firm_month_signals,
    build_matched_control_signals,
    expected_shortfall_5_pct,
    maximum_drawdown_pct,
    profit_factor,
)


def _classified(insider_class: InsiderClass, *, accession: str, month: int) -> ClassifiedPurchase:
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
    return ClassifiedPurchase(observation=observation, insider_class=insider_class)


def _signal(insider_class: InsiderClass, *, cik: str, instrument_id: int = 7) -> FirmMonthSignal:
    return FirmMonthSignal(
        issuer_cik=cik,
        instrument_id=instrument_id,
        insider_class=insider_class,
        signal_year=2024,
        signal_month=1,
        accession_numbers=("a",),
        disclosed_value=Decimal("1000"),
        latest_acceptance=None,
    )


def _outcome(
    insider_class: InsiderClass,
    *,
    cik: str,
    long_net: float,
    short_net: float,
    weight_value: str,
) -> FirmMonthOutcome:
    return FirmMonthOutcome(
        signal=_signal(insider_class, cik=cik),
        entry_date=date(2024, 2, 1),
        exit_date=date(2024, 2, 29),
        net_return_pct=long_net,
        short_net_return_pct=short_net,
        gross_return_pct=long_net,
        weight_value=Decimal(weight_value),
        median_dollar_volume=Decimal("20000000"),
        market_relative_pct=long_net,
        short_market_relative_pct=short_net,
        sector_relative_pct=long_net,
        short_sector_relative_pct=short_net,
        sector_symbol="XLK",
    )


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
        maximum_single_firm_weight_pct=50,
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


def test_control_never_uses_a_treated_firm_class_month() -> None:
    signals = build_firm_month_signals(
        [
            _classified("opportunistic", accession="a", month=1),
            _classified("opportunistic", accession="b", month=2),
        ]
    )
    controls, counts = build_matched_control_signals(signals)
    assert controls
    assert {item.signal_month for item in controls} == {3}
    assert counts["matched_control_firm_months"] + counts["control_cell_unmatched_signals"] == len(signals)


def test_control_excludes_treated_months_across_both_classes() -> None:
    signals = build_firm_month_signals(
        [
            _classified("opportunistic", accession="a", month=1),
            _classified("routine", accession="b", month=2),
        ]
    )
    controls, _ = build_matched_control_signals(signals)
    assert controls
    assert {item.signal_month for item in controls} == {3}


def test_portfolio_groups_different_usable_entry_days_in_same_target_month() -> None:
    opportunistic = _outcome("opportunistic", cik="1", long_net=3, short_net=-4, weight_value="1")
    routine = _outcome("routine", cik="2", long_net=1, short_net=-2, weight_value="1")
    routine = FirmMonthOutcome(**{**routine.__dict__, "entry_date": date(2024, 2, 2)})
    monthly = _portfolio_months([opportunistic, routine], Counter())
    assert len(monthly) == 1
    assert monthly[0].entry_date == date(2024, 2, 1)


def test_primary_weights_by_disclosed_value_and_charges_short_routine_leg() -> None:
    monthly = _portfolio_months(
        [
            _outcome("opportunistic", cik="1", long_net=0, short_net=-1, weight_value="1"),
            _outcome("opportunistic", cik="2", long_net=8, short_net=-9, weight_value="3"),
            _outcome("routine", cik="3", long_net=4, short_net=-6, weight_value="2"),
        ],
        Counter(),
    )
    assert len(monthly) == 1
    assert monthly[0].opportunistic_pct == 6
    assert monthly[0].routine_pct == 4
    assert monthly[0].spread_pct == 0


def test_partial_frontier_month_and_late_acceptance_are_refused() -> None:
    window = FirmMonthWindow(
        signal=_signal("opportunistic", cik="1"),
        series_id=1,
        target_month_complete=False,
        entry_date=date(2024, 2, 1),
        entry_open=Decimal("10"),
        exit_date=date(2024, 2, 29),
        exit_close=Decimal("11"),
        holding_sessions=20,
        holding_usable=True,
        prior_close=Decimal("10"),
        prior_close_usable=True,
        prior_sessions=20,
        valid_liquidity_sessions=20,
        median_dollar_volume=Decimal("20000000"),
    )
    assert _eligible_window(window) == "incomplete_target_month_at_corpus_frontier"
    late = FirmMonthWindow(
        **{
            **window.__dict__,
            "target_month_complete": True,
            "signal": FirmMonthSignal(
                **{**window.signal.__dict__, "latest_acceptance": datetime(2024, 2, 1, 12, tzinfo=UTC)}
            ),
        }
    )
    assert _eligible_window(late) == "acceptance_not_before_formation"


def test_sql_selects_usable_sessions_and_rejects_partial_frontier_month() -> None:
    assert _WINDOW_SQL.count("WHERE e.return_usable") == 1
    assert "rn <= 20 AND return_usable" in _WINDOW_SQL
    assert "target_end <= date_trunc('month'" in _WINDOW_SQL


def test_portfolio_tail_metrics_use_monthly_spread_distribution() -> None:
    monthly = [_month(10), _month(-5), _month(4), _month(-2)]
    assert profit_factor(monthly) == 14 / 7
    assert expected_shortfall_5_pct(monthly) == -5
    assert maximum_drawdown_pct(monthly) is not None
    assert maximum_drawdown_pct(monthly) < 0  # type: ignore[operator]
