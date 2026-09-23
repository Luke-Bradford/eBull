"""#3334 item 3 — time-weighted pot return over the wealth series (pure)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from app.services.strategy_wealth import StrategyWealthPoint, time_weighted_returns

_D0 = date(2026, 9, 1)


def _series(*rows: tuple[str, str | None]) -> list[StrategyWealthPoint]:
    """``(principal, total_pnl)`` per day; ``None`` pnl = incomplete point."""
    points: list[StrategyWealthPoint] = []
    previous = Decimal("0")
    for index, (principal_text, pnl_text) in enumerate(rows):
        principal = Decimal(principal_text)
        pnl = None if pnl_text is None else Decimal(pnl_text)
        points.append(
            StrategyWealthPoint(
                date=_D0 + timedelta(days=index),
                principal=principal,
                external_flow=principal - previous,
                realised_pnl=pnl,
                unrealised_pnl=None if pnl is None else Decimal("0"),
                total_pnl=pnl,
                pot_value=None if pnl is None else principal + pnl,
                complete=pnl is not None,
                incomplete_reasons=() if pnl is not None else ("owned_position_mark_missing",),
            )
        )
        previous = principal
    return points


def _day(index: int) -> date:
    return _D0 + timedelta(days=index)


def test_inception_from_zero_dates_the_return_from_the_funding_close() -> None:
    result = time_weighted_returns(_series(("0", "0"), ("0", "0"), ("500", "5"), ("500", "10")))
    assert [p.period_return for p in result.points[:2]] == [None, None]
    assert result.points[2].period_return == Decimal("0.01")
    assert result.points[3].cumulative_return == Decimal("0.02")
    assert result.total_return_available
    assert result.return_since == _day(2)
    assert result.unavailable_reason is None


def test_a_top_up_is_not_a_gain() -> None:
    # 100 → 110 (+10%), then +100 funding with P&L unchanged: 0% that day.
    result = time_weighted_returns(_series(("100", "0"), ("100", "10"), ("200", "10")))
    assert result.points[2].period_return == Decimal("0")
    assert result.points[2].cumulative_return == Decimal("0.1")
    assert result.return_since == _day(0)


def test_a_withdrawal_is_not_a_loss() -> None:
    # 200 → 220 (+10%), withdraw 100 with P&L unchanged: 120 / (220 − 100) = 0%.
    result = time_weighted_returns(_series(("200", "0"), ("200", "20"), ("100", "20")))
    assert result.points[2].period_return == Decimal("0")
    assert result.points[2].cumulative_return == Decimal("0.1")


def test_a_no_flow_gap_is_bridged_exactly() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("100", None), ("100", "10")))
    assert result.points[1].period_return is None
    assert result.points[2].period_return == Decimal("0.1")
    assert result.points[2].period_start == _day(0)
    assert result.points[2].cumulative_return == Decimal("0.1")


def test_a_flow_inside_a_gap_breaks_the_chain_but_later_days_still_return() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("200", None), ("200", "10"), ("200", "20")))
    assert result.points[2].period_return is None
    assert result.points[2].cumulative_return is None
    assert result.points[3].period_return == Decimal("220") / Decimal("210") - 1
    assert result.points[3].cumulative_return is None
    assert not result.total_return_available
    assert result.return_since is None
    assert result.unavailable_reason == "chain_broken"


def test_a_flow_on_the_point_that_closes_a_gap_also_breaks() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("100", None), ("200", "10")))
    assert result.points[2].period_return is None
    assert result.unavailable_reason == "chain_broken"


def test_a_net_zero_pair_of_flows_inside_a_gap_still_breaks() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("200", None), ("100", None), ("100", "5")))
    assert result.points[3].period_return is None
    assert result.unavailable_reason == "chain_broken"


def test_a_pot_at_or_below_zero_breaks() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("100", "-100"), ("200", "-100")))
    assert result.points[1].period_return is None
    assert result.unavailable_reason == "chain_broken"


def test_refunding_a_pot_that_went_to_zero_does_not_resume() -> None:
    result = time_weighted_returns(_series(("100", "0"), ("100", "0"), ("0", "0"), ("100", "1")))
    assert result.points[3].period_return is None
    assert result.unavailable_reason == "chain_broken"


def test_leading_incomplete_points_are_not_a_gap() -> None:
    result = time_weighted_returns(_series(("100", None), ("100", "0"), ("100", "1")))
    assert result.points[2].period_return == Decimal("0.01")
    assert result.return_since == _day(1)


def test_an_unfunded_pot_publishes_no_return() -> None:
    result = time_weighted_returns(_series(("0", "0"), ("0", "0")))
    assert not result.total_return_available
    assert result.unavailable_reason == "unfunded"


def test_empty_and_single_point_series() -> None:
    assert time_weighted_returns([]).unavailable_reason == "no_complete_point"
    assert time_weighted_returns(_series(("100", None))).unavailable_reason == "no_complete_point"
    single = time_weighted_returns(_series(("100", "3")))
    assert not single.total_return_available
    assert single.unavailable_reason == "insufficient_history"


def test_availability_describes_the_last_complete_point() -> None:
    # A trailing incomplete point does not hide the return the strip is showing.
    result = time_weighted_returns(_series(("100", "0"), ("100", "10"), ("100", None)))
    assert result.total_return_available
    assert result.points[1].cumulative_return == Decimal("0.1")
