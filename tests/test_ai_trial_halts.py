"""#3471 §9 loss halt: per-trade realised + unrealised P&L and the per-leg breach rule (pure)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.services.ai_trial_halts import (
    TRIAL_LEG_CAPITAL_USD,
    TRIAL_LOSS_LIMIT_USD,
    LegLoss,
    LegTrade,
    leg_losses,
    loss_breach,
    trade_pnl_usd,
)
from app.services.ai_trial_policy import FROZEN_CONSTANTS
from app.services.ai_trial_readout import CloseRow

AT = datetime(2026, 10, 1, 15, tzinfo=UTC)


def _slice(pnl: str | None, units: str | None) -> CloseRow:
    return CloseRow(
        executed_at=AT,
        recorded_at=AT,
        realized_pnl_usd=None if pnl is None else Decimal(pnl),
        investment_usd=Decimal("50"),
        fees_usd=Decimal(0),
        price=Decimal("90"),
        stop_rate=None,
        take_rate=None,
        units=None if units is None else Decimal(units),
    )


def _trade(
    *,
    leg: str = "arm",
    status: str = "open",
    usd: bool = True,
    units: str | None = "2",
    bid: str | None = "90",
    closes: tuple[CloseRow, ...] = (),
) -> LegTrade:
    return LegTrade(
        leg=leg,
        status=status,
        usd=usd,
        opened_units=None if units is None else Decimal(units),
        average_price=None if units is None else Decimal("100"),
        bid=None if bid is None else Decimal(bid),
        closes=closes,
    )


def test_the_limit_is_twenty_percent_of_the_fixed_leg_capital() -> None:
    # §8: 4 concurrent × the $250 full ticket; §9 names the $200 limit itself.
    assert (TRIAL_LEG_CAPITAL_USD, TRIAL_LOSS_LIMIT_USD) == (Decimal(1000), Decimal(200))
    assert FROZEN_CONSTANTS["ai_trial_halts.TRIAL_LOSS_HALT_PCT"] == Decimal("20")


@pytest.mark.parametrize(
    ("trade", "expected"),
    [
        # Open, unmarked by any close: 2 units × (90 − 100).
        (_trade(), Decimal("-20")),
        # A partial close realised −5 on 1 unit; the remaining unit is marked at the bid.
        (_trade(closes=(_slice("-5", "1"),)), Decimal("-15")),
        # Every unit closed by slices while the trade is not yet `closed`: realised only.
        (_trade(bid=None, closes=(_slice("-5", "1"), _slice("-7", "1"))), Decimal("-12")),
        # Closed: realised only, whatever the quote says.
        (_trade(status="closed", closes=(_slice("-5", "2"),)), Decimal("-5")),
        # Never filled: nothing at risk.
        (_trade(units=None, bid=None), Decimal(0)),
    ],
)
def test_trade_pnl(trade: LegTrade, expected: Decimal) -> None:
    assert trade_pnl_usd(trade) == expected


@pytest.mark.parametrize(
    "trade",
    [
        _trade(bid=None),
        _trade(bid="0"),
        _trade(usd=False),
        _trade(closes=(_slice(None, "1"),)),
        _trade(closes=(_slice("-5", None),)),
        # History ingest lags the close: a closed trade with no slice is not a zero.
        _trade(status="closed"),
    ],
)
def test_an_unmeasurable_trade_is_none_never_zero(trade: LegTrade) -> None:
    assert trade_pnl_usd(trade) is None


def test_leg_losses_sum_per_leg_and_count_the_unmeasured() -> None:
    losses = leg_losses(
        [
            _trade(),
            _trade(bid="80"),
            _trade(bid=None),
            _trade(leg="control", bid="110"),
        ]
    )
    assert losses == [LegLoss("arm", Decimal("-60"), 1), LegLoss("control", Decimal("20"), 0)]
    assert losses[0].loss_usd == Decimal("60")


def test_the_breach_is_reached_at_the_limit_on_either_leg() -> None:
    below = [LegLoss("arm", Decimal("-199.99"), 0), LegLoss("control", Decimal("500"), 0)]
    assert loss_breach(below) is None
    at = [LegLoss("arm", Decimal("10"), 0), LegLoss("control", Decimal("-200"), 0)]
    assert loss_breach(at) == at[1]
    # An unmeasured trade never pushes a leg over: it contributes nothing.
    assert loss_breach([LegLoss("arm", Decimal("-150"), 3)]) is None
