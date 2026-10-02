"""#2842 slice 5c-ii-a — the §7.4 loss check's arithmetic (pure)."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerDirectPositionInvestment,
    BrokerInstrumentInvestment,
)
from app.services import ranking_pot_loss as rl

D = Decimal
NOW = datetime(2026, 11, 2, 15, 30, tzinfo=UTC)


def _mark(pid: int, pnl: str, *, units: str = "1", fees: str | None = "0", is_buy: bool = True, altered: bool = False):
    return BrokerDirectPositionInvestment(
        position_id=pid,
        instrument_id=2842,
        is_buy=is_buy,
        units=D(units),
        amount=D(40),
        unrealized_pnl=D(pnl),
        market_value=D(40) + D(pnl),
        is_partially_altered=altered,
        close_rate=D(100),
        close_conversion_rate=D(1),
        asset_currency_id=1,
        total_fees=None if fees is None else D(fees),
    )


def _close(net: str | None, *, fees: str | None = "0", units: str | None = "1") -> rl.PotClose:
    return rl.PotClose(
        None if net is None else D(net), None if fees is None else D(fees), None if units is None else D(units)
    )


def _trade(*positions: rl.PotPosition, status: str = "open", amount: str = "40", stop: str = "94") -> rl.PotExposure:
    # Bound = 40 × (6/100 + 1/100) = 2.80.
    return rl.PotExposure(1, status, D(amount), D(100), D(stop), positions)


def _pos(pid: int, *closes: rl.PotClose, active: bool = True, open_units: str | None = "1") -> rl.PotPosition:
    return rl.PotPosition(pid, active, None if open_units is None else D(open_units), closes)


def test_stop_bound_is_stop_distance_plus_cost_cap_rounded_up() -> None:
    assert rl.stop_bound(D(40), D(100), D(94)) == D("2.80")
    assert rl.stop_bound(D(33), D(97), D("91.123457")) == D("2.33")  # 2.329… rounded UP
    assert rl.stop_bound(D(40), D(0), D(94)) is None
    assert rl.stop_bound(D("NaN"), D(100), D(94)) is None


@pytest.mark.parametrize(
    ("exposure", "marks", "net"),
    [
        # Marked open position: the broker's unrealised P&L.
        (_trade(_pos(1)), {1: _mark(1, "-3.5")}, D("-3.5")),
        # Positive totalFees is a charge, a negative one (a refund, per the portal) is not credited.
        (_trade(_pos(1)), {1: _mark(1, "-3.5", fees="0.25")}, D("-3.75")),
        (_trade(_pos(1)), {1: _mark(1, "-3.5", fees="-0.25")}, D("-3.5")),
        # No ownership row yet (submitted / uncertain): the stop bound. A failed trade with none committed nothing.
        (_trade(status="submitted"), {}, D("-2.80")),
        (_trade(status="failed"), {}, D(0)),
        # Fully realised: closes cover the open units; |fees_usd| charged whatever its sign.
        (_trade(_pos(1, _close("7.5", fees="-0.1"), active=False), status="closed"), {}, D("7.4")),
        # Released with no close event yet, or an active position missing from the snapshot: closes + bound.
        (_trade(_pos(1, active=False), status="closed"), {}, D("-2.80")),
        (_trade(_pos(1)), {}, D("-2.80")),
        # A missing final close: closes cover 0.4 of 1 unit → the bound on top of the partial's profit.
        (_trade(_pos(1, _close("5", units="0.4"), active=False), status="closed"), {}, D("2.20")),
        # A stale mark beside an ingested full close: units do not reconcile → bound, never a double-counted gain.
        (_trade(_pos(1, _close("5"), active=False), status="closed"), {1: _mark(1, "5")}, D("2.20")),
        # …while an unreconciled mark's LOSS still counts.
        (_trade(_pos(1, _close("5"), active=False), status="closed"), {1: _mark(1, "-5")}, D("-2.80")),
        # A partial close and the remaining units' mark reconcile exactly.
        (_trade(_pos(1, _close("-1", units="0.5"))), {1: _mark(1, "-2", units="0.5")}, D(-3)),
        # A released ownership still marked by the broker is still exposure: its mark counts.
        (_trade(_pos(1, active=False)), {1: _mark(1, "-4")}, D(-4)),
        # A fresh fill whose open event is not ingested yet: an unaltered mark is whole…
        (_trade(_pos(1, open_units=None)), {1: _mark(1, "-4")}, D(-4)),
        # …a partially closed one is not (its close has not landed): its loss side plus the bound.
        (_trade(_pos(1, open_units=None)), {1: _mark(1, "3", altered=True)}, D("-2.80")),
        # A mark short of the open units with no close ingested: the same.
        (_trade(_pos(1)), {1: _mark(1, "-1", units="0.5")}, D("-3.80")),
    ],
)
def test_net_pnl(exposure: rl.PotExposure, marks: dict, net: Decimal) -> None:
    assert rl.pot_net_pnl([exposure], marks) == net


@pytest.mark.parametrize(
    ("exposures", "marks"),
    [
        ([_trade(_pos(1, _close(None), active=False))], {}),  # netProfit NULL
        ([_trade(_pos(1, _close("1", fees=None), active=False))], {}),  # fees_usd NULL
        ([_trade(_pos(1, _close("1", units=None), active=False))], {}),  # close units NULL
        ([_trade(_pos(1))], {1: _mark(1, "1", fees=None)}),  # totalFees NULL
        ([_trade(_pos(1))], {1: _mark(1, "1", is_buy=False)}),  # the pot is long only
        ([_trade(_pos(1))], {1: _mark(1, "NaN")}),
        ([_trade(_pos(1)), replace(_trade(_pos(1)), strategy_trade_id=2)], {1: _mark(1, "1")}),  # owned twice
        ([_trade(amount="NaN")], {}),
        ([_trade(_pos(1, _close("9", units="2"), active=False))], {}),  # over-closed
    ],
)
def test_data_defects_are_unavailable(exposures: list[rl.PotExposure], marks: dict) -> None:
    assert rl.pot_net_pnl(exposures, marks) is None


def test_the_limit_is_inclusive_at_minus_twenty_percent() -> None:
    assert rl.breached(D(-2400), D(12000)) is True
    assert rl.breached(D("-2399.99"), D(12000)) is False


def _risk(*rows: BrokerDirectPositionInvestment, currency: int | None = 1, counted: int | None = None):
    count = len(rows) if counted is None else counted
    return BrokerAccountRiskSnapshot(
        available_cash=D(600),
        total_invested=D(0),
        unrealized_pnl=D(0),
        equity=D(1000),
        # A mirror-only instrument carries no direct count.
        instrument_investments=(
            BrokerInstrumentInvestment(2842, D(40), D(40), count, 0),
            BrokerInstrumentInvestment(2843, D(40), D(0), 0, 0),
        ),
        observed_at=NOW,
        raw_payload={},
        direct_positions=rows,
        account_currency_id=currency,
    )


def test_snapshot_marks_refuse_an_unusable_snapshot() -> None:
    assert rl.snapshot_marks(_risk(_mark(1, "1"))) == {1: _mark(1, "1")}
    assert rl.snapshot_marks(_risk(_mark(1, "1"), currency=None)) is None
    assert rl.snapshot_marks(_risk(_mark(1, "1"), currency=2)) is None
    assert rl.snapshot_marks(_risk(_mark(1, "1"), _mark(1, "2"))) is None
    # Per-position rows must be exactly the direct positions the snapshot counts.
    assert rl.snapshot_marks(_risk(counted=1)) is None
    assert rl.snapshot_marks(_risk()) == {}
