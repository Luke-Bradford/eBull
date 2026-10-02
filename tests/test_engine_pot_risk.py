"""Engine-pot drawdown arithmetic and snapshot refusals (#3541 slice 1). Pure: no database."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.engine_pot_risk import PotNav, _check, link, observe_pot_nav
from app.services.strategy_engine_capital import EngineCapitalObservationError

EPOCH = datetime(2026, 9, 18, tzinfo=UTC)
T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def _pot(principal: str, pnl: str, minutes: int = 0) -> PotNav:
    return PotNav(Decimal(principal), EPOCH, Decimal(pnl), Decimal("0"), T0 + timedelta(minutes=minutes))


def _chain(*pots: PotNav) -> Decimal:
    state = None
    for pot in pots:
        state = link(state, pot)
    assert state is not None
    return state.last_drawdown_pct


def test_first_observation_is_the_base() -> None:
    state = link(None, _pot("100", "-30"))
    assert (state.nav_index, state.index_high_water, state.last_drawdown_pct) == (1, 1, 0)
    assert (state.last_nav, state.last_pnl) == (Decimal("70"), Decimal("-30"))


def test_a_gain_then_a_loss_measures_from_the_peak() -> None:
    # 100 -> 110 (peak) -> 99: the index falls 10% from its high water.
    assert _chain(_pot("100", "0"), _pot("100", "10", 1), _pot("100", "-1", 2)) == Decimal("10")


def test_an_allocation_after_a_loss_does_not_dilute_the_drawdown() -> None:
    # A 10% loss, then +100 principal with no P&L change: still 10%, not 5%.
    assert _chain(_pot("100", "0"), _pot("100", "-10", 1), _pot("200", "-10", 2)) == Decimal("10")


def test_a_withdrawal_without_a_pnl_change_leaves_the_drawdown_unchanged() -> None:
    assert _chain(_pot("100", "0"), _pot("100", "-10", 1), _pot("50", "-10", 2)) == Decimal("10")


def test_a_loss_after_an_allocation_is_measured_on_the_larger_nav() -> None:
    # NAV 190 after the allocation; a further 19 loss is another -10%: 0.9 x 0.9 = 0.81.
    assert _chain(_pot("100", "0"), _pot("100", "-10", 1), _pot("200", "-10", 2), _pot("200", "-29", 3)) == Decimal(
        "19"
    )


def test_a_flow_inside_a_lossy_span_takes_the_smaller_base() -> None:
    # Half withdrawn and a 5 loss in one span: when is unobserved, so 5/50, not 5/100.
    assert _chain(_pot("100", "0"), _pot("50", "-5", 1)) == Decimal("10")
    # An allocation and a 5 loss in one span: 5/100 (the pre-allocation base), not 5/200.
    assert _chain(_pot("100", "0"), _pot("200", "-5", 1)) == Decimal("5")


def test_a_gain_across_a_withdrawal_raises_the_peak_on_the_smaller_base() -> None:
    # +5 across a half withdrawal is +10% on 50, so a later 5.5 loss on 55 is 10% off that peak.
    assert _chain(_pot("100", "0"), _pot("50", "5", 1), _pot("50", "-0.5", 2)) == Decimal("10")


def test_an_unchanged_pnl_relinks_at_a_zero_return() -> None:
    assert _chain(_pot("100", "0"), _pot("100", "-5", 1), _pot("100", "-5", 1)) == Decimal("5")


def test_a_loss_beyond_the_nav_refuses_instead_of_clamping() -> None:
    with pytest.raises(EngineCapitalObservationError) as exc:
        _chain(_pot("100", "0"), _pot("100", "-100", 1))
    assert exc.value.reason_code == "engine_capital_population_incomplete"


def test_state_checks_refuse_stale_and_foreign_epoch_but_accept_equal_time() -> None:
    state = link(None, _pot("100", "0", 5))
    assert _check(None, _pot("100", "0")) is None
    assert _check(state, _pot("100", "0", 4)) == "engine_pot_risk_stale"
    assert _check(state, _pot("100", "0", 5)) is None
    assert _check(state, replace(_pot("100", "0", 6), epoch_started_at=EPOCH + timedelta(days=1))) == (
        "engine_pot_epoch_mismatch"
    )


def _snapshot(**overrides: object) -> BrokerAccountRiskSnapshot:
    base = BrokerAccountRiskSnapshot(
        available_cash=Decimal("1000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        instrument_investments=(),
        observed_at=T0,
        raw_payload={},
        account_currency_id=1,
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


def _position(position_id: int) -> BrokerDirectPositionInvestment:
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=1,
        is_buy=True,
        units=Decimal("1"),
        amount=Decimal("10"),
        unrealized_pnl=Decimal("1"),
        market_value=Decimal("11"),
        is_partially_altered=False,
        close_rate=Decimal("11"),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=1,
    )


@pytest.mark.parametrize(
    "snapshot",
    [
        _snapshot(account_currency_id=None),
        _snapshot(account_currency_id=2),
        _snapshot(observed_at=T0.replace(tzinfo=None)),
        _snapshot(direct_positions=(_position(7), _position(7))),
    ],
    ids=["currency-missing", "currency-not-usd", "naive-time", "duplicate-position"],
)
def test_an_unusable_snapshot_refuses_before_any_read_even_with_an_empty_book(
    snapshot: BrokerAccountRiskSnapshot,
) -> None:
    conn = MagicMock()
    with pytest.raises(EngineCapitalObservationError) as exc:
        observe_pot_nav(conn, snapshot)
    assert exc.value.reason_code == "engine_capital_snapshot_unusable"
    conn.execute.assert_not_called()
