"""#3541 slice 2 — both exposure caps on the engine pot, not the demo account.

Spec: ``docs/proposals/execution/2026-10-02-3541-engine-pot-exposure.md``.
"""

from __future__ import annotations

from decimal import Decimal

from app.services.strategy_paper_executor import _Capacities, _capacities, _capacity_terms, _pot_exposure

_POOL = Decimal("40000")


def _terms(*, portfolio: Decimal, instrument: Decimal) -> _Capacities | str:
    return _capacity_terms(
        pool_base=_POOL,
        committed=Decimal("3000"),
        active_committed=Decimal("3000"),
        deployment_base=Decimal("3000"),
        deployment_reserved=Decimal("0"),
        cash=Decimal("20000"),
        portfolio=portfolio,
        instrument=instrument,
        mandate_cash_reserve_pct=Decimal("10"),
        mandate_active_risk_budget_pct=Decimal("30"),
        mandate_max_loss_per_position_pct=Decimal("1"),
        stop_loss_pct=Decimal("25"),
    )


def test_the_instrument_cap_is_a_share_of_the_pot_less_the_engines_own_commitment() -> None:
    # 10% of the $40,000 pot less $3,000 the engine already has in the instrument.
    result = _terms(
        portfolio=_pot_exposure(_POOL, Decimal("3000"), Decimal("100")),
        instrument=_pot_exposure(_POOL, Decimal("3000"), Decimal("10")),
    )
    assert isinstance(result, _Capacities)
    assert (result.portfolio, result.instrument) == (Decimal("37000"), Decimal("1000"))


def test_an_over_cap_room_floors_at_zero_and_does_not_refuse_by_name() -> None:
    # Over the cap is a zero bound (`risk_capacity_exhausted` at sizing), as before.
    result = _terms(portfolio=Decimal("-1"), instrument=_pot_exposure(_POOL, Decimal("4500"), Decimal("10")))
    assert isinstance(result, _Capacities)
    assert (result.portfolio, result.instrument) == (Decimal("0"), Decimal("0"))


def test_the_account_adapter_keeps_the_frozen_preview_arithmetic() -> None:
    # `_capacities` serves ONLY the policy-hashed v1 start gate: equity-scaled, less the
    # account's invested amounts and the pending orders, exactly as declaration 16 froze it.
    result = _capacities(
        pool_base=_POOL,
        committed=Decimal("3000"),
        active_committed=Decimal("3000"),
        deployment_base=Decimal("3000"),
        deployment_reserved=Decimal("0"),
        equity=Decimal("100000"),
        total_invested=Decimal("60000"),
        available_cash=Decimal("20000"),
        pending_total=Decimal("500"),
        pending_instrument=Decimal("200"),
        current_instrument=Decimal("7000"),
        max_portfolio_exposure_pct=Decimal("80"),
        max_instrument_exposure_pct=Decimal("10"),
        mandate_cash_reserve_pct=Decimal("10"),
        mandate_active_risk_budget_pct=Decimal("30"),
        mandate_max_loss_per_position_pct=Decimal("1"),
        stop_loss_pct=Decimal("25"),
    )
    assert isinstance(result, _Capacities)
    assert (result.cash, result.portfolio, result.instrument) == (
        Decimal("19500"),
        Decimal("19500"),
        Decimal("2800"),
    )
