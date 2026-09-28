"""#3471 slice 2c-i — the §8 step-0 capacity preview's pure half.

The DB assembler is ``test_ai_trial_start_gate_db.py``.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from app.services.ai_trial_start_gate import (
    PREVIEW_STOP_PCT,
    PREVIEW_TICKET,
    PreviewLeg,
    PreviewShared,
    start_gate_reason,
)

# A growth-profile pot of $10,000 on a $100,000 account: comfortably admits one pair.
SHARED = PreviewShared(
    within_bound=True,
    pool_base=Decimal("10000"),
    committed=Decimal("0"),
    equity=Decimal("100000"),
    total_invested=Decimal("0"),
    available_cash=Decimal("5000"),
    pending_total=Decimal("0"),
    drawdown_pct=Decimal("0"),
    open_lifecycles=0,
    daily_realised_pnl=Decimal("0"),
    mandate_max_drawdown_pct=Decimal("25"),
    mandate_max_loss_per_position_pct=Decimal("1"),
    mandate_max_daily_loss_pct=Decimal("2"),
    mandate_active_risk_budget_pct=Decimal("30"),
    mandate_cash_reserve_pct=Decimal("10"),
    mandate_max_concurrent_positions=12,
)
LEG = PreviewLeg(
    deployment_base=Decimal("1000"),
    deployment_reserved=Decimal("0"),
    open_trades=0,
    max_ticket_amount=Decimal("250"),
    max_portfolio_exposure_pct=Decimal("80"),
    max_instrument_exposure_pct=Decimal("30"),
    max_drawdown_pct=Decimal("20"),
)


def test_preview_terms_are_frozen() -> None:
    assert (PREVIEW_STOP_PCT, PREVIEW_TICKET) == (Decimal("25"), Decimal("125"))


def test_a_comfortable_pot_admits_the_pair() -> None:
    assert start_gate_reason(SHARED, (LEG, LEG)) is None


def test_the_measured_2026_09_28_pot_refuses() -> None:
    # Spec §8: event 4, $500 cautious (0.5% per-position loss, 10% active risk, 25% reserve,
    # 4 concurrent). Worst-stop capacity is $500 x 0.5% / 25% = $10 < $125.
    cautious = replace(
        SHARED,
        pool_base=Decimal("500"),
        mandate_max_loss_per_position_pct=Decimal("0.5"),
        mandate_active_risk_budget_pct=Decimal("10"),
        mandate_cash_reserve_pct=Decimal("25"),
        mandate_max_concurrent_positions=4,
    )
    assert start_gate_reason(cautious, (LEG, LEG)) == "trial_capacity_unavailable:loss_at_stop"
    # And core consuming the active-risk budget refuses by name before any sizing (§8 question).
    assert (
        start_gate_reason(replace(cautious, committed=Decimal("250")), (LEG, LEG))
        == "trial_capacity_unavailable:portfolio_active_risk_limit"
    )


@pytest.mark.parametrize(
    ("committed", "expected"),
    [
        # Active-risk room = 3000 - committed; the pair needs 2 x 125 jointly.
        (Decimal("2750"), None),
        (Decimal("2750.01"), "trial_capacity_unavailable:active_risk"),
    ],
)
def test_shared_terms_hold_both_legs_jointly(committed: Decimal, expected: str | None) -> None:
    assert start_gate_reason(replace(SHARED, committed=committed), (LEG, LEG)) == expected
    # One leg alone would fit in the same room.
    assert start_gate_reason(replace(SHARED, committed=committed), (LEG,)) is None


@pytest.mark.parametrize(
    ("shared", "leg", "expected"),
    [
        (replace(SHARED, within_bound=False), LEG, "sandbox_exceeded"),
        (replace(SHARED, open_lifecycles=11), LEG, "portfolio_concurrency_limit"),
        (replace(SHARED, daily_realised_pnl=Decimal("-200")), LEG, "portfolio_daily_loss_limit"),
        (replace(SHARED, drawdown_pct=Decimal("20")), LEG, "account_drawdown_limit"),
        (SHARED, replace(LEG, open_trades=4), "trial_leg_slots_full"),
        (SHARED, replace(LEG, max_ticket_amount=Decimal("124.99")), "max_ticket_amount"),
        (SHARED, replace(LEG, deployment_reserved=Decimal("900")), "deployment_remaining"),
        (replace(SHARED, equity=Decimal("400")), LEG, "instrument_exposure"),
        (replace(SHARED, available_cash=Decimal("249.99")), LEG, "available_cash"),
        (replace(SHARED, pending_total=Decimal("4800")), LEG, "available_cash"),
    ],
)
def test_each_limit_refuses_by_name(shared: PreviewShared, leg: PreviewLeg, expected: str) -> None:
    assert start_gate_reason(shared, (leg, LEG)) == "trial_capacity_unavailable:" + expected


def test_no_legs_refuses() -> None:
    assert start_gate_reason(SHARED, ()) == "trial_capacity_unavailable:no_legs"
