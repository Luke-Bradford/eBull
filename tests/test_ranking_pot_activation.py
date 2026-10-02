"""#2842 slice 5c-ii-b — the activation's pure rules (spec §7.3, "The activation script and precheck")."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from app.services import ranking_pot_activation as act
from app.services.ai_trial_decision import STOP_PCT_MAX
from app.services.ai_trial_start_gate import PreviewShared

SHARED = PreviewShared(
    within_bound=True,
    pool_base=Decimal("40000"),
    committed=Decimal("16000"),
    active_committed=Decimal("1000"),
    equity=Decimal("40000"),
    total_invested=Decimal("17000"),
    available_cash=Decimal("23000"),
    pending_total=Decimal("0"),
    drawdown_pct=Decimal("1"),
    open_lifecycles=4,
    daily_realised_pnl=Decimal("0"),
    mandate_max_drawdown_pct=Decimal("25"),
    mandate_max_loss_per_position_pct=Decimal("1"),
    mandate_max_daily_loss_pct=Decimal("2.5"),
    mandate_active_risk_budget_pct=Decimal("30"),
    mandate_cash_reserve_pct=Decimal("10"),
    mandate_max_concurrent_positions=30,
)
POLICY: dict[str, Any] = {
    "n": 25,
    "stop_loss_pct": Decimal("25"),
    "max_instrument_exposure_pct": Decimal("10"),
    "max_portfolio_exposure_pct": Decimal("100"),
    "max_drawdown_pct": Decimal("25"),
}


@pytest.mark.parametrize(
    ("entered", "reason"),
    [
        (None, "pot_capital_not_entered"),
        (Decimal("1500"), None),
        (Decimal("12000"), None),
        (Decimal("12500"), "pot_capital_invalid"),
        (Decimal("0"), "pot_capital_invalid"),
        (Decimal("-500"), "pot_capital_invalid"),
        (Decimal("1250"), "pot_capital_invalid"),
        (Decimal("NaN"), "pot_capital_invalid"),
        (Decimal("Infinity"), "pot_capital_invalid"),
    ],
)
def test_capital_entry(entered: Decimal | None, reason: str | None) -> None:
    assert act.capital_entry_reason(entered) == reason


def test_max_open_minimum_skips_unenterable_and_fails_closed() -> None:
    found = ("found", True, Decimal("10"))
    rows = {
        1: found,
        2: ("found", True, Decimal("50")),
        3: ("not_found", None, None),
        4: ("found", False, Decimal("1000")),
    }
    assert act.max_open_minimum([1, 2, 3, 4], rows) == Decimal("50")
    # A NULL allow_open_position is enterable: its minimum counts.
    assert act.max_open_minimum([1, 5], {1: found, 5: ("found", None, Decimal("25"))}) == Decimal("25")
    assert act.max_open_minimum([1, 9], {1: found}) == act.MINIMUM_UNOBSERVED  # missing from the snapshot
    for bad in (None, Decimal("NaN"), Decimal("-1")):
        assert act.max_open_minimum([1, 6], {1: found, 6: ("found", True, bad)}) == act.MINIMUM_UNOBSERVED
    assert act.max_open_minimum([3], {3: ("not_found", None, None)}) == act.MINIMUM_UNOBSERVED  # nothing enterable
    assert act.max_open_minimum([], {}) == act.MINIMUM_UNOBSERVED


def test_preview_binds_on_active_risk_and_rounds_to_500() -> None:
    preview = act.preview_capital(SHARED, **POLICY)
    assert isinstance(preview, act.CapitalPreview)
    # 30% x 40,000 - 1,000 non-core = 11,000: below the cap, cash, reserve (36,000 - 16,000) and loss-at-stop.
    assert (preview.binding_term, preview.binding_value, preview.preview) == ("active_risk", Decimal("11000.00"), 11000)
    assert preview.terms["loss_at_stop"] == Decimal("40000.00")  # 25 x 40,000 x 1% / 25
    assert preview.terms["instrument_exposure"] == Decimal("100000.00")  # 25 x 10% x 40,000
    rounded = act.preview_capital(replace(SHARED, active_committed=Decimal("1001")), **POLICY)
    assert isinstance(rounded, act.CapitalPreview) and rounded.preview == Decimal("10500")


def test_preview_cap_and_tie_order() -> None:
    capped = act.preview_capital(
        replace(SHARED, active_committed=Decimal("0"), mandate_active_risk_budget_pct=Decimal("40")), **POLICY
    )
    assert isinstance(capped, act.CapitalPreview) and capped.binding_term == "pot_capital_cap"
    # A tie between two terms is named by the listed order (shared terms first).
    tied = act.preview_capital(replace(SHARED, available_cash=Decimal("11000"), total_invested=Decimal("0")), **POLICY)
    assert isinstance(tied, act.CapitalPreview) and tied.binding_term == "available_cash"


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"within_bound": False}, "sandbox_exceeded"),
        ({"open_lifecycles": 6}, "portfolio_concurrency_limit"),  # 6 + 25 > 30
        ({"daily_realised_pnl": Decimal("-1000")}, "portfolio_daily_loss_limit"),
        ({"drawdown_pct": Decimal("25")}, "account_drawdown_limit"),
        ({"active_committed": Decimal("12000")}, "portfolio_active_risk_limit"),
        ({"active_committed": Decimal("11800")}, "active_risk"),  # $200 left: rounds to a $0 preview
    ],
)
def test_preview_refusals(change: dict[str, Any], code: str) -> None:
    assert act.preview_capital(replace(SHARED, **change), **POLICY) == act.CAPITAL_UNAVAILABLE + code


def test_preview_mandate_drawdown_after_policy_drawdown() -> None:
    shared = replace(SHARED, drawdown_pct=Decimal("20"), mandate_max_drawdown_pct=Decimal("18"))
    assert act.preview_capital(shared, **POLICY) == act.CAPITAL_UNAVAILABLE + "portfolio_drawdown_limit"


def test_capital_reason() -> None:
    preview = act.CapitalPreview("active_risk", Decimal("11250.00"), Decimal("11000"), {})
    assert act.capital_reason(Decimal("11000"), preview) is None
    assert act.capital_reason(Decimal("11500"), preview) == act.CAPITAL_UNAVAILABLE + "active_risk"
    assert act.capital_reason(Decimal("10500"), preview) == "pot_capital_not_preview"


def test_execution_policy_and_slot_ticket() -> None:
    policy = act.execution_policy(Decimal("1500"), 25)
    assert policy["fixed_ticket_amount"] == Decimal("60.00")
    assert policy["max_ticket_amount"] == Decimal("1500")
    assert policy["stop_loss_pct"] == Decimal("25")
    assert act.slot_ticket(Decimal("1000"), 3) == Decimal("333.33")


def _config(capital: Decimal) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    deployment = {"mode": "paper", "currency": "USD", "enabled": True, "capital_limit": capital}
    policy = {k: (str(v) if isinstance(v, Decimal) else v) for k, v in act.execution_policy(capital, 25).items()}
    policy["revision"] = 1  # extra columns are ignored
    return deployment, policy, {"max_position_age_seconds": None, "ratchet_variant_id": None}


def test_configuration_drift() -> None:
    capital = Decimal("1500")
    deployment, policy, manager = _config(capital)
    kw = {"pot_capital": capital, "n": 25}
    assert not act.configuration_drift(**kw, deployment=deployment, policy=policy, manager=manager)
    assert act.configuration_drift(**kw, deployment=None, policy=policy, manager=manager)
    assert act.configuration_drift(**kw, deployment=deployment, policy=policy, manager=None)
    assert act.configuration_drift(
        **kw, deployment={**deployment, "capital_limit": "2000"}, policy=policy, manager=manager
    )
    assert act.configuration_drift(**kw, deployment={**deployment, "enabled": False}, policy=policy, manager=manager)
    assert act.configuration_drift(
        **kw, deployment=deployment, policy={**policy, "stop_loss_pct": "30"}, manager=manager
    )
    assert act.configuration_drift(
        **kw, deployment=deployment, policy={**policy, "ticket_fraction": "0.1"}, manager=manager
    )
    assert act.configuration_drift(
        **kw, deployment=deployment, policy=policy, manager={**manager, "max_position_age_seconds": 60}
    )
    assert act.configuration_drift(
        **kw, deployment=deployment, policy=policy, manager={**manager, "ratchet_variant_id": 1}
    )


def test_the_stop_cap_is_v1s_and_lives_in_the_hashed_file() -> None:
    assert act.POT_EXECUTION_POLICY["stop_loss_pct"] == Decimal(str(STOP_PCT_MAX))
