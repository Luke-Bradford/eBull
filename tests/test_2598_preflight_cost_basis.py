"""``cost_basis`` and its constraint agree (#2598 step 4).

``strategy_entry_preflights.cost_basis`` records WHICH PATH priced
``stressed_cost_amount``. Two independent declarations govern it — the Python
vocabulary the writer binds from, and `sql/342`'s CHECK — and **the CHECK does
not read the constant**. A value added on one side alone fails at INSERT in
production rather than in review, which is the drift this file exists to catch.

⚠ Same shape as #2653's ``test_the_deployment_currency_refusal_and_its_constraint_agree``,
and for the same reason: the safe-looking edit is the one-sided one.

⚠ DB-free. The constraint is read from the migration TEXT, because that file is
the artefact a reviewer changes; an applied database is downstream of it (and
`app/db/migrations.py` hashes applied files, so the text cannot drift silently
from what was applied).
"""

from __future__ import annotations

import pathlib
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from app.services.strategy_paper_executor import (
    COST_BASES,
    COST_BASIS_BROKER_PREFLIGHT_AMOUNT,
    COST_BASIS_BROKER_PREFLIGHT_VALUE,
)

SQL = pathlib.Path(__file__).resolve().parents[1] / "sql"
#: ⚠ The VOCABULARY moved to sql/343 (#2598 step 3 split it by which response field
#: carried the money); the allocated-row rule still lives in sql/342 and is untouched.
MIGRATION = SQL / "343_strategy_entry_preflight_cost_basis_field.sql"
ALLOCATED_RULE_MIGRATION = SQL / "342_strategy_entry_preflight_cost_basis.sql"


def _vocabulary_in_the_check() -> set[str]:
    """The literals inside the ``cost_basis IN (...)`` CHECK, from the migration."""
    body = MIGRATION.read_text()
    match = re.search(r"CHECK \(cost_basis IS NULL OR cost_basis IN \(([^)]*)\)\)", body)
    assert match is not None, "the cost_basis vocabulary CHECK is no longer in sql/343 in a readable form"
    return set(re.findall(r"'([^']+)'", match.group(1)))


def test_the_python_vocabulary_and_the_sql_check_are_the_same_set() -> None:
    assert _vocabulary_in_the_check() == set(COST_BASES)


def test_both_members_are_reachable_and_admitted() -> None:
    """Each is returned by its own branch of ``_component_amount``; a rename on
    one side would surface as a constraint violation at allocation time."""
    assert {COST_BASIS_BROKER_PREFLIGHT_AMOUNT, COST_BASIS_BROKER_PREFLIGHT_VALUE} == set(COST_BASES)


def test_the_static_band_bound_is_absent_on_purpose() -> None:
    """⚠ THE POINT OF THIS ONE IS THE MESSAGE IT CARRIES, not the assertion.

    #2598's scope text names ``static_band_bound`` as a second basis. It is not
    implemented, and this run's band-stratified census argues against it: the
    worst broker quote observed (ETR, 381.5 bps) is 1.55x the MAXIMUM spread in
    its band's whole calibration snapshot, so no percentile of that snapshot
    bounds what the broker charges. Declaring the value would imply a second
    priced path exists.

    So this failing is not a bug — it is the reminder that adding the value
    means adding the path, the migration and the writer branch together.
    """
    assert "static_band_bound" not in COST_BASES


def test_an_allocated_row_must_carry_a_basis_and_a_rejection_need_not() -> None:
    """A rejection before the cost step priced nothing; recording a basis there
    would record a pricing that never happened."""
    assert "CHECK (verdict <> 'allocated' OR cost_basis IS NOT NULL)" in ALLOCATED_RULE_MIGRATION.read_text()
    # sql/343 owns the VOCABULARY only. Redefining the allocated-row rule there would
    # split one invariant across two migrations, where the later silently wins.
    assert "ADD CONSTRAINT strategy_entry_preflights_allocated_cost_basis" not in MIGRATION.read_text()


# --- the narrowing itself (#2598 step 3) ------------------------------------------
#
# ⚠ `_costs` is the gate that decides whether real money is committed, and it is pure
# over its arguments, so it is table-tested here rather than only through the db-tier
# executor path. The intent is built with every field spelled out: a factory that
# defaulted them would silently absorb a new one.

_NOW = datetime(2026, 8, 13, 15, 0, tzinfo=UTC)


def _intent(**overrides: Any) -> Any:
    from app.services.strategy_paper_executor import _Intent

    base: dict[str, Any] = {
        "signal_id": 1,
        "strategy_id": "s1",
        "strategy_version": "v1",
        "instrument_id": 1001,
        "symbol": "AAPL",
        "deployment_id": 1,
        "currency": "USD",
        "deployment_limit": Decimal("1000"),
        "pool_limit": Decimal("1000"),
        "capital_mode": "fixed",
        "pool_reserved": Decimal("0"),
        "mandate_max_drawdown_pct": Decimal("10"),
        "mandate_max_loss_per_position_pct": Decimal("1"),
        "mandate_max_daily_loss_pct": Decimal("2"),
        "mandate_active_risk_budget_pct": Decimal("5"),
        "mandate_cash_reserve_pct": Decimal("5"),
        "mandate_max_concurrent_positions": 5,
        "forecast_id": 1,
        "ranking_member_id": 1,
        "policy_revision": 1,
        "ticket_sizing_mode": "fixed",
        "ticket_fraction": None,
        "fixed_ticket_amount": Decimal("1000"),
        "max_ticket_amount": Decimal("1000"),
        "stop_loss_pct": Decimal("5"),
        "take_profit_pct": Decimal("10"),
        "max_quote_age_seconds": 60,
        "max_scan_age_seconds": 60,
        "max_halt_feed_age_seconds": 60,
        "max_cost_age_seconds": 3600,
        "max_reconciliation_age_seconds": 60,
        "max_instrument_exposure_pct": Decimal("10"),
        "max_portfolio_exposure_pct": Decimal("50"),
        "max_drawdown_pct": Decimal("10"),
        "min_net_expectancy_pct": Decimal("0"),
        "cost_stress_multiplier": Decimal("2"),
        "quote_at": _NOW,
        "ask": Decimal("100"),
        "scan_at": _NOW,
        "halt_feed_at": _NOW,
        "gross_expectancy_ci_low_pct": Decimal("3"),
        "reserved": Decimal("0"),
    }
    base.update(overrides)
    return _Intent(**base)


def _component(cost_type: str, *, amount: str | None = None, value: str | None = None, currency: str = "USD") -> Any:
    from app.providers.broker import BrokerCostComponent

    return BrokerCostComponent(
        cost_type=cost_type,
        amount=None if amount is None else Decimal(amount),
        value=None if value is None else Decimal(value),
        currency=currency,
        raw_payload={},
    )


def _response(*components: Any) -> Any:
    from app.providers.broker import BrokerWhatIfCostResponse

    return BrokerWhatIfCostResponse(
        instrument_id=1001, symbol="AAPL", costs=tuple(components), last_updated=_NOW, raw_payload={}
    )


def _assess(*components: Any) -> Any:
    from app.services.strategy_paper_executor import _costs

    return _costs(_response(*components), intent=_intent(), amount=Decimal("1000"), now=_NOW)


class TestWhichFieldCarriedTheMoney:
    def test_the_undocumented_value_field_is_accepted_and_recorded_as_such(self) -> None:
        """What the live demo response actually sends: keys are
        ``['costType', 'currency', 'value']``, ``amount`` absent as a KEY."""
        assessed = _assess(_component("marketSpread", value="0.30"))
        assert assessed.basis == COST_BASIS_BROKER_PREFLIGHT_VALUE
        assert assessed.stressed == Decimal("0.60")  # x2 stress multiplier

    def test_the_documented_amount_field_is_accepted_and_recorded_as_such(self) -> None:
        """The contract eToro publishes. Unreachable on today's responses, but it is
        a real branch — this is what makes the second vocabulary member honest."""
        assessed = _assess(_component("marketSpread", amount="0.30"))
        assert assessed.basis == COST_BASIS_BROKER_PREFLIGHT_AMOUNT

    def test_both_fields_present_is_still_a_refusal(self) -> None:
        """Never observed, and the two could disagree with no documented winner."""
        assert _assess(_component("marketSpread", amount="0.30", value="0.30")) == "cost_unit_undocumented"

    def test_neither_field_present_is_a_refusal(self) -> None:
        assert _assess(_component("marketSpread")) == "cost_unit_undocumented"

    def test_a_response_mixing_the_two_conventions_is_a_refusal(self) -> None:
        """⚠ Summing a documented field and an off-spec one into one total assumes
        they mean the same thing, which is exactly what a mixed response leaves open."""
        assert (
            _assess(_component("marketSpread", value="0.30"), _component("markup", amount="0.10"))
            == "cost_unit_undocumented"
        )


class TestTheRefusalsThatMustSurviveTheNarrowing:
    def test_a_recurring_cost_carried_in_value_still_refuses(self) -> None:
        """⚠⚠ THE TRAP IN THIS CHANGE. The recurring-cost gate used to read
        ``component.amount``, non-NULL only because the old refusal rejected every row
        without it. Reading the unresolved field once ``value`` is accepted would
        compare ``None > 0`` — and a rewrite that skipped the row instead would let an
        unmodelled carry through the gate that exists to stop it (#2363)."""
        assert (
            _assess(_component("marketSpread", value="0.30"), _component("overnightFee", value="0.50"))
            == "recurring_cost_horizon_unmodelled"
        )

    def test_a_zero_recurring_cost_does_not_refuse(self) -> None:
        """0.0 on every observation to date; refusing it would refuse every response.
        ⚠ It is not evidence carry IS zero — 68 observations of unleveraged LONG."""
        assert (
            _assess(_component("marketSpread", value="0.30"), _component("overnightFee", value="0.0")).basis
            == COST_BASIS_BROKER_PREFLIGHT_VALUE
        )

    def test_a_negative_value_is_invalid(self) -> None:
        assert _assess(_component("marketSpread", value="-0.30")) == "cost_currency_or_value_invalid"

    def test_a_foreign_currency_is_invalid_even_when_the_field_is_readable(self) -> None:
        assert _assess(_component("marketSpread", value="0.30", currency="EUR")) == "cost_currency_or_value_invalid"

    def test_an_empty_cost_list_is_still_costs_missing(self) -> None:
        assert _assess() == "costs_missing"
