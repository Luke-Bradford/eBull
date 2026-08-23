"""#2859 — ``configure_execution_policy`` bounds its limits before touching the DB.

Recovered from ``feature/2770-operator-promotion``, which the 08-21 rebuild lost.
Classified as a REAL GAP by the #2859 audit rather than ported blind: main had a
finiteness check on ``fixed_ticket_amount`` alone and no bound at all on
``min_net_expectancy_pct``, the one numeric limit on
``strategy_execution_policies`` with neither a service bound nor a schema CHECK.

⚠ These are deliberately PURE tests, where the branch's originals passed a real
connection and a ``deployment_id`` that did not exist. That version cannot
distinguish "refused by the bound" from "refused because the deployment is
missing", so it does not actually test the property its name claims. Passing a
connection that raises on ANY attribute access does, and it keeps the module on
the fast pre-push gate.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, cast

import pytest

from app.services.strategy_control_plane import StrategyControlError, configure_execution_policy


class _ExplodingConn:
    """A connection stand-in that fails the test if anything reaches the DB."""

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"database was accessed via conn.{name} before the limits were validated")


def _configure(**overrides: Any) -> None:
    """Call the real chokepoint with one field overridden and no DB available."""
    kwargs: dict[str, Any] = {
        "deployment_id": 1,
        "ticket_sizing_mode": "percent",
        "ticket_fraction": Decimal("0.1"),
        "fixed_ticket_amount": None,
        "max_ticket_amount": Decimal("10"),
        "stop_loss_pct": Decimal("5"),
        "take_profit_pct": Decimal("10"),
        "max_quote_age_seconds": 30,
        "max_scan_age_seconds": 300,
        "max_halt_feed_age_seconds": 300,
        "max_cost_age_seconds": 3600,
        "max_reconciliation_age_seconds": 60,
        "max_instrument_exposure_pct": Decimal("20"),
        "max_portfolio_exposure_pct": Decimal("50"),
        "max_drawdown_pct": Decimal("10"),
        "min_net_expectancy_pct": Decimal("0.5"),
        "cost_stress_multiplier": Decimal("2"),
        "changed_by": "operator",
        "reason": "limits test",
    }
    kwargs.update(overrides)
    configure_execution_policy(cast(Any, _ExplodingConn()), **kwargs)


@pytest.mark.parametrize(
    "field",
    [
        "ticket_fraction",
        "max_ticket_amount",
        "stop_loss_pct",
        "take_profit_pct",
        "max_instrument_exposure_pct",
        "max_portfolio_exposure_pct",
        "max_drawdown_pct",
        "min_net_expectancy_pct",
        "cost_stress_multiplier",
    ],
)
def test_a_non_finite_limit_is_one_named_refusal_and_never_an_invalid_operation(field: str) -> None:
    """Every limit, not just the one that happened to be checked.

    ⚠ The failure this replaces is not a wrong answer, it is an exception class:
    ``Decimal("NaN") <= 0`` raises ``InvalidOperation``, so before the sweep a NaN
    limit surfaced as a 500 from whichever comparison reached it first, and which
    comparison that was depended on the field. Asserting ``StrategyControlError``
    per field is what pins the sweep to the full set rather than to the one case
    a regression test happened to use.
    """
    with pytest.raises(StrategyControlError, match=f"{field} must be finite"):
        _configure(**{field: Decimal("NaN")})


def test_a_non_finite_fixed_ticket_amount_is_still_refused_under_fixed_sizing() -> None:
    """The pre-existing check kept its meaning — the sweep did not displace it.

    ``fixed_ticket_amount`` is ``None`` under percent sizing, so the sweep skips
    it there; this is the arm where it carries a value.
    """
    with pytest.raises(StrategyControlError, match="fixed_ticket_amount must be finite"):
        _configure(
            ticket_sizing_mode="fixed",
            ticket_fraction=None,
            fixed_ticket_amount=Decimal("NaN"),
        )


def test_a_negative_net_expectancy_floor_is_refused() -> None:
    """A negative floor inverts the paper executor's own gate.

    ``strategy_paper_executor`` refuses on
    ``net_expectancy < intent.min_net_expectancy_pct``, so ``-0.01`` admits a
    signal whose stressed cost exceeds its forecast expectancy — and writes no
    refusal row, because nothing refused.
    """
    with pytest.raises(StrategyControlError, match="min_net_expectancy_pct must be non-negative"):
        _configure(min_net_expectancy_pct=Decimal("-0.01"))


def test_a_zero_net_expectancy_floor_reaches_the_database() -> None:
    """Zero is admissible, and the boundary is load-bearing.

    The value replaced a hardcoded ``net_expectancy <= 0`` refusal; expressing
    "strictly positive" is now the operator's to do with a positive floor. So the
    bound must be ``< 0``, not ``<= 0`` — and the proof that zero passed
    validation is that the call goes on to touch the connection.
    """
    with pytest.raises(AssertionError, match="database was accessed"):
        _configure(min_net_expectancy_pct=Decimal("0"))
