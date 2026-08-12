from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from scripts.evaluate_2582_schedule13d_outcomes import (
    ACKNOWLEDGEMENT,
    OutcomeGateRefusal,
    bucket,
    match_tie_break,
    next_regular_session_strictly_after,
    nth_regular_session,
    require_outcome_gate,
    total_return_pct,
)


def test_gate_refuses_without_explicit_acknowledgement() -> None:
    with pytest.raises(OutcomeGateRefusal, match="remain closed"):
        require_outcome_gate(
            acknowledgement=None,
            contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
        )


def test_gate_refuses_even_with_ack_until_trial_is_declared() -> None:
    with pytest.raises(OutcomeGateRefusal, match="absent from trial-register"):
        require_outcome_gate(
            acknowledgement=ACKNOWLEDGEMENT,
            contract_path=Path("docs/proposals/ta/contracts/schedule13d-public-catalyst-v1.json"),
        )


def test_next_session_is_strict_and_observes_weekend_and_holiday() -> None:
    assert next_regular_session_strictly_after(date(2026, 7, 2)) == date(2026, 7, 6)
    assert next_regular_session_strictly_after(date(2026, 8, 10)) == date(2026, 8, 11)
    assert nth_regular_session(date(2026, 7, 6), 10) == date(2026, 7, 17)


def test_total_return_uses_adjustment_factor_and_subtracts_adverse_cost() -> None:
    # Raw price halves, but the 2-for-1 adjustment factor doubles: zero gross.
    result = total_return_pct(
        entry_open=Decimal("100"),
        entry_close=Decimal("100"),
        entry_adj_close=Decimal("50"),
        exit_close=Decimal("50"),
        exit_adj_close=Decimal("50"),
    )
    assert result == Decimal("-0.500")


@pytest.mark.parametrize("bad", [Decimal("0"), Decimal("NaN"), Decimal("-1")])
def test_total_return_refuses_bad_adjustment_inputs(bad: Decimal) -> None:
    with pytest.raises(ValueError):
        total_return_pct(
            entry_open=Decimal("10"),
            entry_close=bad,
            entry_adj_close=Decimal("10"),
            exit_close=Decimal("11"),
            exit_adj_close=Decimal("11"),
        )


def test_matching_helpers_are_boundary_stable_and_deterministic() -> None:
    edges = tuple(map(Decimal, ("5", "10", "25")))
    assert bucket(Decimal("9.99"), edges) == 1
    assert bucket(Decimal("10"), edges) == 2
    assert match_tie_break("a", "b") == match_tie_break("a", "b")
    assert match_tie_break("a", "b") != match_tie_break("a", "c")
