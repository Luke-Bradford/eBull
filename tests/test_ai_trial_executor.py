"""#3471 slice 2b-ii — the trial executor's pure gates: the §8 cost cap and protective levels.

The DB-backed submission path is ``test_ai_trial_executor_db.py``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.ai_trial_executor import (
    TRIAL_COST_CAP_PCT,
    protective_levels_reason,
    trial_cost_cap_reason,
)


def test_the_cap_is_one_percent_frozen() -> None:
    assert TRIAL_COST_CAP_PCT == Decimal("1.0")


@pytest.mark.parametrize(
    ("stressed", "amount", "expected"),
    [
        (Decimal("1.25"), Decimal("125"), None),  # exactly 1.0%: "exceeds" is the refusal
        (Decimal("1.250001"), Decimal("125"), "trial_cost_cap"),
        (Decimal("0"), Decimal("125"), None),
        (Decimal("2.50"), Decimal("250"), None),
        (Decimal("2.51"), Decimal("250"), "trial_cost_cap"),
        # A reduced amount raises the cost's share: the cap is on the ACTUAL amount.
        (Decimal("1.25"), Decimal("60"), "trial_cost_cap"),
        (Decimal("-0.01"), Decimal("125"), "trial_cost_cap"),
        (Decimal("1"), Decimal("0"), "trial_cost_cap"),
        (Decimal("NaN"), Decimal("125"), "trial_cost_cap"),
        (Decimal("1"), Decimal("Infinity"), "trial_cost_cap"),
    ],
)
def test_cost_cap(stressed: Decimal, amount: Decimal, expected: str | None) -> None:
    assert trial_cost_cap_reason(stressed, amount) == expected


@pytest.mark.parametrize(
    ("ask", "stop", "take", "expected"),
    [
        (Decimal("100"), Decimal("92"), Decimal("116"), None),
        (Decimal("100"), Decimal("100"), Decimal("116"), "protective_levels_invalid"),
        (Decimal("100"), Decimal("92"), Decimal("100"), "protective_levels_invalid"),
        (Decimal("100"), Decimal("0"), Decimal("116"), "protective_levels_invalid"),
        (Decimal("100"), Decimal("-1"), Decimal("116"), "protective_levels_invalid"),
        (Decimal("100"), Decimal("116"), Decimal("92"), "protective_levels_invalid"),
        (Decimal("NaN"), Decimal("92"), Decimal("116"), "protective_levels_invalid"),
        (Decimal("100"), Decimal("92"), Decimal("Infinity"), "protective_levels_invalid"),
        # A sub-cent ask whose 6dp ROUND_DOWN stop collapses to zero is refused, not sent.
        (Decimal("0.000001"), Decimal("0.000000"), Decimal("0.000001"), "protective_levels_invalid"),
    ],
)
def test_protective_levels(ask: Decimal, stop: Decimal, take: Decimal, expected: str | None) -> None:
    assert protective_levels_reason(ask=ask, stop_rate=stop, take_rate=take) == expected
