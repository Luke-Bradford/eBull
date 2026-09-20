"""Pure tests for #2840 arm 2's forward-panel census.

No database. What is tested is the classification order and the value guards, because a
wrong order or an unguarded comparison produces a plausible tally rather than an error.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.strategies.s12_cheapest_band_price_gated_breakout import CHEAPEST_BAND
from scripts.census_2840_forward_panel_supply import classify_panel_member

GATE = Decimal("100")


def classify(**overrides: object) -> str:
    base: dict[str, object] = {
        "in_universe": True,
        "latest_close": Decimal("250"),
        "stale": False,
        "gate_lower": GATE,
    }
    base.update(overrides)
    return classify_panel_member(**base)  # type: ignore[arg-type]


def test_the_gate_the_tests_use_is_the_strategy_s_own() -> None:
    """⚠ If the cost table is re-measured the cheapest band can move.

    The census imports ``CHEAPEST_BAND`` rather than typing a literal; this asserts the
    fixture still describes the real rule. A failure is not a bug — it is the signal that
    every number in the readout now answers a different question.
    """
    assert CHEAPEST_BAND.lower == GATE


def test_below_gate_is_a_position_not_a_rejection() -> None:
    """⚠⚠ THE CORRECTION CODEX FORCED, PINNED AS AN ASSERTION.

    A sub-gate member is the half of the panel carrying the entries the CONTROL takes and
    the candidate refuses. On a panel where every member clears the gate the two books are
    identical and the paired difference is identically zero, so a census that treated
    ``below_gate`` as a failure would have recommended exactly the panel that guarantees a
    null. Both verdicts are positions; neither is a disqualification.
    """
    assert classify(latest_close=Decimal("88.22")) == "below_gate"
    assert classify(latest_close=Decimal("250")) == "at_or_above_gate"


def test_a_priced_up_etf_is_refused_before_the_gate_is_consulted() -> None:
    """SPY at ~$760 clears $100 and is outside the validated universe.

    Checking price first would report three of the eight declared panel members as
    carrying a strategy that cannot trade any of them.
    """
    assert classify(in_universe=False, latest_close=Decimal("761.69")) == "outside_strategy_universe"


@pytest.mark.parametrize(
    ("latest_close", "expected"),
    [
        (Decimal("335.93"), "at_or_above_gate"),  # AAPL
        (GATE, "at_or_above_gate"),  # inclusive, as `close >= lower` is
        (Decimal("99.99"), "below_gate"),
        (Decimal("4.52"), "below_gate"),  # CENN today
    ],
)
def test_a_fresh_in_universe_member_is_decided_by_its_latest_close(latest_close: Decimal, expected: str) -> None:
    assert classify(latest_close=latest_close) == expected


def test_a_stale_bar_is_named_rather_than_priced() -> None:
    """The freshness rule applies to the panel AND the population.

    The first version applied it only to the population, so a panel member whose last bar
    was months old counted as admitted against a rule its neighbours had to meet.
    """
    assert classify(stale=True, latest_close=Decimal("250")) == "stale_bar"
    assert classify(stale=True, latest_close=Decimal("1")) == "stale_bar"


def test_a_missing_bar_is_not_silently_below_the_gate() -> None:
    """Keeps an unobserved instrument distinguishable from an observed cheap one."""
    assert classify(latest_close=None) == "no_daily_bar"


@pytest.mark.parametrize(
    "value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), Decimal("0"), Decimal("-1")]
)
def test_a_non_price_is_refused_rather_than_compared(value: Decimal) -> None:
    """⚠ ``Decimal('NaN') < x`` RAISES and ``Infinity`` compares as above the gate.

    Both reached the threshold in the first version. Neither is a price, and an exception
    escaping a census is a worse outcome than a named refusal because it takes the whole
    readout with it.
    """
    assert classify(latest_close=value) == "unusable_close"


def test_the_universe_check_precedes_the_value_guards() -> None:
    """An out-of-universe member with a broken price is still out of universe.

    Reporting ``unusable_close`` there would suggest the member is a data problem to fix
    when it is a scope decision that no repair changes.
    """
    assert classify(in_universe=False, latest_close=Decimal("NaN")) == "outside_strategy_universe"
