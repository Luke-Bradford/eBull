"""#3284 item 4a — which repair outcomes count as refusals. Pure logic, no DB.

The arithmetic of a streak is `+1` and `= 0`; what needs testing is the CLASSIFICATION,
because every way of getting it wrong is silent. Folding ``reconcile_required`` into
"refused" raises a second alarm for a condition that already has one. Folding it into
"cleared" hides a real one. And a state added to ``PositionManagerResult`` without a
mapping here would be counted as whichever side a default happened to pick — which is why
there is no default and why the vocabularies are pinned to each other below.
"""

from __future__ import annotations

import typing

import pytest

from app.services.strategy_position_manager import PositionManagerResult
from app.services.strategy_position_repair_streak import (
    _OUTCOME_BY_STATE,
    RepairStreakError,
    RepairVisitState,
    classify_repair_visit,
)


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        # No gap was observed: the position already carries both levels.
        ("no_change", "cleared"),
        # Confirmed against the broker's own rates by `_edit_landed`.
        ("applied", "cleared"),
        # The arm declined: broker capability, an unsafe quote, or a standing prior
        # refusal of this exact edit. All three leave the position unprotected.
        ("rejected", "refused"),
        # A submitted edit that is demonstrably NOT in effect.
        ("pending", "refused"),
        # Acknowledged but unconfirmed, and uncertain-outcome mutations.
        ("submitted", "unknown"),
        ("reconcile_required", "unknown"),
    ],
)
def test_every_manager_state_maps_to_its_effect_on_the_streak(state: str, expected: str) -> None:
    assert classify_repair_visit(state) == expected


def test_an_uncertain_broker_edit_is_not_counted_as_a_refusal() -> None:
    """Stated as its own test because it is the one judgement call in the mapping.

    ``reconcile_required`` means the edit MAY have landed. Counting it would let the
    alert say "the stop cannot be set" about a stop that may well be set — and the
    condition is already loud by other means: ``_submit_edit`` sets
    ``strategy_trades.status='reconcile_required'`` and the resume path acts on it.
    """
    assert classify_repair_visit("reconcile_required") == "unknown"
    assert classify_repair_visit("rejected") == "refused"


def test_only_broker_confirmed_state_clears_a_streak() -> None:
    """The correction Codex checkpoint 2 forced, asserted so it cannot be undone.

    The first draft mapped ``submitted`` to ``cleared`` and ``pending`` to ``unknown``,
    reasoning that an accepted edit settles the episode. It does not:
    ``_resume_operation`` never terminalises a ``submitted`` edit whose levels fail to
    arrive — it returns ``broker_edit_pending`` from ABOVE the repair arm, forever — so
    that mapping reset the streak at the submit and then never touched it again, while the
    unresolved-operation index blocked any fresh attempt. A permanently naked position
    reading zero refusals.

    The property that fixes it, stated directly: acceptance is not protection.
    """
    # Acknowledgement preserves whatever the streak was; it does not clear it.
    assert classify_repair_visit("submitted") == "unknown"
    # An edit observed not in effect is the refusal, on every visit it stays that way.
    assert classify_repair_visit("pending") == "refused"
    # Exactly one state is evidence of protection from the broker's own rates, plus the
    # no-gap observation the arm makes directly.
    assert {state for state, outcome in _OUTCOME_BY_STATE.items() if outcome == "cleared"} == {
        "applied",
        "no_change",
    }


def test_an_unmapped_state_raises_rather_than_defaulting() -> None:
    """No default, deliberately — see the module docstring."""
    with pytest.raises(RepairStreakError, match="unknown repair visit state"):
        classify_repair_visit("exempt")


def test_the_two_state_vocabularies_can_never_drift_apart() -> None:
    """A tripwire, not a calculation.

    ``RepairVisitState`` mirrors ``PositionManagerResult.state`` rather than importing it
    (``strategy_position_manager`` imports the streak module, so the dependency only runs
    one way). That leaves them free to diverge, and divergence is exactly the silent
    failure the raise above is written to catch at run time — this catches it at push
    time instead.
    """
    manager_states = set(typing.get_args(typing.get_type_hints(PositionManagerResult)["state"]))
    assert set(typing.get_args(RepairVisitState)) == manager_states
    # ...and every state in the vocabulary is actually mapped, so the raise above is a
    # guard against a NEW state rather than a hole in the existing ones.
    assert set(_OUTCOME_BY_STATE) == manager_states
