"""#2843 — the autonomy flag's pure rules.

⚠ SPLIT FROM `tests/test_2843_autonomy_approval_mode_db.py` DELIBERATELY, NOT FOR
TIDINESS. `tests/conftest.py` auto-applies the `db` marker per MODULE, so one DB test
in this file would drag every rule below out of the fast pre-push gate. These are the
rules that must run on every push: they are what stops the policy approver quietly
gaining an action or losing a refusal.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.strategy_autonomous_promotion import (
    AUTONOMOUS_ACTIONS,
    AUTONOMOUS_APPROVER,
    AUTONOMY_POLICY_VERSION,
    cycle_precondition_refusal,
    planned_action,
)
from app.services.strategy_control_plane import (
    UNCONFIGURED_MANDATE,
    ApprovalMode,
    PaperPool,
    Stage,
    mandate_for_profile,
    resolve_approval_mode,
)
from app.services.strategy_operator_promotion import _EVIDENCE_ACTIONS, _NEXT_STAGE

_CONFIGURED = mandate_for_profile("balanced")


def _pool(*, approval_mode: ApprovalMode = "autonomous", enabled: bool = True, configured: bool = True) -> PaperPool:
    return PaperPool(
        event_id=1,
        enabled=enabled,
        capital_limit=Decimal("1000"),
        currency="USD",
        capital_mode="fixed",
        mandate=_CONFIGURED if configured else UNCONFIGURED_MANDATE,
        approval_mode=approval_mode,
    )


@pytest.mark.parametrize(
    ("pool", "expected"),
    [
        (_pool(), None),
        (_pool(approval_mode="manual"), "approval_mode_manual"),
        (_pool(configured=False), "mandate_unconfigured"),
        (_pool(enabled=False), "paper_pool_disabled"),
        # Manual wins over every other refusal: an authority that is not asking for a
        # policy approver has no further question to answer.
        (_pool(approval_mode="manual", enabled=False, configured=False), "approval_mode_manual"),
    ],
)
def test_cycle_precondition_refusal(pool: PaperPool, expected: str | None) -> None:
    assert cycle_precondition_refusal(pool) == expected


def test_a_default_constructed_pool_is_never_autonomous() -> None:
    """The read side of a safety flag must fail closed on an unconfigured install.

    `load_paper_pool` returns exactly this shape when the table is empty, which is
    the state every install starts in and the state dev is in today.
    """
    assert cycle_precondition_refusal(PaperPool(None, False, Decimal("0"))) == "approval_mode_manual"


@pytest.mark.parametrize(
    ("stage", "expected_action", "expected_skip"),
    [
        (None, None, "action_not_evidence_backed"),
        ("research_candidate", "validate_historical", None),
        ("historical_validated", "start_forward_observation", None),
        ("forward_observation", "enable_paper", None),
        ("paper_enabled", None, "stage_terminal"),
        ("live_enabled", None, "stage_terminal"),
        ("paused", None, "stage_terminal"),
        ("retired", None, "stage_terminal"),
    ],
)
def test_planned_action(stage: Stage | None, expected_action: str | None, expected_skip: str | None) -> None:
    assert planned_action(stage) == (expected_action, expected_skip)


def test_planned_action_is_total_over_the_declared_stage_graph() -> None:
    """No stage may make the planner raise.

    A stage the planner cannot classify would abort the whole cycle on the first
    strategy that reached it, which is a far worse failure than a skip.
    """
    for stage in _NEXT_STAGE:
        action, skip = planned_action(stage)
        assert (action is None) != (skip is None)


def test_registration_is_not_a_policy_action() -> None:
    """`register_research_candidate` carries no evidence, so a policy approver acting
    on evidence has nothing to act on. Asserted from the graph rather than restated:
    the entry stage's action must be the one the planner refuses."""
    assert planned_action(None) == (None, "action_not_evidence_backed")


def test_autonomous_actions_are_exactly_the_evidence_actions() -> None:
    """⚠ The whole safety argument for this module in one assertion.

    The policy approver may take an action IFF that action carries evidence. Writing
    the set out by hand would let the two drift the moment a stage is added — a new
    evidence action silently outside policy reach, or worse, a zero-evidence action
    silently inside it. Equality is what keeps "approves on evidence" true by
    construction rather than by review.
    """
    assert AUTONOMOUS_ACTIONS == _EVIDENCE_ACTIONS


def test_the_approver_stamp_is_a_usable_promoted_by() -> None:
    """`promote_strategy` runs `_require_text(promoted_by, ...)`, so an empty or
    whitespace stamp would refuse every autonomous promotion at the last step."""
    assert AUTONOMOUS_APPROVER == f"policy@{AUTONOMY_POLICY_VERSION}"
    assert AUTONOMOUS_APPROVER.strip() == AUTONOMOUS_APPROVER
    assert AUTONOMOUS_APPROVER.strip() != ""


@pytest.mark.parametrize(
    ("requested", "current", "expected"),
    [
        # ⚠ THE ONE THAT MATTERS. Every existing client of PUT /strategies/paper-pool
        # omits the field, so if omission meant "manual" the next capital-limit edit
        # would silently revoke autonomy and return 200.
        (None, "autonomous", "autonomous"),
        (None, "manual", "manual"),
        ("autonomous", "manual", "autonomous"),
        ("manual", "autonomous", "manual"),
        ("autonomous", "autonomous", "autonomous"),
    ],
)
def test_resolve_approval_mode(requested: ApprovalMode | None, current: ApprovalMode, expected: ApprovalMode) -> None:
    assert resolve_approval_mode(requested, current) == expected
