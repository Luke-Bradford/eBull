"""#3514 — every AI-trial "nothing happened" state is named, and no two look alike."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from app.api import ai_trial as api
from app.services.ai_trial_status import (
    DecisionOutcome,
    ExecutionOutcome,
    JobFire,
    LegFunding,
    LegLossStatus,
    OpenLeg,
    RunFacts,
    SessionStatus,
    TrialStatus,
    decision_state,
    execution_states,
    expected_decision_session,
)

SESSION = date(2026, 10, 5)  # a Monday
IN_WINDOW_FIRE = datetime(2026, 10, 5, 15, 0, 5, tzinfo=UTC)


def _run(status: str = "decided", *, reason: str | None = None, decisions: int = 0, legs: int = 0, **refusals: int):
    return RunFacts(SESSION, status, reason, decisions, refusals, legs)


@pytest.mark.parametrize(
    ("run", "state", "label"),
    [
        (None, "not_run", "not_run"),
        (_run("claimed"), "deciding", "deciding"),
        (_run("refused", reason="trial_capacity_unavailable:account_risk_unavailable"), "refused",
         "refused:trial_capacity_unavailable:account_risk_unavailable"),
        (_run(decisions=0), "abstained", "abstained"),
        (_run(decisions=2, legs=0, no_valid_plan=1, already_held=1), "no_valid_plan", "no_valid_plan"),
        (_run(decisions=3, legs=4, not_in_shortlist=1), "legs_published", "legs_published:4"),
    ],
)  # fmt: skip
def test_each_decision_outcome_is_named(run: RunFacts | None, state: str, label: str) -> None:
    outcome = decision_state(run)
    assert (outcome.state, outcome.label) == (state, label)


def test_a_run_whose_decisions_were_all_refused_says_why() -> None:
    outcome = decision_state(_run(decisions=2, legs=0, no_valid_plan=1, already_held=1))
    assert outcome.decision_refusals == {"no_valid_plan": 1, "already_held": 1}


def _labels(legs: list[LegFunding], *, today: date = SESSION, fires: list[datetime] | None = None) -> list[str]:
    return [o.label for o in execution_states(SESSION, legs, today=today, execute_fires=fires or [])]


def test_submitted_and_refused_legs_are_counted_by_verdict_and_code() -> None:
    legs = [
        LegFunding("arm", "allocated", "all_trial_entry_gates_passed"),
        LegFunding("control", "rejected", "trial_cost_cap"),
        LegFunding("arm", "rejected", "quote_stale"),
        LegFunding("control", "rejected", "quote_stale"),
    ]
    assert _labels(legs, fires=[IN_WINDOW_FIRE]) == ["submitted:1", "refused:quote_stale×2", "refused:trial_cost_cap×1"]


def test_an_unfunded_leg_awaits_its_fire_then_is_not_run() -> None:
    legs = [LegFunding("arm", None, None), LegFunding("control", None, None)]
    # A later session, and today before the in-window fire (a pre-15:00 boot catch-up does not count).
    assert _labels(legs, today=date(2026, 10, 2)) == ["awaiting_execution:2"]
    early = datetime(2026, 10, 5, 13, 0, tzinfo=UTC)
    assert _labels(legs, fires=[early]) == ["awaiting_execution:2"]
    # Today after the in-window fire, and any later day: the fire did not reach them.
    assert _labels(legs, fires=[IN_WINDOW_FIRE]) == ["not_run:2"]
    assert _labels(legs, today=date(2026, 10, 6), fires=[IN_WINDOW_FIRE]) == ["not_run:2"]


def test_the_expected_session_is_the_latest_fire_target() -> None:
    # Before Monday's 23:30 fire, the latest fire was Sunday's, which targets Monday.
    fire, session = expected_decision_session(datetime(2026, 10, 5, 20, 0, tzinfo=UTC))
    assert (fire, session) == (datetime(2026, 10, 4, 23, 30, tzinfo=UTC), SESSION)
    # Monday's fire targets Tuesday.
    fire, session = expected_decision_session(datetime(2026, 10, 5, 23, 45, tzinfo=UTC))
    assert (fire, session) == (datetime(2026, 10, 5, 23, 30, tzinfo=UTC), date(2026, 10, 6))


def test_loss_headroom_is_limit_less_loss() -> None:
    assert LegLossStatus("arm", Decimal("-150"), 0, Decimal("600")).headroom_usd == Decimal("450")
    assert LegLossStatus("arm", Decimal("-601"), 0, Decimal("600")).headroom_usd == Decimal("-1")


def test_the_endpoint_serialises_every_field(monkeypatch: pytest.MonkeyPatch) -> None:
    fire = JobFire("ai_trial_execute", IN_WINDOW_FIRE, IN_WINDOW_FIRE, IN_WINDOW_FIRE, "success", "legs=2")
    status = TrialStatus(
        state="halted_operator",
        declaration_id=7,
        state_reason="loss_halt_unproven:leg=arm",
        state_at=IN_WINDOW_FIRE,
        decision_job=fire,
        execute_job=fire,
        sessions=[
            SessionStatus(
                SESSION,
                DecisionOutcome("legs_published", "legs_published:2", legs=2),
                [ExecutionOutcome("refused", 1, "trial_cost_cap"), ExecutionOutcome("submitted", 1)],
            )
        ],
        open_legs=[
            OpenLeg(
                "arm", 0, 11, "AAPL", "open", Decimal("100"), Decimal("95"), Decimal("110"), None, None, None, SESSION
            )
        ],  # fmt: skip
        loss=[LegLossStatus("arm", Decimal("-5"), 1, Decimal("600"))],
    )
    monkeypatch.setattr(api, "load_trial_status", lambda _conn: status)
    body: dict[str, Any] = api.get_ai_trial_status(conn=object()).model_dump(mode="json")  # type: ignore[arg-type]
    assert body["state"] == "halted_operator"
    assert body["sessions"][0]["execution"] == [
        {"state": "refused", "label": "refused:trial_cost_cap×1", "count": 1, "reason": "trial_cost_cap"},
        {"state": "submitted", "label": "submitted:1", "count": 1, "reason": None},
    ]
    assert body["open_legs"][0]["broker_stop"] is None and body["open_legs"][0]["pnl_usd"] is None
    assert body["loss"] == [{"leg": "arm", "pnl_usd": "-5", "unmeasured": 1, "limit_usd": "600", "headroom_usd": "595"}]
