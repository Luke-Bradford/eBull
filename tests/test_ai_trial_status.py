"""#3514 — every AI-trial "nothing happened" state is named, and no two look alike."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any

import pytest
from fastapi import HTTPException

from app.api import ai_trial as api
from app.services.ai_trial_status import (
    DECISION_JOBS,
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
    latest_fire_row,
    missable_sessions,
    next_fire,
)
from app.services.ai_trial_version import FUND_V1, TRIAL_VERSIONS, V1
from app.workers.scheduler import SCHEDULED_JOBS

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


def _labels(legs: list[LegFunding], *, completed: date, fires: list[datetime] | None = None) -> list[str]:
    return [o.label for o in execution_states(SESSION, legs, completed=completed, execute_fires=fires or [])]


def test_submitted_and_refused_legs_are_counted_by_verdict_and_code() -> None:
    legs = [
        LegFunding("arm", "allocated", "all_trial_entry_gates_passed"),
        LegFunding("control", "rejected", "trial_cost_cap"),
        LegFunding("arm", "rejected", "quote_stale"),
        LegFunding("control", "rejected", "quote_stale"),
    ]
    assert _labels(legs, completed=SESSION, fires=[IN_WINDOW_FIRE]) == [
        "submitted:1",
        "refused:quote_stale×2",
        "refused:trial_cost_cap×1",
    ]


def test_an_unfunded_leg_awaits_until_its_fire_or_its_close_then_is_not_run() -> None:
    legs = [LegFunding("arm", None, None), LegFunding("control", None, None)]
    before_close = date(2026, 10, 2)  # the latest closed session is Friday's
    # Its session has not closed and no in-window fire has run (a pre-15:00 boot catch-up does not count).
    assert _labels(legs, completed=before_close) == ["awaiting_execution:2"]
    early = datetime(2026, 10, 5, 13, 0, tzinfo=UTC)
    assert _labels(legs, completed=before_close, fires=[early]) == ["awaiting_execution:2"]
    # The in-window fire ran and left them (it raised on them).
    assert _labels(legs, completed=before_close, fires=[IN_WINDOW_FIRE]) == ["not_run:2"]
    # The session closed with no fire at all (the worker was down): too late to execute.
    assert _labels(legs, completed=SESSION) == ["not_run:2"]


def test_the_expected_session_is_the_latest_fire_target() -> None:
    # Before Monday's 23:30 fire, the latest fire was Sunday's, which targets Monday.
    fire, session = expected_decision_session(datetime(2026, 10, 5, 20, 0, tzinfo=UTC))
    assert (fire, session) == (datetime(2026, 10, 4, 23, 30, tzinfo=UTC), SESSION)
    # Monday's fire targets Tuesday.
    fire, session = expected_decision_session(datetime(2026, 10, 5, 23, 45, tzinfo=UTC))
    assert (fire, session) == (datetime(2026, 10, 5, 23, 30, tzinfo=UTC), date(2026, 10, 6))


def test_every_session_a_fire_since_genesis_could_decide_is_listed() -> None:
    # Tuesday 23:45: Tuesday's fire targeted Wednesday. Started the previous Wednesday 12:00 UTC.
    latest_fire, expected = expected_decision_session(datetime(2026, 10, 6, 23, 45, tzinfo=UTC))
    genesis = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
    assert missable_sessions(latest_fire, expected, genesis) == [
        date(2026, 10, 7),
        date(2026, 10, 6),
        SESSION,
        date(2026, 10, 2),
        date(2026, 10, 1),  # Wednesday 23:30's fire came after the start; Tuesday's did not.
    ]
    assert missable_sessions(latest_fire, expected, genesis, limit=2) == [date(2026, 10, 7), date(2026, 10, 6)]


def test_a_session_no_fire_has_reached_since_genesis_is_not_listed() -> None:
    # Saturday noon: Friday's fire targeted Monday, but the trial started after it; Sunday's
    # fire, still ahead, is the first that can decide Monday.
    latest_fire, expected = expected_decision_session(datetime(2026, 10, 3, 12, 0, tzinfo=UTC))
    assert expected == SESSION
    assert missable_sessions(latest_fire, expected, datetime(2026, 10, 3, 9, 0, tzinfo=UTC)) == []


def test_loss_headroom_is_limit_less_loss() -> None:
    assert LegLossStatus("arm", Decimal("-150"), 0, Decimal("600")).headroom_usd == Decimal("450")
    assert LegLossStatus("arm", Decimal("-601"), 0, Decimal("600")).headroom_usd == Decimal("-1")


def test_the_endpoint_serialises_every_field(monkeypatch: pytest.MonkeyPatch) -> None:
    fire = JobFire("ai_trial_execute", IN_WINDOW_FIRE, IN_WINDOW_FIRE, IN_WINDOW_FIRE, "success", "legs=2")
    status = TrialStatus(
        arm_strategy_id=FUND_V1.arm_strategy_id,
        strategy_version="v1",
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
                "arm",
                0,
                11,
                "AAPL",
                "open",
                Decimal("100"),
                Decimal("95"),
                Decimal("110"),
                False,
                None,
                None,
                None,
                SESSION,
            )
        ],  # fmt: skip
        loss=[LegLossStatus("arm", Decimal("-5"), 1, Decimal("600"))],
    )
    asked: list[Any] = []
    monkeypatch.setattr(api, "load_trial_status", lambda _conn, *, version: asked.append(version) or status)
    body: dict[str, Any] = api.get_ai_trial_status(
        arm=FUND_V1.arm_strategy_id,
        version="v1",
        conn=object(),  # type: ignore[arg-type]
    ).model_dump(mode="json")
    assert asked == [FUND_V1]
    assert (body["arm_strategy_id"], body["strategy_version"]) == (FUND_V1.arm_strategy_id, "v1")
    assert body["state"] == "halted_operator"
    assert body["sessions"][0]["execution"] == [
        {"state": "refused", "label": "refused:trial_cost_cap×1", "count": 1, "reason": "trial_cost_cap"},
        {"state": "submitted", "label": "submitted:1", "count": 1, "reason": None},
    ]
    assert body["open_legs"][0]["broker_stop"] is None and body["open_legs"][0]["pnl_usd"] is None
    assert body["loss"] == [{"leg": "arm", "pnl_usd": "-5", "unmeasured": 1, "limit_usd": "600", "headroom_usd": "595"}]


def test_an_unregistered_version_is_a_404_not_another_versions_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "load_trial_status", lambda *_a, **_k: pytest.fail("must not load"))
    with pytest.raises(HTTPException) as raised:
        api.get_ai_trial_status(arm="ai-discretionary-v9", version="v1", conn=object())  # type: ignore[arg-type]
    assert raised.value.status_code == 404


def test_every_registered_version_has_a_scheduled_decision_job() -> None:
    scheduled = {job.name for job in SCHEDULED_JOBS}
    assert set(DECISION_JOBS) == {version.arm_strategy_id for version in TRIAL_VERSIONS}
    assert {job.name for job in DECISION_JOBS.values()} <= scheduled
    # Each version's own job: no two versions read one job's fires.
    assert len({job.name for job in DECISION_JOBS.values()}) == len(DECISION_JOBS)


def _cadence(name: str) -> Any:
    return next(job.cadence for job in SCHEDULED_JOBS if job.name == name)


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        # Daytime: the half-hourly slots are outside the window until 23:30.
        (datetime(2026, 10, 5, 14, 0, tzinfo=UTC), datetime(2026, 10, 5, 23, 30, tzinfo=UTC)),
        # Inside the window the next slot is the next retry.
        (datetime(2026, 10, 5, 23, 40, tzinfo=UTC), datetime(2026, 10, 6, 0, 0, tzinfo=UTC)),
        # 04:10 UTC is after New York midnight (EDT): the window has closed until tonight.
        (datetime(2026, 10, 6, 4, 10, tzinfo=UTC), datetime(2026, 10, 6, 23, 30, tzinfo=UTC)),
    ],
)
def test_v1_next_fire_is_the_next_in_window_slot(now: datetime, expected: datetime) -> None:
    job = DECISION_JOBS[V1.arm_strategy_id]
    assert next_fire(_cadence(job.name), now, job.in_window) == expected


def test_fund_v1_reads_its_own_daily_fire() -> None:
    job = DECISION_JOBS[FUND_V1.arm_strategy_id]
    now = datetime(2026, 10, 5, 23, 40, tzinfo=UTC)
    assert job.opens_utc == time(23, 45)
    assert next_fire(_cadence(job.name), now, job.in_window) == datetime(2026, 10, 5, 23, 45, tzinfo=UTC)
    # Before Monday's 23:45 fire, the latest was Sunday's, targeting Monday.
    assert expected_decision_session(now, opens_utc=job.opens_utc) == (
        datetime(2026, 10, 4, 23, 45, tzinfo=UTC),
        SESSION,
    )


def _row(hour: int, minute: int, status: str) -> tuple[datetime, None, str, str]:
    return (datetime(2026, 10, 6, hour, minute, tzinfo=UTC), None, status, status)


def test_out_of_window_skips_never_hide_the_nights_decision() -> None:
    in_window = DECISION_JOBS[V1.arm_strategy_id].in_window
    decided = _row(0, 0, "success")
    rows = [_row(14, 30, "skipped"), _row(14, 0, "skipped"), decided]
    assert latest_fire_row(rows, in_window) == decided
    # A manual fire outside the window ran the body, so it is the last fire.
    manual = _row(15, 0, "success")
    assert latest_fire_row([manual, *rows], in_window) == manual
    # With only skips in the scan, the newest is shown: still true. No rows: never run.
    assert latest_fire_row(rows[:2], in_window) == rows[0]
    assert latest_fire_row([], in_window) is None
    # An unwindowed job shows its newest row, whatever it was.
    assert latest_fire_row(rows, None) == rows[0]
