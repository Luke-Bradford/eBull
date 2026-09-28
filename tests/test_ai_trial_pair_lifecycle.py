"""#3471 slice 2c-iii-b — the pair lifecycle's event derivation and O11 inclusion rule (pure)."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from app.services.ai_trial_pair_lifecycle import (
    UNCLASSIFIED,
    LegFacts,
    _LazyRegime,
    broken_reasons,
    clock_instant,
    entry_regime_label,
    leg_outcome,
    next_leg_events,
    pair_unit_state,
    previous_session,
)
from app.services.market_regime import Regime
from app.services.market_regime_provider import BenchmarkUnavailableError, MarketRegimeProvider

TARGET = date(2026, 10, 5)  # a Monday
IN_SESSION = datetime(2026, 10, 5, 15, 0, tzinfo=UTC)
DEADLINE = date(2026, 10, 19)
# Ten sessions after the deadline (Columbus Day and Election Day are NYSE sessions).
CENSOR_AT = datetime(2026, 11, 2, 15, 0, tzinfo=UTC)
# Ten sessions after the target session.
UNRESOLVED_AT = datetime(2026, 10, 19, 15, 0, tzinfo=UTC)
OPEN = LegFacts(
    funding_verdict="allocated", trade_status="open", broker_order_ref="1", filled_at=IN_SESSION, exit_deadline=DEADLINE
)


def test_the_clocks_run_to_15_utc_ten_sessions_on() -> None:
    assert clock_instant(DEADLINE, 10) == CENSOR_AT
    assert clock_instant(TARGET, 10) == UNRESOLVED_AT


@pytest.mark.parametrize(
    ("facts", "last", "owed"),
    [
        # Not yet executed, refused, or planned (the broker call may be in flight).
        (LegFacts(), None, []),
        (LegFacts(funding_verdict="rejected", refusal_code="trial_cost_cap"), None, []),
        (LegFacts(funding_verdict="allocated", trade_status="planned"), None, []),
        # Rejected by the broker: the call was made.
        (LegFacts(funding_verdict="allocated", trade_status="failed"), None, ["submitted"]),
        # Accepted by the broker, not yet filled.
        (LegFacts(funding_verdict="allocated", trade_status="submitted", broker_order_ref="1"), None, ["submitted"]),
        # Submission outcome unknown.
        (LegFacts(funding_verdict="allocated", trade_status="reconcile_required"), None, ["submitted", "uncertain"]),
        # Uncertain already recorded: nothing new until it resolves.
        (LegFacts(funding_verdict="allocated", trade_status="reconcile_required"), "uncertain", []),
        # A reconcile_required AFTER a fill (the manager's own use) is not an uncertain submission.
        (
            LegFacts(funding_verdict="allocated", trade_status="reconcile_required", filled_at=IN_SESSION),
            None,
            ["submitted", "filled"],
        ),
        # An uncertainty that resolved between two passes is not invented.
        (OPEN, None, ["submitted", "filled"]),
        (OPEN, "uncertain", ["filled"]),
        (OPEN, "filled", []),
    ],
)
def test_leg_events_follow_the_observed_state(facts: LegFacts, last: str | None, owed: list[str]) -> None:
    assert next_leg_events(facts, last, IN_SESSION) == owed  # type: ignore[arg-type]


def test_an_open_leg_is_censored_ten_sessions_after_its_own_deadline() -> None:
    assert next_leg_events(OPEN, "filled", CENSOR_AT - timedelta(seconds=1)) == []
    assert next_leg_events(OPEN, "filled", CENSOR_AT) == ["censored"]
    closed = replace(OPEN, trade_status="closed", closed_at=CENSOR_AT + timedelta(days=1))
    assert next_leg_events(closed, "censored", CENSOR_AT) == ["closed"]


def test_a_closed_leg_is_judged_by_its_close_time_not_by_when_the_pass_saw_it() -> None:
    late_pass = CENSOR_AT + timedelta(days=5)
    on_time = replace(OPEN, trade_status="closed", closed_at=CENSOR_AT - timedelta(seconds=1))
    overdue = replace(OPEN, trade_status="closed", closed_at=CENSOR_AT)
    assert next_leg_events(on_time, None, late_pass) == ["submitted", "filled", "closed"]
    assert next_leg_events(overdue, None, late_pass) == ["submitted", "filled", "censored", "closed"]


@pytest.mark.parametrize(
    ("facts", "now", "outcome"),
    [
        (OPEN, IN_SESSION, "ok"),
        # 23:30 New York is still the target session.
        (LegFacts(filled_at=datetime(2026, 10, 6, 3, 30, tzinfo=UTC)), IN_SESSION, "ok"),
        (LegFacts(filled_at=datetime(2026, 10, 6, 14, 0, tzinfo=UTC)), IN_SESSION, "late_fill"),
        (LegFacts(filled_at=datetime(2026, 10, 2, 15, 0, tzinfo=UTC)), IN_SESSION, "early_fill"),
        (LegFacts(funding_verdict="rejected", refusal_code="below_broker_minimum"), IN_SESSION, "below_broker_minimum"),
        (LegFacts(funding_verdict="rejected", refusal_code="Odd:Code-1"), IN_SESSION, "odd_code_1"),
        (LegFacts(funding_verdict="rejected", refusal_code="9"), IN_SESSION, "refused"),
        (LegFacts(funding_verdict="allocated", trade_status="failed"), IN_SESSION, "broker_rejected"),
        # Undetermined until the unresolved clock runs out, whatever the leg's state.
        (LegFacts(funding_verdict="allocated", trade_status="reconcile_required"), IN_SESSION, None),
        (LegFacts(), UNRESOLVED_AT - timedelta(seconds=1), None),
        (LegFacts(funding_verdict="allocated", trade_status="reconcile_required"), UNRESOLVED_AT, "unresolved"),
        (LegFacts(), UNRESOLVED_AT, "unresolved"),
    ],
)
def test_leg_outcomes(facts: LegFacts, now: datetime, outcome: str | None) -> None:
    assert leg_outcome(facts, TARGET, now) == outcome


@pytest.mark.parametrize(
    ("outcomes", "reasons"),
    [
        (["ok", "ok"], None),
        # A pair breaks only once both legs are determined, so the census carries both reasons.
        (["trial_cost_cap", None], None),
        (["ok", "late_fill"], ["late_fill"]),
        (["unresolved", "trial_cost_cap"], ["trial_cost_cap", "unresolved"]),
        (["late_fill", "late_fill"], ["late_fill"]),
    ],
)
def test_broken_reasons(outcomes: list[str | None], reasons: list[str] | None) -> None:
    assert broken_reasons(outcomes) == reasons


@pytest.mark.parametrize(
    ("events", "state"),
    [
        ([], "open"),
        ([("arm", "submitted"), ("arm", "filled"), ("control", "submitted")], "open"),
        # O11: an uncertain leg blocks the pair until reconciliation resolves it.
        ([("arm", "submitted"), ("arm", "uncertain"), ("control", "submitted"), ("control", "filled")], "blocked"),
        ([("arm", "submitted"), ("arm", "uncertain"), ("arm", "filled")], "open"),
        (
            [("arm", "submitted"), ("arm", "filled"), ("arm", "closed")]
            + [("control", "submitted"), ("control", "filled"), ("control", "censored")],
            "unit",
        ),
        (
            [("arm", "submitted"), ("arm", "filled"), ("arm", "closed"), ("control", "submitted")]
            + [("control", "uncertain"), (None, "broken")],
            "broken",
        ),
    ],
)
def test_pair_unit_state(events: list[tuple[str | None, str]], state: str) -> None:
    assert pair_unit_state(events) == state


# --- §9 regime label (slice 2c-iv-a) ---


@pytest.mark.parametrize(
    ("day", "expected"),
    [
        (date(2026, 9, 29), date(2026, 9, 28)),  # Tuesday → Monday
        (date(2026, 9, 28), date(2026, 9, 25)),  # Monday → Friday
        (date(2025, 12, 26), date(2025, 12, 24)),  # Christmas closed; Dec 24 is a half day, still a session
    ],
)
def test_previous_session_skips_weekends_and_holidays(day: date, expected: date) -> None:
    assert previous_session(day) == expected


def test_entry_label_is_the_regime_at_the_prior_session_close() -> None:
    provider = MarketRegimeProvider(
        regime_by_date={date(2026, 9, 25): Regime.BEAR_VOLATILE, date(2026, 9, 28): Regime.BULL_QUIET}
    )
    # Filled Monday: Friday's close is what was known; Monday's own close is not.
    assert entry_regime_label(provider, date(2026, 9, 28)) == "bear_volatile"


def test_entry_label_waits_for_a_missing_bar_and_names_warm_up() -> None:
    provider = MarketRegimeProvider(regime_by_date={date(2026, 9, 24): Regime.BULL_QUIET, date(2026, 9, 28): None})
    # No bar for Friday: never fall back to Thursday's.
    assert entry_regime_label(provider, date(2026, 9, 28)) is None
    # A bar that exists but is still in warm-up is labelled, not deferred.
    assert entry_regime_label(provider, date(2026, 9, 29)) == UNCLASSIFIED


def test_a_failed_benchmark_load_is_not_repeated_within_a_pass() -> None:
    calls: list[int] = []

    def unavailable(_: object) -> MarketRegimeProvider:
        calls.append(1)
        raise BenchmarkUnavailableError("no SPY")

    regime = _LazyRegime(unavailable)
    for _ in range(3):
        with pytest.raises(BenchmarkUnavailableError):
            regime.get(None)  # type: ignore[arg-type]
    assert calls == [1]
