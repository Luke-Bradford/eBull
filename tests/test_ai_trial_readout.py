"""#3471 §9 readout — pure arithmetic: leg valuation, exit labels, the primary p, harm looks, cohort
timing and the assembled readout."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from app.services.ai_trial_deadline import exit_deadline_session
from app.services.ai_trial_policy import FROZEN_CONSTANTS
from app.services.ai_trial_readout import (
    COHORT_SESSIONS,
    FLOW_WINDOW_SESSIONS,
    INSUFFICIENT,
    READING,
    READOUT_WAIT_SESSIONS,
    CensorMark,
    CloseRow,
    LegValue,
    PairRecord,
    Readout,
    build_readout,
    cohort,
    exit_label,
    harm_looks,
    primary,
    profit_factor,
    readout_seed,
    value_censored_leg,
    value_closed_leg,
)

SESSION_1 = date(2026, 10, 1)  # a Thursday NYSE session
CLOSE_AT = datetime(2026, 10, 8, 18, 0, tzinfo=UTC)
LATER = date(2027, 6, 1)


def _row(
    pnl: str | None = "5",
    investment: str | None = "100",
    *,
    recorded_at: datetime = CLOSE_AT,
    fees: str = "0",
    price: str = "105",
    stop: str | None = "95",
    take: str | None = "110",
) -> CloseRow:
    return CloseRow(
        executed_at=CLOSE_AT,
        recorded_at=recorded_at,
        realized_pnl_usd=None if pnl is None else Decimal(pnl),
        investment_usd=None if investment is None else Decimal(investment),
        fees_usd=Decimal(fees),
        price=Decimal(price),
        stop_rate=None if stop is None else Decimal(stop),
        take_rate=None if take is None else Decimal(take),
    )


def _leg(net: float | None, *, label: str = "deadline", opened: float = 100.0) -> LegValue:
    return LegValue(net, opened, None if net is None else net * opened / 100, label)


def _pair(
    seq: int,
    session: date,
    d: float,
    *,
    state: str = "unit",
    arm_label: str = "deadline",
    regime: str | None = "bull",
    confidence: int | None = 3,
) -> PairRecord:
    return PairRecord(
        pair_seq=seq,
        session_date=session,
        state=state,  # type: ignore[arg-type]
        broken_reasons=(),
        arm=_leg(d, label=arm_label) if state == "unit" else None,
        control=_leg(0.0) if state == "unit" else None,
        regime_label=regime,
        confidence=confidence,
        resolved_session=exit_deadline_session(session, 5) if state == "unit" else None,
    )


def _sessions(n: int, start: date = SESSION_1) -> list[date]:
    return [start] + [exit_deadline_session(start, i) for i in range(1, n)]


# -- leg valuation ---------------------------------------------------------------------------


def test_a_closed_leg_nets_its_counted_slices_and_restates_a_late_one() -> None:
    late = _row("-50", "100", recorded_at=CLOSE_AT + timedelta(days=14))
    value = value_closed_leg([_row("5", "100"), _row("-1", "50"), late], "exit_deadline")
    assert value.net_pct == pytest.approx(100 * 4 / 150)
    assert (value.open_amount, value.pnl_usd, value.exit_label) == (150.0, 4.0, "deadline")
    assert (value.restated_rows, value.unvalued_reason) == (1, None)


def test_a_closed_leg_is_unvalued_never_repaired() -> None:
    assert value_closed_leg([], None).unvalued_reason == "close_flow_missing"
    assert value_closed_leg([_row(pnl=None)], None).unvalued_reason == "close_flow_incomplete"
    assert value_closed_leg([_row(investment="0")], None).unvalued_reason == "close_flow_incomplete"


def test_a_nonzero_fee_is_counted_not_subtracted() -> None:
    value = value_closed_leg([_row("5", "100", fees="0.5")], None)
    assert (value.net_pct, value.fee_rows) == (pytest.approx(5.0), 1)


@pytest.mark.parametrize(
    ("trigger", "price", "expected"),
    [
        ("exit_deadline", "105", "deadline"),
        ("protection_failed", "105", "protection_failed"),
        ("operator_close", "105", "operator_close"),
        (None, "95", "stop"),
        (None, "90", "stop"),  # gapped through the stop
        (None, "110", "target"),
        (None, "112", "target"),
        (None, "100", "broker_other"),
    ],
)
def test_exit_labels(trigger: str | None, price: str, expected: str) -> None:
    assert exit_label([_row(price=price)], trigger) == expected


def test_a_broker_close_without_recorded_levels_is_not_mechanical() -> None:
    assert exit_label([_row(price="90", stop=None, take=None)], None) == "broker_other"
    assert exit_label([], None) == "broker_other"


def _mark(**overrides: object) -> CensorMark:
    fields: dict[str, object] = {
        "average_price": Decimal(100),
        "bid": Decimal("108.9"),
        "ask": Decimal("111.1"),
        "usd": True,
        "partial_close": False,
    }
    return CensorMark(**{**fields, **overrides})  # type: ignore[arg-type]


def test_a_censored_leg_is_marked_at_the_recorded_bid() -> None:
    # The recorded mid (110) less half its spread (1.1) is the bid, 108.9.
    value = value_censored_leg(_mark(), Decimal("2"))
    assert value.net_pct == pytest.approx(8.9)
    assert (value.open_amount, value.pnl_usd, value.exit_label) == (200.0, pytest.approx(17.8), "censored")


@pytest.mark.parametrize(
    ("mark", "reason"),
    [
        (_mark(usd=False), "non_usd_price"),
        (_mark(partial_close=True), "partial_close_before_censor"),
        (_mark(average_price=None), "entry_missing"),
        (_mark(bid=None), "quote_missing"),
        (_mark(bid=Decimal(112)), "quote_missing"),  # crossed quote
    ],
)
def test_a_censored_leg_without_a_clean_mark_is_unvalued(mark: CensorMark, reason: str) -> None:
    value = value_censored_leg(mark, Decimal(1))
    assert (value.net_pct, value.unvalued_reason, value.exit_label) == (None, reason, "censored")


# -- statistics -------------------------------------------------------------------------------


def test_the_primary_refuses_below_the_unit_or_cluster_minimum() -> None:
    sessions = _sessions(10)
    # 29 units over 10 clusters: too few units.
    units = [_pair(i, sessions[i % 10], 1.0) for i in range(29)]
    assert primary(units, seed=1).verdict == INSUFFICIENT
    assert primary(units, seed=1).p is None
    # 30 units over 9 clusters: too few clusters.
    units = [_pair(i, sessions[i % 9], 1.0) for i in range(30)]
    assert primary(units, seed=1).verdict == INSUFFICIENT


def test_the_primary_is_the_exact_sign_flip_p_at_ten_clusters() -> None:
    sessions = _sessions(10)
    units = [_pair(i, sessions[i % 10], 1.0) for i in range(30)]
    result = primary(units, seed=1)
    # Every cluster positive: only the identity reaches T_obs, so p = 1 / 2^10.
    assert (result.units, result.clusters, result.mean_d) == (30, 10, 1.0)
    assert result.p == pytest.approx(1 / 1024)
    assert result.verdict == READING


def test_harm_looks_wait_for_eight_clusters_and_halt_on_a_one_sided_p() -> None:
    sessions = _sessions(10)
    few = [_pair(i, sessions[i % 5], -1.0) for i in range(10)]
    looks = harm_looks(few, seed=1, today=LATER)
    assert [(look.k, look.skipped, look.halts) for look in looks] == [(1, "too_few_clusters", False)]

    harmful = [_pair(i, sessions[i], -1.0) for i in range(10)]
    (look,) = harm_looks(harmful, seed=1, today=LATER)
    assert (look.k, look.units, look.clusters) == (1, 10, 10)
    assert look.p_less == pytest.approx(1 / 1024)
    assert look.threshold == pytest.approx(0.025)
    assert look.halts


def test_a_pair_ahead_in_entry_order_that_is_not_a_unit_holds_the_look() -> None:
    sessions = _sessions(12)
    pairs = [_pair(0, sessions[0], 0.0, state="blocked")] + [_pair(i, sessions[i], -1.0) for i in range(1, 12)]
    assert harm_looks(pairs, seed=1, today=LATER) == []
    # A broken pair is census only and does not hold anything.
    pairs[0] = _pair(0, sessions[0], 0.0, state="broken")
    assert [look.k for look in harm_looks(pairs, seed=1, today=LATER)] == [1]


def test_an_unvalued_unit_holds_the_looks_only_until_its_flow_window_closes() -> None:
    sessions = _sessions(12)
    unvalued = replace(_pair(0, sessions[0], 0.0), control=LegValue(None, None, None, "deadline", "close_flow_missing"))
    assert unvalued.resolved_session is not None
    pairs = [unvalued] + [_pair(i, sessions[i], -1.0) for i in range(1, 12)]
    window_end = exit_deadline_session(unvalued.resolved_session, FLOW_WINDOW_SESSIONS)
    assert harm_looks(pairs, seed=1, today=window_end) == []
    (look,) = harm_looks(pairs, seed=1, today=exit_deadline_session(window_end, 1))
    assert (look.units, look.clusters) == (10, 10)


def test_profit_factor_reports_counts_without_losses() -> None:
    assert profit_factor([2.0, -1.0, 3.0, -4.0]).value == pytest.approx(1.0)
    no_losses = profit_factor([1.0, 2.0])
    assert (no_losses.value, no_losses.wins, no_losses.losses) == (None, 2, 0)
    assert profit_factor([]).trades == 0


# -- cohort and readout -----------------------------------------------------------------------


def test_cohort_timing() -> None:
    last = exit_deadline_session(SESSION_1, COHORT_SESSIONS - 1)
    assert cohort([], None, last).status == "no_fills"
    assert cohort([], SESSION_1, last).status == "not_due"
    after = exit_deadline_session(last, 1)
    blocked = _pair(1, SESSION_1, 0.0, state="blocked")
    assert "not yet resolved" in cohort([blocked], SESSION_1, after).detail
    unit = _pair(1, last, 1.0)  # resolves 5 sessions after the last cohort session
    due = exit_deadline_session(exit_deadline_session(last, 5), READOUT_WAIT_SESSIONS)
    # The due session must have finished: a row recorded late on it still counts.
    assert cohort([unit], SESSION_1, due).status == "not_due"
    state = cohort([unit], SESSION_1, exit_deadline_session(due, 1))
    assert (state.status, state.due_session) == ("due", due)
    # A unit whose regime label the lifecycle writer deferred keeps the cohort pending.
    assert "not yet resolved: [1]" in cohort([replace(unit, regime_label=None)], SESSION_1, LATER).detail


def test_a_broken_pair_with_a_live_leg_holds_the_readout_until_it_exits() -> None:
    last = exit_deadline_session(SESSION_1, COHORT_SESSIONS - 1)
    later = exit_deadline_session(last, 30)
    live = replace(_pair(1, SESSION_1, 0.0, state="broken"), live_legs=1)
    assert "not yet resolved: [1]" in cohort([live], SESSION_1, later).detail
    # Once the filled leg exits, its exit session anchors the wait.
    exited_at = exit_deadline_session(last, 12)
    exited = replace(live, live_legs=0, resolved_session=exited_at)
    state = cohort([exited], SESSION_1, later)
    assert (state.status, state.due_session) == ("due", exit_deadline_session(exited_at, READOUT_WAIT_SESSIONS))


def _build(pairs: list[PairRecord], as_of: datetime) -> Readout:
    return build_readout(
        declaration_id=7,
        strategy_version="v1",
        declaration_sha256="ab" * 32,
        pairs=pairs,
        first_fill=SESSION_1,
        run_census={"decided": 3},
        decision_census={"accepted": 3},
        run_costs=[0.5, 1.5],
        as_of=as_of,
    )


def test_the_readout_computes_no_primary_before_it_is_due() -> None:
    readout = _build([replace(_pair(1, SESSION_1, 1.0), pool_size=12)], datetime(2026, 10, 2, 22, tzinfo=UTC))
    assert readout.primary is None
    assert readout.arm_interval is None
    assert readout.pool_sizes == {1: 12}
    assert readout.model_cost_usd_per_run == 1.0


def test_the_due_readout_splits_cohort_from_exploratory_and_tabulates() -> None:
    sessions = _sessions(COHORT_SESSIONS + 1)
    units = [_pair(i, sessions[i % 10], 1.0 + (i % 2), confidence=1 + i % 2) for i in range(30)]
    units.append(_pair(30, sessions[3], -2.0, arm_label="operator_close"))
    exploratory = _pair(31, sessions[COHORT_SESSIONS], 5.0)
    as_of = datetime.combine(exit_deadline_session(sessions[COHORT_SESSIONS], 15), datetime.min.time(), tzinfo=UTC)
    readout = _build([*units, exploratory], as_of + timedelta(hours=22))
    assert readout.cohort.status == "due"
    assert readout.primary is not None and readout.mechanical_only is not None
    assert readout.primary.units == 31
    assert readout.mechanical_only.units == 30
    assert readout.exploratory_units == 1
    assert readout.arm_interval is not None
    assert readout.arm_interval.cluster_count == 10
    interval = readout.arm_interval
    assert interval.ci_low_pct <= interval.point_estimate_pct <= interval.ci_high_pct
    assert readout.order_parity == {"arm_first": 16, "control_first": 15}
    assert readout.exit_labels["arm"] == {"deadline": 30, "operator_close": 1}
    assert [row.group for row in readout.per_confidence] == ["1", "2", "3"]


def test_the_seed_is_declared_by_construction_and_the_stopping_rules_are_hashed_by_value() -> None:
    assert readout_seed("ab" * 32) == readout_seed("ab" * 32) != readout_seed("cd" * 32)
    assert FROZEN_CONSTANTS["ai_trial_readout.MECHANICAL_EXITS"] == ("censored", "deadline", "stop", "target")
    assert FROZEN_CONSTANTS["ai_trial_readout.MIN_UNITS"] == 30
