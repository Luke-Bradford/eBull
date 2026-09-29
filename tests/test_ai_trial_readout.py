"""#3471 §9 readout — pure arithmetic: leg valuation, exit labels, the primary p, harm looks, cohort
timing and the assembled readout."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

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
    close_return_pct,
    cohort,
    exit_label,
    fill_gap_pct,
    harm_looks,
    primary,
    profit_factor,
    readout_seed,
    round_trip_spread_pct,
    spy_capital,
    spy_pairs,
    turnover,
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
    assert look.halts and look.flows_final
    # The same look inside the last unit's flow window is provisional.
    last_window = exit_deadline_session(harmful[-1].resolved_session or LATER, FLOW_WINDOW_SESSIONS)
    (early,) = harm_looks(harmful, seed=1, today=last_window)
    assert early.halts and not early.flows_final


def test_an_open_pair_ahead_in_entry_order_holds_the_look() -> None:
    sessions = _sessions(12)
    pairs = [_pair(0, sessions[0], 0.0, state="open")] + [_pair(i, sessions[i], -1.0) for i in range(1, 12)]
    assert harm_looks(pairs, seed=1, today=LATER) == []
    # Broken pairs are census only; a blocked one is excluded until it resolves (§9).
    for state in ("broken", "blocked"):
        pairs[0] = _pair(0, sessions[0], 0.0, state=state)
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


def _build(pairs: list[PairRecord], as_of: datetime, **spy: Any) -> Readout:
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
        leg_capital_usd=Decimal("1000"),
        **spy,
    )


def test_the_readout_computes_no_primary_before_it_is_due() -> None:
    readout = _build([replace(_pair(1, SESSION_1, 1.0), pool_size=12)], datetime(2026, 10, 2, 22, tzinfo=UTC))
    assert readout.primary is None
    assert readout.arm_interval is None
    assert readout.pool_sizes == {1: 12}
    assert readout.model_cost_usd_per_run == 1.0


def test_fill_versus_ask_is_measured_on_every_filled_leg_and_never_estimated() -> None:
    assert fill_gap_pct(Decimal("100"), Decimal("100.5")) == pytest.approx(0.5)
    assert fill_gap_pct(Decimal("100"), Decimal("99")) == pytest.approx(-1.0)
    # A missing or non-positive price is a missing gap, not a zero.
    assert [fill_gap_pct(None, Decimal("1")), fill_gap_pct(Decimal("0"), Decimal("1"))] == [None, None]
    assert fill_gap_pct(Decimal("1"), None) is None
    pairs = [
        replace(_pair(1, SESSION_1, 1.0), fill_gaps={"arm": 0.5, "control": -1.0}),
        # A broken pair whose arm filled with no stored ask (pre-sql/438), control never filled.
        replace(_pair(2, SESSION_1, 0.0, state="broken"), fill_gaps={"arm": None}),
        replace(_pair(3, SESSION_1, 0.0, state="broken"), fill_gaps={"arm": 1.5}),
    ]
    # Printed before the cohort is due: it describes execution, not the outcome.
    readout = _build(pairs, datetime(2026, 10, 2, 22, tzinfo=UTC))
    assert readout.primary is None
    arm, control = readout.fill_vs_ask["arm"], readout.fill_vs_ask["control"]
    assert (arm.legs, arm.mean_pct, arm.max_pct, arm.ask_missing) == (2, pytest.approx(1.0), 1.5, 1)
    assert (control.legs, control.mean_pct, control.max_pct, control.ask_missing) == (1, -1.0, -1.0, 0)


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


# -- SPY references and turnover (O13) ---------------------------------------------------------


def _spanned(net: float | None, entry: date, exit_: date, *, opened: float | None = 100.0) -> LegValue:
    pnl = None if net is None or opened is None else net * opened / 100
    return LegValue(net, opened, pnl, "deadline", entry_session=entry, exit_session=exit_)


def test_close_return_and_round_trip_spread() -> None:
    s = _sessions(3)
    closes = {s[0]: Decimal("400"), s[1]: Decimal("404"), s[2]: None}
    assert close_return_pct(closes, s[0], s[1]) == pytest.approx(1.0)
    # A masked (None), missing or non-positive close is no reference, never a zero.
    assert close_return_pct(closes, s[0], s[2]) is None
    assert close_return_pct(closes, s[0], LATER) is None
    assert close_return_pct({s[0]: Decimal("0"), s[1]: Decimal("1")}, s[0], s[1]) is None
    assert round_trip_spread_pct(Decimal("99.9"), Decimal("100.1")) == pytest.approx(0.2)
    assert [round_trip_spread_pct(None, Decimal("1")), round_trip_spread_pct(Decimal("2"), Decimal("1"))] == [
        None,
        None,
    ]


def test_spy_per_pair_spans_the_arm_legs_own_sessions() -> None:
    s = _sessions(6)
    closes: dict[date, Decimal | None] = {s[0]: Decimal("100"), s[2]: Decimal("102"), s[3]: Decimal("99")}
    units = [
        replace(_pair(1, s[0], 0.0), arm=_spanned(5.0, s[0], s[2])),  # SPY +2 → excess +3
        replace(_pair(2, s[0], 0.0), arm=_spanned(-1.0, s[0], s[3])),  # SPY −1 → excess 0
        replace(_pair(3, s[0], 0.0), arm=_spanned(4.0, s[0], s[5])),  # no close on s[5]
        _pair(4, s[0], 1.0),  # the loader set no sessions
    ]
    refs = spy_pairs(units, closes)
    assert (refs.units, refs.missing) == (2, 2)
    assert refs.mean_spy_pct == pytest.approx(0.5)
    assert refs.mean_arm_minus_spy_pct == pytest.approx(1.5)


def test_spy_capital_level_charges_one_round_trip_and_sets_the_arm_beside_it() -> None:
    s = _sessions(3)
    closes: dict[date, Decimal | None] = {s[0]: Decimal("500"), s[2]: Decimal("510")}
    legs = [
        _spanned(10.0, s[0], s[1], opened=250.0),
        _spanned(-4.0, s[1], s[2], opened=250.0),
        _spanned(None, s[0], s[1]),
    ]
    ref = spy_capital(
        legs, closes, (Decimal("499.5"), Decimal("500.5")), capital_usd=Decimal("1000"), start=s[0], end=s[2]
    )
    assert ref.gross_pct == pytest.approx(2.0)
    assert ref.spread_pct == pytest.approx(0.2)
    assert ref.net_pct == pytest.approx(1.8)
    assert ref.net_usd == pytest.approx(18.0)
    # (25 − 10) ÷ 1000; the unvalued leg is counted, never estimated.
    assert (ref.arm_pct, ref.arm_legs, ref.arm_unvalued) == (pytest.approx(1.5), 2, 1)
    # No recorded quote: the gross stands, the net is not invented.
    bare = spy_capital(legs, closes, (None, None), capital_usd=Decimal("1000"), start=s[0], end=s[2])
    assert (bare.gross_pct, bare.spread_pct, bare.net_pct, bare.net_usd) == (pytest.approx(2.0), None, None, None)


def test_turnover_is_opened_over_mean_committed_per_twenty_sessions() -> None:
    s = _sessions(3)
    legs = [_spanned(1.0, s[0], s[1]), _spanned(1.0, s[1], s[2]), _spanned(None, s[0], s[2], opened=None)]
    result = turnover(legs)
    # Committed by session: 100, 200, 100 → mean 133.33; opened 200 over 3 sessions.
    assert (result.legs, result.opened_usd, result.sessions, result.missing) == (2, 200.0, 3, 1)
    assert result.mean_committed_usd == pytest.approx(400 / 3)
    assert result.per_20_sessions == pytest.approx(200 / (400 / 3) * 20 / 3)
    assert turnover([]).per_20_sessions is None
    # Only unvalued legs: no ratio, but the window they span is still reported (review bot).
    unvalued = turnover([legs[2]])
    assert (unvalued.legs, unvalued.sessions, unvalued.per_20_sessions, unvalued.missing) == (0, 3, None, 1)
    # An unvalued leg's dates still bound the window (Codex ckpt-2): 4 sessions, committed
    # 100, 200, 100, 0 → mean 100.
    later = _sessions(4)
    wider = turnover([*legs[:2], _spanned(None, later[0], later[3], opened=None)])
    assert (wider.sessions, wider.mean_committed_usd, wider.missing) == (4, pytest.approx(100.0), 1)


def test_the_due_readout_carries_spy_references_and_turnover() -> None:
    sessions = _sessions(COHORT_SESSIONS + 1)
    closes: dict[date, Decimal | None] = {day: Decimal(400 + i) for i, day in enumerate(sessions)}
    units = [
        replace(
            _pair(i, sessions[i], 1.0),
            arm=_spanned(2.0, sessions[i], sessions[i + 1]),
            control=_spanned(1.0, sessions[i], sessions[i + 1]),
        )
        for i in range(30)
    ]
    as_of = datetime.combine(exit_deadline_session(sessions[COHORT_SESSIONS], 15), datetime.min.time(), tzinfo=UTC)
    before = _build(units, datetime(2026, 10, 2, 22, tzinfo=UTC), spy_closes=closes)
    assert (before.spy_per_pair, before.spy_capital, before.turnover) == (None, None, {})
    readout = _build(units, as_of + timedelta(hours=22), spy_closes=closes, spy_quote=(Decimal("1"), Decimal("1")))
    assert readout.spy_per_pair is not None and readout.spy_per_pair.units == 30
    assert readout.spy_capital is not None
    assert (readout.spy_capital.start_session, readout.spy_capital.end_session) == (
        SESSION_1,
        readout.cohort.due_session,
    )
    # The due session lies past the stubbed closes, so the capital-level gross is honestly None.
    assert readout.spy_capital.gross_pct is None
    assert readout.spy_capital.arm_pct == pytest.approx(100 * 30 * 2.0 / 1000)
    assert set(readout.turnover) == {"arm", "control"} and readout.turnover["arm"].legs == 30
