"""#3471 v6 readout additions (§16.7, §16.11(b)) — pure: exit mix beside the library, the
per-leg calibration, pair geometry and the pool/refusal tables."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services.ai_trial_deadline import exit_deadline_session
from app.services.ai_trial_readout import (
    COHORT_SESSIONS,
    CloseRow,
    Dist,
    LegPlan,
    LegValue,
    PairPlan,
    PairRecord,
    PlanFacts,
    build_readout,
    calibration,
    dist,
    exit_mix,
    fraction_value,
    leg_geometry,
    partial_close,
    plan_readout,
    realised_net_r,
)

SESSION_1 = date(2026, 10, 1)


def _value(net: float | None, label: str = "deadline") -> LegValue:
    return LegValue(net, 100.0, None if net is None else net, label)


def _pair(
    seq: int, arm: LegValue | None, control: LegValue | None, *, state: str = "unit", session: date = SESSION_1
) -> PairRecord:
    return PairRecord(
        pair_seq=seq,
        session_date=session,
        state=state,  # type: ignore[arg-type]
        broken_reasons=(),
        arm=arm,
        control=control,
        regime_label="bull",
        confidence=3,
        resolved_session=exit_deadline_session(session, 5),
    )


def _leg_plan(stop_pct: str | None = "2", **overrides: Any) -> LegPlan:
    base: dict[str, Any] = {
        "stop_atr_multiple": Decimal("1.5"),
        "r_multiple": Decimal("2.5"),
        "stop_pct": None if stop_pct is None else Decimal(stop_pct),
        "target_pct": Decimal("5"),
        "close": Decimal("100"),
        "ask": Decimal("101"),
        "amount": Decimal("250"),
        "invalidation_age": 4,
        "target_age": None,
        "refusal": None,
    }
    return LegPlan(**{**base, **overrides})


def _plan(
    setup: str = "pullback_rising_sma20",
    horizon: int = 10,
    *,
    position: int = 0,
    pool: int = 5,
    self_draw: bool = False,
    arm: LegPlan | None = None,
    control: LegPlan | None = None,
) -> PairPlan:
    return PairPlan(
        setup_type=setup,
        horizon_days=horizon,
        response_position=position,
        base_rate_train="-1/10",
        base_rate_holdout="1/20",
        pool_size=pool,
        self_draw=self_draw,
        legs={"arm": arm or _leg_plan(), "control": control or _leg_plan()},
    )


def _holdout(stop: str, target: str, time: str) -> dict[str, Any]:
    return {"pct_stop": stop, "pct_target": target, "pct_time": time}


def test_the_exit_mix_counts_other_exits_outside_the_denominator_and_weights_the_library() -> None:
    pairs = [
        _pair(0, _value(-2.0, "stop"), _value(1.0)),
        _pair(1, _value(5.0, "target"), _value(1.0, "censored")),
        _pair(2, _value(1.0, "deadline"), _value(1.0, "operator_close")),
        _pair(3, _value(3.0, "target"), _value(1.0)),
    ]
    plans = {0: _plan(), 1: _plan(), 2: _plan(), 3: _plan("breakout_donchian20", 5)}
    library = {
        ("pullback_rising_sma20", 10): _holdout("30", "10", "60"),
        ("breakout_donchian20", 5): _holdout("20", "40", "40"),
    }
    arm = exit_mix(pairs, plans, library, "arm")
    assert (arm.legs, arm.pct_stop, arm.pct_target, arm.pct_time) == (4, 25.0, 50.0, 25.0)
    # The library side carries the leg's own cell weights: 3 legs at 10 %, 1 at 40 %.
    assert arm.library_pct_target == pytest.approx(17.5)
    (breakout, pullback) = arm.cells
    assert (breakout.setup_type, breakout.legs, breakout.pct_target, breakout.library_pct_target) == (
        "breakout_donchian20",
        1,
        100.0,
        40.0,
    )
    assert (pullback.legs, pullback.stop, pullback.target, pullback.time) == (3, 1, 1, 1)
    # A censored exit is §9's clock, not the library's time exit: counted, not in the rates.
    control = exit_mix(pairs, plans, library, "control")
    assert (control.legs, control.pct_time, control.other) == (2, 100.0, {"censored": 1, "operator_close": 1})
    # A cell without its library row leaves the pooled library figure unprinted, never re-weighted.
    partial = exit_mix(pairs, plans, {("pullback_rising_sma20", 10): _holdout("30", "10", "60")}, "arm")
    assert partial.library_pct_target is None and partial.cells[0].library_pct_target is None


def test_realised_net_r_is_net_pct_over_the_applied_stop_pct() -> None:
    assert realised_net_r(_value(4.0), Decimal("2")) == (2.0, None)
    assert realised_net_r(_value(None), Decimal("2")) == (None, "unvalued")
    assert realised_net_r(_value(float("nan")), Decimal("2")) == (None, "unvalued")
    for stop in (None, Decimal("0"), Decimal("-1"), Decimal("NaN")):
        assert realised_net_r(_value(4.0), stop) == (None, "stop_pct_missing")


def test_calibration_counts_what_it_cannot_value_and_pools_by_valued_legs() -> None:
    pairs = [
        _pair(0, _value(4.0, "target"), _value(-2.0, "stop")),
        _pair(1, _value(-2.0, "stop"), _value(1.0, "operator_close")),
        _pair(2, None, None, state="broken"),
        _pair(3, _value(3.0), _value(None)),
        _pair(4, _value(6.0), _value(1.0)),
    ]
    plans = {
        0: _plan(),
        1: _plan(),
        2: _plan(),
        3: _plan("breakout_donchian20", 5, arm=_leg_plan("3"), control=_leg_plan("1")),
        4: _plan("breakout_donchian20", 5, arm=_leg_plan("3"), control=_leg_plan(None)),
    }
    arm = calibration(pairs, plans, "arm")
    # pullback: R 2 and −1 (mean 0.5); breakout: R 1 and 2 (mean 1.5); pooled by 2 and 2 legs.
    assert [(c.setup_type, c.legs, c.mean_net_r) for c in arm.cells] == [
        ("breakout_donchian20", 2, 1.5),
        ("pullback_rising_sma20", 2, 0.5),
    ]
    assert (arm.legs, arm.mean_net_r, arm.counted) == (4, pytest.approx(1.0), {"pair_broken": 1})
    # The baselines are the decision rows' fractions, pooled with the same weights.
    assert (arm.library_train_mean_net_r, arm.library_holdout_mean_net_r) == (pytest.approx(-0.1), pytest.approx(0.05))
    control = calibration(pairs, plans, "control")
    assert [(c.legs, c.mean_net_r) for c in control.cells] == [(0, None), (1, -1.0)]
    assert control.cells[0].counted == {"stop_pct_missing": 1, "unvalued": 1}
    assert control.counted == {
        "exit:operator_close": 1,
        "pair_broken": 1,
        "stop_pct_missing": 1,
        "unvalued": 1,
    }
    # The empty cell carries no weight; the pooled figure is the one valued leg's.
    assert (control.legs, control.mean_net_r) == (1, -1.0)
    assert calibration([_pair(0, None, None, state="broken")], {0: _plan()}, "arm").mean_net_r is None


def test_a_malformed_baseline_is_not_read() -> None:
    assert fraction_value("-778148871698593/8089000000000000") == pytest.approx(-0.096198, abs=1e-6)
    assert [fraction_value(None), fraction_value("1/0"), fraction_value("x")] == [None, None, None]


def test_geometry_reports_each_figure_and_counts_what_is_missing() -> None:
    geometry = leg_geometry(
        [
            _leg_plan(),
            _leg_plan("4", ask=None, amount=None, invalidation_age=None, target_age=7),
        ]
    )
    assert (geometry.stop_pct.n, geometry.stop_pct.mean, geometry.stop_pct.max) == (2, 3.0, 4.0)
    # Stop % × funded amount: 2 % of $250.
    assert (geometry.dollar_risk_usd.n, geometry.dollar_risk_usd.mean, geometry.dollar_risk_usd.missing) == (
        1,
        5.0,
        1,
    )
    assert (geometry.ask_over_close.mean, geometry.ask_over_close.missing) == (pytest.approx(1.01), 1)
    assert (geometry.invalidation_age_bars.n, geometry.target_age_bars.n) == (1, 1)
    assert dist([]) == Dist(0, None, None, None, None, 0)
    assert dist([None]).missing == 1


def test_the_plan_readout_tabulates_pools_refusals_and_response_position() -> None:
    units = [_pair(0, _value(3.0), _value(1.0)), _pair(1, _value(1.0), _value(2.0))]
    broken = _pair(2, None, None, state="broken")
    plans = {
        0: _plan(position=0, pool=1, self_draw=True),
        1: _plan(position=1, pool=9, self_draw=True),
        2: _plan(position=0, pool=4, arm=_leg_plan(refusal=None), control=_leg_plan(refusal="plan_invalidated")),
    }
    facts = PlanFacts(plans, {"breakout_donchian20": 2}, {}, "ab" * 32, "caveat")
    readout = plan_readout([*units, broken], units, facts)
    assert (readout.pairs, readout.self_draws, readout.singleton_self_pools) == (3, 2, 1)
    assert readout.plan_invalidated == {"arm": 0, "control": 1}
    assert readout.exhausted_by_setup == {"breakout_donchian20": 2}
    assert [(row.group, row.units, row.mean_d) for row in readout.d_by_response_position] == [
        ("0", 1, 2.0),
        ("1", 1, -1.0),
    ]
    assert readout.pool_size.median == 4
    # Geometry covers the units only: the broken pair's legs are not in it.
    assert readout.geometry["arm"].stop_pct.n == 2


def _build(pairs: list[PairRecord], as_of: datetime, plan_facts: PlanFacts | None) -> Any:
    return build_readout(
        declaration_id=7,
        strategy_version="v1",
        declaration_sha256="ab" * 32,
        pairs=pairs,
        first_fill=SESSION_1,
        run_census={},
        decision_census={},
        run_costs=[],
        as_of=as_of,
        leg_capital_usd=Decimal("1000"),
        plan_facts=plan_facts,
    )


def test_the_plan_readout_is_a_due_readout_input_only() -> None:
    pairs = [_pair(0, _value(1.0), _value(0.5))]
    facts = PlanFacts({0: _plan()}, {}, {}, None, None)
    early = _build(pairs, datetime(2026, 10, 2, 22, tzinfo=UTC), facts)
    assert early.plans is None
    last = exit_deadline_session(SESSION_1, COHORT_SESSIONS - 1)
    due_at = datetime.combine(exit_deadline_session(last, 10), datetime.min.time(), tzinfo=UTC) + timedelta(hours=22)
    due = _build(pairs, due_at, facts)
    assert due.cohort.status == "due" and due.plans is not None
    assert due.plans.calibration["arm"].legs == 1
    assert _build(pairs, due_at, None).plans is None
    # An exploratory pair (after the cohort) is outside every v6 table.
    later = replace(pairs[0], pair_seq=1, session_date=exit_deadline_session(last, 1))
    with_later = _build([*pairs, later], due_at, PlanFacts({0: _plan(), 1: _plan()}, {}, {}, None, None))
    assert with_later.plans is not None
    assert (with_later.plans.pairs, with_later.plans.calibration["arm"].legs) == (1, 1)


def _slice(minute: int, price: str) -> CloseRow:
    at = datetime(2026, 10, 8, 15, minute, tzinfo=UTC)
    return CloseRow(at, at, Decimal("1"), Decimal("50"), Decimal("0"), Decimal(price), Decimal("95"), Decimal("110"))


def test_a_partial_close_before_the_final_exit_is_recognised() -> None:
    requested = datetime(2026, 10, 8, 15, 30, tzinfo=UTC)
    # Engine close: a slice executed before the close was requested is a partial.
    assert partial_close([_slice(10, "103"), _slice(31, "104")], "exit_deadline", requested)
    assert not partial_close([_slice(31, "104"), _slice(31, "104")], "exit_deadline", requested)
    # Broker close: a slice between stop and target beside a final stop slice.
    assert partial_close([_slice(10, "103"), _slice(40, "94")], None, None)
    assert not partial_close([_slice(40, "94"), _slice(41, "93")], None, None)


def test_a_partial_close_is_an_other_exit_and_is_not_calibrated() -> None:
    mixed = replace(_value(3.0, "target"), partial_close=True)
    pairs = [_pair(0, mixed, _value(1.0))]
    plans = {0: _plan()}
    mix = exit_mix(pairs, plans, {}, "arm")
    assert (mix.legs, mix.other) == (0, {"partial_close": 1})
    assert calibration(pairs, plans, "arm").counted == {"exit:partial_close": 1}
