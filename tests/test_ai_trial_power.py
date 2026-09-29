"""#3471 §9 sign-flip p and the v6 planning-table path simulator (slice v6-4c; pure; no DB, no model)."""

from __future__ import annotations

import itertools
from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction

import numpy as np
import pytest

from app.services.ai_trial_decision import measure_atr
from app.services.ai_trial_halts import TRIAL_LEG_CAPITAL_USD
from app.services.ai_trial_levels import LEVEL_IDS, Level
from app.services.ai_trial_stats import EXACT_MAX_CLUSTERS, flip_set, sign_flip_p, sign_flip_p_batch
from scripts.ai_trial_power import (
    FROZEN_TERMS,
    LEGS,
    PATH_SESSIONS,
    SHIFTS_PCT,
    Decision,
    Diagnostics,
    LegName,
    LegPlan,
    Name,
    PathContext,
    Terms,
    Trade,
    assert_order_6,
    build_panel,
    decide,
    leg_loss,
    make_name,
    nyse_sessions,
    open_leg,
    render,
    simulate_path,
    summarise,
    valid_bar,
    walk,
)
from scripts.ai_trial_setup_base_rates import Evaluation

# ---------------------------------------------------------------------------
# ai_trial_stats
# ---------------------------------------------------------------------------


def _brute_force_p(sums: list[float]) -> float:
    t_obs = sum(sums)
    flips = list(itertools.product((1, -1), repeat=len(sums)))
    return sum(1 for f in flips if sum(s * x for s, x in zip(f, sums, strict=True)) >= t_obs - 1e-12) / len(flips)


@pytest.mark.parametrize("sums", [[1.0, 2.0, 3.0], [1.0, -1.0, 0.0], [0.5, -2.0, 1.5, 0.25], [-1.0, -2.0]])
def test_exact_p_matches_brute_force_enumeration(sums: list[float]) -> None:
    assert sign_flip_p(sums, flip_set(len(sums), seed=0)) == pytest.approx(_brute_force_p(sums))


def test_exact_identity_is_counted_so_the_smallest_p_is_two_to_minus_k() -> None:
    assert sign_flip_p([1.0] * 5, flip_set(5, seed=0)) == pytest.approx(2**-5)


def test_ties_count_as_greater_or_equal() -> None:
    # Every flip of an all-zero vector ties T_obs, so p is 1, not 1/2^K.
    assert sign_flip_p([0.0, 0.0, 0.0], flip_set(3, seed=0)) == 1.0
    # A reordered float sum that reproduces T_obs is still a tie.
    sums = [0.1, 0.2, 0.3, -0.6]
    assert sign_flip_p(sums, flip_set(4, seed=0)) == pytest.approx(_brute_force_p(sums))


def test_the_tie_tolerance_is_scale_invariant() -> None:
    # A real inequality at a tiny scale must not be absorbed as a tie (Codex ckpt-3 on #3492).
    fs = flip_set(1, seed=0)
    assert sign_flip_p([1e-13], fs) == 0.5
    assert sign_flip_p([1e-13, 2e-13, 3e-13], flip_set(3, seed=0)) == sign_flip_p([1.0, 2.0, 3.0], flip_set(3, seed=0))
    # Large sums that cancel: T_obs = 0.5 and the flipped −0.5 is a real inequality, not a tie
    # (Codex ckpt-3 counterexample on #3492 — a 1e-12 relative tolerance gave 0.75).
    assert sign_flip_p([1e12, -1e12 + 0.5], flip_set(2, seed=0)) == 0.5


def test_less_alternative_is_the_mirror_of_greater() -> None:
    sums = [1.0, 2.0, 3.0]
    fs = flip_set(3, seed=0)
    assert sign_flip_p(sums, fs, alternative="less") == sign_flip_p([-x for x in sums], fs)


def test_exact_below_the_cap_and_monte_carlo_above() -> None:
    assert flip_set(EXACT_MAX_CLUSTERS, seed=0).exact
    mc = flip_set(EXACT_MAX_CLUSTERS + 1, seed=7, flips=499)
    assert not mc.exact and mc.signs.shape == (499, EXACT_MAX_CLUSTERS + 1)
    # Same seed, same flips — the readout's p is reproducible from the declared seed.
    assert np.array_equal(mc.signs, flip_set(EXACT_MAX_CLUSTERS + 1, seed=7, flips=499).signs)


def test_monte_carlo_p_adds_the_identity_once() -> None:
    # All-positive cluster sums: no flip other than the all-plus row can reach T_obs, so
    # p = (1 + #all-plus rows) / (1 + B).
    fs = flip_set(20, seed=3, flips=999)
    all_plus = int(np.all(fs.signs == 1.0, axis=1).sum())
    assert sign_flip_p(np.ones(20), fs) == pytest.approx((1 + all_plus) / 1000)


def test_batch_equals_row_by_row_and_survives_chunking(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.services.ai_trial_stats as stats

    monkeypatch.setattr(stats, "_BLOCK_ELEMENTS", 50)  # force several chunks
    rng = np.random.default_rng(1)
    sums = rng.normal(size=(13, 18))
    fs = flip_set(18, seed=5, flips=40)
    batch = sign_flip_p_batch(sums, fs)
    assert batch.tolist() == [sign_flip_p(row, fs) for row in sums]


def test_shape_and_finiteness_are_refused() -> None:
    with pytest.raises(ValueError, match="shape"):
        sign_flip_p([1.0, 2.0], flip_set(3, seed=0))
    with pytest.raises(ValueError, match="finite"):
        sign_flip_p([1.0, float("nan")], flip_set(2, seed=0))
    with pytest.raises(ValueError, match="at least one"):
        flip_set(0, seed=0)


# ---------------------------------------------------------------------------
# v6 grid: bars, walk, fill, loss
# ---------------------------------------------------------------------------

D = Decimal
Row = tuple[str, str, str, str] | None
START = date(2025, 3, 3)  # a Monday
CAL = nyse_sessions(START, START + timedelta(days=700))
FLAT: Row = ("100", "101", "99", "100")


def _name(rows: list[Row], *, iid: int = 1, positions: list[int] | None = None, **kw: object) -> Name:
    bars = [(None, None, None, None, None) if r is None else (*(D(x) for x in r), 1000) for r in rows]
    return make_name(iid, list(range(len(rows))) if positions is None else positions, bars, **kw)  # type: ignore[arg-type]


STOP, TARGET = D("95"), D("110")


def test_a_bar_is_valid_only_when_finite_positive_and_consistent() -> None:
    assert valid_bar((D(100), D(101), D(99), D(100), None))
    assert not valid_bar((D(100), D(101), D(101), D(100), 1))  # low above the open
    assert not valid_bar((D(100), D(99), D(98), D(100), 1))  # high below the close
    assert not valid_bar((D(100), D(101), D(0), D(100), 1))
    assert not valid_bar((D(100), D("NaN"), D(99), D(100), 1))


def test_the_fill_bar_is_intrabar_only_and_a_touch_of_both_takes_the_stop() -> None:
    target = walk(_name([("100", "111", "99", "105"), FLAT, FLAT, FLAT]), 0, 3, STOP, TARGET, CAL)
    assert (target.label, target.price, target.session) == ("target", TARGET, CAL[0])
    both = walk(_name([FLAT, ("100", "112", "94", "100"), FLAT, FLAT]), 0, 3, STOP, TARGET, CAL)
    assert (both.label, both.price) == ("stop", STOP)


def test_a_later_gap_through_a_level_fills_at_the_open() -> None:
    down = walk(_name([FLAT, ("90", "91", "88", "90"), FLAT, FLAT]), 0, 3, STOP, TARGET, CAL)
    assert (down.label, down.price, down.session) == ("stop", D(90), CAL[1])
    up = walk(_name([FLAT, ("115", "116", "114", "115"), FLAT, FLAT]), 0, 3, STOP, TARGET, CAL)
    assert (up.label, up.price) == ("target", D(115))


def test_the_deadline_counts_nyse_sessions_and_no_later_bar_matters() -> None:
    rows: list[Row] = [FLAT, FLAT, FLAT, ("100", "103", "99", "102"), ("50", "50", "50", "50")]
    exit_ = walk(_name(rows), 0, 3, STOP, TARGET, CAL)
    assert (exit_.label, exit_.price, exit_.session) == ("deadline", D(102), CAL[3])
    # A coverage hole inside the hold does not lengthen it: the deadline is still CAL[3].
    holed = walk(_name([FLAT, FLAT, ("100", "104", "99", "103")], positions=[0, 1, 3]), 0, 3, STOP, TARGET, CAL)
    assert (holed.price, holed.session) == (D(103), CAL[3])
    # An invalid bar is an absent session: its low of 80 is never read.
    bad = walk(_name([FLAT, ("100", "101", "101", "80"), FLAT, FLAT]), 0, 3, STOP, TARGET, CAL)
    assert (bad.label, bad.price) == ("deadline", D(100))


def test_a_missing_deadline_bar_exits_at_the_next_executable_open() -> None:
    exit_ = walk(_name([FLAT, FLAT, FLAT, ("98", "99", "97", "98")], positions=[0, 1, 2, 5]), 0, 3, STOP, TARGET, CAL)
    assert (exit_.label, exit_.price, exit_.session) == ("deadline", D(98), CAL[5])


def test_no_executable_bar_before_the_censor_session_censors_at_the_latest_close() -> None:
    ended = walk(_name([FLAT, ("100", "101", "96", "97")]), 0, 3, STOP, TARGET, CAL)
    assert (ended.label, ended.price, ended.session) == ("censored", D(97), CAL[13])
    # An unresolved break ends the segment: bars at the new scale are never read.
    split = walk(_name([FLAT, ("100", "101", "96", "97"), FLAT, FLAT], unresolved_breaks=[2]), 0, 3, STOP, TARGET, CAL)
    assert (split.label, split.price) == ("censored", D(97))


PLAN = LegPlan(1, Fraction(96), Fraction(110), Fraction(955, 10), D("4.5"), D("10"))


def _open(name: Name, *, slots: int = 0, plan: LegPlan = PLAN) -> Trade | str:
    return open_leg(name, CAL[0], CAL[1], plan, 5, calendar=CAL, fill_position=1, slots_held=slots)


def test_open_leg_refusals_in_order() -> None:
    assert _open(_name([FLAT], positions=[0])) == "no_fill"
    assert _open(_name([FLAT, None])) == "no_fill"
    assert _open(_name([FLAT] * 8, breaks=(CAL[1],))) == "plan_invalidated"
    for open_ in ("95.5", "96", "110"):
        assert _open(_name([FLAT, (open_, "111", "95", open_)] + [FLAT] * 6)) == "plan_invalidated"
    # Capacity is judged after the plan: an invalidated plan is named first.
    assert _open(_name([FLAT, ("96", "97", "95", "96")] + [FLAT] * 6), slots=4) == "plan_invalidated"
    assert _open(_name([FLAT] * 8), slots=4) == "capacity"


def test_a_candidate_concurrency_moves_only_the_capacity_refusal() -> None:
    assert _open(_name([FLAT] * 8), slots=4) == "capacity"
    trade = open_leg(
        _name([FLAT] * 8), CAL[0], CAL[1], PLAN, 5, calendar=CAL, fill_position=1, slots_held=4, max_concurrent=8
    )
    assert isinstance(trade, Trade)


def test_the_default_terms_are_the_frozen_trial_terms() -> None:
    # The grid's defaults must stay the declaration's: leg capital as ai_trial_halts derives it, and the
    # pre-parameterisation path length (20 start + 40 cohort + 20 hold + 10 censor + 6 wait → 100).
    assert FROZEN_TERMS == Terms()
    assert FROZEN_TERMS.leg_capital_usd == TRIAL_LEG_CAPITAL_USD
    assert FROZEN_TERMS.loss_limit_usd == pytest.approx(200.0)
    assert PATH_SESSIONS == 100
    wider = Terms(max_concurrent=8, cohort_sessions=80)
    assert (wider.leg_capital_usd, wider.loss_limit_usd, wider.path_sessions) == (
        Decimal(2000),
        pytest.approx(400.0),
        140,
    )


def test_open_leg_places_the_executor_rates_on_the_open_and_nets_the_tariff() -> None:
    trade = _open(_name([FLAT, ("100", "101", "99", "100")] + [FLAT] * 6))
    assert isinstance(trade, Trade)
    assert (trade.entry, trade.exit.label, trade.exit.session) == (D(100), "deadline", CAL[6])
    assert trade.net_pct == pytest.approx(-0.3)
    stopped = _open(_name([FLAT, FLAT, ("100", "101", "95", "96")] + [FLAT] * 5))
    assert isinstance(stopped, Trade) and stopped.exit.price == D("95.5")


def _trade(fill: int, exit_: int, price: str, lows: dict[int, str], *, label: str = "deadline") -> Trade:
    from scripts.ai_trial_power import Exit

    entry = D(100)
    return Trade(
        1,
        CAL[fill],
        entry,
        Exit(D(price), CAL[exit_], label, tuple((CAL[k], D(v)) for k, v in sorted(lows.items()))),  # type: ignore[arg-type]
        float(100 * (D(price) - entry) / entry) - 0.3,
    )


def test_leg_loss_is_realised_plus_the_session_low_bound() -> None:
    open_trade = _trade(0, 5, "100", {0: "97"})
    assert leg_loss([open_trade], CAL[0]) == pytest.approx(250 * 0.03)
    # No bar read on CAL[1]: the latest earlier low since the fill.
    assert leg_loss([open_trade], CAL[1]) == pytest.approx(250 * 0.03)
    # Exiting this session: min(low, exit) plus the tariff; no intraday gain offsets it.
    stopped = _trade(0, 1, "95", {0: "99", 1: "94"}, label="stop")
    assert leg_loss([stopped], CAL[1]) == pytest.approx(250 * (0.06 + 0.003))
    # After its exit it is realised.
    assert leg_loss([stopped], CAL[2]) == pytest.approx(250 * 0.053)
    # No low read at all: marked at the entry.
    assert leg_loss([_trade(0, 5, "100", {})], CAL[0]) == 0.0


# ---------------------------------------------------------------------------
# v6 grid: decisions and whole paths (a fixed feasible structure on every name)
# ---------------------------------------------------------------------------


def _evaluation(setups: tuple[str, ...] = ("breakout_donchian20",)) -> Evaluation:
    # close 100, ATR 2: invalidation 96 → stop 95.5 (2.25 ATR, 4.5%); target 110 (10%, R 2.22).
    levels: dict[str, Level | None] = dict.fromkeys(LEVEL_IDS, None)
    levels["swing_low_1"] = Level(Fraction(96), 5)
    levels["donchian20_high"] = Level(Fraction(110), 5)
    return Evaluation(frozenset(setups), levels, measure_atr(D(2), D(100)))


def _context(rows: list[Row] | None = None, *, n: int = 6, evaluation: Evaluation | None = None) -> PathContext:
    bars: list[Row] = [FLAT] * len(CAL) if rows is None else rows
    names = {iid: _name(bars, iid=iid) for iid in range(1, n + 1)}
    position = {d: p for p, d in enumerate(CAL)}
    ev = _evaluation() if evaluation is None else evaluation

    def evaluate(iid: int, session: date) -> Evaluation | None:
        return ev if names[iid].index_of(position[session]) is not None and evaluation is not False else None

    return PathContext(names, CAL, position, tuple(names), s0=1, seed=7, replicate=0, evaluate=evaluate)


def test_decide_excludes_arm_and_control_holdings_and_is_reproducible() -> None:
    ctx = _context(n=3)
    holdings: dict[LegName, frozenset[int]] = {"arm": frozenset({1}), "control": frozenset({2})}

    def run() -> tuple[list[Decision], Diagnostics]:
        diagnostics = Diagnostics()
        out = decide(
            ctx,
            horizon=5,
            session=CAL[0],
            fill=CAL[1],
            holdings=holdings,
            next_pair_seq=0,
            hex_="a" * 64,
            diagnostics=diagnostics,  # type: ignore[arg-type]
        )
        return out, diagnostics

    decisions, diagnostics = run()
    assert {d.arm.instrument_id for d in decisions} <= {2, 3}
    assert all(d.control.instrument_id in {1, 3} for d in decisions)
    assert [d.pair_seq for d in decisions] == list(range(len(decisions)))
    assert diagnostics.arm_decisions == 2
    # Two draws from a 2-name pool with no replacement: both pairs are created.
    assert len(decisions) + diagnostics.control_pool_exhausted == 2
    assert [(d.arm.instrument_id, d.control.instrument_id) for d in decisions] == [
        (d.arm.instrument_id, d.control.instrument_id) for d in run()[0]
    ]


def test_a_flat_path_enrolls_hits_capacity_and_reads_d_zero() -> None:
    result = simulate_path(_context(), horizon=5, shift=0.0)
    diag = result.diagnostics
    assert not result.halted_loss and not result.no_start
    assert set(diag.refused_legs) <= {"capacity"} and diag.refused_legs["capacity"] > 0
    assert result.units == diag.pairs - diag.broken_pairs
    assert result.sum_d == pytest.approx(0.0)
    assert result.outcome in ("insufficient", "not_reject")


def test_a_wider_concurrency_refuses_fewer_legs_for_capacity() -> None:
    frozen = simulate_path(_context(), horizon=5, shift=0.0).diagnostics.refused_legs["capacity"]
    wider = simulate_path(_context(), horizon=5, shift=0.0, terms=Terms(max_concurrent=8))
    assert wider.diagnostics.refused_legs["capacity"] < frozen


def test_a_crash_halts_entries_on_the_loss_bound_and_positions_run_out() -> None:
    crash: Row = ("40", "41", "39", "40")
    rows: list[Row] = [FLAT] * 10 + [crash] * (len(CAL) - 10)
    result = simulate_path(_context(rows), horizon=20, shift=0.0)
    assert result.halted_loss and result.pairs_at_loss_halt is not None
    # No pair enters after the halt, so the cohort is exactly the pairs filled by then.
    assert result.units == result.pairs_at_loss_halt
    assert result.outcome == "insufficient"


def test_a_harmful_planted_shift_trips_the_harm_stop() -> None:
    assert simulate_path(_context(), horizon=5, shift=-5.0).outcome == "halted_harm"


def test_a_path_with_nothing_evaluable_never_starts() -> None:
    result = simulate_path(_context(evaluation=False), horizon=5, shift=0.0)  # type: ignore[arg-type]
    assert (result.no_start, result.units, result.outcome) == (True, 0, "insufficient")


def test_summary_and_render_cover_every_cell_with_every_path_in_the_denominator() -> None:
    ctx = _context()
    results = [simulate_path(ctx, horizon=h, shift=s) for h in (5, 10, 20) for s in SHIFTS_PCT]
    cells = summarise(results)
    assert set(cells) == {"5", "10", "20"}
    assert all(cells[h]["by_shift"]["0.0"]["outcomes"]["insufficient"]["n"] == 1 for h in cells)
    assert "### horizon 5" in render(
        {
            "window": ["a", "b"],
            "frontier": "f",
            "instruments": 6,
            "replicates": 1,
            "seed": 7,
            "terms": {"max_concurrent": 4, "cohort_sessions": 40, "leg_capital_usd": "1000", "frozen": True},
            "cells": cells,
        }
    )
    assert LEGS == ("arm", "control")


def test_order_6_passes_for_every_setup_and_horizon_in_the_frozen_library() -> None:
    assert_order_6()


def test_build_panel_needs_room_for_a_whole_path() -> None:
    panel = build_panel({}, CAL)
    assert panel.starts and max(panel.starts) + PATH_SESSIONS < len(CAL)
    with pytest.raises(RuntimeError, match="no start session"):
        build_panel({}, CAL[:PATH_SESSIONS])


def test_a_weekend_dated_break_still_cuts_at_the_next_retained_bar() -> None:
    from scripts.ai_trial_power import segment_cuts

    kept = [date(2025, 3, 6), date(2025, 3, 7), date(2025, 3, 10), date(2025, 3, 11)]
    # Saturday 2025-03-08: the NYSE cut dropped that bar, the scale change still starts on the 10th.
    assert segment_cuts(kept, [date(2025, 3, 8)]) == [2]
    # Before the first or after the last retained bar: nothing to split.
    assert segment_cuts(kept, [date(2025, 1, 2), date(2025, 4, 1)]) == []
