"""#3471 slice 3a — §9 sign-flip p and the planning-table leg simulator (pure; no DB, no model)."""

from __future__ import annotations

import itertools
import math
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
import pytest

from app.services.ai_trial_stats import EXACT_MAX_CLUSTERS, flip_set, sign_flip_p, sign_flip_p_batch
from app.services.indicator_series import BarSeries
from scripts.ai_trial_power import (
    Cell,
    Census,
    Leg,
    PanelInstrument,
    atr_at,
    draw_pair_differences,
    flip_set_sensitivity,
    in_universe,
    nyse_sessions,
    rejection_rates,
    simulate_leg,
)

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
# simulate_leg
# ---------------------------------------------------------------------------

D = Decimal
START = date(2025, 3, 3)  # a Monday
SESSIONS = nyse_sessions(START, START + timedelta(days=600))
FRONTIER = date(2026, 9, 25)


def _series(bars: list[tuple[str | None, ...]]) -> BarSeries:
    dates = tuple(SESSIONS[: len(bars)])
    rows = tuple(
        {"open": o and D(o), "high": h and D(h), "low": lo and D(lo), "close": c and D(c), "volume": 1000}
        for o, h, lo, c in bars
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def _inst(
    bars: list[tuple[str, str, str, str]], *, atr: float | None = 3.0, breaks: tuple[date, ...] = ()
) -> PanelInstrument:
    """Bars as (open, high, low, close) on consecutive NYSE sessions; bar 0 is the signal bar. The
    ATR is pinned through the cache so the bracket arithmetic is exact."""
    series = _series(list(bars))
    inst = PanelInstrument(1, series, {d: i for i, d in enumerate(series.dates)}, breaks)
    inst.atr_cache.update(dict.fromkeys(range(len(bars)), atr))
    return inst


# ATR 3.0 on a close of 100 → ATR14% = 3; k = 1.5 → stop 4.5%, R = 2 → target 9%
# (entry 100: SL 95.5, TP 109). Horizon 3: fill = bar 1 (session 0), deadline = bar 4.
CELL = Cell(stop_atr_multiple=D("1.5"), reward_risk=D("2"), horizon=3)
FLAT = ("100", "101", "99", "100")


def _leg(bars: list[tuple[str, str, str, str]], *, cell: Cell = CELL, frontier: date = FRONTIER, **kw: object) -> Leg:
    return simulate_leg(_inst(bars, **kw), SESSIONS[0], SESSIONS[1], cell, frontier=frontier)  # type: ignore[arg-type]


def test_target_touch_fills_at_the_target() -> None:
    leg = _leg([FLAT, ("100", "111", "99", "105"), FLAT, FLAT, FLAT])
    assert (leg.outcome, leg.return_pct) == ("tp_hit", pytest.approx(9.0))


def test_gap_through_the_stop_fills_at_the_open() -> None:
    leg = _leg([FLAT, FLAT, ("90", "91", "88", "90"), FLAT, FLAT])
    assert (leg.outcome, leg.return_pct) == ("sl_hit", pytest.approx(-10.0))


def test_ambiguous_bar_takes_the_stop_first() -> None:
    leg = _leg([FLAT, FLAT, ("100", "112", "94", "100"), FLAT, FLAT])
    assert (leg.outcome, leg.return_pct) == ("ambiguous_stop", pytest.approx(-4.5))


def test_horizon_exit_is_the_deadline_close_and_no_later_bar_matters() -> None:
    deadline = ("100", "101", "99", "103")
    # A bar after the deadline that gaps through the stop, and one that is missing entirely.
    assert _leg([FLAT, FLAT, FLAT, FLAT, deadline, ("80", "81", "79", "80")]).return_pct == pytest.approx(3.0)
    leg = _leg([FLAT, FLAT, FLAT, FLAT, deadline])
    assert (leg.outcome, leg.return_pct) == ("expired", pytest.approx(3.0))


def test_a_touch_on_the_deadline_session_counts() -> None:
    leg = _leg([FLAT, FLAT, FLAT, FLAT, ("100", "110", "99", "104")])
    assert (leg.outcome, leg.return_pct) == ("tp_hit", pytest.approx(9.0))


def test_series_ending_before_the_frontier_is_a_delisting_at_its_last_close() -> None:
    leg = _leg([FLAT, FLAT, FLAT, ("100", "101", "96", "97")])
    assert (leg.outcome, leg.return_pct) == ("delisted", pytest.approx(-3.0))


def test_unresolved_break_inside_the_hold_is_refused_not_booked_across_the_scale_change() -> None:
    # A 1:10 reverse split on bar 3 would read as a +900% target touch without the segment bound.
    bars = [FLAT, FLAT, FLAT, ("1000", "1010", "990", "1000"), FLAT]
    leg = _leg(bars, breaks=(SESSIONS[3],))
    assert (leg.return_pct, leg.refusal) == (None, "series_break")


def test_series_ending_at_the_frontier_is_refused_not_booked() -> None:
    leg = _leg([FLAT, FLAT, FLAT, FLAT], frontier=SESSIONS[3])
    assert (leg.return_pct, leg.refusal) == (None, "corpus_edge")


def test_levels_outside_the_section_5_bounds_are_ineligible() -> None:
    assert _leg([FLAT] * 5, atr=0.5).refusal == "levels_outside_bounds"  # stop 0.75% < 2%
    assert _leg([FLAT] * 5, atr=20.0).refusal == "levels_outside_bounds"  # stop 30% > 25%


def test_levels_are_judged_on_the_quantized_control_derivation() -> None:
    # ATR14% = q(100 × 1.333333 / 100) = 1.3333; k = 1.5 → stop q(1.99995) = 2.0000, which the
    # §5 floor admits. On raw floats the stop is 1.9999995% and would be refused.
    leg = _leg([FLAT] * 5, atr=1.333333)
    assert leg.refusal is None and leg.outcome == "expired"


def test_missing_atr_and_missing_fill_session_are_refused() -> None:
    assert _leg([FLAT] * 5, atr=None).refusal == "atr_invalid"
    inst = _inst([FLAT] * 5)
    # The next session passed in is not the instrument's next bar (a provider coverage hole).
    assert simulate_leg(inst, SESSIONS[0], SESSIONS[2], CELL, frontier=FRONTIER).refusal == (
        "next_bar_not_next_session"
    )
    assert simulate_leg(inst, SESSIONS[4], SESSIONS[5], CELL, frontier=FRONTIER).refusal == "no_next_bar"


def test_atr_window_lets_an_old_masked_bar_age_out() -> None:
    clean = ("100", "101", "99", "100")
    bars: list[tuple[str | None, ...]] = [clean] * 400
    bars[5] = ("100", None, None, "100")  # a quarantined wick early in the history
    inst = PanelInstrument(1, _series(bars), {}, ())
    assert atr_at(inst, 100) is None  # masked bar still inside the trailing window
    assert atr_at(inst, 399) == pytest.approx(2.0)  # aged out: the pack sees a clean window


def test_atr_needs_min_bars_inside_the_signal_segment() -> None:
    bars: list[tuple[str | None, ...]] = [("100", "101", "99", "100")] * 200
    series = _series(bars)
    inst = PanelInstrument(1, series, {}, (series.dates[150],))
    assert math.isnan(atr_at(inst, 170) or 0.0)  # 21 bars since the break < MIN_BARS
    assert atr_at(inst, 149) == pytest.approx(2.0)
    fresh = PanelInstrument(1, series, {}, ())
    assert math.isnan(atr_at(fresh, 30) or 0.0)
    assert _leg([FLAT] * 5, atr=math.nan).refusal == "too_few_bars"


def test_universe_floor_reads_the_signal_close() -> None:
    inst = _inst([("3", "3.1", "2.9", "2.99"), ("3", "3.1", "2.9", "3.00")])
    assert not in_universe(inst, SESSIONS[0])
    assert in_universe(inst, SESSIONS[1])
    assert not in_universe(inst, SESSIONS[9])


def test_nyse_sessions_skip_weekends_and_holidays() -> None:
    week = nyse_sessions(date(2025, 12, 22), date(2025, 12, 28))
    assert week == [date(2025, 12, 22), date(2025, 12, 23), date(2025, 12, 24), date(2025, 12, 26)]


# ---------------------------------------------------------------------------
# draw + rejection rates
# ---------------------------------------------------------------------------


def test_draw_pairs_arm_minus_control_and_replaces_thin_sessions() -> None:
    sessions = [date(2025, 3, 3), date(2025, 3, 4), date(2025, 3, 5)]
    # Session 1 has only refused legs → thin; the others return the instrument's position as a %.
    universes = [[0, 1, 2, 3], [0, 1, 2, 3], [4, 5, 6, 7]]

    def leg_for(j: int, n: int) -> Leg:
        return Leg(None, refusal="next_bar_not_next_session") if j == 1 else Leg(float(n), outcome="expired")

    census = Census()
    out = draw_pair_differences(
        np.random.default_rng(0), sessions, universes, leg_for, replicates=4, clusters=2, census=census
    )
    assert out.shape == (4, 2, 2)
    assert census.thin_sessions == {sessions[1]}
    # Four distinct legs per session, so the pair differences are bounded by the session's spread.
    assert np.all(np.abs(out) <= 3.0) and np.all(out != 0.0)

    with pytest.raises(RuntimeError, match="eligible legs"):
        draw_pair_differences(
            np.random.default_rng(0), sessions, universes, leg_for, replicates=1, clusters=3, census=Census()
        )


def test_flip_set_sensitivity_is_the_largest_rate_gap() -> None:
    a = {0.0: {15: 0.05, 20: 0.06}, 1.0: {15: 0.2, 20: 0.3}}
    b = {0.0: {15: 0.05, 20: 0.05}, 1.0: {15: 0.2, 20: 0.33}}
    assert flip_set_sensitivity(a, b) == pytest.approx(0.03)


def test_rejection_rates_respond_to_the_planted_shift() -> None:
    base = np.random.default_rng(2).normal(scale=10.0, size=(200, 20, 2))
    rates = rejection_rates(base, cluster_counts=(10, 20), shifts_pct=(0.0, 20.0), alpha=0.05, seed=1, flips=999)
    assert rates[0.0][10] < 0.15 and rates[0.0][20] < 0.15
    assert rates[20.0][20] > 0.95
