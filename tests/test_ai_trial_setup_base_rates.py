"""#3471 spec v6 §16.5 — the setup base-rate library's pure pieces (no DB)."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from fractions import Fraction

from app.services.ai_trial_decision import measure_atr
from app.services.ai_trial_levels import LEVEL_IDS, Level
from scripts.ai_trial_setup_base_rates import (
    Bar,
    Cell,
    CellKey,
    Evaluation,
    Trade,
    half_of,
    in_universe,
    run_setup,
    summarise,
    walk_exit,
)

SETUP = "breakout_donchian20"


def _bar(o: str, h: str, lo: str, c: str, v: int | None = 1_000_000) -> Bar:
    return (Decimal(o), Decimal(h), Decimal(lo), Decimal(c), v)


FLAT = _bar("100", "100", "100", "100")


def test_halves_are_inclusive_and_nothing_outside_them_belongs() -> None:
    assert half_of(date(2023, 1, 1)) == ("train", date(2025, 6, 30))
    assert half_of(date(2025, 6, 30)) == ("train", date(2025, 6, 30))
    assert half_of(date(2025, 7, 1)) == ("holdout", date(2026, 9, 25))
    assert half_of(date(2022, 12, 30)) is None
    assert half_of(date(2026, 9, 26)) is None


def test_universe_is_point_in_time_over_the_trailing_252_bars() -> None:
    liquid = [_bar("10", "10", "10", "10", 200_000)] * 300  # $2M a day
    assert not in_universe(liquid, 259)  # fewer than 260 prior bars
    assert in_universe(liquid, 260)
    thin = [_bar("10", "10", "10", "10", 50_000)] * 300
    assert not in_universe(thin, 280)
    cheap = [_bar("2", "2", "2", "2", 10_000_000)] * 300
    assert not in_universe(cheap, 280)
    # Only the trailing 252 bars count: an old illiquid stretch is forgotten, a later one is not.
    assert in_universe(thin[:40] + liquid[:260], 299)
    assert not in_universe(liquid[:150] + thin[:150], 299)


def test_walk_exit_rules() -> None:
    stop, target = Decimal("95"), Decimal("110")
    entry = [FLAT]
    assert walk_exit([*entry, _bar("94", "101", "93", "100")], 0, 1, stop, target) == ("stop", Decimal("94"))
    assert walk_exit([*entry, _bar("111", "112", "109", "111")], 0, 1, stop, target) == ("target", Decimal("111"))
    assert walk_exit([*entry, _bar("100", "101", "95", "100")], 0, 1, stop, target) == ("stop", stop)
    assert walk_exit([*entry, _bar("100", "110", "99", "105")], 0, 1, stop, target) == ("target", target)
    assert walk_exit([*entry, _bar("100", "115", "90", "100")], 0, 1, stop, target) == ("stop", stop)  # both: stop
    assert walk_exit([*entry, FLAT, _bar("101", "102", "99", "101")], 0, 2, stop, target) == ("time", Decimal("101"))
    holed: Bar = (Decimal("100"), None, None, Decimal("100"), 1)
    assert walk_exit([*entry, holed, FLAT], 0, 2, stop, target) is None


def _feasible() -> Evaluation:
    """close 100, ATR 2: support 96 → stop 95.5 (4.5%), target 110 (10%) — feasible at 5d."""
    levels: dict[str, Level | None] = dict.fromkeys(LEVEL_IDS)
    levels["sma20"] = Level(Fraction(96), None)
    levels["donchian20_high"] = Level(Fraction(110), None)
    return Evaluation(frozenset({SETUP}), levels, measure_atr(Decimal(2), Decimal(100)))


def _run(dates: list[date], states: list[Evaluation | None], bars: list[Bar] | None = None) -> dict[CellKey, Cell]:
    cells: dict[CellKey, Cell] = {}
    run_setup(7, dates, bars or [FLAT] * len(dates), states, SETUP, 5, cells)
    return cells


def _days(start: date, n: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def test_only_the_first_feasible_firing_of_a_run_is_taken_and_a_gap_resets_the_run() -> None:
    dates = _days(date(2024, 1, 1), 20)
    ok = _feasible()
    states: list[Evaluation | None] = [ok, ok, ok, None, ok, ok, *([None] * 14)]
    cell = _run(dates, states)[(SETUP, 5, "train")]
    assert (cell.firings, cell.deduped, len(cell.trades)) == (5, 3, 2)
    assert [t.entry_date for t in cell.trades] == [dates[0], dates[4]]
    # Flat bars: time exit at 100, net −0.3; stop by the executor's rule at 95.5.
    assert cell.trades[0].outcome == "time"
    assert cell.trades[0].net_pct == Decimal("-0.300000000000")
    assert cell.trades[0].net_r == Decimal("-0.066666666667")


def test_a_firing_without_a_plan_counts_no_valid_plan_and_does_not_consume_the_run() -> None:
    dates = _days(date(2024, 1, 1), 20)
    ok = _feasible()
    no_plan = Evaluation(frozenset({SETUP}), dict.fromkeys(LEVEL_IDS), ok.atr)
    cell = _run(dates, [no_plan, ok, *([None] * 18)])[(SETUP, 5, "train")]
    assert (cell.firings, cell.no_valid_plan, len(cell.trades)) == (2, 1, 1)
    assert cell.trades[0].entry_date == dates[1]


def test_a_run_resets_at_the_half_boundary_and_exits_never_cross_it() -> None:
    dates = _days(date(2025, 6, 26), 20)  # 06-26 … 07-15
    ok = _feasible()
    cells = _run(dates, [ok] * 10 + [None] * 10)
    train, holdout = cells[(SETUP, 5, "train")], cells[(SETUP, 5, "holdout")]
    # 06-26…06-30 fire; each exit (t+5) lands in July, so all are purged, none taken.
    assert (train.firings, train.purged, len(train.trades)) == (5, 5, 0)
    # 07-01 opens a new run in the holdout: one trade, the other four deduped.
    assert (holdout.firings, len(holdout.trades), holdout.deduped) == (5, 1, 4)
    assert holdout.trades[0].entry_date == date(2025, 7, 1)


def test_a_segment_ending_before_the_half_truncates() -> None:
    dates = _days(date(2024, 1, 1), 8)
    ok = _feasible()
    cell = _run(dates, [None, None, None, ok, ok, None, None, None])[(SETUP, 5, "train")]
    assert (cell.firings, cell.truncated, len(cell.trades)) == (2, 2, 0)


def test_a_hole_inside_the_walk_truncates_and_the_next_firing_can_still_be_taken() -> None:
    dates = _days(date(2024, 1, 1), 12)
    ok = _feasible()
    bars = [FLAT] * 12
    bars[3] = (Decimal("100"), None, None, Decimal("100"), 1)
    cell = _run(dates, [ok, ok, ok, ok, *([None] * 8)], bars)[(SETUP, 5, "train")]
    # Firings at t=0,1,2 walk through the hole at t=3; t=3's own walk (4…8) is clean.
    assert (cell.firings, cell.truncated, len(cell.trades)) == (4, 3, 1)
    assert cell.trades[0].entry_date == dates[3]


def test_summary_carries_exact_means_and_nulls_empty_subsets() -> None:
    empty = summarise(Cell(firings=3, no_valid_plan=3))
    assert empty["plans"] == 0 and empty["mean_net_r"] is None and empty["pct_stop"] is None
    assert empty["avg_win_net_pct"] is None
    trades = [
        Trade(1, date(2024, 1, 1), "stop", Decimal("-1"), Decimal("-1")),
        Trade(1, date(2024, 2, 1), "target", Decimal("2"), Decimal("2")),
        Trade(2, date(2024, 3, 1), "time", Decimal("0"), Decimal("0")),
    ]
    row = summarise(Cell(firings=5, trades=trades))
    assert row["plans"] == 3 and row["n_names"] == 2
    assert row["mean_net_r"] == "1/3" and row["mean_net_r_display"] == "0.333333"
    assert row["pct_stop"] == "33.333333"
    assert row["avg_win_net_pct"] == "2.000000" and row["avg_loss_net_pct"] == "-0.500000"
