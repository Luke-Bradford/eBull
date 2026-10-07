"""#3609 step 2 slice 3c-v(f): the segment signal block (cost band × size cell) and the FF-12 marginal IC block, on
synthetic inputs only (no corpus, no stage B)."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date

import pytest

from app.services.factor_book import COMPOSITE, Exact, Scores, compare, composite_scores
from app.services.factor_book_path import HoldingReturn, Month, entry_band
from app.services.factor_book_series import ARMS
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3609_step2_operations import Window
from scripts.report_3609_step2_segments import (
    OUTSIDE_CELL,
    PRICE_UNAVAILABLE,
    UNIVERSE_CELL,
    SegmentMonth,
    band_of,
    cell_monthly,
    industry_monthly,
    segment_month,
    segments,
)
from scripts.report_3609_step2_signals import SIGNALS, ic

M1 = date(2020, 1, 31)
M2 = date(2020, 2, 29)
CHEAP, DEAR = 3.0, 50.0
LOW, HIGH = entry_band(CHEAP)[0], entry_band(DEAR)[0]


def _holding(r: float, arms: tuple[str, ...] = ARMS) -> HoldingReturn:
    return HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(arms, r))  # type: ignore[arg-type]


# --------------------------------------------------------------------------- cells


def test_bands_are_step_0s_and_a_bad_close_is_price_unavailable() -> None:
    assert LOW != HIGH
    assert (band_of(CHEAP), band_of(DEAR)) == (LOW, HIGH)
    for close in (None, math.nan, math.inf, 0.0, -1.0):
        assert band_of(close) == PRICE_UNAVAILABLE


def _panel_name(n: int, industry: str = "Manuf") -> PanelName:
    # Three families per name, varied so z-scores move when the group changes.
    signed = {"gp_at": float(n % 7), "be_me": float((3 * n) % 11), "at_gr1": float((5 * n) % 13)}
    return PanelName(n, 1e9 * (100 - n), industry, signed, _holding(0.01 * n), 3720)


def test_each_size_cell_is_scored_on_its_own_names_and_bands_come_from_the_close() -> None:
    names = {n: _panel_name(n) for n in range(40)}
    close = {n: (CHEAP if n % 2 else DEAR) for n in range(40)} | {39: math.nan}
    del close[38]  # no bar at all
    month = PanelMonth(M1, M1, names, close, {})
    universe = list(range(20))
    industry = {n: "Manuf" for n in names}

    def signed(chosen: list[int]) -> dict[str, dict[int, float]]:
        return {c: {n: names[n].signed[c] for n in chosen} for c in ("gp_at", "be_me", "at_gr1")}

    book = composite_scores(universe, industry, signed(universe))
    out = segment_month(month, universe, book)
    assert out.scores[UNIVERSE_CELL] is book
    outside = list(range(20, 40))
    expected = composite_scores(outside, industry, signed(outside)).composite
    assert out.scores[OUTSIDE_CELL].composite.keys() == expected.keys() == set(outside)
    assert all(compare(out.scores[OUTSIDE_CELL].composite[n], expected[n]) == 0 for n in outside)
    # Standardised within the cell: pooling both cells would score an outside name differently.
    pooled = composite_scores(list(names), industry, signed(list(names))).composite
    assert any(compare(pooled[n], expected[n]) != 0 for n in outside)
    assert out.cells[0] == (HIGH, UNIVERSE_CELL) and out.cells[1] == (LOW, UNIVERSE_CELL)
    assert out.cells[21] == (LOW, OUTSIDE_CELL)
    assert out.cells[38] == out.cells[39] == (PRICE_UNAVAILABLE, OUTSIDE_CELL)
    assert out.price_unavailable == 2


def test_a_universe_name_outside_the_admitted_population_is_a_contract_error() -> None:
    month = PanelMonth(M1, M1, {1: _panel_name(1)}, {1: DEAR}, {})
    with pytest.raises(ValueError, match="outside the admitted population"):
        segment_month(month, [1, 2], Scores())


# --------------------------------------------------------------------------- monthly blocks


def _segment(
    formation: date, cells: Mapping[int, tuple[str, str]], industry: Mapping[int, str], sign: Mapping[int, float]
) -> SegmentMonth:
    """A universe-cell name scores ``n`` and an outside one ``-n``; every return is ``sign[n] * n`` per mille. So
    each cell's IC is ``sign`` on its own cell's scores, and flips on the other cell's."""
    inside = Scores({COMPOSITE: {n: Exact.raw(float(n)) for n in cells}})
    outside = Scores({COMPOSITE: {n: Exact.raw(-float(n)) for n in cells}})
    returns = {n: _holding(sign[n] * 0.001 * n) for n in cells}
    return SegmentMonth(formation, {UNIVERSE_CELL: inside, OUTSIDE_CELL: outside}, cells, industry, returns)


#: 0..29 universe, high band (IC +1); 30..59 universe, low band (IC −1); 60..89 outside, high band (IC +1 on the
#: outside scores, whose sign is reversed).
CELLS = {n: (HIGH, UNIVERSE_CELL) for n in range(30)} | {n: (LOW, UNIVERSE_CELL) for n in range(30, 60)}
CELLS |= {n: (HIGH, OUTSIDE_CELL) for n in range(60, 90)}
SIGN = {n: (1.0 if n < 30 else -1.0) for n in CELLS}
#: Two industries inside the universe, interleaved across the bands; the outside names share "Manuf".
INDUSTRY = {n: ("Manuf" if n % 2 else "Money") for n in range(60)} | dict.fromkeys(range(60, 90), "Manuf")


def test_a_cells_block_uses_only_its_own_names_keyed_by_the_holding_month() -> None:
    months = [_segment(M1, CELLS, INDUSTRY, SIGN), _segment(M2, CELLS, INDUSTRY, SIGN)]
    arm = ARMS[0]
    high = cell_monthly(months, (HIGH, UNIVERSE_CELL), COMPOSITE, arm)
    low = cell_monthly(months, (LOW, UNIVERSE_CELL), COMPOSITE, arm)
    assert high.ic == {(2020, 2): pytest.approx(1.0), (2020, 3): pytest.approx(1.0)}
    assert low.ic[(2020, 2)] == pytest.approx(-1.0) and low.names[(2020, 2)] == 30
    assert high.spread[(2020, 2)] == pytest.approx(0.001 * (26.5 - 2.5))  # Q1 = 24..29, Q5 = 0..5
    outside = cell_monthly(months, (HIGH, OUTSIDE_CELL), COMPOSITE, arm)
    assert outside.ic[(2020, 2)] == pytest.approx(1.0)
    absent = cell_monthly(months, (PRICE_UNAVAILABLE, OUTSIDE_CELL), COMPOSITE, arm)
    assert absent.ic[(2020, 2)] is None and absent.names[(2020, 2)] == 0


def test_a_name_without_the_arms_return_or_the_signals_score_leaves_the_population() -> None:
    month = _segment(M1, CELLS, INDUSTRY, SIGN)
    returns = dict(month.returns) | {0: _holding(0.0, arms=(ARMS[1],))}
    scores = Scores({COMPOSITE: {n: s for n, s in month.scores[UNIVERSE_CELL].composite.items() if n != 1}})
    month = SegmentMonth(M1, {UNIVERSE_CELL: scores, OUTSIDE_CELL: scores}, CELLS, INDUSTRY, returns)
    block = cell_monthly([month], (HIGH, UNIVERSE_CELL), COMPOSITE, ARMS[0])
    assert block.names[(2020, 2)] == 28 and block.ic[(2020, 2)] is None  # below 30 names


def test_the_industry_block_partitions_the_universe_and_ignores_outside_names() -> None:
    # 60 universe names: "Money" holds the even ones, 15 per band (IC +1 on 0..29, −1 on 30..59), so its IC mixes
    # both bands; outside "Manuf" names would add 30 more if they leaked in.
    month = _segment(M1, CELLS, INDUSTRY, SIGN)
    ics, sizes = industry_monthly([month], "Manuf", COMPOSITE, ARMS[0])
    assert sizes[(2020, 2)] == 30
    money, money_sizes = industry_monthly([month], "Money", COMPOSITE, ARMS[0])
    assert money_sizes[(2020, 2)] == 30

    def expected(parity: int) -> float | None:
        chosen = [n for n in range(60) if n % 2 == parity]
        return ic({n: Exact.raw(float(n)) for n in chosen}, {n: SIGN[n] * 0.001 * n for n in chosen})

    assert ics[(2020, 2)] == expected(1) and money[(2020, 2)] == expected(0)
    mixed = money[(2020, 2)]
    assert mixed is not None and abs(mixed) < 1.0


def test_segments_cover_every_cell_industry_arm_and_signal() -> None:
    months = [_segment(M1, CELLS, INDUSTRY, SIGN), _segment(M2, CELLS, INDUSTRY, SIGN)]
    held: tuple[Month, ...] = ((2020, 2), (2020, 3))
    out = segments(months, [Window("w", held, held)])
    for arm in ARMS:
        assert set(out.cells[arm]) == {(HIGH, UNIVERSE_CELL), (LOW, UNIVERSE_CELL), (HIGH, OUTSIDE_CELL)}
        assert set(out.industries[arm]) == {"Manuf", "Money"}
        block = out.cells[arm][(LOW, UNIVERSE_CELL)][COMPOSITE]
        assert set(out.cells[arm][(HIGH, OUTSIDE_CELL)]) == set(SIGNALS)
        # Two defined months: below the summary floor, so only the count is filled in.
        assert block.ic["w"].defined == 2 and block.ic["w"].mean is None
        assert out.industries[arm]["Money"][COMPOSITE].summaries["w"].defined == 2
    assert out.price_unavailable == {(2020, 1): 0, (2020, 2): 0}
