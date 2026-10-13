"""#3609 step 2 slice 3c-v(e): premise 3's universe table on every formation, the book at or below the NYSE median,
and the notional classes, on synthetic inputs only (no corpus, no stage B)."""

from __future__ import annotations

import gzip
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date

import pytest

from app.services.factor_book import BookRefusal
from app.services.factor_book_path import Decision, HoldingReturn, Month, PathResult, Trade, TradeCategory
from app.services.factor_book_series import ARMS, Scenario, SeriesRun
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3609_step2_universe import (
    POOLED,
    STAGE_A,
    STAGE_B,
    NotionalClass,
    _me_sum,  # pyright: ignore[reportPrivateUsage]
    diagnostics,
    notional_by_class,
    notional_class,
    read_cutoffs,
    summarise,
    universe_month,
)

A = date(2021, 4, 30)
B = date(2021, 5, 31)
CUTOFF = 5e9


def _name(me: float, signed: Sequence[str] = ("gp_at", "be_me", "at_gr1"), sic: int | None = 3720) -> PanelName:
    holding = HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, 0.0))
    return PanelName(1, me, "Manuf", dict.fromkeys(signed, 1.0), holding, sic)


def _month(formation: date, names: Mapping[int, PanelName]) -> PanelMonth:
    return PanelMonth(formation, formation, names, {}, {})


def _decision(formation: date, targets: Sequence[int]) -> Decision:
    return Decision(formation, tuple(targets), {}, {}, {})


#: Six admitted names; 1..4 form the universe. 1 and 2 are above the cutoff, 3 sits exactly on it (at or below),
#: 5 is above it but outside the universe.
NAMES = {
    1: _name(9e9),
    2: _name(8e9, signed=("gp_at", "ni_me")),
    3: _name(CUTOFF, signed=("gp_at",), sic=6221),
    4: _name(4e9, sic=None),
    5: _name(6e9),
    6: _name(1e9),
}
UNIVERSE = (1, 2, 5, 3)


def _payload(rows: Sequence[Sequence[object]]) -> bytes:
    return gzip.compress("".join(json.dumps(list(r)) + "\n" for r in rows).encode())


# --------------------------------------------------------------------------- cutoffs


def test_cutoffs_are_the_p50_rows_normalised_to_usd() -> None:
    payload = _payload(
        [
            ["nyse_p20", "2021-04-30", "1000", "usd_millions"],
            ["nyse_p50", "2021-04-30", "5000", "usd_millions"],
            ["nyse_p50", "2021-05-31", "5100.5", "usd_millions"],
        ]
    )
    assert read_cutoffs(payload) == {A: 5e9, B: 5.1005e9}


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        pytest.param(["nyse_p50", "2021-05-31", "5000", "usd"], "unit", id="unit"),
        pytest.param(["nyse_p50", "2021-05-31", "0", "usd_millions"], "not finite and positive", id="zero"),
        pytest.param(["nyse_p50", "2021-05-31", "-1", "usd_millions"], "not finite and positive", id="negative"),
        pytest.param(["nyse_p50", "2021-05-31", "NaN", "usd_millions"], "not finite and positive", id="nan"),
        pytest.param(
            ["nyse_p50", "2021-05-31", "1e305", "usd_millions"],
            "not finite and positive",
            id="infinite after normalisation",
        ),  # fmt: skip
        pytest.param(["nyse_p50", "2021-05-31", "n/a", "usd_millions"], "not a date and a number", id="not a number"),
        pytest.param(["nyse_p50", "2021-04-31", "5000", "usd_millions"], "not a date and a number", id="not a date"),
        pytest.param(["nyse_p50", "2021-04-30", "5100", "usd_millions"], "repeated", id="repeated"),
    ],
)
def test_an_invalid_published_cutoff_refuses(row: list[str], reason: str) -> None:
    with pytest.raises(BookRefusal, match=reason) as raised:
        read_cutoffs(_payload([["nyse_p50", "2021-04-30", "5000", "usd_millions"], row]))
    assert raised.value.code == "CUTOFF_INVALID"


def test_another_series_is_not_checked() -> None:
    assert read_cutoffs(_payload([["nyse_p80", "2021-04-30", "-1", "usd"]])) == {}


# --------------------------------------------------------------------------- per formation


def test_premise_3s_row_and_the_book_at_or_below_the_cutoff() -> None:
    row = universe_month(_month(A, NAMES), UNIVERSE, _decision(A, (1, 3, 5, 2)), CUTOFF)
    assert (row.admitted, row.me_rank_last) == (6, CUTOFF)
    # The value family (the book's only one): 1, 2 and 5 have a member's value; 3 has only gp_at.
    assert (row.top_all_families, row.top_sic6221) == (3, 1)
    # Above: 1, 2, 5 (3 equals the cutoff, so it is at or below); 4 and 6 are outside the universe.
    assert (row.above, row.shared, row.top_at_or_below, row.above_outside_top) == (3, 3, 1, 0)
    assert row.above_me_share_in_top == 1.0
    assert (row.book_holdings, row.book_at_or_below, row.book_weight_at_or_below) == (4, 1, 0.25)


def test_above_cutoff_me_outside_the_universe_lowers_the_share() -> None:
    row = universe_month(_month(A, NAMES), (1, 2, 3, 4), _decision(A, (4,)), CUTOFF)
    assert (row.shared, row.top_at_or_below, row.above_outside_top) == (2, 2, 1)
    assert row.above_me_share_in_top == pytest.approx(17e9 / 23e9)
    assert (row.book_at_or_below, row.book_weight_at_or_below) == (1, 1.0)


def test_an_unpublished_cutoff_leaves_every_cutoff_field_unavailable() -> None:
    row = universe_month(_month(A, NAMES), UNIVERSE, _decision(A, (1,)), None)
    cutoff_fields = (
        row.cutoff,
        row.above,
        row.shared,
        row.top_at_or_below,
        row.above_outside_top,
        row.above_me_share_in_top,
        row.book_at_or_below,
        row.book_weight_at_or_below,
    )
    assert cutoff_fields == (None,) * len(cutoff_fields)
    assert (row.admitted, row.top_all_families, row.book_holdings) == (6, 3, 1)


def test_no_name_above_the_cutoff_has_no_share_and_a_book_in_cash_weighs_nothing() -> None:
    row = universe_month(_month(A, NAMES), UNIVERSE, _decision(A, ()), 1e12)
    assert (row.above, row.above_me_share_in_top) == (0, None)
    assert (row.book_holdings, row.book_at_or_below, row.book_weight_at_or_below) == (0, 0, 0.0)


def test_a_universe_holding_no_above_cutoff_name_has_a_share_of_zero() -> None:
    row = universe_month(_month(A, NAMES), (4, 6), _decision(A, ()), CUTOFF)
    assert (row.above, row.shared, row.above_outside_top, row.above_me_share_in_top) == (3, 0, 3, 0.0)


def test_an_empty_universe_is_a_contract_error() -> None:
    with pytest.raises(ValueError, match="universe is empty"):
        universe_month(_month(A, NAMES), (), _decision(A, ()), CUTOFF)


def test_an_me_sum_that_overflows_refuses() -> None:
    names = {1: _name(1.5e308), 2: _name(1.5e308), 3: _name(1.0)}
    with pytest.raises(BookRefusal) as raised:
        universe_month(_month(A, names), (1, 2), _decision(A, ()), 1.0)
    assert raised.value.code == "ME_INVALID"


@pytest.mark.parametrize("values", [[1.5e308, 1.5e308], [math.inf, -math.inf]], ids=["overflow", "inf-inf"])
def test_a_sum_fsum_raises_on_refuses_as_me_invalid(values: list[float]) -> None:
    with pytest.raises(BookRefusal) as raised:
        _me_sum(values, "fixture", empty_ok=True)
    assert raised.value.code == "ME_INVALID"


def test_a_book_name_outside_the_universe_is_a_contract_error() -> None:
    with pytest.raises(ValueError, match="outside the admitted population"):
        universe_month(_month(A, NAMES), UNIVERSE, _decision(A, (6,)), CUTOFF)


# --------------------------------------------------------------------------- notional classes

ME: dict[Month, dict[int, float]] = {(2021, 4): {1: 9e9, 2: CUTOFF}, (2021, 5): {1: 9e9}}
CUTOFFS: dict[Month, float] = {(2021, 4): CUTOFF}


def _trade(month: Month, name: int, notional: float, category: TradeCategory = TradeCategory.ENTRY) -> Trade:
    return Trade(month, name, category, notional, 0.0, 1.0)


@pytest.mark.parametrize(
    ("trade", "expected"),
    [
        pytest.param(
            _trade((2024, 8), 9, 1.0, TradeCategory.FINAL_LIQUIDATION),
            NotionalClass.FINAL_LIQUIDATION,
            id="final liquidation first, whatever its ME",
        ),  # fmt: skip
        pytest.param(
            _trade((2021, 5), 9, 1.0, TradeCategory.FORCED_EXIT),
            NotionalClass.NO_ME,
            id="no ME before an unpublished cutoff",
        ),  # fmt: skip
        pytest.param(_trade((2021, 5), 1, 1.0), NotionalClass.CUTOFF_UNAVAILABLE, id="cutoff unavailable"),
        pytest.param(_trade((2021, 4), 1, 1.0), NotionalClass.ABOVE, id="above"),
        pytest.param(_trade((2021, 4), 2, 1.0), NotionalClass.AT_OR_BELOW, id="equal is at or below"),
    ],
)
def test_notional_classes_follow_their_precedence(trade: Trade, expected: NotionalClass) -> None:
    assert notional_class(trade, ME, CUTOFFS) is expected


def test_the_classes_reconcile_to_the_total_order_notional() -> None:
    trades = [_trade((2021, 4), 1, 0.3), _trade((2021, 4), 2, 0.2), _trade((2021, 5), 1, 0.1)]
    result = notional_by_class(trades, ME, CUTOFFS)
    assert result.by_class == {
        NotionalClass.NO_ME: 0.0,
        NotionalClass.CUTOFF_UNAVAILABLE: 0.1,
        NotionalClass.ABOVE: 0.3,
        NotionalClass.AT_OR_BELOW: 0.2,
        NotionalClass.FINAL_LIQUIDATION: 0.0,
    }
    assert math.fsum(result.by_class.values()) == pytest.approx(result.total) and result.total == pytest.approx(0.6)


# --------------------------------------------------------------------------- the whole block


def _run(trades: Mapping[str, Sequence[Trade]]) -> SeriesRun:
    book: dict[Scenario, PathResult] = {
        (arm, cost): PathResult(trades=list(trades[arm]) if cost == "net" else [])
        for arm in ARMS
        for cost in ("gross", "net")
    }
    return SeriesRun((), book, {}, {}, {}, {}, (), ())


def test_windows_split_at_the_boundary_formation_and_stage_b_holds_the_final_liquidation() -> None:
    panel = [_month(A, NAMES), _month(B, NAMES)]
    decisions = [_decision(A, (1, 3)), _decision(B, (1,))]
    a, b = ARMS
    trades = {
        a: [
            _trade((2021, 4), 1, 0.5),
            _trade((2021, 4), 3, 0.5),
            _trade((2021, 5), 3, 0.5, TradeCategory.DISCRETIONARY_EXIT),
            _trade((2024, 8), 1, 1.0, TradeCategory.FINAL_LIQUIDATION),
        ],
        b: [_trade((2021, 4), 1, 2.0)],
    }
    out = diagnostics(panel, [UNIVERSE, UNIVERSE], decisions, _run(trades), {A: CUTOFF})
    assert [r.book_weight_at_or_below for r in out.rows] == [0.5, None]
    # Stage A is formation 2021-04 only; the unpublished 2021-05 cutoff is unavailable in stage B.
    weight = out.summaries[STAGE_A]["book_weight_at_or_below"]
    assert (weight.covered, weight.formations, weight.low, weight.high) == (1, 1, 0.5, 0.5)
    assert (out.summaries[STAGE_B]["above"].covered, out.summaries[STAGE_B]["above"].formations) == (0, 1)
    assert out.summaries[POOLED]["admitted"].covered == 2
    stage_a, stage_b, pooled = (out.notional[a][w] for w in (STAGE_A, STAGE_B, POOLED))
    assert (stage_a.by_class[NotionalClass.ABOVE], stage_a.by_class[NotionalClass.AT_OR_BELOW]) == (0.5, 0.5)
    assert stage_b.by_class[NotionalClass.CUTOFF_UNAVAILABLE] == 0.5
    assert stage_b.by_class[NotionalClass.FINAL_LIQUIDATION] == 1.0
    assert pooled.total == stage_a.total + stage_b.total == 2.5
    assert out.notional[b][POOLED].by_class[NotionalClass.ABOVE] == 2.0


def test_summaries_count_only_available_formations() -> None:
    rows = [
        universe_month(_month(A, NAMES), UNIVERSE, _decision(A, ()), CUTOFF),
        universe_month(_month(A, NAMES), UNIVERSE, _decision(A, ()), None),
    ]
    above = summarise(rows, POOLED)["above"]
    assert (above.covered, above.formations, above.low, above.high) == (1, 2, 3.0, 3.0)
    empty = summarise(rows, STAGE_B)["above"]
    assert (empty.covered, empty.formations, empty.low, empty.high) == (0, 0, None, None)
