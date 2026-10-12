"""#3616 slice 2 — the comparator and the pot restated in GBP (pure).

Spec: docs/specs/metrics/2026-10-12-3616-gbp-isa-comparator.md
"""

from __future__ import annotations

import ast
import inspect
from datetime import date
from decimal import Decimal

import pytest

from app.services import portfolio_eod
from app.services.strategy_comparator import (
    Close,
    Comparator,
    FxRate,
    GbpComparison,
    PotWindow,
    compare,
    gbp_comparison,
    pot_window,
)
from app.services.strategy_wealth import time_weighted_returns
from tests.test_3616_strategy_comparator import _CSPX, _LIVE, _close, _day, _series


def _rate(day: int, value: str) -> FxRate:
    return FxRate(rate_date=_day(day), rate=Decimal(value))


def _index(day: date) -> int:
    return (day - _day(0)).days


def _available(window: PotWindow, start_close: str = "100", end_close: str = "102") -> Comparator:
    result = compare(
        _CSPX, window, _close(_index(window.start), start_close), _close(_index(window.end), end_close), _LIVE
    )
    assert result.comparator_return is not None
    return result


# --- the identity ---------------------------------------------------------


@pytest.mark.parametrize("unfunded_base", [False, True])
def test_gbp_chain_equals_the_endpoint_form_exactly(unfunded_base: bool) -> None:
    # V 100 -> 110; +40 at the start of sub-period 2, so D = 150 -> 120; then 120 -> 150.
    # Rates are chosen so every quotient terminates in Decimal: equality, not a tolerance.
    rows = [("100", "0"), ("100", "10"), ("140", "-20"), ("140", "10")]
    rates = [Decimal("0.8"), Decimal("0.75"), Decimal("0.6"), Decimal("0.96")]
    if unfunded_base:
        # The first funding arrives at the start of the first sub-period, which runs from
        # the unfunded base S, so it converts at S's rate, not return_since's.
        rows = [("0", "0"), *rows[1:]]
        rows[1] = ("100", "10")
    history = _series(*rows)
    returns = time_weighted_returns(history)
    window = pot_window(history, returns)
    assert window is not None and window.start == _day(0)

    # Independent per-sub-period GBP chain: D at the start rate, V at the end rate.
    growth = Decimal("1")
    for index in range(1, len(history)):
        previous, point = history[index - 1], history[index]
        assert previous.pot_value is not None and point.pot_value is not None
        denominator = (previous.pot_value + point.external_flow) * rates[index - 1]
        growth *= point.pot_value * rates[index] / denominator

    comparator = _available(window)
    gbp = gbp_comparison(comparator, window, FxRate(_day(0), rates[0]), FxRate(_day(len(rates) - 1), rates[-1]))
    assert gbp.unavailable_reason is None
    assert gbp.engine_return == growth - 1
    assert gbp.fx_multiplier == Decimal("1.2")


def test_worked_example_from_the_spec() -> None:
    window = PotWindow(start=date(2026, 9, 17), end=date(2026, 10, 9), pot_return=Decimal("0.010937"))
    comparator = compare(
        _CSPX,
        window,
        Close(date(2026, 9, 17), Decimal("822.86")),
        Close(date(2026, 10, 9), Decimal("841.59")),
        _LIVE,
    )
    gbp = gbp_comparison(
        comparator,
        window,
        FxRate(date(2026, 9, 17), Decimal("0.74758")),
        FxRate(date(2026, 10, 9), Decimal("0.75641")),
    )
    assert gbp.unavailable_reason is None
    assert gbp.comparator_return is not None and gbp.engine_return is not None and gbp.engine_excess is not None
    assert round(gbp.comparator_return, 8) == Decimal("0.03484237")
    assert round(gbp.engine_return, 8) == Decimal("0.02287763")
    assert round(gbp.engine_excess, 8) == Decimal("-0.01196474")
    assert (gbp.fx_start_date, gbp.fx_end_date) == (date(2026, 9, 17), date(2026, 10, 9))


@pytest.mark.parametrize("end_close", ["90", "100", "130"])
def test_gbp_excess_is_the_usd_excess_rescaled_and_keeps_its_sign(end_close: str) -> None:
    window = PotWindow(start=_day(0), end=_day(10), pot_return=Decimal("0.05"))
    comparator = _available(window, end_close=end_close)
    assert comparator.engine_excess is not None
    gbp = gbp_comparison(comparator, window, _rate(0, "0.8"), _rate(10, "0.75"))
    assert gbp.engine_excess is not None and gbp.fx_multiplier is not None
    assert gbp.engine_excess == gbp.fx_multiplier * comparator.engine_excess
    assert (gbp.engine_excess > 0) == (comparator.engine_excess > 0)
    assert (gbp.engine_excess == 0) == (comparator.engine_excess == 0)


def test_zero_and_negative_usd_returns() -> None:
    window = PotWindow(start=_day(0), end=_day(10), pot_return=Decimal("0"))
    gbp = gbp_comparison(_available(window, end_close="80"), window, _rate(0, "0.8"), _rate(10, "0.8"))
    assert gbp.engine_return == 0
    assert gbp.comparator_return == Decimal("-0.2")


# --- endpoints and precedence ---------------------------------------------

_W = PotWindow(start=_day(5), end=_day(20), pot_return=Decimal("0.01"))


def test_unavailable_comparator_wins_and_nulls_every_field() -> None:
    unavailable = compare(None, _W, None, None, _LIVE)
    assert gbp_comparison(unavailable, _W, None, None) == GbpComparison()
    assert GbpComparison().unavailable_reason == "comparator_unavailable"
    assert unavailable.gbp == GbpComparison()


@pytest.mark.parametrize(
    ("start", "end", "start_date", "end_date"),
    [
        (None, _rate(20, "0.75"), None, _day(20)),
        (_rate(5, "0.8"), None, _day(5), None),
        (None, None, None, None),
    ],
)
def test_missing_rate_keeps_whichever_date_resolved(
    start: FxRate | None, end: FxRate | None, start_date: date | None, end_date: date | None
) -> None:
    gbp = gbp_comparison(_available(_W), _W, start, end)
    assert gbp.unavailable_reason == "fx_rate_missing"
    assert (gbp.fx_start_date, gbp.fx_end_date) == (start_date, end_date)
    assert gbp.fx_multiplier is None and gbp.comparator_return is None and gbp.engine_return is None
    assert gbp.engine_excess is None


def test_missing_at_s_precedes_stale_at_e() -> None:
    gbp = gbp_comparison(_available(_W), _W, None, _rate(10, "0.75"))
    assert gbp.unavailable_reason == "fx_rate_missing"


@pytest.mark.parametrize(
    ("start_day", "end_day", "reason"),
    [(1, 20, None), (0, 20, "fx_rate_stale"), (5, 16, None), (5, 15, "fx_rate_stale")],
)
def test_four_day_rate_age_boundary_on_each_end(start_day: int, end_day: int, reason: str | None) -> None:
    gbp = gbp_comparison(_available(_W), _W, _rate(start_day, "0.8"), _rate(end_day, "0.75"))
    assert gbp.unavailable_reason == reason
    assert (gbp.fx_start_date, gbp.fx_end_date) == (_day(start_day), _day(end_day))
    assert (gbp.fx_multiplier is None) == (reason is not None)


def test_s_and_e_sharing_one_fresh_rate_is_a_multiplier_of_one() -> None:
    window = PotWindow(start=_day(5), end=_day(7), pot_return=Decimal("0.01"))
    comparator = compare(_CSPX, window, _close(5, "100"), _close(7, "101"), _LIVE)
    shared = _rate(4, "0.8")
    gbp = gbp_comparison(comparator, window, shared, shared)
    assert gbp.unavailable_reason is None
    assert gbp.fx_multiplier == 1
    assert gbp.comparator_return == comparator.comparator_return


def test_rates_are_read_at_the_pots_dates_even_when_a_close_resolved_earlier() -> None:
    # The end close resolved two days before E; the rate is still E's.
    comparator = compare(_CSPX, _W, _close(5, "100"), _close(18, "110"), _LIVE)
    assert comparator.end_close_date == _day(18)
    gbp = gbp_comparison(comparator, _W, _rate(5, "0.8"), _rate(20, "0.75"))
    assert gbp.fx_end_date == _day(20)
    assert gbp.comparator_return == Decimal("1.1") * Decimal("0.75") / Decimal("0.8") - 1


# --- binding --------------------------------------------------------------


def test_the_eod_snapshot_requests_fx_history() -> None:
    """The job that dates the pot's points also requests rates up to each of them."""
    source = inspect.getsource(portfolio_eod.compute_and_store_eod_snapshot)
    calls = [
        node.func.id
        for node in ast.walk(ast.parse(source.lstrip()))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    assert "ensure_fx_history" in calls
