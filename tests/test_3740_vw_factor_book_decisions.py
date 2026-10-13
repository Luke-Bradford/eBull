"""#3740 parent slice 2: the step-3 holdings rule (eligibility, MAX filter, forced exits, capped weights), synthetic."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.avoidance_filters import AvoidanceError, MaxMissing, MaxReading
from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_path import HoldingReturn, TradeCategory
from app.services.factor_panel_prices import HoldingStatus
from app.services.vw_factor_book import NAME_CAP
from app.services.vw_factor_book_decisions import (
    ExitReason,
    Formation,
    book_decisions,
    exit_reasons,
    max_flagged,
)
from app.services.vw_factor_book_path import Captured, Missed, value_path

OBSERVED, TERMINAL = HoldingStatus.OBSERVED, HoldingStatus.TERMINAL
N = 40  # universe size; the decile is the best 4 names, the tercile the best 14


def _ret(r: float = 0.0, status: HoldingStatus = OBSERVED) -> HoldingReturn:
    return HoldingReturn(status, {"best_case": r, "worst_case": r})


def _formation(
    month: int,
    order: Sequence[int],
    *,
    universe: Sequence[int] | None = None,
    me: Mapping[int, float] | None = None,
    group: Mapping[int, str] | None = None,
    close: Mapping[int, float] | None = None,
    aged: Sequence[int] | None = None,
    flagged: Sequence[int] = (),
    tradable: Sequence[int] | None = None,
    returns: Mapping[int, HoldingReturn] | None = None,
) -> Formation:
    """``order`` is the composite order (best first); every name closes at $50 with ME 1 in group "A"."""
    names = list(universe if universe is not None else range(N))
    every: list[int] = sorted({*names, *order, *(returns or {})})
    day = date(2027, month, 28)
    return Formation(
        formation=day,
        session=day,
        universe=frozenset(names),
        me={**dict.fromkeys(names, 1.0), **(me or {})},
        group={**dict.fromkeys(names, "A"), **(group or {})},
        bands=bands({name: Exact.raw(float(-rank)) for rank, name in enumerate(order)}),
        close={**dict.fromkeys(every, 50.0), **(close or {})},
        aged=frozenset(every if aged is None else aged),
        max_flagged=frozenset(flagged),
        tradable=None if tradable is None else frozenset(tradable),
        returns={**{n: _ret() for n in every}, **(returns or {})},
    )


# --------------------------------------------------------------------------- MAX flag


def _reading(value: float | None, missing: MaxMissing | None = None) -> MaxReading:
    return MaxReading(value, 20, 0, missing)


def test_the_max_flag_is_the_top_decile_with_ties_at_the_cutoff_flagging() -> None:
    readings = {i: _reading(float(i)) for i in range(20)}  # cutoff = the 18th smallest = 17.0
    readings[16] = _reading(17.0)
    assert max_flagged(readings) == {16, 17, 18, 19}


def test_screened_data_flag_and_an_uncomputable_value_does_not() -> None:
    readings = {i: _reading(float(i)) for i in range(10)}
    readings[100] = _reading(None, MaxMissing.SCREENED)
    readings[101] = _reading(None, MaxMissing.SHORT)
    readings[102] = _reading(None, MaxMissing.ZERO_HEAVY)
    assert max_flagged(readings) == {8, 9, 100}  # cutoff = the ceil(0.9 · 10) = 9th smallest = 8.0


def test_no_max_value_at_all_refuses() -> None:
    with pytest.raises(AvoidanceError, match="MAX_EMPTY"):
        max_flagged({1: _reading(None, MaxMissing.SHORT)})


# --------------------------------------------------------------------------- eligibility and exits


def test_entry_needs_price_age_no_max_flag_and_tradability() -> None:
    order = list(range(N))
    f = _formation(1, order, close={0: 4.99}, aged=range(2, N), flagged=[2], tradable=[n for n in order if n != 3])
    # 0 is below $5, 1 too young, 2 MAX-flagged, 3 not tradable: none of the decile is eligible.
    assert f.eligible == frozenset(range(4, N))
    step = book_decisions([f]).steps[0]
    assert isinstance(step, Captured) and step.targets == () and step.cash == 1.0


def test_no_etoro_rule_on_the_historical_path() -> None:
    assert 3 in _formation(1, list(range(N)), tradable=None).eligible


def test_forced_exits_come_in_test_order_before_the_discretionary_one() -> None:
    order = list(range(N))
    f = _formation(2, order[::-1], close={0: 4.0}, flagged=[0, 1], tradable=range(1, N))
    assert exit_reasons(f, 0) == (
        ExitReason.BELOW_PRICE_FLOOR,
        ExitReason.MAX_FLAGGED,
        ExitReason.NOT_TRADABLE,
        ExitReason.LEFT_TERCILE,
    )
    assert exit_reasons(f, 1) == (ExitReason.MAX_FLAGGED, ExitReason.LEFT_TERCILE)
    assert exit_reasons(f, 2) == (ExitReason.LEFT_TERCILE,)


def test_holdings_are_sold_forced_or_discretionary_by_primary_reason() -> None:
    order = list(range(N))
    first = _formation(1, order)
    # Next month: 0 is MAX-flagged, 1 untradable, 2 and 3 fall out of the tercile.
    second = _formation(2, [0, 1, *range(4, N), 2, 3], flagged=[0], tradable=[n for n in range(N) if n != 1])
    out = book_decisions([first, second])
    step = out.steps[1]
    assert isinstance(step, Captured)
    assert step.sales == {
        0: TradeCategory.FORCED_EXIT,
        1: TradeCategory.FORCED_EXIT,
        2: TradeCategory.DISCRETIONARY_EXIT,
        3: TradeCategory.DISCRETIONARY_EXIT,
    }
    assert out.reasons[second.formation][0][0] is ExitReason.MAX_FLAGGED
    assert out.reasons[second.formation][1][0] is ExitReason.NOT_TRADABLE
    assert set(step.targets) == {4, 5}  # the decile's 0 and 1 are ineligible; 2 and 3 were sold


def test_a_holding_without_a_valid_close_refuses() -> None:
    first = _formation(1, list(range(N)))
    second = _formation(2, list(range(N)), universe=range(4, N + 4))
    second = replace(second, close={k: v for k, v in second.close.items() if k != 0})
    with pytest.raises(BookRefusal, match="PRICE_INVALID"):
        book_decisions([first, second])


# --------------------------------------------------------------------------- weights


def test_targets_are_value_weighted_under_the_name_cap() -> None:
    me = {0: 100.0, 1: 1.0, 2: 1.0, 3: 1.0}
    out = book_decisions([_formation(1, list(range(N)), me=me)])
    step = out.steps[0]
    assert isinstance(step, Captured)
    # Four targets cannot reach 1 under a 10% name cap: each takes 0.10 and 0.6 is cash.
    assert step.weights == dict.fromkeys(range(4), NAME_CAP)
    assert step.cash == pytest.approx(0.6, abs=1e-12)
    assert out.insufficient == (date(2027, 1, 28),)


def test_the_sector_cap_reference_is_the_whole_universe() -> None:
    order = list(range(N))
    group = {n: "A" if n < 2 else "B" for n in range(N)}
    me = {n: 1.0 for n in range(N)}
    out = book_decisions([_formation(1, order, me=me, group=group)])
    solution = out.caps[date(2027, 1, 28)]
    # A holds 2/40 of the universe's ME, so its cap is 0.10; the targets 0..3 are half A by value.
    assert math.fsum(solution.weights[n] for n in (0, 1)) == pytest.approx(0.10, abs=1e-12)
    assert solution.sector_capped == {"A"}


# --------------------------------------------------------------------------- missed formations and the path


def test_a_missed_formation_carries_holdings_and_drops_realised_ones() -> None:
    first = _formation(1, list(range(N)))
    missed = Missed(date(2027, 2, 28), "SESSION_MISSING", {0: _ret(), 1: _ret(-1.0, TERMINAL), 2: _ret(), 3: _ret()})
    third = _formation(3, [0, 2, 3, *range(4, N), 1])  # 1 has left the tercile
    out = book_decisions([first, missed, third])
    step = out.steps[2]
    assert isinstance(step, Captured)
    assert step.sales == {}  # 1 was realised to cash in month 3, so it is not a holding to sell
    assert set(step.targets) == {0, 2, 3, 4}
    result = value_path(out.steps, arm="best_case", cost_multiplier=1.0)
    assert result.missed == {(2027, 2): "SESSION_MISSING"}
    for row in result.ledger:
        assert row.pre_positions + row.pre_cash == pytest.approx(
            row.post_positions + row.post_cash + row.cost, abs=1e-12
        )
