"""#3609 step 2 slice 3c-iv(c): every series valued per arm and cost, on synthetic formations only."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_control import control_decisions
from app.services.factor_book_path import Formation, HoldingReturn, TradeCategory, book_decisions, value_path
from app.services.factor_book_series import (
    ARMS,
    WealthNonpositive,
    WealthRefusal,
    boundary_payload,
    boundary_sha256,
    canonical_json,
    check_complete,
    run_series,
    summarise,
    wealth_refusals,
)
from app.services.factor_panel_prices import HoldingStatus

SEASONED = date(2000, 1, 3)
COSTS = {"gross": 0.0, "net": 1.0, "stress_2x": 2.0}
NAMES = list(range(100, 130))


def _returns(r: float, worst: float | None = None) -> HoldingReturn:
    return HoldingReturn(HoldingStatus.OBSERVED, {"best_case": r, "worst_case": r if worst is None else worst})


def _formation(month: int, order: Sequence[int], returns: Mapping[int, HoldingReturn] | None = None) -> Formation:
    """Universe = ``order`` (best first); $50 closes, seasoned, a distinct observed return per name unless
    overridden (so different draws value differently)."""
    day = date(2015, month, 28)
    return Formation(
        formation=day,
        session=day,
        universe=frozenset(order),
        bands=bands({name: Exact.raw(float(-rank)) for rank, name in enumerate(order)}),
        close=dict.fromkeys(order, 50.0),
        first_bar=dict.fromkeys(order, SEASONED),
        returns={**{n: _returns(0.01 + n * 1e-4) for n in order}, **(returns or {})},
    )


def _formations(returns_at: Mapping[int, Mapping[int, HoldingReturn]] | None = None) -> list[Formation]:
    """Four formations; the order rotates so the book trades."""
    out = []
    for i, month in enumerate(range(1, 5)):
        order = NAMES[i:] + NAMES[:i]
        out.append(_formation(month, order, (returns_at or {}).get(month)))
    return out


def _b1(returns: Sequence[float] = (0.01,) * 4) -> dict[str, list[object]]:
    return {
        "months": [f"2015-{m:02d}" for m in range(2, 6)],
        "continuing": list(returns),
        "rebalance_cost": [0.0] * len(returns),
    }


def _run(formations: Sequence[Formation], *, draws: int = 3, boundary: date | None = None, b1: object = None):
    me = [{n: float(n) for n in f.universe} for f in formations]
    return run_series(
        formations,
        me,
        b1_saved=b1 or _b1(),  # type: ignore[arg-type]
        b1_close=400.0,
        costs=COSTS,
        draws=draws,
        boundary=boundary,
    )


def test_every_series_is_valued_under_every_arm_and_cost_scenario() -> None:
    run = _run(_formations())
    scenarios = {(arm, cost) for arm in ARMS for cost in COSTS}
    assert run.months == ((2015, 2), (2015, 3), (2015, 4), (2015, 5))
    for paths in (run.book, run.equal_weight, run.cap_weighted, run.control):
        assert set(paths) == scenarios
    assert set(run.b1) == set(COSTS)
    assert all(len(draws) == 3 for draws in run.control.values())
    # Gross charges nothing; stress charges twice net.
    assert all(t.cost == 0.0 for t in run.book[("worst_case", "gross")].trades)
    # The initial purchase trades the same notional in every scenario, so its charge scales by the multiplier.
    net, stress = (run.book[("worst_case", c)].trades[0] for c in ("net", "stress_2x"))
    assert net.category is TradeCategory.INITIAL_PURCHASE and net.notional == stress.notional == 1 / 3
    assert net.cost > 0 and stress.cost == 2 * net.cost
    assert run.b1["gross"].entry_cost == 0.0 and run.b1["stress_2x"].entry_cost == 2 * run.b1["net"].entry_cost
    assert wealth_refusals(run) == ()
    check_complete(run)


def _stopped(formations: Sequence[Formation], **kwargs: object) -> tuple[WealthRefusal, ...]:
    with pytest.raises(WealthNonpositive) as caught:
        _run(formations, **kwargs)  # type: ignore[arg-type]
    assert caught.value.code == "WEALTH_NONPOSITIVE"
    return caught.value.refusals


def test_each_control_summary_is_its_own_draw_valued_by_the_books_path() -> None:
    formations = _formations()
    run = _run(formations)
    book = book_decisions(formations)
    for draw in range(3):
        decisions = control_decisions(formations, book, draw).decisions
        expected = summarise(value_path(decisions, arm="best_case", cost_multiplier=1.0))
        assert run.control[("best_case", "net")][draw] == expected
    draws = run.control[("best_case", "net")]
    assert draws[0] != draws[1]  # the draw index reaches the seed


def test_summarise_totals_reconcile_to_the_trades() -> None:
    path = _run(_formations()).book[("worst_case", "net")]
    summary = summarise(path)
    assert sum(n for n, _ in summary.categories.values()) == pytest.approx(sum(t.notional for t in path.trades))
    assert sum(c for _, c in summary.categories.values()) == pytest.approx(sum(t.cost for t in path.trades))
    assert set(summary.categories) >= {TradeCategory.INITIAL_PURCHASE, TradeCategory.FINAL_LIQUIDATION}


def test_the_stopping_month_is_the_earliest_and_lists_every_series_stopped_in_it() -> None:
    # Every name returns -100% in the April formation's holding month (2015-05) under the worst arm only.
    wipe = {n: _returns(0.01, worst=-1.0) for n in NAMES}
    found = _stopped(_formations({4: wipe}))
    assert {r.month for r in found} == {(2015, 5)}
    assert {r.arm for r in found} == {"worst_case"}
    expected = {("book", c, None) for c in COSTS} | {("equal_weight", c, None) for c in COSTS}
    expected |= {("cap_weighted", c, None) for c in COSTS} | {("control", c, d) for c in COSTS for d in range(3)}
    assert {(r.series, r.cost, r.draw) for r in found} == expected
    assert list(found) == sorted(found)


def test_a_series_that_stops_earlier_is_the_only_one_reported() -> None:
    wipe = {n: _returns(0.01, worst=-1.0) for n in NAMES}
    # B1 stops in 2015-03; the books stop in 2015-05. Only B1 is reported, under every cost.
    found = _stopped(_formations({4: wipe}), b1=_b1((0.01, -1.0, 0.01, 0.01)))
    assert found == tuple(WealthRefusal((2015, 3), "b1", None, c) for c in sorted(COSTS))


def test_an_incomplete_series_refuses_comparator_invalid() -> None:
    run = _run(_formations())
    check_complete(run)
    del run.book[("best_case", "net")].returns[(2015, 3)]
    with pytest.raises(BookRefusal) as caught:
        check_complete(run)
    assert caught.value.code == "COMPARATOR_INVALID"
    run = _run(_formations())
    run.control[("best_case", "gross")][2].returns[(2015, 4)] = math.inf  # type: ignore[index]
    with pytest.raises(BookRefusal, match="control draw 2"):
        check_complete(run)


def test_a_selection_refusal_comes_before_b1_and_wealth() -> None:
    wipe = {n: _returns(0.01, worst=-1.0) for n in NAMES}
    formations = _formations({2: wipe})
    formations[2] = replace(formations[2], close={**formations[2].close, NAMES[0]: math.nan})
    bad_b1 = _b1((0.01, math.nan, 0.01, 0.01))
    with pytest.raises(BookRefusal) as caught:
        _run(formations, b1=bad_b1)
    assert caught.value.code == "PRICE_INVALID"
    # Without the selection refusal, B1's incompleteness comes before the wealth stop.
    with pytest.raises(BookRefusal) as caught:
        _run(_formations({2: wipe}), b1=bad_b1)
    assert caught.value.code == "COMPARATOR_INVALID"


def test_the_boundary_payload_is_canonical_and_covers_every_valued_path() -> None:
    boundary = date(2015, 3, 28)
    run = _run(_formations(), boundary=boundary)
    payload = boundary_payload(run)
    assert payload == canonical_json(json.loads(payload))  # re-encoding is the identity
    assert boundary_sha256(run) == hashlib.sha256(payload).hexdigest()
    assert boundary_sha256(_run(_formations(), boundary=boundary)) == boundary_sha256(run)
    data = json.loads(payload)
    assert set(data) == {"book", "control", "equal_weight", "cap_weighted"}
    for series in data.values():
        assert set(series) == set(ARMS) and all(set(by_cost) == set(COSTS) for by_cost in series.values())
    draws = data["control"]["worst_case"]["net"]
    assert [d["draw"] for d in draws] == [0, 1, 2]
    book = data["book"]["worst_case"]["net"]
    state = run.book[("worst_case", "net")].boundary
    assert state is not None and book == state.record() and book["formation"] == "2015-03-28"
    keys = [p["name_key"] for p in book["positions"]]
    assert keys == sorted(keys) and len(keys) > 1


def test_the_boundary_payload_refuses_a_path_without_a_state() -> None:
    with pytest.raises(ValueError, match="captured no boundary state"):
        boundary_payload(_run(_formations()))


def test_canonical_json_refuses_nan() -> None:
    assert canonical_json({"b": 1.0, "a": [0.1]}) == b'{"a":[0.1],"b":1.0}'
    with pytest.raises(ValueError):
        canonical_json({"x": math.nan})


def test_pools_keep_the_smallest_pool_any_draw_met() -> None:
    formations = _formations()
    # Half the names close below $5 at the second formation, so a draw's pool depends on which it holds.
    formations[1] = replace(formations[1], close={**formations[1].close, **dict.fromkeys(NAMES[3:18], 4.0)})
    run = _run(formations, draws=8)
    book = book_decisions(formations)
    per_draw = [control_decisions(formations, book, d).pools for d in range(8)]
    assert [p.formation for p in run.pools] == [date(2015, m, 28) for m in range(1, 5)]
    assert [p.pool for p in run.pools] == [min(draw[i].pool for draw in per_draw) for i in range(4)]
    assert len({draw[1].pool for draw in per_draw}) > 1  # the draws differ, so the minimum is a real choice
    assert all(p.pool >= p.needed for p in run.pools)
