"""#3609 step 2 slice 3c: the equal-weight and cap-weighted universe references, on synthetic formations only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_declaration import canonical_json
from app.services.factor_book_path import ExitReason, Formation, HoldingReturn, TradeCategory, value_path
from app.services.factor_book_references import b1_path, reference_decisions
from app.services.factor_panel_prices import HoldingStatus


def _formation(
    month: int, names: Sequence[int], returns: Mapping[int, float] | None = None, leavers: Sequence[int] = ()
) -> Formation:
    day = date(2015, month, 28)
    got = {**dict.fromkeys(names, 0.0), **(returns or {})}
    return Formation(
        formation=day,
        session=day,
        universe=frozenset(names),
        bands=bands({n: Exact.raw(float(-i)) for i, n in enumerate(names)}),
        close=dict.fromkeys([*names, *leavers], 4.0),  # below $5: the references hold the whole universe anyway
        first_bar=dict.fromkeys(names, date(2015, 1, 1)),  # unseasoned, likewise
        returns={n: HoldingReturn(HoldingStatus.OBSERVED, {"best_case": r, "worst_case": r}) for n, r in got.items()},
    )


def test_the_equal_weight_reference_holds_the_whole_universe_and_force_sells_leavers() -> None:
    formations = [_formation(1, [1, 2, 3]), _formation(2, [2, 3, 4], leavers=[1])]
    first, second = reference_decisions(formations)
    assert first.targets == (1, 2, 3) and first.weights is None
    assert second.targets == (2, 3, 4)
    assert second.sales == {1: TradeCategory.FORCED_EXIT}
    assert second.reasons == {1: (ExitReason.LEFT_UNIVERSE,)}


def test_the_cap_weighted_reference_weights_by_me_and_rebalances_on_its_own_turnover() -> None:
    formations = [_formation(1, [1, 2], {1: 0.10, 2: -0.10}), _formation(2, [1, 2], {1: 0.20, 2: 0.0})]
    me = [{1: 3.0, 2: 1.0}, {1: 1.0, 2: 3.0}]
    decisions = reference_decisions(formations, me)
    assert decisions[0].weights == {1: 0.75, 2: 0.25}
    path = value_path(decisions, arm="best_case", cost_multiplier=0.0)
    assert path.returns[(2015, 2)] == pytest.approx(0.75 * 0.10 + 0.25 * -0.10, abs=1e-15)
    # Month-2 NAV 1.05 holds 0.825 / 0.225; the 25/75 targets are 0.2625 / 0.7875.
    assert path.turnover[(2015, 2)] == pytest.approx((0.5625 + 0.5625) / 2 / 1.05, abs=1e-15)
    assert path.returns[(2015, 3)] == pytest.approx(0.25 * 0.20, abs=1e-15)


@pytest.mark.parametrize("bad", [None, 0.0, float("nan")])
def test_a_missing_or_invalid_me_refuses(bad: float | None) -> None:
    caps = {1: 1.0} if bad is None else {1: 1.0, 2: bad}
    with pytest.raises(BookRefusal, match="ME_INVALID"):
        reference_decisions([_formation(1, [1, 2])], [caps])


def test_weights_that_do_not_cover_the_targets_or_sum_to_one_are_a_contract_error() -> None:
    (decision,) = reference_decisions([_formation(1, [1, 2])], [{1: 1.0, 2: 1.0}])
    for weights in ({1: 0.5, 2: 0.6}, {1: 1.0}, {1: 1.5, 2: -0.5}):
        with pytest.raises(ValueError, match="weights must be"):
            value_path([replace(decision, weights=weights)], arm="best_case", cost_multiplier=0.0)


def test_one_me_map_per_formation() -> None:
    with pytest.raises(ValueError, match="ME maps"):
        reference_decisions([_formation(1, [1])], [])


def test_equal_weight_divides_exactly_as_before_weights_existed() -> None:
    """The book and control keep slice 3b's ``amount / n``; ``amount * (1 / n)`` can differ by an ulp."""
    formations = [_formation(1, [1, 2, 3], dict.fromkeys([1, 2, 3], 0.13)), _formation(2, [1, 2, 3], {1: 0.1})]
    path = value_path(reference_decisions(formations), arm="best_case", cost_multiplier=0.0)
    # Slice 3b's arithmetic, replayed: cost 0, so NAV is the position sum plus 0.0 cash.
    pre = sum([1.0 / 3 * 1.13] * 3) + 0.0
    assert pre / 3 != pre * (1 / 3)  # the fixture distinguishes the two
    end = sum([pre / 3 * 1.1, pre / 3, pre / 3]) + 0.0
    assert path.returns == {(2015, 2): pre / 1.0 - 1.0, (2015, 3): end / pre - 1.0}


def test_an_empty_universe_holds_cash_under_cap_weights_too() -> None:
    empty = replace(_formation(1, []), universe=frozenset())
    (decision,) = reference_decisions([empty], [{}])
    assert decision.targets == () and decision.weights is None


# --------------------------------------------------------------------------- B1


def _saved(months: Sequence[str], returns: Sequence[float], costs: Sequence[float] | None = None) -> dict:
    return {"months": list(months), "continuing": list(returns), "rebalance_cost": list(costs or [0.0] * len(months))}


def test_b1_takes_the_window_and_charges_entry_and_exit_at_the_start_band() -> None:
    saved = _saved(["2014-09", "2014-10", "2014-11", "2014-12"], [0.5, 0.02, -0.01, 0.03])
    got = b1_path(saved, first=(2014, 10), last=(2014, 12), close=197.0, cost_multiplier=1.0)
    h = 0.00161  # >=$100
    assert got.band == ">=$100"
    assert got.returns == {
        (2014, 10): (1.0 - h) * 1.02 - 1.0,
        (2014, 11): -0.01,
        (2014, 12): 1.03 * (1.0 - h) - 1.0,
    }
    assert got.entry_cost == h
    assert got.exit_cost == pytest.approx((1.0 - h) * 1.02 * 0.99 * 1.03 * h, rel=1e-15)
    stress = b1_path(saved, first=(2014, 10), last=(2014, 12), close=197.0, cost_multiplier=2.0)
    assert stress.entry_cost == 2 * h


def test_b1_refuses_a_gap_or_a_step_0_rebalance_cost_inside_the_window() -> None:
    with pytest.raises(BookRefusal, match="COMPARATOR_INVALID.*finite return"):
        gap = _saved(["2014-10", "2014-12"], [0.0, 0.0])
        b1_path(gap, first=(2014, 10), last=(2014, 12), close=197.0, cost_multiplier=1.0)
    costly = _saved(["2014-10", "2014-11"], [0.0, 0.0], [0.0, 0.001])
    with pytest.raises(BookRefusal, match="COMPARATOR_INVALID.*rebalance costs"):
        b1_path(costly, first=(2014, 10), last=(2014, 11), close=197.0, cost_multiplier=1.0)


# --------------------------------------------------------------------------- boundary record


def test_the_boundary_record_carries_the_specs_fields_sorted_by_name_key() -> None:
    formations = [_formation(1, [30, 4, 200], {30: 0.1}), _formation(2, [30, 4, 200])]
    path = value_path(
        reference_decisions(formations), arm="worst_case", cost_multiplier=0.0, boundary=date(2015, 2, 28)
    )
    assert path.boundary is not None
    record = path.boundary.record()
    assert [p["name_key"] for p in record["positions"]] == [4, 30, 200]
    # Exact by design: value_path's own operations in its own order (docs/review-prevention-log.md, #3609 3c-ii).
    assert record["positions"][1] == {"name_key": 30, "value": 1.0 / 3 * 1.1, "entry": "2015-01-28", "band": "<$5"}
    assert set(record) == {"formation", "positions", "cash", "nav"}
    canonical_json(record)  # serialises under the spec's canonical rules (no NaN, ASCII)


@pytest.mark.parametrize("bad", [float("inf"), float("nan"), 0.0, -197.0])
def test_b1_refuses_an_invalid_start_close(bad: float) -> None:
    saved = _saved(["2014-10"], [0.0])
    with pytest.raises(BookRefusal, match="PRICE_INVALID"):
        b1_path(saved, first=(2014, 10), last=(2014, 10), close=bad, cost_multiplier=1.0)


def test_b1_refuses_a_non_finite_saved_return() -> None:
    with pytest.raises(BookRefusal, match="COMPARATOR_INVALID"):
        b1_path(
            _saved(["2014-10"], [float("nan")]), first=(2014, 10), last=(2014, 10), close=197.0, cost_multiplier=1.0
        )


@pytest.mark.parametrize("multiplier", [-1.0, float("nan"), float("inf")])
def test_an_inverted_window_or_an_invalid_cost_multiplier_is_a_contract_error(multiplier: float) -> None:
    saved = _saved(["2014-10", "2014-11"], [0.0, 0.0])
    with pytest.raises(ValueError, match="inverted"):
        b1_path(saved, first=(2014, 11), last=(2014, 10), close=197.0, cost_multiplier=1.0)
    with pytest.raises(ValueError, match="cost multiplier"):
        b1_path(saved, first=(2014, 10), last=(2014, 11), close=197.0, cost_multiplier=multiplier)
    (decision,) = reference_decisions([_formation(1, [1, 2])])
    with pytest.raises(ValueError, match="cost multiplier"):
        value_path([decision], arm="best_case", cost_multiplier=multiplier)
