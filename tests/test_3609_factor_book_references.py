"""#3609 step 2 slice 3c: the equal-weight and cap-weighted universe references, on synthetic formations only."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date

import pytest

from app.services.factor_book import BookRefusal, Exact, bands
from app.services.factor_book_path import ExitReason, Formation, HoldingReturn, TradeCategory, value_path
from app.services.factor_book_references import reference_decisions
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
    formations = [_formation(1, [1, 2], {1: 0.10, 2: -0.10}), _formation(2, [1, 2])]
    me = [{1: 3.0, 2: 1.0}, {1: 1.0, 2: 1.0}]
    decisions = reference_decisions(formations, me)
    assert decisions[0].weights == {1: 0.75, 2: 0.25}
    path = value_path(decisions, arm="best_case", cost_multiplier=0.0)
    assert path.returns[(2015, 2)] == pytest.approx(0.75 * 0.10 + 0.25 * -0.10, abs=1e-15)
    # Month-2 NAV 1.05 holds 0.825 / 0.225; the 50/50 targets are 0.525 each.
    assert path.turnover[(2015, 2)] == pytest.approx((0.3 + 0.3) / 2 / 1.05, abs=1e-15)


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
