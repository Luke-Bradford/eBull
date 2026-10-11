"""#3740 parent slice 2: the step-3 composite and cap solution, on synthetic formations only."""

from __future__ import annotations

import math
import random

import pytest

from app.services import factor_book
from app.services.factor_book import COMPOSITE, MIN_GROUP
from app.services.vw_factor_book import (
    FAMILIES,
    NAME_CAP,
    cap_solution,
    composite_scores,
    membership_patterns,
    min_families,
    sector_caps,
)

TOL = 1e-12


# --------------------------------------------------------------------------- composite


@pytest.mark.parametrize(("k", "expected"), [(1, 1), (2, 2), (3, 2)])
def test_the_composite_needs_a_strict_majority_of_the_chosen_families(k: int, expected: int) -> None:
    assert min_families(list(FAMILIES)[:k]) == expected


def _signed(n: int, seed: int) -> dict[str, dict[int, float]]:
    rng = random.Random(seed)
    return {c: {i: rng.gauss(0, 1) for i in range(n)} for members in FAMILIES.values() for c in members}


def test_a_name_with_one_of_three_family_scores_has_no_composite_and_two_of_three_has_one() -> None:
    n = 3 * MIN_GROUP
    signed = _signed(n, 1)
    for c in ("ni_me", "ocf_me", "gp_at"):
        del signed[c][0]  # name 0 keeps be_me (value) and at_gr1 (investment): 2 of 3
    for c in ("be_me", "ni_me", "ocf_me", "gp_at"):
        del signed[c][1]  # name 1 keeps at_gr1 only: 1 of 3
    for values in signed.values():
        del values[2]  # name 2 has no input at all
    scores = composite_scores(range(n), dict.fromkeys(range(n), "Other"), signed, list(FAMILIES))
    assert 0 in scores.composite
    assert 1 not in scores.composite and 1 in scores.by_operation["investment"]
    assert membership_patterns(range(n), scores, list(FAMILIES)) == {
        ("value", "GP/A", "investment"): n - 3,
        ("value", "investment"): 1,
        ("investment",): 1,
        (): 1,
    }


def test_two_of_two_is_required_when_two_families_are_chosen() -> None:
    n = 3 * MIN_GROUP
    signed = _signed(n, 2)
    del signed["gp_at"][0]
    scores = composite_scores(range(n), dict.fromkeys(range(n), "Other"), signed, ["value", "GP/A"])
    assert 0 in scores.by_operation["value"] and 0 not in scores.composite
    assert "investment" not in scores.by_operation and "at_gr1" not in scores.by_operation


def test_the_value_family_alone_reproduces_step_2s_composite_exactly() -> None:
    n = 4 * MIN_GROUP
    signed = _signed(n, 3)
    del signed["be_me"][4]
    industry = {i: ("Other", "Money", "unclassified")[i % 3] for i in range(n)}
    ours = composite_scores(range(n), industry, signed, ["value"])
    theirs = factor_book.composite_scores(range(n), industry, signed)
    assert ours.composite == theirs.composite
    assert ours.uninformative == theirs.uninformative


@pytest.mark.parametrize("chosen", [[], ["GP/A", "value"], ["value", "value"], ["momentum"]])
def test_the_chosen_families_must_be_an_ordered_subset(chosen: list[str]) -> None:
    with pytest.raises(ValueError, match="subset"):
        composite_scores([], {}, {}, chosen)


def test_the_composite_is_standardised_within_industry() -> None:
    n = 2 * MIN_GROUP
    industry = {i: "A" if i < MIN_GROUP else "B" for i in range(n)}
    scores = composite_scores(range(n), industry, _signed(n, 4), list(FAMILIES))
    for group in ("A", "B"):
        values = [float(v) for name, v in scores.composite.items() if industry[name] == group]
        assert len(values) == MIN_GROUP
        assert math.fsum(values) == pytest.approx(0, abs=TOL)
        assert math.fsum(v * v for v in values) / len(values) == pytest.approx(1)
    assert COMPOSITE in scores.by_operation


# --------------------------------------------------------------------------- caps


def test_sector_caps_are_twice_each_groups_me_share_of_the_reference() -> None:
    caps = sector_caps({1: 1.0, 2: 3.0, 3: 4.0}, {1: "A", 2: "A", 3: "B"})
    assert caps == {"A": pytest.approx(1.0), "B": pytest.approx(1.0)}


def test_no_binding_cap_leaves_pure_value_weights() -> None:
    me = {i: float(i + 1) for i in range(20)}  # largest share 20/210 < 0.10
    sol = cap_solution(me, dict.fromkeys(me, "A"), {"A": 2.0})
    total = sum(me.values())
    assert all(sol.weights[i] == pytest.approx(me[i] / total, abs=TOL) for i in me)
    assert sol.cash == 0 and not sol.name_capped and not sol.sector_capped


def test_a_name_over_the_cap_is_held_at_it_and_the_rest_scale_together() -> None:
    me = {0: 100.0, **{i: 1.0 for i in range(1, 21)}}
    sol = cap_solution(me, dict.fromkeys(me, "A"), {"A": 2.0})
    assert sol.weights[0] == NAME_CAP and sol.name_capped == {0}
    assert all(sol.weights[i] == pytest.approx(0.9 / 20, abs=TOL) for i in range(1, 21))


def test_a_group_over_its_cap_is_held_at_it_with_its_names_in_proportion() -> None:
    me = {**{i: 4.0 for i in range(10)}, **{i: 1.0 for i in range(10, 30)}}  # A holds 40/60 of value
    group = {i: "A" if i < 10 else "B" for i in me}
    sol = cap_solution(me, group, {"A": 0.5, "B": 2.0})
    assert sum(sol.weights[i] for i in range(10)) == pytest.approx(0.5, abs=TOL)
    assert all(sol.weights[i] == pytest.approx(0.05, abs=TOL) for i in range(10))
    assert all(sol.weights[i] == pytest.approx(0.025, abs=TOL) for i in range(10, 30))
    assert sol.sector_capped == {"A"}


@pytest.mark.parametrize("dominant", [1e7, 1e12, 1e15])
def test_a_dominant_name_does_not_erase_the_small_names_precision_at_a_sector_cap(dominant: float) -> None:
    """Codex checkpoint 2: decrementing the unsaturated sum by the dominant name lost the small names' share."""
    me = {0: dominant, 1: 1.0, 2: 1.0}
    sol = cap_solution(me, dict.fromkeys(me, "A"), {"A": 0.15})
    assert sol.sector_capped == {"A"} and sol.weights[0] == NAME_CAP
    assert sol.weights[1] == sol.weights[2] == pytest.approx(0.025, abs=TOL)
    assert sol.cash == pytest.approx(0.85, abs=TOL)


def test_infeasible_caps_hold_every_group_at_its_ceiling_and_leave_cash() -> None:
    me = {**{i: 1.0 for i in range(5)}, 5: 2.0, 6: 1.0, 7: 1.0}
    group = {i: "A" if i < 5 else "B" for i in me}
    sol = cap_solution(me, group, {"A": 1.0, "B": 0.2})  # A ≤ 5 × 0.10, B ≤ 0.2: at most 0.7 invested
    assert all(sol.weights[i] == NAME_CAP for i in range(5))
    assert sol.weights[5] == pytest.approx(0.1, abs=TOL) and sol.weights[6] == pytest.approx(0.05, abs=TOL)
    assert sol.cash == pytest.approx(0.3, abs=TOL) and sol.theta is None
    assert sol.sector_capped == {"B"}


def test_zero_targets_leave_the_book_all_cash() -> None:
    sol = cap_solution({}, {}, {})
    assert sol.weights == {} and sol.cash == 1.0


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf])
def test_an_invalid_target_me_is_refused(bad: float) -> None:
    with pytest.raises(ValueError, match="non-positive"):
        cap_solution({1: 1.0, 2: bad}, {1: "A", 2: "A"}, {"A": 2.0})


def _bisect(lo: float, hi: float, fn, target: float) -> float:
    """Independent oracle: the smallest x in [lo, hi] with fn(x) >= target, fn non-decreasing."""
    for _ in range(200):
        mid = (lo + hi) / 2
        lo, hi = (lo, mid) if fn(mid) >= target else (mid, hi)
    return hi


def _random_formation(rng: random.Random) -> tuple[dict[int, float], dict[int, str], dict[str, float]]:
    n = rng.randint(1, 60)
    me = {i: math.exp(rng.gauss(0, 2)) for i in range(n)}
    groups = [f"g{j}" for j in range(rng.randint(1, 6))]
    group = {i: rng.choice(groups) for i in me}
    reference = {**me, **{1000 + j: math.exp(rng.gauss(0, 2)) for j in range(rng.randint(0, 40))}}
    reference_group = {**group, **{k: rng.choice(groups) for k in reference if k >= 1000}}
    return me, group, sector_caps(reference, reference_group)


@pytest.mark.parametrize("seed", range(300))
def test_the_cap_solution_meets_the_spec_invariants_and_matches_a_bisection_oracle(seed: int) -> None:
    me, group, caps = _random_formation(random.Random(f"3740-caps:{seed}"))
    sol = cap_solution(me, group, caps)
    total = math.fsum(me.values())
    u = {i: v / total for i, v in me.items()}
    members = {g: [i for i in me if group[i] == g] for g in set(group.values())}

    # Σ w + cash = 1, each cap holds, no negative weight.
    assert math.fsum(sol.weights.values()) + sol.cash == pytest.approx(1, abs=TOL)
    assert all(0 < w <= NAME_CAP + TOL for w in sol.weights.values())
    for g, names in members.items():
        assert math.fsum(sol.weights[i] for i in names) <= caps[g] + TOL

    # Within a group, w/u is equal across names not at the name cap; uncapped groups share the global scale θ.
    scales = {g: {sol.weights[i] / u[i] for i in names if sol.weights[i] < NAME_CAP} for g, names in members.items()}
    for g, values in scales.items():
        assert not values or max(values) - min(values) <= 1e-9 * max(values)
        if sol.theta is not None and g not in sol.sector_capped and values:
            assert min(values) == pytest.approx(sol.theta, rel=1e-9)

    def f(g: str, s: float) -> float:
        return math.fsum(min(NAME_CAP, s * u[i]) for i in members[g])

    ceiling = math.fsum(min(caps[g], NAME_CAP * len(names)) for g, names in members.items())
    if ceiling < 1:
        assert sol.theta is None and sol.cash == pytest.approx(1 - ceiling, abs=TOL)
        return
    # θ is the smallest scale at which Σ min(f_g(θ), c_g) = 1 (the oracle bisects it independently).
    hi = max(NAME_CAP / v for v in u.values())
    oracle = _bisect(0.0, hi, lambda t: math.fsum(min(f(g, t), caps[g]) for g in members), 1.0)
    assert sol.theta == pytest.approx(oracle, rel=1e-9)
    for g, names in members.items():
        s = oracle if f(g, oracle) <= caps[g] else _bisect(0.0, oracle, lambda t, g=g: f(g, t), caps[g])
        for i in names:
            assert sol.weights[i] == pytest.approx(min(NAME_CAP, s * u[i]), abs=1e-9)
