"""#3609 step 2 slice 3a: the book's scoring core, on synthetic formations only."""

from __future__ import annotations

import math
import statistics

import pytest

from app.services.factor_book import (
    CHARACTERISTICS,
    COMPOSITE,
    MIN_GROUP,
    UNCLASSIFIED,
    UNIVERSE_SIZE,
    BookRefusal,
    Exact,
    bands,
    compare,
    composite_scores,
    rank_z,
    universe,
)


def _names(n: int, base: int = 0) -> list[int]:
    return list(range(base, base + n))


def _raw(values: dict[int, float]) -> dict[int, Exact]:
    return {name: Exact.raw(value) for name, value in values.items()}


# --------------------------------------------------------------------------- universe


def test_the_universe_is_the_top_1000_by_me_with_ties_by_name_key() -> None:
    me = {name: float(1000 + i) for i, name in enumerate(_names(1200))}
    me[5000] = me[2] = me[1199]  # integer keys: 2 < 1199 < 5000, where string order would put "5000" before "2"
    picked = universe(me)
    assert len(picked) == UNIVERSE_SIZE
    assert picked[:3] == [2, 1199, 5000]
    assert picked[-1] == 202  # 1,201 names: the top three, then 1198 down to 202


@pytest.mark.parametrize("bad", [0.0, -1.0, math.nan, math.inf])
def test_an_invalid_me_refuses_the_run(bad: float) -> None:
    me = {name: 1.0 for name in _names(UNIVERSE_SIZE)}
    me[5] = bad
    with pytest.raises(BookRefusal, match="ME_INVALID") as caught:
        universe(me)
    assert caught.value.code == "ME_INVALID"


def test_fewer_than_1000_admitted_names_refuses_the_run() -> None:
    with pytest.raises(BookRefusal) as caught:
        universe({name: 1.0 for name in _names(UNIVERSE_SIZE - 1)})
    assert caught.value.code == "UNIVERSE_SHORT"


# --------------------------------------------------------------------------- z-scores


def test_rank_z_averages_tied_ranks_and_standardises_with_the_population_sd() -> None:
    values = {name: float(i) for i, name in enumerate(_names(MIN_GROUP))}
    values[1] = values[2] = 1.5  # ranks 2 and 3 tie at 2.5
    z = rank_z(_raw(values))
    assert z is not None
    assert z[1] == z[2]
    ranks = [1, 2.5, 2.5, 4, 5, 6, 7, 8, 9, 10]
    mean, sd = statistics.fmean(ranks), statistics.pstdev(ranks)
    assert float(z[9]) == pytest.approx((10 - mean) / sd)
    assert statistics.fmean(map(float, z.values())) == pytest.approx(0, abs=1e-12)
    assert statistics.pstdev(map(float, z.values())) == pytest.approx(1)


def test_a_small_or_constant_group_is_uninformative() -> None:
    assert rank_z(_raw({name: float(i) for i, name in enumerate(_names(MIN_GROUP - 1))})) is None
    assert rank_z(_raw({name: 1.0 for name in _names(MIN_GROUP)})) is None


def test_mathematically_equal_family_inputs_tie_exactly() -> None:
    """Codex checkpoint 2: member ranks (1, 1, 3) and (2, 2, 1) average to the same value family input, which float
    sums split by an ulp. Exact scores tie them, so they share the family's average rank."""
    names = _names(MIN_GROUP)
    a, b, c = names[:3]
    others = names[3:]
    ranks = {
        "be_me": {a: 1, b: 2, **{n: 3 + i for i, n in enumerate(others)}, c: 10},
        "ni_me": {a: 1, b: 2, **{n: 3 + i for i, n in enumerate(others)}, c: 10},
        "ocf_me": {a: 3, b: 1, c: 2, **{n: 4 + i for i, n in enumerate(others)}},
    }
    signed: dict[str, dict[int, float]] = {k: {n: float(r) for n, r in v.items()} for k, v in ranks.items()}
    signed["gp_at"] = {n: float(n) for n in names}
    signed["at_gr1"] = {n: float(n) for n in names}
    scores = composite_scores(names, dict.fromkeys(names, "Durbl"), signed)
    value = scores.by_operation["value"]
    assert compare(value[a], value[b]) == 0 and value[a] == value[b]


def test_a_zero_score_converts_to_a_float() -> None:
    assert type(float(Exact.of({}))) is float and float(Exact.of({})) == 0.0


# --------------------------------------------------------------------------- composite


def _formation(industries: dict[str, int]) -> tuple[list[int], dict[int, str], dict[str, dict[int, float]]]:
    """Names in each industry (keys from 1000 × its position) with every characteristic equal to the name's index
    within its industry."""
    names: list[int] = []
    industry: dict[int, str] = {}
    signed: dict[str, dict[int, float]] = {c: {} for c in CHARACTERISTICS}
    for position, (group, size) in enumerate(industries.items()):
        for i, name in enumerate(_names(size, base=1000 * position)):
            names.append(name)
            industry[name] = group
            for c in CHARACTERISTICS:
                signed[c][name] = float(i)
    return names, industry, signed


def test_scores_rank_within_industry_not_across_it() -> None:
    names, industry, signed = _formation({"Durbl": 12, "HiTec": 12})
    for c in CHARACTERISTICS:  # every HiTec value far above every Durbl value
        for name in names:
            if industry[name] == "HiTec":
                signed[c][name] += 1000.0
    composite = composite_scores(names, industry, signed).composite
    assert composite[11] == composite[1011]
    assert composite[0] == composite[1000]


def test_a_small_industry_gets_no_score_and_no_cross_industry_fallback() -> None:
    names, industry, signed = _formation({"Durbl": 12, "Telcm": MIN_GROUP - 1})
    scores = composite_scores(names, industry, signed)
    telcm = tuple(n for n in names if industry[n] == "Telcm")
    assert not set(telcm) & set(scores.composite)
    assert scores.uninformative[("be_me", "Telcm")] == telcm
    assert ("be_me", "Durbl") not in scores.uninformative


def test_unclassified_names_are_their_own_group() -> None:
    names, industry, signed = _formation({UNCLASSIFIED: 12, "Durbl": 12})
    scores = composite_scores(names, industry, signed)
    assert scores.composite[11] == scores.composite[1011]


def test_a_family_averages_the_members_present() -> None:
    names, industry, signed = _formation({"Durbl": 12})
    # One name loses two of its three value members; its value family input is its one remaining member's z.
    del signed["ni_me"][5], signed["ocf_me"][5]
    scores = composite_scores(names, industry, signed)
    assert 5 in scores.by_operation["value"] and 5 not in scores.by_operation["ni_me"]


def test_the_composite_is_the_value_family_alone() -> None:
    names, industry, signed = _formation({"Durbl": 12})
    for member in ("be_me", "ni_me", "ocf_me"):
        del signed[member][3]  # no value member: no family, so no composite
    del signed["be_me"][4], signed["ni_me"][4]  # one member left: the family, so the composite
    scores = composite_scores(names, industry, signed)
    assert 3 not in scores.composite and 4 in scores.composite
    assert "gp_a" not in scores.by_operation and "investment" not in scores.by_operation
    # One family: the composite orders names exactly as the value family does.
    assert bands(scores.composite).order == bands(scores.by_operation["value"]).order


def test_a_non_finite_value_is_no_input() -> None:
    names, industry, signed = _formation({"Durbl": 12})
    signed["be_me"][2] = math.nan
    scores = composite_scores(names, industry, signed)
    assert 2 not in scores.by_operation["be_me"] and 2 in scores.composite


def test_a_name_without_an_industry_is_a_caller_error() -> None:
    names, industry, signed = _formation({"Durbl": 12})
    del industry[0]
    with pytest.raises(ValueError, match="no industry"):
        composite_scores(names, industry, signed)


# --------------------------------------------------------------------------- bands


def test_bands_take_the_ceiling_of_a_tenth_and_a_third() -> None:
    composite = _raw({name: -float(i) for i, name in enumerate(_names(31))})
    got = bands(composite)
    assert got.order[0] == 0
    assert len(got.decile) == 4 and len(got.tercile) == 11
    assert got.decile_by_identifier == frozenset() and got.tercile_by_identifier == frozenset()


def test_a_tie_straddling_a_boundary_is_decided_by_name_key_and_reported() -> None:
    values = {name: -float(i) for i, name in enumerate(_names(20))}
    values[10] = values[2] = values[3] = -1.0  # straddles the decile cut at 2; 10 sorts after 2 and 3 as an int
    del values[1]
    got = bands(_raw(values))
    assert got.decile == {0, 2}
    assert got.decile_by_identifier == {2, 3, 10}


def test_bands_of_no_composite_are_empty() -> None:
    got = bands({})
    assert got.order == () and got.decile == frozenset() and got.decile_by_identifier == frozenset()
    assert COMPOSITE == "composite"


def test_exact_comparison_agrees_with_floats_wherever_floats_can_tell() -> None:
    import random
    from fractions import Fraction

    rng = random.Random(3609)
    variances = [Fraction(2), Fraction(3), Fraction(33, 4)]
    checked = 0
    for _ in range(2000):
        x = Exact.of({v: Fraction(rng.randint(-20, 20), rng.randint(1, 6)) for v in variances})
        y = Exact.of({v: Fraction(rng.randint(-20, 20), rng.randint(1, 6)) for v in variances})
        gap = float(x) - float(y)
        if abs(gap) > 1e-9:
            assert compare(x, y) == (1 if gap > 0 else -1)
            checked += 1
        assert compare(x, y) == -compare(y, x)
    assert checked > 1900
    same = Exact.of({Fraction(2): Fraction(1), Fraction(3): Fraction(-1)})
    assert compare(same, same) == 0
