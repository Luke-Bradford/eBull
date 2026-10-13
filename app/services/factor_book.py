"""The #3609 step 2 book's scoring core: universe, industry-relative z-scores, families, composite and bands.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"The book" steps 1, 2 and 4, and §"Source rules"
(PR #3666). Pure functions over one formation; the report (slice 3) supplies the panel rows and runs the path. Names
are step 1's integer ``name_key``.

* **z-score** (QMJ, Asness, Frazzini & Pedersen 2019, p. 9 eq. 2): rank the input within the group, ties taking the
  average rank, and standardise the ranks by their mean and population standard deviation (ddof 0, fixed by
  construction; QMJ does not state it).
* **Industry-relative** (QMJ p. 13): every operation ranks within one (FF-12 industry, M) group among universe names
  that have that operation's input. ``sic_null`` and ``sic_unloaded`` names form one "unclassified" group. There is no
  cross-industry fallback.
* **Uninformative groups** give no score: fewer than ``MIN_GROUP`` inputs, or ranks with zero variance.
* **Family** = z(mean of the member z-scores present), at least one member; **composite** = z(mean of the family
  scores present), at least ``MIN_FAMILIES``. With the value family alone (premise 6) the composite ranks exactly as
  the family does.

**Exact ties.** A z-score is ``(R - m) / sqrt(V)`` with the rank R, the mean m and the variance V all rational, so
every score here is held exactly as a sum of ``c / sqrt(V)`` terms over rationals (:class:`Exact`). Mathematically
equal family or composite inputs therefore tie exactly, and the tie takes the average rank or the ``name_key``
tie-break, never float residue (``docs/review-prevention-log.md``, 2026-09-22 #2834: divide at stored precision, no
tolerance). Raw characteristic values are compared as the exact values of their floats.
"""

from __future__ import annotations

import functools
import math
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Final

#: Premises 1 and 6: step 1's value family, the one family whose intended loading (HML) condition 3 can test with
#: power on stage B. GP/A and investment passed step 1 but are not in the book: on stage A their RMW and CMA
#: coefficients in the three-family book were too weak for a powered stage-B test, and value with GP/A cancelled
#: HML (``scripts/plan_3609_step2_exposure.py``).
FAMILIES: Final[Mapping[str, tuple[str, ...]]] = {
    "value": ("be_me", "ni_me", "ocf_me"),
}
CHARACTERISTICS: Final = tuple(c for members in FAMILIES.values() for c in members)
#: Premise 3: the book universe is the top 1,000 by ME at s(M).
UNIVERSE_SIZE: Final = 1000
#: §"Source rules", fixed by construction: a z-score over fewer peers is mostly rank noise.
MIN_GROUP: Final = 10
MIN_FAMILIES: Final = 1
UNCLASSIFIED: Final = "unclassified"
COMPOSITE: Final = "composite"


class BookRefusal(RuntimeError):
    """A data refusal that stops the run, with its §"Decision rule" reason code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def universe(me: Mapping[int, float]) -> list[int]:
    """The top ``UNIVERSE_SIZE`` admitted names by ME at s(M), ties by ``name_key`` ascending.

    Refuses ``ME_INVALID`` on any non-finite or non-positive ME and ``UNIVERSE_SHORT`` on fewer admitted names."""
    bad = sorted(name for name, value in me.items() if not (math.isfinite(value) and value > 0))
    if bad:
        raise BookRefusal("ME_INVALID", f"{len(bad)} admitted names have a non-finite or non-positive ME: {bad[:5]}")
    if len(me) < UNIVERSE_SIZE:
        raise BookRefusal("UNIVERSE_SHORT", f"{len(me)} admitted names, fewer than {UNIVERSE_SIZE}")
    return sorted(me, key=lambda name: (-me[name], name))[:UNIVERSE_SIZE]


# --------------------------------------------------------------------------- exact scores


@dataclass(frozen=True)
class Exact:
    """``sum(c / sqrt(V))`` over ``terms`` of ``(V, c)``, V > 0 rational, c non-zero rational, V distinct."""

    terms: tuple[tuple[Fraction, Fraction], ...]

    @classmethod
    def of(cls, by_variance: Mapping[Fraction, Fraction]) -> Exact:
        return cls(tuple(sorted((v, c) for v, c in by_variance.items() if c != 0)))

    @classmethod
    def raw(cls, value: float) -> Exact:
        """A raw characteristic value: its float's exact value, over sqrt(1)."""
        return cls.of({Fraction(1): Fraction(value)})

    def __float__(self) -> float:
        return math.fsum(float(c) / math.sqrt(float(v)) for v, c in self.terms)  # 0.0, never int 0, when empty


def mean(values: Sequence[Exact]) -> Exact:
    total: dict[Fraction, Fraction] = defaultdict(Fraction)
    for value in values:
        for v, c in value.terms:
            total[v] += c
    return Exact.of({v: c / len(values) for v, c in total.items()})


def _root_sum_cmp(left: Sequence[Fraction], right: Sequence[Fraction]) -> int:
    """Sign of ``sum(sqrt(a) for a in left) - sum(sqrt(b) for b in right)``, all radicands >= 0, at most 3 roots."""
    if len(left) < len(right):
        return -_root_sum_cmp(right, left)
    if not right:
        return 1 if any(left) else 0
    if len(left) == 1:  # one against one
        return (left[0] > right[0]) - (left[0] < right[0])
    if len(left) == 2 and len(right) == 1:  # sqrt(a1) + sqrt(a2) vs sqrt(b): square both sides
        (a1, a2), (b,) = left, right
        rest = b - a1 - a2  # compare 2 sqrt(a1 a2) with rest
        if rest < 0:
            return 1
        return (4 * a1 * a2 > rest * rest) - (4 * a1 * a2 < rest * rest)
    raise ValueError(f"{len(left)} + {len(right)} roots: a score compares at most 3 terms by construction")


def compare(x: Exact, y: Exact) -> int:
    """Exact sign of ``x - y``."""
    diff: dict[Fraction, Fraction] = defaultdict(Fraction)
    for v, c in x.terms:
        diff[v] += c
    for v, c in y.terms:
        diff[v] -= c
    positive = [c * c / v for v, c in diff.items() if c > 0]
    negative = [c * c / v for v, c in diff.items() if c < 0]
    return _root_sum_cmp(positive, negative)


# --------------------------------------------------------------------------- z-scores


def rank_z(values: Mapping[int, Exact]) -> dict[int, Exact] | None:
    """QMJ's z-score of one group's input: average ranks standardised by their mean and population SD.

    ``None`` when the group is uninformative (fewer than ``MIN_GROUP`` inputs, or zero rank variance)."""
    if len(values) < MIN_GROUP:
        return None
    ordered = sorted(values, key=functools.cmp_to_key(lambda a, b: compare(values[a], values[b])))
    ranks: dict[int, Fraction] = {}
    start = 0
    while start < len(ordered):
        end = start
        while end + 1 < len(ordered) and compare(values[ordered[end + 1]], values[ordered[start]]) == 0:
            end += 1
        for name in ordered[start : end + 1]:
            ranks[name] = Fraction(start + end, 2) + 1
        start = end + 1
    m = sum(ranks.values(), Fraction(0)) / len(ranks)
    variance = sum(((r - m) ** 2 for r in ranks.values()), Fraction(0)) / len(ranks)
    if variance == 0:
        return None
    return {name: Exact.of({variance: rank - m}) for name, rank in ranks.items()}


@dataclass
class Scores:
    """One formation's scores per operation (each characteristic, each family, the composite)."""

    by_operation: dict[str, dict[int, Exact]] = field(default_factory=dict)
    #: (operation, industry) -> the names of an uninformative group, which got no score from that operation.
    uninformative: dict[tuple[str, str], tuple[int, ...]] = field(default_factory=dict)

    @property
    def composite(self) -> dict[int, Exact]:
        return self.by_operation.get(COMPOSITE, {})


def _score_within_industry(
    operation: str, inputs: Mapping[int, Exact], industry: Mapping[int, str], scores: Scores
) -> None:
    groups: dict[str, dict[int, Exact]] = defaultdict(dict)
    for name, value in inputs.items():
        groups[industry[name]][name] = value
    out: dict[int, Exact] = {}
    for group, values in groups.items():
        z = rank_z(values)
        if z is None:
            scores.uninformative[(operation, group)] = tuple(sorted(values))
        else:
            out.update(z)
    scores.by_operation[operation] = out


def _means(names: Sequence[int], parts: Sequence[Mapping[int, Exact]], minimum: int) -> dict[int, Exact]:
    out: dict[int, Exact] = {}
    for name in names:
        present = [part[name] for part in parts if name in part]
        if len(present) >= minimum:
            out[name] = mean(present)
    return out


def composite_scores(
    names: Sequence[int],
    industry: Mapping[int, str],
    signed: Mapping[str, Mapping[int, float]],
) -> Scores:
    """§"The book" step 2 over the universe ``names``.

    ``industry`` maps every name to its FF-12 short name or ``UNCLASSIFIED``; ``signed`` maps each characteristic to
    its JKP Table 9-signed values (a missing or non-finite value is no input). Exclusions applied later (price,
    seasoning) never change these ranking populations."""
    missing = [name for name in names if name not in industry]
    if missing:
        raise ValueError(f"{len(missing)} universe names have no industry (use {UNCLASSIFIED!r}): {missing[:5]}")
    scores = Scores()
    for characteristic in CHARACTERISTICS:
        values = signed.get(characteristic, {})
        inputs = {n: Exact.raw(values[n]) for n in names if n in values and math.isfinite(values[n])}
        _score_within_industry(characteristic, inputs, industry, scores)
    for family, members in FAMILIES.items():
        inputs = _means(names, [scores.by_operation[m] for m in members], 1)
        _score_within_industry(family, inputs, industry, scores)
    inputs = _means(names, [scores.by_operation[f] for f in FAMILIES], MIN_FAMILIES)
    _score_within_industry(COMPOSITE, inputs, industry, scores)
    return scores


# --------------------------------------------------------------------------- bands


@dataclass(frozen=True)
class Bands:
    """§"The book" step 4: the ordering and its entry (top decile) and hold (top tercile) bands."""

    order: tuple[int, ...]
    decile: frozenset[int]
    tercile: frozenset[int]
    #: §"Source rules", identifier-decided selections: the names whose side of each boundary ``name_key`` decided
    #: (every name sharing the composite that straddles the boundary); empty when no tie straddles it.
    decile_by_identifier: frozenset[int]
    tercile_by_identifier: frozenset[int]


def _straddling(order: Sequence[int], same: Callable[[int, int], bool], cut: int) -> frozenset[int]:
    if cut >= len(order) or not same(order[cut - 1], order[cut]):
        return frozenset()
    return frozenset(name for name in order if same(name, order[cut]))


def bands(composite: Mapping[int, Exact]) -> Bands:
    """Names with a composite by composite descending, ties by ``name_key``; top ⌈n/10⌉ and ⌈n/3⌉, no tie grouping."""

    def descending(a: int, b: int) -> int:
        return -compare(composite[a], composite[b]) or (a > b) - (a < b)

    def same(a: int, b: int) -> bool:
        return compare(composite[a], composite[b]) == 0

    order = tuple(sorted(composite, key=functools.cmp_to_key(descending)))
    decile, tercile = math.ceil(len(order) / 10), math.ceil(len(order) / 3)
    return Bands(
        order,
        frozenset(order[:decile]),
        frozenset(order[:tercile]),
        _straddling(order, same, decile),
        _straddling(order, same, tercile),
    )


__all__ = [
    "CHARACTERISTICS",
    "COMPOSITE",
    "FAMILIES",
    "MIN_FAMILIES",
    "MIN_GROUP",
    "UNCLASSIFIED",
    "UNIVERSE_SIZE",
    "Bands",
    "BookRefusal",
    "Exact",
    "Scores",
    "bands",
    "compare",
    "composite_scores",
    "mean",
    "rank_z",
    "universe",
]
