"""The #3740 (#3609 step 3) book's construction core: the several-family composite and the exact cap solution.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"Source rules", "Fixed by construction" (PR #3755). Pure
functions over one formation, reusing step 2's scoring primitives (``factor_book``) unchanged. They live in their own
module because step 2's and #3621 v2's declared runs hash ``factor_book*.py`` into their construction hashes; an
edit there would make those concluded trials refuse their own re-run on ``main``.

* **Composite** (spec "Composite with several families"): a name has one when a strict majority of the chosen
  families have a family score (1 of 1, 2 of 2, 2 of 3); its input is the mean of the family scores present,
  standardised by step 2's industry-relative rule. Step 2's first-draft ``MIN_FAMILIES = 2`` for three families,
  extended to every set size.
* **Cap solution** (spec "The cap solution"): value weights u_i = ME_i / Σ ME over the targets, each FF-12 group
  capped at c_g = 2 W_g (W_g its ME share of the cap reference) and each name at ``NAME_CAP``. With
  f_g(s) = Σ_{i∈g} min(cap, s u_i), a group takes min(f_g(θ), c_g) at the smallest global scale θ where those sum to
  1. Solved exactly on the merged breakpoints (every piece is linear in θ); no clip-and-redistribute loop. When even
  every group at min(c_g, cap · n_g) sums below 1, each group takes that and the residual is cash.
"""

from __future__ import annotations

import bisect
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from app.services.factor_book import COMPOSITE, UNCLASSIFIED, Exact, Scores, _means, _score_within_industry

#: Spec premise 1, in the table's order (the order the planner's candidate sets and seeds use).
FAMILIES: Final[Mapping[str, tuple[str, ...]]] = {
    "value": ("be_me", "ni_me", "ocf_me"),
    "GP/A": ("gp_at",),
    "investment": ("at_gr1",),
}
#: §"Source rules", name cap: a −50% gap straight after a rebalance costs at most 5% of NAV.
NAME_CAP: Final = 0.10
#: §"Source rules", sector cap: each FF-12 group at most this multiple of its ME share of the cap reference.
SECTOR_CAP_MULTIPLE: Final = 2.0


def min_families(chosen: Sequence[str]) -> int:
    """The strict majority of the chosen families: 1 of 1, 2 of 2, 2 of 3."""
    return len(chosen) // 2 + 1


def composite_scores(
    names: Sequence[int],
    industry: Mapping[int, str],
    signed: Mapping[str, Mapping[int, float]],
    chosen: Sequence[str],
) -> Scores:
    """Step 2's §"The book" step 2 over the universe ``names`` for the ``chosen`` families, strict-majority composite.

    Arguments as ``factor_book.composite_scores``; ``chosen`` is a non-empty subset of ``FAMILIES`` in its order."""
    if not chosen or len(set(chosen)) != len(chosen) or [f for f in FAMILIES if f in chosen] != list(chosen):
        raise ValueError(f"chosen families must be a non-empty subset of {list(FAMILIES)} in that order: {chosen}")
    missing = [name for name in names if name not in industry]
    if missing:
        raise ValueError(f"{len(missing)} universe names have no industry (use {UNCLASSIFIED!r}): {missing[:5]}")
    scores = Scores()
    for family in chosen:
        for characteristic in FAMILIES[family]:
            values = signed.get(characteristic, {})
            inputs = {n: Exact.raw(values[n]) for n in names if n in values and math.isfinite(values[n])}
            _score_within_industry(characteristic, inputs, industry, scores)
        inputs = _means(names, [scores.by_operation[m] for m in FAMILIES[family]], 1)
        _score_within_industry(family, inputs, industry, scores)
    inputs = _means(names, [scores.by_operation[f] for f in chosen], min_families(chosen))
    _score_within_industry(COMPOSITE, inputs, industry, scores)
    return scores


def membership_patterns(names: Sequence[int], scores: Scores, chosen: Sequence[str]) -> dict[tuple[str, ...], int]:
    """Per formation, how many universe ``names`` hold each combination of family scores (printed, as step 2 prints).

    A name with no family score counts under the empty pattern, so the counts always sum to the universe."""
    patterns: dict[tuple[str, ...], int] = defaultdict(int)
    for name in names:
        patterns[tuple(f for f in chosen if name in scores.by_operation.get(f, {}))] += 1
    return dict(patterns)


# --------------------------------------------------------------------------- caps


def _positive_me(me: Mapping[int, float], what: str) -> None:
    bad = sorted(name for name, value in me.items() if not (math.isfinite(value) and value > 0))
    if bad:
        raise ValueError(f"{len(bad)} {what} names have a non-finite or non-positive ME: {bad[:5]}")


def sector_caps(reference_me: Mapping[int, float], group: Mapping[int, str]) -> dict[str, float]:
    """c_g = ``SECTOR_CAP_MULTIPLE`` × the group's ME share of the cap reference (the formation's top 1,000)."""
    _positive_me(reference_me, "cap-reference")
    total = math.fsum(reference_me.values())
    by_group: dict[str, list[float]] = defaultdict(list)
    for name, value in reference_me.items():
        by_group[group[name]].append(value)
    return {g: SECTOR_CAP_MULTIPLE * math.fsum(values) / total for g, values in by_group.items()}


@dataclass(frozen=True)
class CapSolution:
    """One formation's target weights; ``cash`` is the residual a long-only book holds when the caps are infeasible."""

    weights: dict[int, float]
    cash: float
    #: The global scale; ``None`` when the caps are infeasible (every group sits at min(c_g, cap · n_g)).
    theta: float | None
    #: Groups held at their sector cap (scale s_g below θ, or every capped group when infeasible).
    sector_capped: frozenset[str]
    #: Names held at ``NAME_CAP``.
    name_capped: frozenset[int]


@dataclass(frozen=True)
class _Group:
    u: tuple[float, ...]  # descending
    cap: float
    #: s_g, the smallest scale at which f_g reaches the cap; ``math.inf`` when it never does (cap ≥ NAME_CAP · n_g).
    s_cap: float

    def f(self, s: float) -> float:
        return math.fsum(min(NAME_CAP, s * u) for u in self.u)


def _cap_scale(u: Sequence[float], cap: float) -> float:
    """s_g: walk the breakpoints NAME_CAP / u_i (u descending); within a piece f = NAME_CAP k + s · Σ unsaturated u."""
    if cap >= NAME_CAP * len(u):
        return math.inf
    for k, ui in enumerate(u):
        # The crossing if no further name saturates first. The unsaturated sum is re-summed, never decremented: a
        # dominant name subtracted from the total would erase the small names' precision.
        s = (cap - NAME_CAP * k) / math.fsum(u[k:])
        if s <= NAME_CAP / ui:
            return s
    raise AssertionError("unreachable: cap < NAME_CAP · n_g crosses before every name saturates")


def cap_solution(me: Mapping[int, float], group: Mapping[int, str], caps: Mapping[str, float]) -> CapSolution:
    """The authoritative allocation for the formation's targets (``me`` over T), by the spec's exact rule.

    ``group`` gives every target's FF-12 group (the unclassified bucket is a group); ``caps`` every target group's
    c_g from :func:`sector_caps`. Zero targets leave the book all cash, with no weights computed."""
    if not me:
        return CapSolution({}, 1.0, None, frozenset(), frozenset())
    _positive_me(me, "target")
    total = math.fsum(me.values())
    members: dict[str, list[float]] = defaultdict(list)
    for name, value in me.items():
        members[group[name]].append(value / total)
    groups: dict[str, _Group] = {}
    for g, us in members.items():
        if not caps[g] > 0:
            raise ValueError(f"group {g!r} holds targets but has sector cap {caps[g]}")
        us.sort(reverse=True)
        groups[g] = _Group(tuple(us), caps[g], _cap_scale(us, caps[g]))

    def held(theta: float) -> float:
        return math.fsum(min(gr.f(theta), gr.cap) for gr in groups.values())

    def scale(gr: _Group, theta: float | None) -> float:
        return gr.s_cap if theta is None else min(theta, gr.s_cap)

    if math.fsum(min(gr.cap, NAME_CAP * len(gr.u)) for gr in groups.values()) < 1:
        theta: float | None = None
    else:
        points = sorted(
            {NAME_CAP / u for gr in groups.values() for u in gr.u if NAME_CAP / u < gr.s_cap}
            | {gr.s_cap for gr in groups.values() if math.isfinite(gr.s_cap)}
        )
        # H reaches its maximum (≥ 1) at the last point; the clamp absorbs a last-ulp shortfall there.
        k = min(bisect.bisect_left(points, True, key=lambda p: held(p) >= 1), len(points) - 1)
        low = points[k - 1] if k else 0.0
        # On (low, points[k]] no breakpoint lies inside: H(θ) = fixed + θ · slope, linear.
        fixed: list[float] = []
        slope: list[float] = []
        for gr in groups.values():
            if gr.s_cap <= low:
                fixed.append(gr.cap)
                continue
            for u in gr.u:
                if NAME_CAP / u <= low:
                    fixed.append(NAME_CAP)
                else:
                    slope.append(u)
        theta = (1 - math.fsum(fixed)) / math.fsum(slope)
    weights: dict[int, float] = {}
    for name, value in me.items():
        gr = groups[group[name]]
        weights[name] = min(NAME_CAP, scale(gr, theta) * value / total)
    cash = max(0.0, 1 - math.fsum(weights.values())) if theta is None else 0.0
    capped = frozenset(g for g, gr in groups.items() if math.isfinite(gr.s_cap) and (theta is None or gr.s_cap < theta))
    return CapSolution(weights, cash, theta, capped, frozenset(n for n, w in weights.items() if w >= NAME_CAP))


__all__ = [
    "FAMILIES",
    "NAME_CAP",
    "SECTOR_CAP_MULTIPLE",
    "CapSolution",
    "cap_solution",
    "composite_scores",
    "membership_patterns",
    "min_families",
    "sector_caps",
]
