"""#3621 slice 5b: the SI filter's run-side pieces for v2 (pure).

Spec: ``docs/research/2026-10-09-3621-slice5-short-interest.md`` §"The study" and §"Slices" items 5b and 5c (PR #3734).
``app.services.short_interest_flag`` reads each name; this module joins those readings to v1's flags and populations:

- :func:`with_si` adds :attr:`Filter.SI` to the flagged names' v1 flags;
- :func:`si_counts` and :func:`si_name_lines` are premise 2's counts table and per-name file
  (``scripts.measure_3621_short_interest_premise``), which 5c's reproduction must equal cell for cell and by sha256;
- :func:`coverage_shortfalls` and :func:`si_pair_result` are condition 5, the coverage floor;
- :func:`stage_b_identity` refuses a rebuilt stage B that differs from v1's in anything but ``prices.holding``.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import date
from typing import Any, Final

from app.services.avoidance_filters import Filter, NameFlags
from app.services.short_interest_flag import Reading, SiState
from scripts.report_3621_books import PairResult, PairVerdict, Population

#: Premise 2's per-population columns, in ``docs/research/3621-si-premise-counts.csv``'s order.
COUNTED: Final = (
    "admitted",
    *(str(s) for s in SiState if s is not SiState.NO_SETTLEMENT),
    "flagged",
    "split_carried",
)
#: Condition 5: at every covered formation, names with an SI value are at least this share of U(P, M).
SI_COVERAGE_FLOOR: Final = 0.9
CONDITION_5: Final = "5 SI coverage"
#: Amendment 3's only input added to the rebuilt stage B.
STAGE_B_ADDED_INPUT: Final = "inputs/reference_snapshot_jkp_return_cutoffs.jsonl.gz"


class SiRunError(RuntimeError):
    pass


def with_si(flags: Mapping[int, NameFlags], si_flagged: frozenset[int]) -> dict[int, NameFlags]:
    """v1's flags at one formation with :attr:`Filter.SI` added to every SI-flagged name."""
    outside = sorted(si_flagged - flags.keys())
    if outside:
        raise SiRunError(f"SI-flagged names without v1 flags: {outside[:5]}")
    return {name: replace(f, flagged=f.flagged | {Filter.SI}) if name in si_flagged else f for name, f in flags.items()}


def si_counts(
    pops: Mapping[Population, frozenset[int]], readings: Mapping[int, Reading], si_flagged: frozenset[int]
) -> dict[Population, dict[str, int]]:
    """Premise 2's counts at one covered formation: per population, ``admitted``, each state, ``flagged`` and
    ``split_carried``."""
    out: dict[Population, dict[str, int]] = {}
    for population in Population:
        tally = dict.fromkeys(COUNTED, 0)
        for name in pops[population]:
            reading = readings[name]
            if reading.state is SiState.NO_SETTLEMENT:
                raise SiRunError(f"name {name} reads no_settlement at a covered formation")
            tally["admitted"] += 1
            tally[str(reading.state)] += 1
            tally["flagged"] += name in si_flagged
            tally["split_carried"] += reading.split_carried
        out[population] = tally
    return out


def counts_rows(formation: date, counts: Mapping[Population, Mapping[str, int]]) -> list[list[str]]:
    """``docs/research/3621-si-premise-counts.csv`` rows for one formation, as strings."""
    return [[formation.isoformat(), str(p), *(str(counts[p][c]) for c in COUNTED)] for p in Population]


def si_name_lines(
    formation: date, me: Mapping[int, float], readings: Mapping[int, Reading], si_flagged: frozenset[int]
) -> list[str]:
    """Premise 2's per-name lines at one covered formation: ME descending, then ``name_key`` ascending, each
    ``json.dumps([M, name_key, state, settlement or null, SIR or null, flagged])`` with compact separators."""
    if me.keys() != readings.keys():
        raise SiRunError(f"{formation}: ME and readings name different sets")
    lines = []
    for name in sorted(me, key=lambda n: (-me[n], n)):
        r = readings[name]
        used = None if r.used is None else r.used.isoformat()
        lines.append(
            json.dumps(
                [formation.isoformat(), name, str(r.state), used, r.sir, name in si_flagged], separators=(",", ":")
            )
        )
    return lines


def coverage_shortfalls(
    members: Sequence[frozenset[int]], readings: Sequence[Mapping[int, Reading] | None], formations: Sequence[date]
) -> list[date]:
    """Covered formations (``readings`` not ``None``) at which names with an SI value are under
    ``SI_COVERAGE_FLOOR`` of U(P, M). An empty U at a covered formation is a shortfall: there is no coverage to show."""
    if not (len(members) == len(readings) == len(formations)):
        raise SiRunError("members, readings and formations differ in length")
    short = []
    for u, read, formation in zip(members, readings, formations, strict=True):
        if read is None:
            continue
        valued = sum(1 for name in u if read[name].sir is not None)
        if not u or valued < SI_COVERAGE_FLOOR * len(u):
            short.append(formation)
    return short


def si_pair_result(result: PairResult, shortfalls: Sequence[date]) -> PairResult:
    """Condition 5 on an SI set's verdict: a population under the floor at any covered formation is NOT ELIGIBLE
    with condition 5 named. REFUSED and NO_EFFECT pairs are left as they are: neither can be cited."""
    if not shortfalls or result.verdict not in (PairVerdict.ELIGIBLE, PairVerdict.NOT_ELIGIBLE):
        return result
    return replace(result, verdict=PairVerdict.NOT_ELIGIBLE, failed=(*result.failed, CONDITION_5))


def valueless_weight(filtered: frozenset[int], readings: Mapping[int, Reading]) -> float | None:
    """The equal-weight share of U_F with no SI value; ``None`` for an empty book."""
    if not filtered:
        return None
    return sum(1 for name in filtered if readings[name].sir is None) / len(filtered)


def si_overlap(flags: Mapping[int, NameFlags]) -> dict[str, int]:
    """SI-flagged names at one formation, and how many of them each v1 filter also flags."""
    si = [f for f in flags.values() if Filter.SI in f.flagged]
    return {"si": len(si), **{str(v1): sum(1 for f in si if v1 in f.flagged) for v1 in Filter if v1 is not Filter.SI}}


def _without_holding(row: Mapping[str, Any]) -> str:
    """The row less ``prices.holding``, as canonical JSON: type-sensitive, so ``false`` never equals ``0``."""
    out = copy.deepcopy(dict(row))
    prices = out.get("prices")
    if isinstance(prices, dict):
        prices.pop("holding", None)
    return json.dumps(out, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _keyed(rows: Iterable[Mapping[str, Any]], which: str) -> dict[tuple[str, int], str]:
    out: dict[tuple[str, int], str] = {}
    for row in rows:
        key = (row["M"], row["name_key"])
        if key in out:
            raise SiRunError(f"{which} stage B repeats (M, name_key) {key}")
        out[key] = _without_holding(row)
    return out


def stage_b_identity(
    v1_rows: Iterable[Mapping[str, Any]],
    v2_rows: Iterable[Mapping[str, Any]],
    v1_inputs: Mapping[str, str],
    v2_inputs: Mapping[str, str],
) -> int:
    """Refuses unless the rebuilt stage B equals v1's: the same (M, ``name_key``) keys over every row, admitted or
    not; each row equal to its v1 row after deleting ``prices.holding`` from both; every v1 input's sha256 equal in
    v2, whose only extra input is ``STAGE_B_ADDED_INPUT``. Returns the rows compared."""
    old, new = _keyed(v1_rows, "v1"), _keyed(v2_rows, "v2")
    if old.keys() != new.keys():
        raise SiRunError(
            f"stage B keys differ: {len(old.keys() - new.keys())} only in v1, {len(new.keys() - old.keys())} only in v2"
        )
    changed = sorted(k for k in old if old[k] != new[k])
    if changed:
        raise SiRunError(f"{len(changed)} stage-B rows differ outside prices.holding: {changed[:3]}")
    moved = sorted(name for name, sha in v1_inputs.items() if v2_inputs.get(name) != sha)
    if moved:
        raise SiRunError(f"stage-B inputs changed or missing in v2: {moved}")
    if set(v2_inputs) - set(v1_inputs) != {STAGE_B_ADDED_INPUT}:
        raise SiRunError(f"v2 stage B's added inputs are {sorted(set(v2_inputs) - set(v1_inputs))}")
    return len(old)
