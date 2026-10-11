"""#3740 (#3609 step 3): the historical path's formation inputs, from a verified stage A or stage B artefact.

Spec: ``docs/research/2026-10-10-3740-step3-vw-book.md`` §"Population and eligibility" (the historical path keeps step
2's population and eligibility: the Intrader admission, archive seasoning, no security-type rule and no eToro rule),
§"The book" steps 1-3 and §"Source rules" (PR #3755). The planner (premise 5) and the historical diagnostic run consume
these :class:`~app.services.vw_factor_book_decisions.Formation` values; this module computes no statistic.

Each input is read the way a concluded trial already read it, so nothing is re-derived:

* **Panel** (admitted names, ME, FF-12 group, raw closes, first bars, holding returns): step 2's loader
  (:func:`scripts.report_3609_step2.read_panel`), unchanged.
* **MAX readings** over every admitted name: #3621's (:func:`scripts.run_3621_avoidance.load_stage`), the readings
  #3743 recorded fidelity for; the flag is :func:`app.services.vw_factor_book_decisions.max_flagged`.
* **GP/A and investment members** (``gp_at``, ``at_gr1``): step 2's loader signs only the value members, so one more
  pass over the same verified row bytes signs the rest by the artefact's frozen Table 9 copy, and refuses unless it
  finds exactly the admitted names step 2's loader found.
* **Composite:** :func:`app.services.vw_factor_book.composite_scores` (strict majority of the chosen families) on step
  2's top-1,000 universe; step 2's bands.
* **Age rule:** step 2's archive seasoning; a scored name with no first admitted bar refuses, as step 2's
  ``eligible_to_enter`` does. ``tradable`` is ``None``.
"""

from __future__ import annotations

import gzip
import io
import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import date
from typing import Any, Final

from app.services.avoidance_filters import MaxReading
from app.services.factor_book import CHARACTERISTICS
from app.services.factor_book import bands as book_bands
from app.services.factor_book import universe as book_universe
from app.services.factor_book_path import archive_seasoned
from app.services.factor_panel_reference import parse_table9_signs
from app.services.vw_factor_book import FAMILIES, composite_scores
from app.services.vw_factor_book_decisions import Formation, max_flagged
from scripts.build_3609_factor_panel import VerifiedArtefact
from scripts.report_3609_step2 import TABLE9_SIGNS, PanelMonth, ReportError, ScoredMonth
from scripts.run_3621_avoidance import STAGE_KEEP, load_stage

#: The family members step 2's loader does not sign (it reads the value family only).
EXTRA_MEMBERS: Final = tuple(c for members in FAMILIES.values() for c in members if c not in CHARACTERISTICS)
#: The artefact files :func:`assemble` consumes; pass as ``keep`` to ``read_verified_artefact``.
KEEP: Final = STAGE_KEEP

#: ``formation -> name_key -> {member: JKP-signed value}`` for the members in ``EXTRA_MEMBERS``.
Members = Mapping[date, Mapping[int, Mapping[str, float]]]


def _lines(payload: bytes) -> Iterator[Any]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as handle:
        for line in handle:
            yield json.loads(line)


def read_members(verified: VerifiedArtefact) -> dict[date, dict[int, dict[str, float]]]:
    """Every admitted row's ``EXTRA_MEMBERS``, signed by the artefact's Table 9 copy; a ``None`` value is no input.

    Refuses a member with no Table 9 sign, a repeated admitted ``(M, name_key)`` and a value that is not a number."""
    signs = parse_table9_signs(verified.files[TABLE9_SIGNS])
    unsigned = [c for c in EXTRA_MEMBERS if c not in signs]
    if unsigned:
        raise ReportError(f"Table 9 holds no sign for {unsigned}")
    out: dict[date, dict[int, dict[str, float]]] = {}
    for row in _lines(verified.rows):
        if row["exclusion"] is not None:
            continue
        formation, name = date.fromisoformat(row["M"]), row["name_key"]
        names = out.setdefault(formation, {})
        if name in names:
            raise ReportError(f"admitted (M, name_key) {(row['M'], name)} is repeated")
        values: dict[str, float] = {}
        for c in EXTRA_MEMBERS:
            try:
                raw = row["characteristics"][c]["value"]
            except (KeyError, TypeError) as exc:
                raise ReportError(f"row {(row['M'], name)}: no {c} characteristic value") from exc
            if raw is None:
                continue
            if isinstance(raw, bool) or not isinstance(raw, int | float):
                raise ReportError(f"row {(row['M'], name)}: {c} value {raw!r} is not a number")
            values[c] = signs[c] * float(raw)
        names[name] = values
    return out


def check_members(months: Sequence[PanelMonth], members: Members) -> None:
    """Refuses unless the extra pass found exactly step 2's admitted names at exactly its formations."""
    if set(members) - {m.formation for m in months}:
        raise ReportError(f"admitted rows at formations step 2's loader did not read: {sorted(set(members))[:3]}")
    for month in months:
        found = set(members.get(month.formation, {}))
        if found != set(month.admitted):
            raise ReportError(
                f"{month.formation}: the member pass found {len(found)} admitted names, step 2's loader "
                f"{len(month.admitted)}; differing {sorted(found ^ set(month.admitted))[:5]}"
            )


def score(month: PanelMonth, members: Mapping[int, Mapping[str, float]], chosen: Sequence[str]) -> ScoredMonth:
    """§"The book" steps 1, 2 and 4: step 2's universe, the chosen families' strict-majority composite, its bands."""
    names = book_universe({name: panel.me for name, panel in month.admitted.items()})
    unread = [name for name in names if name not in members]
    if unread:
        raise ReportError(f"{month.formation}: {len(unread)} universe names have no member row: {unread[:5]}")
    industry = {name: month.admitted[name].industry for name in names}
    signed: dict[str, dict[int, float]] = {c: {} for family in chosen for c in FAMILIES[family]}
    for name in names:
        for c, value in (*month.admitted[name].signed.items(), *members[name].items()):
            if c in signed:
                signed[c][name] = value
    scores = composite_scores(names, industry, signed, chosen)
    return ScoredMonth(tuple(names), scores, book_bands(scores.composite))


def formation_inputs(month: PanelMonth, scored: ScoredMonth, readings: Mapping[int, MaxReading]) -> Formation:
    """One historical formation's inputs: ``readings`` must hold every admitted name (the MAX population)."""
    if set(readings) != set(month.admitted):
        raise ReportError(
            f"{month.formation}: MAX readings cover {len(readings)} names, {len(month.admitted)} admitted"
        )
    unaged = sorted(name for name in scored.bands.order if name not in month.first_bar)
    if unaged:
        raise ReportError(f"{month.formation}: {len(unaged)} scored names have no first admitted bar: {unaged[:5]}")
    universe = frozenset(scored.universe)
    return Formation(
        formation=month.formation,
        session=month.session,
        universe=universe,
        me={name: month.admitted[name].me for name in universe},
        group={name: month.admitted[name].industry for name in universe},
        bands=scored.bands,
        close=month.close,
        aged=frozenset(name for name, first in month.first_bar.items() if archive_seasoned(first, month.session)),
        max_flagged=max_flagged(readings),
        tradable=None,
        returns={name: month.admitted[name].holding for name in universe},
    )


def assemble(verified: VerifiedArtefact, chosen: Sequence[str]) -> list[Formation]:
    """Every formation of one verified stage artefact (read with ``keep=KEEP``), in order, for the ``chosen`` set."""
    stage = load_stage(verified)
    members = read_members(verified)
    check_members(stage.months, members)
    return [
        formation_inputs(month, score(month, members.get(month.formation, {}), chosen), readings)
        for month, readings in zip(stage.months, stage.readings, strict=True)
    ]


__all__ = [
    "EXTRA_MEMBERS",
    "KEEP",
    "assemble",
    "check_members",
    "formation_inputs",
    "read_members",
    "score",
]
