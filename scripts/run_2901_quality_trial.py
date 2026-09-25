"""#2901 quality arm: the sealed runner. Stage 1 — the artefact's books and the census-time assertions.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Portfolios", "Schedule assertions"). This
stage reads only the verified construction artefact (``scripts/build_2901_quality_input.py``): no price, no return.
It builds the four books per formation and refuses the run (``REFUSED_PRE_GATE``) on any schema or schedule
violation, which is a fact about the construction, not an outcome:

- **A(D)**: ``in_arm`` rows; **C(D)**: all of E(D) (``rung = eligible``);
- **C′(D)**: every P(D) row with no exclusion reason (``exclusion_reasons``): executable, a recorded non-financial
  SIC, FPI not ``true``, and no RESOLVED ``Assets`` ≤ 0 or ``StockholdersEquity`` < 0 — the screens bind only where
  the artefact resolved them;
- **D₀(D)**: E(D) rows in decile 0 of rule 7's frozen formula ⌊10·r/n⌋ on the (GP/A, CIK) order, re-derived here
  and asserted equal to the artefact's ``decile`` and ``in_arm`` fields.

The parity, identity gate, statistics and verdict stages (part 2c-ii) consume :func:`schedules`.

Usage::

    PYTHONPATH=. uv run python -m scripts.run_2901_quality_trial --input <artefact dir> --input-sha256 <manifest sha>
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any, Final

from app.services import r6_quality_universe as quality
from app.services.market_calendar import us_market_status
from app.services.pit_fundamentals import canonical_decimal
from app.services.r6_exclusion_trial import WINDOW_END, Schedule
from app.services.r6_monthly_trial import MonthlySchedule, Portfolio, last_session_of_month
from scripts.build_2901_quality_input import YEARS, load_quality_input

#: Spec lines 85–88: the only values the run accepts.
FPI_VALUES: Final = frozenset({"true", "false", "not_evaluated"})
SIC_FIELD_VALUES: Final = frozenset({"missing", "financial", "other"})
COMPONENT_STATUSES: Final = frozenset({"value", "absent", "ambiguous", "blocked_by_rejection"})
#: The two rungs ``classify`` returns before it records a SIC: they carry no ``fields.sic``.
UNRECORDED_SIC_RUNGS: Final = frozenset({"fundamentals_integrity_excluded", "no_companyfacts_entry"})
#: C′'s exclusion reasons, in the spec's exclusive order (line 83).
EXCLUSION_REASONS: Final = (
    "no_companyfacts_entry",
    "fundamentals_integrity_excluded",
    "sic_missing",
    "sic_financial",
    "foreign_private_issuer",
    "assets_nonpositive",
    "equity_negative",
    "not_executable",
)
BUY_HOLD: Final = "buy_hold"


class ArtefactRefusal(RuntimeError):
    """A schema or schedule violation in the artefact: the run does not start (``REFUSED_PRE_GATE``)."""


# --------------------------------------------------------------------------- row schema and C′


def _component_value(row: Mapping[str, Any], name: str) -> Decimal | None:
    """The resolved value of a component, or None when the artefact did not resolve it."""
    component = row["components"].get(name)
    if component is None or component["status"] != "value":
        return None
    return Decimal(component["value"])


def check_row_schema(row: Mapping[str, Any]) -> None:
    """Spec lines 85–88. Anything outside the pinned vocabulary refuses the run."""
    cik = row.get("cik")
    if not isinstance(row.get("executable"), bool):
        raise ArtefactRefusal(f"{cik}: executable is not a boolean")
    fields = row.get("fields")
    if not isinstance(fields, Mapping) or fields.get("fpi") not in FPI_VALUES:
        raise ArtefactRefusal(f"{cik}: fields.fpi is outside {sorted(FPI_VALUES)}")
    sic = row.get("sic")
    if sic is not None and (
        isinstance(sic, bool) or not isinstance(sic, int) or not quality.SIC_RANGE[0] <= sic <= quality.SIC_RANGE[1]
    ):
        raise ArtefactRefusal(f"{cik}: sic is neither null nor an integer in the SIC range")
    if "sic" not in fields:
        if row.get("rung") not in UNRECORDED_SIC_RUNGS or sic is not None:
            raise ArtefactRefusal(f"{cik}: fields.sic is missing outside the no-SIC rungs")
    else:
        recorded = fields["sic"]
        if recorded not in SIC_FIELD_VALUES:
            raise ArtefactRefusal(f"{cik}: fields.sic is outside {sorted(SIC_FIELD_VALUES)}")
        implied = (
            "missing"
            if sic is None
            else "financial"
            if quality.SIC_FINANCIAL[0] <= sic <= quality.SIC_FINANCIAL[1]
            else "other"
        )
        if recorded != implied:
            raise ArtefactRefusal(f"{cik}: fields.sic disagrees with sic")
    components = row.get("components")
    if not isinstance(components, Mapping):
        raise ArtefactRefusal(f"{cik}: components is not a mapping")
    for name, component in components.items():
        status, value = component.get("status"), component.get("value")
        if status not in COMPONENT_STATUSES:
            raise ArtefactRefusal(f"{cik}: component {name} status is outside {sorted(COMPONENT_STATUSES)}")
        if (status == "value") != (value is not None):
            raise ArtefactRefusal(f"{cik}: component {name} carries a value exactly when its status is not value")
        if value is not None:
            try:
                number = Decimal(value) if isinstance(value, str) else None
            except InvalidOperation:
                number = None
            if number is None or not number.is_finite() or canonical_decimal(number) != value:
                raise ArtefactRefusal(f"{cik}: component {name} value is not a finite canonical decimal string")


def exclusion_reasons(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Every reason, in the spec's order, that keeps a P(D) row out of C′ (empty: the row is in C′)."""
    fields = row["fields"]
    assets, equity = _component_value(row, "assets"), _component_value(row, "equity")
    held = {
        "no_companyfacts_entry": row["rung"] == "no_companyfacts_entry",
        "fundamentals_integrity_excluded": row["rung"] == "fundamentals_integrity_excluded",
        "sic_missing": fields.get("sic") == "missing",
        "sic_financial": fields.get("sic") == "financial",
        "foreign_private_issuer": fields["fpi"] == "true",
        "assets_nonpositive": assets is not None and assets <= 0,
        "equity_negative": equity is not None and equity < 0,
        "not_executable": not row["executable"],
    }
    return tuple(reason for reason in EXCLUSION_REASONS if held[reason])


# --------------------------------------------------------------------------- books


@dataclass(frozen=True)
class FormationBooks:
    """One formation's four books, as vendor symbols."""

    formation: date
    x_date: date
    arm: frozenset[str]
    control: frozenset[str]
    complete_case: frozenset[str]
    gate_control: frozenset[str]
    #: E(D) rows outside D₀ whose GP/A equals D₀'s highest: the ties rule 7 splits by CIK at the decile-0 boundary.
    gate_boundary_ties: int


def _gpa(row: Mapping[str, Any]) -> Fraction:
    raw = row.get("gpa")
    if not (isinstance(raw, list) and len(raw) == 2 and all(isinstance(part, str) for part in raw)):
        raise ArtefactRefusal(f"{row.get('cik')}: an eligible row without a GP/A fraction")
    return Fraction(int(raw[0]), int(raw[1]))


def _symbols(rows: Sequence[Mapping[str, Any]], what: str, formation: date) -> frozenset[str]:
    symbols = [row["vendor_symbol"] for row in rows]
    if not symbols:
        raise ArtefactRefusal(f"{formation}: {what} is empty")
    if len(set(symbols)) != len(symbols):
        raise ArtefactRefusal(f"{formation}: {what} has duplicate vendor symbols")
    return frozenset(symbols)


def _first_session_of_july(year: int) -> date:
    day = date(year, 7, 1)
    while us_market_status(day) == "closed":
        day = date(year, 7, day.day + 1)
    return day


def formation_books(document: Mapping[str, Any]) -> FormationBooks:
    """One formation document → its books, with every row-level assertion of the spec."""
    formation = date.fromisoformat(document["formation"])
    x_date = date.fromisoformat(document["x_date"])
    rows = document["rows"]
    for row in rows:
        check_row_schema(row)
        if row.get("x_date") != document["x_date"]:
            raise ArtefactRefusal(f"{formation}: a row's x_date differs from the formation's")
    if len({row["cik"] for row in rows}) != len(rows):
        raise ArtefactRefusal(f"{formation}: duplicate CIKs in P(D)")

    eligible = [row for row in rows if row["rung"] == quality.ELIGIBLE]
    signal = {row["cik"]: _gpa(row) for row in eligible}
    try:
        deciles = quality.top_decile(signal)
    except quality.QualityUniverseError as exc:
        raise ArtefactRefusal(f"{formation}: rule 7 refuses: {exc}") from exc
    for row in rows:
        expected_decile = deciles.decile.get(row["cik"])
        if row.get("decile") != expected_decile or row.get("in_arm") is not (row["cik"] in deciles.arm):
            raise ArtefactRefusal(f"{formation}: {row['cik']} decile or in_arm differs from rule 7's formula")

    arm = [row for row in eligible if row["in_arm"]]
    gate = [row for row in eligible if row["decile"] == 0]
    gate_top = max(signal[row["cik"]] for row in gate) if gate else None
    ties = sum(1 for row in eligible if row["decile"] != 0 and signal[row["cik"]] == gate_top)
    complete_case = [row for row in rows if not exclusion_reasons(row)]

    books = FormationBooks(
        formation=formation,
        x_date=x_date,
        arm=_symbols(arm, "A(D)", formation),
        control=_symbols(eligible, "C(D)", formation),
        complete_case=_symbols(complete_case, "C′(D)", formation),
        gate_control=_symbols(gate, "D₀(D)", formation),
        gate_boundary_ties=ties,
    )
    if not (books.arm <= books.control and books.gate_control <= books.control):
        raise ArtefactRefusal(f"{formation}: A(D) or D₀(D) is not inside E(D)")
    if not books.control <= books.complete_case:
        raise ArtefactRefusal(f"{formation}: E(D) is not inside C′(D)")
    if books.arm & books.gate_control:
        raise ArtefactRefusal(f"{formation}: A(D) and D₀(D) overlap")
    return books


def books_from_artefact(
    documents: Sequence[Mapping[str, Any]], *, years: Sequence[int] = YEARS, window_end: date = WINDOW_END
) -> tuple[FormationBooks, ...]:
    """Spec "Schedule assertions", checked from the artefact before any price is read. D is construction rule 1's
    last NYSE session of June."""
    books = tuple(formation_books(document) for document in documents)
    formations = [book.formation for book in books]
    if formations != sorted(set(formations)) or [d.year for d in formations] != list(years):
        raise ArtefactRefusal(f"the formations are not exactly one per year {years[0]}–{years[-1]}, increasing")
    for index, book in enumerate(books):
        if book.formation != last_session_of_month((book.formation.year, 6)):
            raise ArtefactRefusal(f"{book.formation}: the formation is not the last NYSE session of June (rule 1)")
        if us_market_status(book.x_date) == "closed" or book.x_date != _first_session_of_july(book.formation.year):
            raise ArtefactRefusal(f"{book.formation}: x_date {book.x_date} is not the first NYSE session of July")
        later = books[index + 1].formation if index + 1 < len(books) else window_end
        if not book.formation < book.x_date < later or book.x_date >= window_end:
            raise ArtefactRefusal(f"{book.formation}: x_date {book.x_date} is outside (D, next formation)")
    return books


def schedules(books: Sequence[FormationBooks]) -> dict[str, MonthlySchedule]:
    """The monthly schedules for A, C, C′, D₀ and the literal buy-and-hold of C(first formation)."""
    out: dict[str, MonthlySchedule] = {
        Portfolio.ARM: tuple((book.x_date, book.arm) for book in books),
        Portfolio.CONTROL: tuple((book.x_date, book.control) for book in books),
        Portfolio.COMPLETE_CASE: tuple((book.x_date, book.complete_case) for book in books),
        Portfolio.GATE_CONTROL: tuple((book.x_date, book.gate_control) for book in books),
    }
    out[BUY_HOLD] = ((books[0].x_date, books[0].control),)
    return out


def annual_schedule(schedule: MonthlySchedule, books: Sequence[FormationBooks]) -> Schedule:
    """The same targets keyed by formation, for ``simulate_portfolio`` (parity check 1 compares the sessions)."""
    formation_of = {book.x_date: book.formation for book in books}
    return tuple((datetime.combine(formation_of[x_date], time()), targets) for x_date, targets in schedule)


# --------------------------------------------------------------------------- C′ census (spec lines 78–84)


def complete_case_census(document: Mapping[str, Any]) -> dict[str, Any]:
    """Per formation: C′'s admitted cells with their formation-time weights, and P(D)'s exclusion reasons."""
    rows = document["rows"]
    population = len(rows)
    admitted = [row for row in rows if not exclusion_reasons(row)]

    def status(row: Mapping[str, Any], name: str) -> str:
        component = row["components"].get(name)
        return quality.NOT_EVALUATED if component is None else component["status"]

    cells = Counter(
        (row["fields"]["period"], status(row, "assets"), status(row, "equity"), row["fields"]["fpi"])
        for row in admitted
    )
    exclusive = Counter(reasons[0] for row in rows if (reasons := exclusion_reasons(row)))
    overlapping = Counter(reason for row in rows for reason in exclusion_reasons(row))
    return {
        "population": population,
        "complete_case": len(admitted),
        "cells": [
            {
                "period": period,
                "assets": assets,
                "equity": equity,
                "fpi": fpi,
                "count": count,
                "target_weight": count / len(admitted),
            }
            for (period, assets, equity, fpi), count in sorted(cells.items())
        ],
        "excluded": {
            reason: {
                "exclusive": exclusive[reason],
                "exclusive_share": exclusive[reason] / population,
                "overlapping": overlapping[reason],
                "overlapping_share": overlapping[reason] / population,
            }
            for reason in EXCLUSION_REASONS
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--input-sha256", required=True)
    args = parser.parse_args()
    _, documents = load_quality_input(args.input, expected_manifest_sha256=args.input_sha256)
    books = books_from_artefact(documents)
    print(
        json.dumps(
            {
                "stage": "census_time_assertions",
                "formations": [
                    {
                        "formation": book.formation.isoformat(),
                        "x_date": book.x_date.isoformat(),
                        "arm": len(book.arm),
                        "control": len(book.control),
                        "complete_case": len(book.complete_case),
                        "gate_control": len(book.gate_control),
                        "gate_boundary_ties": book.gate_boundary_ties,
                        "complete_case_census": complete_case_census(document),
                    }
                    for book, document in zip(books, documents, strict=True)
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
