"""#2901 quality arm: the sealed runner.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md``.

**Stage 1 — the artefact's books and the census-time assertions** ("Portfolios", "Schedule assertions"). This stage
reads only the verified construction artefact (``scripts/build_2901_quality_input.py``): no price, no return. It
builds the four books per formation and refuses the run on any schema or schedule violation, which is a fact about
the construction, not an outcome:

- **A(D)**: ``in_arm`` rows; **C(D)**: all of E(D) (``rung = eligible``);
- **C′(D)**: every P(D) row with no exclusion reason (``exclusion_reasons``): executable, a recorded non-financial
  SIC, FPI not ``true``, and no RESOLVED ``Assets`` ≤ 0 or ``StockholdersEquity`` < 0 — the screens bind only where
  the artefact resolved them;
- **D₀(D)**: E(D) rows in decile 0 of rule 7's frozen formula ⌊10·r/n⌋ on the (GP/A, CIK) order, re-derived here
  and asserted equal to the artefact's ``decile`` and ``in_arm`` fields.

**The run** (part 2c-ii), in the spec's order. Every input is verified against the frozen declaration's pins first:
the artefact manifest, the clean mirror commit (also the artefact's own), the global-q archive, the implementation
sha256s, ``termination_identity`` and the power output. Then a holdout-access row is committed, and only then are the
evidence and prices read.

1. **Pre-gate**: A, C, C′ and D₀ under every programme policy at h ∈ {0, HALF_SPREAD}, each checked for parity with
   ``simulate_portfolio``. Any raise is ``REFUSED_PRE_GATE``. The literal buy-and-hold (parity) and SPY (analytic
   identity) are non-gating: a failure makes them ``unavailable``.
2. **Gate**: the gross A − D₀ spread against global-q under every policy. Anything but TRUE is ``GATE_FAIL`` and only
   the gate readouts are published.
3. **Post-gate**: the statistics, the conditions and the verdict. Any raise is ``FAIL(simulator_invariant)``.
4. **Descriptives**, each ``unavailable`` on its own failure.

A raise publishes only its stage and exception type; its traceback is sealed under ``--sealed-dir`` and published as
a sha256.

Usage::

    PYTHONPATH=. uv run python -m scripts.run_2901_quality_trial --census-only \\
        --input <artefact dir> --input-sha256 <manifest sha>
    PYTHONPATH=. uv run python -m scripts.run_2901_quality_trial --acknowledge-open-preregistered-outcomes \\
        --input <artefact dir> --input-sha256 <manifest sha> --price-mirror <mirror> --price-mirror-commit <sha> \\
        --global-q <zip> --global-q-sha256 <sha> --runner-sha256 <sha> --monthly-trial-sha256 <sha> \\
        --exclusion-trial-sha256 <sha> --termination-identity-sha256 <sha> --power-output-sha256 <sha> \\
        --history-rows <|H|> --sealed-dir <dir>
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import os
import statistics
import subprocess
import sys
import traceback
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from fractions import Fraction
from pathlib import Path
from typing import Any, Final

import psycopg

from app.config import settings
from app.services import r6_exclusion_trial, r6_monthly_trial
from app.services import r6_quality_universe as quality
from app.services.market_calendar import us_market_status
from app.services.pit_fundamentals import canonical_decimal
from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    PROGRAMME_POLICIES,
    WINDOW_END,
    ZERO_RECOVERY,
    PortfolioResult,
    PriceSeries,
    Schedule,
    SeriesEvidence,
    evidence_sha256,
    load_series_evidence,
    read_price_series,
    realisation_census,
    simulate_portfolio,
    simulate_under_policies,
    termination_identity,
)
from app.services.r6_monthly_trial import (
    STATISTIC_MONTHS,
    GateReadout,
    GateRefusal,
    Month,
    MonthlyResult,
    MonthlySchedule,
    PolicyStatistics,
    Portfolio,
    Refused,
    SimulationError,
    TotalReturns,
    Tri,
    Unavailable,
    Verdict,
    _close,
    check_parity,
    cohort_active_returns,
    compose_gate,
    decide_verdict,
    evaluate_conditions,
    family_size,
    hac_t,
    holding_year,
    identity_gate,
    last_session_of_month,
    read_global_q_gpa,
    simulate_monthly,
    stale_mark_census,
    summarise_policy,
    turnover_census,
)
from app.services.research_corpus_ingest import normalise_vendor_symbol
from app.services.result_ledger import HoldoutAccess, record_holdout_access
from scripts.build_2901_quality_input import YEARS, load_quality_input
from scripts.evaluate_2908_exclusion import _verify_mirror

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
        day += timedelta(days=1)
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


def census_output(books: Sequence[FormationBooks], documents: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Stage 1's construction facts per formation, plus the spec's nominal £/|A(D)| illustration (gates nothing)."""
    return [
        {
            "formation": book.formation.isoformat(),
            "x_date": book.x_date.isoformat(),
            "arm": len(book.arm),
            "control": len(book.control),
            "complete_case": len(book.complete_case),
            "gate_control": len(book.gate_control),
            "gate_boundary_ties": book.gate_boundary_ties,
            "nominal_gbp_per_arm_name": SLEEVE_MAXIMUM_GBP / len(book.arm),
            "complete_case_census": complete_case_census(document),
        }
        for book, document in zip(books, documents, strict=True)
    ]


# --------------------------------------------------------------------------- the run (part 2c-ii)

#: The #2829 register identity the frozen declaration uses. The holdout-access row carries it, so #2599's gate
#: inside ``record_holdout_access`` checks the run against that declaration.
STRATEGY_ID: Final = "r6-quality-gpa"
STRATEGY_VERSION: Final = "r6-2901-quality-v1"
SPY: Final = "SPY"
HALF_SPREADS: Final = (0.0, HALF_SPREAD)
GATING_BOOKS: Final = (Portfolio.ARM, Portfolio.CONTROL, Portfolio.COMPLETE_CASE, Portfolio.GATE_CONTROL)
#: The books the statistics read (D₀ is used only by the gate).
STATISTIC_BOOKS: Final = (Portfolio.ARM, Portfolio.CONTROL, Portfolio.COMPLETE_CASE)
#: Spec "Minimum notional" and "Capital boundary": the sleeve maximum, and the fee on each of the two conversions.
SLEEVE_MAXIMUM_GBP: Final = 50_000
FX_FEE: Final = 0.007
#: Spec "Descriptive readouts": the halves, split on formation year, complete cohorts only.
FIRST_HALF: Final = range(2013, 2018)
SECOND_HALF: Final = range(2018, 2024)
#: Spec "Descriptive readouts": the SPY regime at D looks back this many calendar days.
REGIME_LOOKBACK_DAYS: Final = 365

#: (policy label, h) → book → path.
Paths = dict[tuple[str, float], dict[str, MonthlyResult]]
#: stage, exception → the sealed traceback's sha256.
Sealer = Callable[[str, BaseException], str]


def check_programme_policies() -> None:
    """Spec "Policies": each programme policy has a status split, so ``_holding_value``'s legacy branch never runs."""
    if not all(policy.needs_evidence for policy in PROGRAMME_POLICIES):
        raise SimulationError("a programme policy has no terminal_fractions")


def simulate_books(
    schedule_set: Mapping[str, MonthlySchedule],
    books: Sequence[FormationBooks],
    prices: Mapping[str, PriceSeries],
    evidence: Mapping[str, SeriesEvidence],
    *,
    window_end: date = WINDOW_END,
) -> Paths:
    """Pre-gate: A, C, C′ and D₀ under every programme policy at h ∈ {0, HALF_SPREAD}, each checked for parity.

    The annual side runs through ``simulate_under_policies``, whose validation is the spec's run-time check: unique
    labels, ``ZERO_RECOVERY`` present, evidence for every priced symbol and loaded bars equal to the stored bounds.
    Any raise refuses the run.
    """
    check_programme_policies()
    annual_schedules: dict[str, Schedule] = {name: annual_schedule(schedule_set[name], books) for name in GATING_BOOKS}
    paths: Paths = {}
    for h in HALF_SPREADS:
        annual = simulate_under_policies(
            schedules=annual_schedules,
            prices=dict(prices),
            policies=PROGRAMME_POLICIES,
            half_spread=h,
            window_end=window_end,
            evidence=evidence,
        )
        for policy in PROGRAMME_POLICIES:
            books_by_name: dict[str, MonthlyResult] = {}
            for name in GATING_BOOKS:
                monthly = simulate_monthly(
                    schedule=schedule_set[name],
                    prices=prices,
                    policy=policy,
                    half_spread=h,
                    evidence=evidence,
                    window_end=window_end,
                )
                check_parity(monthly, annual[policy.label][name], window_end=window_end)
                books_by_name[name] = monthly
            paths[(policy.label, h)] = books_by_name
    return paths


def simulate_buy_hold(
    schedule: MonthlySchedule,
    books: Sequence[FormationBooks],
    prices: Mapping[str, PriceSeries],
    evidence: Mapping[str, SeriesEvidence],
    *,
    window_end: date = WINDOW_END,
) -> Paths | Unavailable:
    """The literal buy-and-hold of C(first formation), with parity. Non-gating: any failure is ``unavailable``."""
    try:
        annual = annual_schedule(schedule, books)
        paths: Paths = {}
        for h in HALF_SPREADS:
            for policy in PROGRAMME_POLICIES:
                monthly = simulate_monthly(
                    schedule=schedule,
                    prices=prices,
                    policy=policy,
                    half_spread=h,
                    evidence=evidence,
                    window_end=window_end,
                )
                frozen = simulate_portfolio(
                    schedule=annual,
                    prices=dict(prices),
                    policy=policy,
                    half_spread=h,
                    window_end=window_end,
                    evidence=evidence,
                )
                check_parity(monthly, frozen, window_end=window_end)
                paths[(policy.label, h)] = {BUY_HOLD: monthly}
        return paths
    except Exception as exc:
        return Unavailable(type(exc).__name__)


def spy_multiples(
    series: PriceSeries | Unavailable,
    evidence: Mapping[str, SeriesEvidence],
    x_date: date,
    *,
    window_end: date = WINDOW_END,
) -> dict[float, float] | Unavailable:
    """SPY bought at the X(first formation) open and sold at the window-end close: the terminal multiple per h,
    checked against its analytic identity G(1 − h)/(1 + h). Non-gating: any failure is ``unavailable``."""
    if isinstance(series, Unavailable):
        return series
    bars = series.by_date
    entry, exit_bar = bars.get(x_date), bars.get(window_end)
    if entry is None or exit_bar is None:
        return Unavailable("SPY has no bar on the first x_date or on the window end")
    stored = evidence.get(SPY)
    if stored is None or (series.bars[0].day, series.bars[-1].day) != (stored.first_bar, stored.last_bar):
        return Unavailable("SPY has no stored series, or its loaded bars differ from the stored bounds")
    growth = exit_bar.adjusted_close / entry.adjusted_open
    out: dict[float, float] = {}
    for h in HALF_SPREADS:
        try:
            monthly = simulate_monthly(
                schedule=((x_date, frozenset({SPY})),),
                prices={SPY: series},
                policy=ZERO_RECOVERY,
                half_spread=h,
                evidence=evidence,
                window_end=window_end,
            )
        except Exception as exc:
            return Unavailable(type(exc).__name__)
        if not _close(monthly.terminal_wealth, growth * (1.0 - h) / (1.0 + h)):
            return Unavailable("SPY's terminal multiple differs from its analytic identity")
        out[h] = monthly.terminal_wealth
    return out


def gate_readouts(
    paths: Paths, reference: Mapping[Month, float] | Refused, months: Sequence[Month] = STATISTIC_MONTHS
) -> dict[str, GateReadout | Refused]:
    """Per policy: the gross (h = 0) spread g(A) − g(D₀) against the reference, refused on a gross ruin of either."""
    out: dict[str, GateReadout | Refused] = {}
    for policy in PROGRAMME_POLICIES:
        gross = paths[(policy.label, 0.0)]
        arm, gate_control = gross[Portfolio.ARM], gross[Portfolio.GATE_CONTROL]
        if isinstance(reference, Refused):
            out[policy.label] = reference
        elif arm.factors.keys() != gate_control.factors.keys():
            out[policy.label] = Refused("A and D₀ carry different mark months")
        else:
            out[policy.label] = identity_gate(
                {month: arm.factors[month] - gate_control.factors[month] for month in arm.factors},
                reference,
                ruined=arm.ruined_within(months) or gate_control.ruined_within(months),
                months=months,
            )
    return out


def policy_statistics(paths: Paths, months: Sequence[Month] = STATISTIC_MONTHS) -> dict[str, PolicyStatistics]:
    """Per policy: net factors and ruin at h = HALF_SPREAD, totals at h = 0 and HALF_SPREAD."""
    out: dict[str, PolicyStatistics] = {}
    for policy in PROGRAMME_POLICIES:
        gross, net = paths[(policy.label, 0.0)], paths[(policy.label, HALF_SPREAD)]
        totals = TotalReturns(
            arm_gross=gross[Portfolio.ARM].total_return,
            arm_net=net[Portfolio.ARM].total_return,
            control_gross=gross[Portfolio.CONTROL].total_return,
            control_net=net[Portfolio.CONTROL].total_return,
            complete_case_gross=gross[Portfolio.COMPLETE_CASE].total_return,
            complete_case_net=net[Portfolio.COMPLETE_CASE].total_return,
        )
        out[policy.label] = summarise_policy(
            arm_factors=net[Portfolio.ARM].factors,
            control_factors=net[Portfolio.CONTROL].factors,
            complete_case_factors=net[Portfolio.COMPLETE_CASE].factors,
            totals=totals,
            ruined=frozenset(name for name in STATISTIC_BOOKS if net[name].ruined_within(months)),
            months=months,
        )
    return out


# --------------------------------------------------------------------------- descriptives (never gate)


def _describe(compute: Callable[[], Any]) -> Any:
    """One descriptive readout, or ``unavailable`` with the exception type."""
    try:
        return compute()
    except Exception as exc:
        return Unavailable(type(exc).__name__)


def cohort_readout(
    arm: MonthlyResult, control: MonthlyResult, months: Sequence[Month] = STATISTIC_MONTHS
) -> dict[str, Any]:
    """The complete holding-year cohorts' compounded active returns, the partial last cohort (its months plus the
    partial final month) and the two halves' means, split on formation year."""
    complete = cohort_active_returns(arm.factors, control.factors, months)
    last_year = holding_year(months[-1])
    partial_months = [month for month in months if holding_year(month) == last_year]
    partial: float | Unavailable = Unavailable("the last cohort is complete")
    if len(partial_months) < 12:
        partial = (
            math.prod(arm.factors[m] for m in partial_months) * arm.partial_factor
            - math.prod(control.factors[m] for m in partial_months) * control.partial_factor
        )

    def half(years: range) -> float | Unavailable:
        values = [value for year, value in complete.items() if year in years]
        return statistics.fmean(values) if values else Unavailable("no complete cohort in the half")

    return {
        "complete": complete,
        "partial": {"formation": last_year, "active_return": partial},
        "halves": {
            f"{FIRST_HALF[0]}-{FIRST_HALF[-1]}": half(FIRST_HALF),
            f"{SECOND_HALF[0]}-{SECOND_HALF[-1]}": half(SECOND_HALF),
        },
    }


def _last_session_on_or_before(day: date) -> date:
    while us_market_status(day) == "closed":
        day -= timedelta(days=1)
    return day


def spy_regime(series: PriceSeries | Unavailable, formations: Iterable[date]) -> dict[date, str]:
    """The sign of SPY's adjusted-close return from the last NYSE session on or before D − 365 days to the last
    session on or before D; ``unavailable`` if SPY has no bar on either session (an older bar never substitutes)."""
    bars = {} if isinstance(series, Unavailable) else series.by_date
    out: dict[date, str] = {}
    for formation in formations:
        start = bars.get(_last_session_on_or_before(formation - timedelta(days=REGIME_LOOKBACK_DAYS)))
        end = bars.get(_last_session_on_or_before(formation))
        if start is None or end is None:
            out[formation] = "unavailable"
            continue
        change = end.adjusted_close - start.adjusted_close
        out[formation] = "positive" if change > 0 else "negative" if change < 0 else "zero"
    return out


def appended_hac(arm: MonthlyResult, control: MonthlyResult, months: Sequence[Month] = STATISTIC_MONTHS) -> Any:
    """The headline HAC t with the partial September appended as a 135th observation. Descriptive only: a
    September-only ruin makes it ``unavailable``, and a ruin inside the months refuses it as the headline does."""
    if arm.ruined_in_partial or control.ruined_in_partial:
        return Unavailable("a ruin in the partial final month")
    active = [arm.factors[m] - control.factors[m] for m in months] + [arm.partial_factor - control.partial_factor]
    return hac_t(active, ruined=arm.ruined_within(months) or control.ruined_within(months))


def path_readout(
    result: MonthlyResult,
    prices: Mapping[str, PriceSeries],
    evidence: Mapping[str, SeriesEvidence],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> dict[str, Any]:
    """Spec "Result per (portfolio, policy, h)": the monthly series, the partial and total returns, the events, the
    realisations and every census. The per-cell valuations are summarised by the stale-mark census, not listed."""
    return {
        "factors": result.factors,
        "partial_return": result.partial_return,
        "total_return": result.total_return,
        "ruin_month": result.ruin_month,
        "ruined_in_partial": result.ruined_in_partial,
        "events": result.events,
        "realisations": result.realisations,
        "realisation_census": _describe(
            lambda: realisation_census(PortfolioResult(result.total_return, result.events, result.realisations))
        ),
        "turnover": _describe(lambda: turnover_census(result, months)),
        "stale_marks": _describe(lambda: stale_mark_census(result, prices=prices, evidence=evidence)),
    }


def spy_readout(spy: dict[float, float] | Unavailable) -> dict[str, float] | Unavailable:
    """Gross multiple G, net multiple G(1 − h)/(1 + h) and the exact spread drag 2hG/(1 + h)."""
    if isinstance(spy, Unavailable):
        return spy
    gross = spy[0.0]
    return {
        "gross_multiple": gross,
        "net_multiple": spy[HALF_SPREAD],
        "spread_drag": 2.0 * HALF_SPREAD * gross / (1.0 + HALF_SPREAD),
    }


def descriptives(
    paths: Paths,
    buy_hold: Paths | Unavailable,
    spy: dict[float, float] | Unavailable,
    spy_series: PriceSeries | Unavailable,
    books: Sequence[FormationBooks],
    prices: Mapping[str, PriceSeries],
    evidence: Mapping[str, SeriesEvidence],
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> dict[str, Any]:
    """Spec "Descriptive readouts". Published only after the gate passed; each is ``unavailable`` on its own
    failure. Cohorts and the appended HAC are on the net (h = HALF_SPREAD) series, as the headline is."""
    by_policy: dict[str, Any] = {}
    for policy in PROGRAMME_POLICIES:
        net = paths[(policy.label, HALF_SPREAD)]
        arm, control = net[Portfolio.ARM], net[Portfolio.CONTROL]
        by_policy[policy.label] = {
            "cohorts": _describe(lambda arm=arm, control=control: cohort_readout(arm, control, months)),
            "hac_with_partial_month": _describe(lambda arm=arm, control=control: appended_hac(arm, control, months)),
            "arm_gbp_net_multiple": _describe(lambda arm=arm: arm.terminal_wealth * (1.0 - FX_FEE) ** 2),
        }
    path_readouts: dict[str, Any] = {}
    for (label, h), results in paths.items():
        extra = {} if isinstance(buy_hold, Unavailable) else buy_hold[(label, h)]
        path_readouts[f"{label}:{h}"] = {
            name: path_readout(result, prices, evidence, months) for name, result in {**results, **extra}.items()
        }
    return {
        "by_policy": by_policy,
        "paths": path_readouts,
        #: Listed under ``paths`` as ``buy_hold`` when available.
        "buy_hold_unavailable": buy_hold if isinstance(buy_hold, Unavailable) else None,
        "spy": spy_readout(spy),
        "spy_regime": spy_regime(spy_series, (book.formation for book in books)),
    }


# --------------------------------------------------------------------------- orchestration


def _refusal(verdict: Verdict, stage: str, error: BaseException, seal: Sealer) -> dict[str, Any]:
    """What a raise publishes: its stage and exception type. The traceback may carry a figure, so it is sealed."""
    return {
        "verdict": verdict,
        "stage": stage,
        "exception_type": type(error).__name__,
        "sealed_traceback_sha256": seal(stage, error),
    }


def run_trial(
    *,
    books: Sequence[FormationBooks],
    prices: Mapping[str, PriceSeries],
    evidence: Mapping[str, SeriesEvidence],
    spy_series: PriceSeries | Unavailable,
    read_reference: Callable[[], Mapping[Month, float]],
    family: int,
    seal: Sealer,
    window_end: date = WINDOW_END,
    months: Sequence[Month] = STATISTIC_MONTHS,
) -> dict[str, Any]:
    """The spec's stages in order. Nothing about an arm is returned before the gate passes."""
    schedule_set = schedules(books)
    try:
        paths = simulate_books(schedule_set, books, prices, evidence, window_end=window_end)
    except Exception as exc:
        return _refusal(Verdict.REFUSED_PRE_GATE, "pre_gate", exc, seal)
    buy_hold = simulate_buy_hold(schedule_set[BUY_HOLD], books, prices, evidence, window_end=window_end)
    spy = spy_multiples(spy_series, evidence, books[0].x_date, window_end=window_end)

    try:
        reference: Mapping[Month, float] | Refused
        try:
            reference = read_reference()
        except GateRefusal as exc:
            reference = Refused(f"the reference could not be read: {exc}")
        readouts = gate_readouts(paths, reference, months)
        gate = compose_gate(readouts)
    except Exception as exc:
        return _refusal(Verdict.GATE_FAIL, "gate", exc, seal)
    gate_output = {"state": gate, "by_policy": readouts}
    if gate is not Tri.TRUE:
        verdict = decide_verdict(None, refused_pre_gate=False, gate=gate, simulator_failed=False)
        return {"verdict": verdict.verdict, "stage": "gate", "gate": gate_output}

    try:
        by_policy = policy_statistics(paths, months)
        verdict = decide_verdict(
            evaluate_conditions(by_policy, family), refused_pre_gate=False, gate=gate, simulator_failed=False
        )
    except Exception as exc:
        return _refusal(Verdict.SIMULATOR_INVARIANT, "post_gate", exc, seal)
    return {
        "verdict": verdict.verdict,
        "stage": "post_gate",
        "gate": gate_output,
        "family_size": family,
        "statistics": by_policy,
        "conditions": verdict.conditions,
        "failed_conditions": verdict.failed,
        "refused_conditions": verdict.refused,
        "descriptives": _describe(
            lambda: descriptives(paths, buy_hold, spy, spy_series, books, prices, evidence, months)
        ),
    }


def jsonable(value: Any) -> Any:
    """Dataclasses, enums, dates, tuple-keyed mappings and non-finite floats → JSON values."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    if isinstance(value, Mapping):
        return {_json_key(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, set | frozenset):
        return [jsonable(item) for item in sorted(value)]
    if isinstance(value, list | tuple):
        return [jsonable(item) for item in value]
    return value


def _json_key(key: Any) -> str:
    if isinstance(key, tuple):
        return ":".join(_json_key(part) for part in key)
    if isinstance(key, Enum):
        return str(key.value)
    if isinstance(key, date):
        return key.isoformat()
    return str(key)


# --------------------------------------------------------------------------- I/O


def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def sealer(root: Path) -> Sealer:
    """Writes each traceback to ``root/<stage>-<sha256>.txt`` (mode 0600, never overwritten); returns the sha256."""

    def seal(stage: str, error: BaseException) -> str:
        body = "".join(traceback.format_exception(error)).encode()
        digest = hashlib.sha256(body).hexdigest()
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{stage}-{digest}.txt"
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            return digest  # the same traceback, already sealed
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
        return digest

    return seal


def load_prices(day_dir: Path, symbols: Iterable[str]) -> dict[str, PriceSeries]:
    """One mirror file per artefact vendor symbol, matched as the artefact builder matched it."""
    files: dict[str, list[Path]] = defaultdict(list)
    for path in day_dir.glob("*.csv"):
        files[normalise_vendor_symbol(path.stem)].append(path)
    out: dict[str, PriceSeries] = {}
    for symbol in sorted(set(symbols)):
        paths = files.get(symbol, [])
        if len(paths) != 1:
            raise RuntimeError(f"{symbol}: {len(paths)} mirror files")
        out[symbol] = read_price_series(paths[0])
    return out


def book_symbols(books: Sequence[FormationBooks]) -> frozenset[str]:
    symbols = frozenset().union(*(book.complete_case | book.control for book in books))
    if any(symbol != symbol.strip().upper() for symbol in symbols):
        raise ArtefactRefusal("a vendor symbol is not in load_series_evidence's upper-case spelling")
    return symbols


def _power_output(zip_path: Path, zip_sha256: str) -> str:
    """Re-executes the power statement; the declaration pins its stdout's sha256."""
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.measure_2901_power", "--zip", str(zip_path), "--sha256", zip_sha256],
        check=True,
        capture_output=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    return hashlib.sha256(completed.stdout).hexdigest()


def _pin(name: str, actual: str, expected: str) -> None:
    if actual != expected:
        raise RuntimeError(f"{name} moved: {actual} != the declared {expected}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--census-only", action="store_true", help="stage 1 only: no price, no return")
    parser.add_argument("--acknowledge-open-preregistered-outcomes", action="store_true")
    parser.add_argument("--price-mirror", type=Path)
    parser.add_argument("--price-mirror-commit")
    parser.add_argument("--global-q", type=Path)
    parser.add_argument("--global-q-sha256")
    parser.add_argument("--runner-sha256")
    parser.add_argument("--monthly-trial-sha256")
    parser.add_argument("--exclusion-trial-sha256")
    parser.add_argument("--termination-identity-sha256")
    parser.add_argument("--power-output-sha256")
    parser.add_argument("--history-rows", type=int)
    parser.add_argument("--sealed-dir", type=Path)
    return parser


RUN_ARGUMENTS: Final = (
    "price_mirror",
    "price_mirror_commit",
    "global_q",
    "global_q_sha256",
    "runner_sha256",
    "monthly_trial_sha256",
    "exclusion_trial_sha256",
    "termination_identity_sha256",
    "power_output_sha256",
    "history_rows",
    "sealed_dir",
)


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    manifest, documents = load_quality_input(args.input, expected_manifest_sha256=args.input_sha256)
    books = books_from_artefact(documents)
    census = census_output(books, documents)
    if args.census_only:
        print(json.dumps({"stage": "census_time_assertions", "formations": census}, indent=2, sort_keys=True))
        return 0
    if not args.acknowledge_open_preregistered_outcomes:
        parser.error("--acknowledge-open-preregistered-outcomes (or --census-only) is required")
    missing = [f"--{name.replace('_', '-')}" for name in RUN_ARGUMENTS if getattr(args, name) is None]
    if missing:
        parser.error(f"the run needs {', '.join(missing)}")

    # Every pin before the holdout row: a mismatch here exposes nothing.
    _pin("the artefact's mirror commit", manifest["input_sha256"]["mirror_commit"], args.price_mirror_commit)
    _verify_mirror(args.price_mirror, args.price_mirror_commit)
    _pin("the global-q archive", sha256_file(args.global_q), args.global_q_sha256)
    _pin("the runner", sha256_file(Path(__file__)), args.runner_sha256)
    _pin("r6_monthly_trial.py", sha256_file(Path(r6_monthly_trial.__file__)), args.monthly_trial_sha256)
    _pin("r6_exclusion_trial.py", sha256_file(Path(r6_exclusion_trial.__file__)), args.exclusion_trial_sha256)
    identity = termination_identity(PROGRAMME_POLICIES, None)
    _pin("termination_identity", canonical_sha256(identity), args.termination_identity_sha256)
    _pin("the power output", _power_output(args.global_q, args.global_q_sha256), args.power_output_sha256)
    family = family_size(args.history_rows)
    symbols = book_symbols(books)
    seal = sealer(args.sealed_dir)

    with psycopg.connect(settings.database_url) as conn:
        access_id = record_holdout_access(
            conn,
            HoldoutAccess(
                strategy_id=STRATEGY_ID,
                strategy_version=STRATEGY_VERSION,
                result_version=None,
                access_kind="read",
                accessed_by="scripts/run_2901_quality_trial.py",
                purpose="open every outcome in preregistered quality arm #2901",
            ),
        )
        conn.commit()
        output: dict[str, Any] = {
            "holdout_access_id": access_id,
            "identity": {
                "input_manifest_sha256": args.input_sha256,
                "price_mirror_commit": args.price_mirror_commit,
                "global_q_sha256": args.global_q_sha256,
                "runner_sha256": args.runner_sha256,
                "monthly_trial_sha256": args.monthly_trial_sha256,
                "exclusion_trial_sha256": args.exclusion_trial_sha256,
                "termination_identity_sha256": args.termination_identity_sha256,
                "power_output_sha256": args.power_output_sha256,
                "history_rows": args.history_rows,
            },
            "formations": census,
        }
        try:
            evidence = load_series_evidence(conn, symbols=symbols)
            day_dir = args.price_mirror / "Data" / "Day"
            prices = load_prices(day_dir, symbols)
        except Exception as exc:
            output["result"] = _refusal(Verdict.REFUSED_PRE_GATE, "load", exc, seal)
            print(json.dumps(jsonable(output), indent=2, sort_keys=True))
            return 0
        output["identity"]["evidence_sha256"] = evidence_sha256(evidence)
        spy_series: PriceSeries | Unavailable
        try:
            evidence = {**evidence, **load_series_evidence(conn, symbols={SPY})}
            spy_series = load_prices(day_dir, {SPY})[SPY]
        except Exception as exc:
            spy_series = Unavailable(type(exc).__name__)

    output["result"] = run_trial(
        books=books,
        prices=prices,
        evidence=evidence,
        spy_series=spy_series,
        read_reference=lambda: read_global_q_gpa(args.global_q),
        family=family,
        seal=seal,
    )
    print(json.dumps(jsonable(output), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
