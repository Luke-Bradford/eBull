"""Paved assembly boundary for the MT-1 in-sample controlled trial (#2769).

The generic backtester remains the only owner of causal signal, fill,
termination, ambiguity, quarantine and cost construction.  This module accepts
only the complete in-memory source measurements from that engine and composes
the frozen four-arm MT-1 statistic.

The ambiguity/quarantine fan is conjunctive under the trial register.  All
four cells must construct and pass their outcome-free structural gates before
the first cell is handed to the return evaluator; a favourable cell can never
be selected from an incomplete fan.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from app.services.backtest_run import (
    AMBIGUITY_ARM_ORDER,
    QUARANTINE_ARM_ORDER,
    ArmMeasurement,
    NamespaceMeasurement,
)
from app.services.research_price_structure_store import QuarantineArm
from app.services.strategy_mt1_books import MT1FourArmBooks, build_mt1_four_arm_books
from app.services.strategy_mt1_trial import MT1TrialResult, evaluate_mt1_trial
from app.services.strategy_result import AmbiguityArm
from app.services.strategy_result_universe import ResultUniverseRecord

MT1_SOURCE_STRATEGY_ID: Final = "s10-relative-strength-leader"
S8_SOURCE_STRATEGY_ID: Final = "s8-range-mean-reversion"

RobustnessKey = tuple[AmbiguityArm, QuarantineArm]
_EXPECTED_KEYS: Final[tuple[RobustnessKey, ...]] = tuple(
    (ambiguity, quarantine) for ambiguity in AMBIGUITY_ARM_ORDER for quarantine in QUARANTINE_ARM_ORDER
)


class MT1RunnerRefused(ValueError):
    """The supplied source pass is not the complete frozen MT-1 experiment."""


@dataclass(frozen=True)
class MT1RobustnessCell:
    ambiguity_arm: AmbiguityArm
    quarantine_arm: QuarantineArm
    books: MT1FourArmBooks
    result: MT1TrialResult


@dataclass(frozen=True)
class MT1HistoricalBundle:
    """All conjunctive cells from one source-derived in-sample invocation."""

    cells: tuple[MT1RobustnessCell, ...]
    axis_dates: tuple[date, ...]
    opportunity_record: ResultUniverseRecord

    @property
    def historical_statistical_conjuncts_pass(self) -> bool:
        return all(cell.result.historical_statistical_conjuncts_pass for cell in self.cells)


def _source_cells(
    measurements: Sequence[ArmMeasurement],
    *,
    expected_strategy_id: str,
) -> dict[RobustnessKey, NamespaceMeasurement]:
    cells: dict[RobustnessKey, NamespaceMeasurement] = {}
    for measurement in measurements:
        if measurement.strategy_id != expected_strategy_id:
            raise MT1RunnerRefused(
                f"expected only {expected_strategy_id!r} source measurements; received {measurement.strategy_id!r}"
            )
        if measurement.ambiguity_arm is None:
            raise MT1RunnerRefused(
                f"{expected_strategy_id}: a survivorship-free source measurement must carry an ambiguity arm"
            )
        key = (measurement.ambiguity_arm, measurement.quarantine_arm)
        if key in cells:
            raise MT1RunnerRefused(f"{expected_strategy_id}: duplicate robustness cell {key}")
        if set(measurement.namespaces) != {"in_sample"}:
            raise MT1RunnerRefused(
                f"{expected_strategy_id}/{key}: MT-1 accepts exactly the in_sample namespace; "
                f"received {sorted(measurement.namespaces)}"
            )
        namespace = measurement.namespaces["in_sample"]
        if namespace.source_book is None:
            raise MT1RunnerRefused(f"{expected_strategy_id}/{key}: causal source book is missing")
        cells[key] = namespace
    expected = set(_EXPECTED_KEYS)
    if set(cells) != expected:
        raise MT1RunnerRefused(
            f"{expected_strategy_id}: robustness fan is incomplete; "
            f"missing={sorted(expected - set(cells))} unexpected={sorted(set(cells) - expected)}"
        )
    return cells


def assemble_mt1_in_sample_bundle(
    *,
    mt1_source_measurements: Sequence[ArmMeasurement],
    s8_source_measurements: Sequence[ArmMeasurement],
) -> MT1HistoricalBundle:
    """Construct and evaluate the complete frozen fan, structural gates first."""
    mt1 = _source_cells(mt1_source_measurements, expected_strategy_id=MT1_SOURCE_STRATEGY_ID)
    s8 = _source_cells(s8_source_measurements, expected_strategy_id=S8_SOURCE_STRATEGY_ID)

    namespaces = tuple((*mt1.values(), *s8.values()))
    axes = {namespace.axis_dates for namespace in namespaces}
    if len(axes) != 1:
        raise MT1RunnerRefused("MT-1 and S-8 robustness cells do not share one exact in-sample metric axis")
    axis_dates = next(iter(axes))
    if not axis_dates:
        raise MT1RunnerRefused("the shared in-sample metric axis is empty")
    opportunities = {namespace.universe_record for namespace in namespaces}
    if len(opportunities) != 1:
        raise MT1RunnerRefused("MT-1 and S-8 robustness cells do not share one pre-mask opportunity population")
    opportunity = next(iter(opportunities))
    first_month = date(axis_dates[0].year, axis_dates[0].month, 1)

    # Phase 1 is intentionally complete before phase 2 begins.  The book
    # constructor applies the outcome-free structural clock/exposure/turnover
    # gate.  If cell four refuses, no return evaluator has seen cells one-three.
    prepared: list[tuple[RobustnessKey, MT1FourArmBooks]] = []
    for key in _EXPECTED_KEYS:
        mt1_book = mt1[key].source_book
        s8_book = s8[key].source_book
        assert mt1_book is not None and s8_book is not None  # narrowed by _source_cells
        prepared.append(
            (
                key,
                build_mt1_four_arm_books(
                    mt1_book=mt1_book,
                    s8_book=s8_book,
                    dates=axis_dates,
                    expected_first_month=first_month,
                ),
            )
        )

    cells: list[MT1RobustnessCell] = []
    for (ambiguity, quarantine), books in prepared:
        result = evaluate_mt1_trial(
            mt1_scaled=books.mt1.scaled,
            mt1_unscaled=books.mt1.unscaled,
            s8_scaled=books.s8.scaled,
            s8_unscaled=books.s8.unscaled,
            mt1_structural=books.mt1.structural,
            s8_structural=books.s8.structural,
        )
        cells.append(
            MT1RobustnessCell(
                ambiguity_arm=ambiguity,
                quarantine_arm=quarantine,
                books=books,
                result=result,
            )
        )
    return MT1HistoricalBundle(cells=tuple(cells), axis_dates=axis_dates, opportunity_record=opportunity)


__all__ = [
    "MT1_SOURCE_STRATEGY_ID",
    "S8_SOURCE_STRATEGY_ID",
    "MT1HistoricalBundle",
    "MT1RobustnessCell",
    "MT1RunnerRefused",
    "assemble_mt1_in_sample_bundle",
]
