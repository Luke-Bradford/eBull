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

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Final

import psycopg

from app.services.backtest_run import (
    AMBIGUITY_ARM_ORDER,
    BACKTEST_UNIVERSE,
    QUARANTINE_ARM_ORDER,
    ArmMeasurement,
    NamespaceMeasurement,
    ProgressCallback,
    corpus_version_for,
    evaluate_level_arms,
    load_corpus,
)
from app.services.cost_model import COST_MODEL_ID
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.position_builder import Window
from app.services.prereg_contract import changed_supersession_terms, declaration_refusals
from app.services.research_price_structure_store import QuarantineArm
from app.services.result_ledger import FrozenPreregistration, holdout_access_counts, load_preregistration
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_mt1_books import (
    MT1BookConstructionRefused,
    MT1FourArmBooks,
    build_mt1_four_arm_books,
)
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_mt1_preregistration import build_declarations
from app.services.strategy_mt1_trial import MT1TrialRefused, MT1TrialResult, evaluate_mt1_trial
from app.services.strategy_result import EVALUATION_WINDOW_START, HOLDOUT_BOUNDARY, AmbiguityArm
from app.services.strategy_result_universe import ResultUniverseRecord

MT1_SOURCE_STRATEGY_ID: Final = "s10-relative-strength-leader"
S8_SOURCE_STRATEGY_ID: Final = "s8-range-mean-reversion"
MT1_IN_SAMPLE_WINDOW: Final = Window(
    start=EVALUATION_WINDOW_START,
    end=HOLDOUT_BOUNDARY - timedelta(days=1),
)
_GIT_OBJECT_ID: Final = re.compile(r"^[0-9a-f]{40}$")

RobustnessKey = tuple[AmbiguityArm, QuarantineArm]
_EXPECTED_KEYS: Final[tuple[RobustnessKey, ...]] = tuple(
    (ambiguity, quarantine) for ambiguity in AMBIGUITY_ARM_ORDER for quarantine in QUARANTINE_ARM_ORDER
)


class MT1RunnerRefused(ValueError):
    """The supplied source pass is not the complete frozen MT-1 experiment."""


class _MT1BundleStructuralRefused(MT1RunnerRefused):
    def __init__(self, detail: str, axis_dates: tuple[date, ...], opportunity: ResultUniverseRecord) -> None:
        super().__init__(detail)
        self.detail = detail
        self.axis_dates = axis_dates
        self.opportunity = opportunity


@dataclass(frozen=True)
class MT1PreregistrationAuthority:
    strategy_id: str
    strategy_version: str
    declaration_id: int
    declaration_sha256: str


@dataclass(frozen=True)
class MT1PreparedCell:
    ambiguity_arm: AmbiguityArm
    quarantine_arm: QuarantineArm
    books: MT1FourArmBooks


@dataclass(frozen=True)
class MT1PreparedBundle:
    """Complete outcome-free structural fan, ready for one frozen evaluation."""

    cells: tuple[MT1PreparedCell, ...]
    axis_dates: tuple[date, ...]
    opportunity_record: ResultUniverseRecord


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


@dataclass(frozen=True)
class MT1InSampleEvaluation:
    authorities: tuple[MT1PreregistrationAuthority, MT1PreregistrationAuthority]
    bundle: MT1HistoricalBundle
    mt1_strategy_version: str
    s8_control_strategy_version: str
    mt1_source_strategy_version: str
    s8_source_strategy_version: str
    corpus_version: str
    runner_source_head: str


@dataclass(frozen=True)
class MT1InSamplePreparation:
    authorities: tuple[MT1PreregistrationAuthority, MT1PreregistrationAuthority]
    prepared: MT1PreparedBundle
    mt1_strategy_version: str
    s8_control_strategy_version: str
    mt1_source_strategy_version: str
    s8_source_strategy_version: str
    corpus_version: str
    runner_source_head: str


@dataclass(frozen=True)
class MT1InSampleStructuralRefusal:
    authorities: tuple[MT1PreregistrationAuthority, MT1PreregistrationAuthority]
    axis_dates: tuple[date, ...]
    opportunity_record: ResultUniverseRecord
    detail: str
    mt1_strategy_version: str
    s8_control_strategy_version: str
    mt1_source_strategy_version: str
    s8_source_strategy_version: str
    corpus_version: str
    runner_source_head: str


class MT1InSampleStructuralRefused(MT1RunnerRefused):
    def __init__(self, evidence: MT1InSampleStructuralRefusal) -> None:
        super().__init__(evidence.detail)
        self.evidence = evidence


def validate_mt1_preregistrations(
    conn: psycopg.Connection[Any],
) -> tuple[MT1PreregistrationAuthority, MT1PreregistrationAuthority]:
    """Require both current, intact, term-exact declarations before corpus I/O."""
    authorities: list[MT1PreregistrationAuthority] = []
    refusals: list[str] = []
    for expected in build_declarations():
        frozen: FrozenPreregistration | None = load_preregistration(
            conn,
            expected.strategy_id,
            expected.strategy_version,
        )
        if frozen is None:
            refusals.append(f"{expected.strategy_id}:preregistration_missing")
            continue
        if not frozen.digest_intact:
            refusals.append(f"{expected.strategy_id}:declaration_digest_mismatch")
        changed = changed_supersession_terms(frozen.declaration, expected)
        if changed:
            refusals.append(f"{expected.strategy_id}:declaration_terms_changed={','.join(changed)}")
        refusals.extend(f"{expected.strategy_id}:{code}" for code in declaration_refusals(frozen.declaration))
        exposure = holdout_access_counts(conn, expected.strategy_id, expected.strategy_version)
        if exposure.holdout_evaluations:
            refusals.append(f"{expected.strategy_id}:holdout_evaluations_already_exist={exposure.holdout_evaluations}")
        if exposure.recorded_accesses:
            refusals.append(f"{expected.strategy_id}:holdout_accesses_already_exist={exposure.recorded_accesses}")
        authorities.append(
            MT1PreregistrationAuthority(
                strategy_id=expected.strategy_id,
                strategy_version=expected.strategy_version,
                declaration_id=frozen.declaration_id,
                declaration_sha256=frozen.declaration_sha256,
            )
        )
    if refusals:
        raise MT1RunnerRefused("MT-1 preregistration authority refused: " + "; ".join(refusals))
    if len(authorities) != 2:  # pragma: no cover - missing declarations append refusals above
        raise MT1RunnerRefused("MT-1 preregistration authority is incomplete")
    return authorities[0], authorities[1]


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


def prepare_mt1_in_sample_bundle(
    *,
    mt1_source_measurements: Sequence[ArmMeasurement],
    s8_source_measurements: Sequence[ArmMeasurement],
) -> MT1PreparedBundle:
    """Construct the complete frozen fan without calculating a return statistic."""
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
    prepared: list[MT1PreparedCell] = []
    try:
        for key in _EXPECTED_KEYS:
            mt1_book = mt1[key].source_book
            s8_book = s8[key].source_book
            assert mt1_book is not None and s8_book is not None  # narrowed by _source_cells
            prepared.append(
                MT1PreparedCell(
                    ambiguity_arm=key[0],
                    quarantine_arm=key[1],
                    books=build_mt1_four_arm_books(
                        mt1_book=mt1_book,
                        s8_book=s8_book,
                        dates=axis_dates,
                        expected_first_month=first_month,
                    ),
                )
            )
    except (MT1BookConstructionRefused, MT1TrialRefused) as exc:
        raise _MT1BundleStructuralRefused(str(exc), axis_dates, opportunity) from exc
    return MT1PreparedBundle(cells=tuple(prepared), axis_dates=axis_dates, opportunity_record=opportunity)


def evaluate_mt1_prepared_bundle(prepared: MT1PreparedBundle) -> MT1HistoricalBundle:
    """Evaluate every cell of one already-complete structural fan."""
    if tuple((cell.ambiguity_arm, cell.quarantine_arm) for cell in prepared.cells) != _EXPECTED_KEYS:
        raise MT1RunnerRefused("prepared MT-1 robustness fan is incomplete or out of frozen order")

    cells: list[MT1RobustnessCell] = []
    for prepared_cell in prepared.cells:
        books = prepared_cell.books
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
                ambiguity_arm=prepared_cell.ambiguity_arm,
                quarantine_arm=prepared_cell.quarantine_arm,
                books=books,
                result=result,
            )
        )
    return MT1HistoricalBundle(
        cells=tuple(cells),
        axis_dates=prepared.axis_dates,
        opportunity_record=prepared.opportunity_record,
    )


def assemble_mt1_in_sample_bundle(
    *,
    mt1_source_measurements: Sequence[ArmMeasurement],
    s8_source_measurements: Sequence[ArmMeasurement],
) -> MT1HistoricalBundle:
    """Compatibility composition: complete all structural cells, then evaluate."""
    return evaluate_mt1_prepared_bundle(
        prepare_mt1_in_sample_bundle(
            mt1_source_measurements=mt1_source_measurements,
            s8_source_measurements=s8_source_measurements,
        )
    )


def prepare_mt1_in_sample_evaluation(
    conn: psycopg.Connection[Any],
    *,
    runner_source_head: str,
    progress: ProgressCallback | None = None,
) -> MT1InSamplePreparation:
    """Build the one full-population in-sample structural fan; never holdout."""
    if _GIT_OBJECT_ID.fullmatch(runner_source_head) is None:
        raise MT1RunnerRefused("MT-1 runner source head must be one exact lower-case Git object ID")
    # This is deliberately the first DB-facing call. Tests pin the ordering so
    # a future convenience corpus preload cannot burn the pre-outcome boundary.
    authorities = validate_mt1_preregistrations(conn)

    corpus = load_corpus(
        conn,
        universe_basis=BACKTEST_UNIVERSE,
        evaluation_window=MT1_IN_SAMPLE_WINDOW,
    )
    if corpus.universe_basis != "survivorship_free":  # pragma: no cover - fixed argument and loader contract
        raise MT1RunnerRefused(f"MT-1 corpus returned unexpected universe {corpus.universe_basis!r}")
    regime_provider = MarketRegimeProvider.load_research(conn)

    source_entries = {
        MT1_SOURCE_STRATEGY_ID: STRATEGY_MANIFEST[MT1_SOURCE_STRATEGY_ID],
        S8_SOURCE_STRATEGY_ID: STRATEGY_MANIFEST[S8_SOURCE_STRATEGY_ID],
    }
    source_identities = {
        strategy_id: entry.identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
        for strategy_id, entry in source_entries.items()
    }
    mt1_trial_identity = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8_trial_identity = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    if mt1_trial_identity.params.get("source_strategy_version") != source_identities[MT1_SOURCE_STRATEGY_ID].version:
        raise MT1RunnerRefused("MT-1 trial identity is not bound to the current S-10 source version")
    if (
        mt1_trial_identity.params.get("decision_clock_strategy_version")
        != source_identities[MT1_SOURCE_STRATEGY_ID].version
    ):
        raise MT1RunnerRefused("MT-1 trial identity is not bound to the current S-10 decision clock")
    if s8_trial_identity.params.get("source_strategy_version") != source_identities[S8_SOURCE_STRATEGY_ID].version:
        raise MT1RunnerRefused("S-8 control identity is not bound to the current S-8 source version")
    if (
        s8_trial_identity.params.get("decision_clock_strategy_version")
        != source_identities[MT1_SOURCE_STRATEGY_ID].version
    ):
        raise MT1RunnerRefused("S-8 control identity is not bound to the current S-10 decision clock")

    measured: dict[str, list[ArmMeasurement]] = {
        MT1_SOURCE_STRATEGY_ID: [],
        S8_SOURCE_STRATEGY_ID: [],
    }
    for strategy_id in (MT1_SOURCE_STRATEGY_ID, S8_SOURCE_STRATEGY_ID):
        for quarantine in QUARANTINE_ARM_ORDER:
            measured[strategy_id].extend(
                evaluate_level_arms(
                    conn,
                    source_entries[strategy_id],
                    corpus=corpus,
                    quarantine_arm=quarantine,
                    identity=source_identities[strategy_id],
                    namespaces=("in_sample",),
                    progress=progress,
                    regime_provider=regime_provider,
                )
            )

    try:
        prepared = prepare_mt1_in_sample_bundle(
            mt1_source_measurements=measured[MT1_SOURCE_STRATEGY_ID],
            s8_source_measurements=measured[S8_SOURCE_STRATEGY_ID],
        )
    except _MT1BundleStructuralRefused as exc:
        raise MT1InSampleStructuralRefused(
            MT1InSampleStructuralRefusal(
                authorities=authorities,
                axis_dates=exc.axis_dates,
                opportunity_record=exc.opportunity,
                detail=exc.detail,
                mt1_strategy_version=mt1_trial_identity.version,
                s8_control_strategy_version=s8_trial_identity.version,
                mt1_source_strategy_version=source_identities[MT1_SOURCE_STRATEGY_ID].version,
                s8_source_strategy_version=source_identities[S8_SOURCE_STRATEGY_ID].version,
                corpus_version=corpus_version_for(BACKTEST_UNIVERSE),
                runner_source_head=runner_source_head,
            )
        ) from exc
    return MT1InSamplePreparation(
        authorities=authorities,
        prepared=prepared,
        mt1_strategy_version=mt1_trial_identity.version,
        s8_control_strategy_version=s8_trial_identity.version,
        mt1_source_strategy_version=source_identities[MT1_SOURCE_STRATEGY_ID].version,
        s8_source_strategy_version=source_identities[S8_SOURCE_STRATEGY_ID].version,
        corpus_version=corpus_version_for(BACKTEST_UNIVERSE),
        runner_source_head=runner_source_head,
    )


def run_mt1_in_sample_evaluation(
    conn: psycopg.Connection[Any],
    *,
    runner_source_head: str,
    progress: ProgressCallback | None = None,
) -> MT1InSampleEvaluation:
    """Build and evaluate in memory; durable callers must use the two-phase store."""
    preparation = prepare_mt1_in_sample_evaluation(
        conn,
        runner_source_head=runner_source_head,
        progress=progress,
    )
    return MT1InSampleEvaluation(
        authorities=preparation.authorities,
        bundle=evaluate_mt1_prepared_bundle(preparation.prepared),
        mt1_strategy_version=preparation.mt1_strategy_version,
        s8_control_strategy_version=preparation.s8_control_strategy_version,
        mt1_source_strategy_version=preparation.mt1_source_strategy_version,
        s8_source_strategy_version=preparation.s8_source_strategy_version,
        corpus_version=preparation.corpus_version,
        runner_source_head=preparation.runner_source_head,
    )


__all__ = [
    "MT1_SOURCE_STRATEGY_ID",
    "MT1_IN_SAMPLE_WINDOW",
    "S8_SOURCE_STRATEGY_ID",
    "MT1HistoricalBundle",
    "MT1InSampleEvaluation",
    "MT1InSamplePreparation",
    "MT1InSampleStructuralRefusal",
    "MT1InSampleStructuralRefused",
    "MT1PreparedBundle",
    "MT1PreparedCell",
    "MT1PreregistrationAuthority",
    "MT1RobustnessCell",
    "MT1RunnerRefused",
    "assemble_mt1_in_sample_bundle",
    "evaluate_mt1_prepared_bundle",
    "prepare_mt1_in_sample_bundle",
    "prepare_mt1_in_sample_evaluation",
    "run_mt1_in_sample_evaluation",
    "validate_mt1_preregistrations",
]
