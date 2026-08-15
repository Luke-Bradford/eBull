"""#2769 complete-fan and structural-first MT-1 assembly tests."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import cast

import pytest

import app.services.strategy_mt1_runner as runner
from app.services.backtest_run import ArmMeasurement, NamespaceMeasurement
from app.services.equity_curve import LegBook
from app.services.strategy_result import AmbiguityArm
from app.services.strategy_result_universe import ResultUniverseRecord
from app.services.strategy_statistics import StrategyMetrics

_AXIS = (date(2000, 1, 3), date(2000, 1, 4))
_OPPORTUNITY = ResultUniverseRecord(
    universe_rule_version="test-universe-v1",
    evaluated_instrument_ids=frozenset({1}),
    validated_universe_ids=frozenset({1}),
)
_KEYS = (
    ("best_case", "admitted"),
    ("best_case", "masked"),
    ("worst_case", "admitted"),
    ("worst_case", "masked"),
)


def _measurement(
    strategy_id: str,
    ambiguity: str,
    quarantine: str,
    *,
    axis: tuple[date, ...] = _AXIS,
    opportunity: ResultUniverseRecord = _OPPORTUNITY,
) -> ArmMeasurement:
    namespace = NamespaceMeasurement(
        namespace="in_sample",
        metrics=cast(StrategyMetrics, object()),
        moments=None,
        daily_returns={},
        universe_record=opportunity,
        position_count=0,
        axis_dates=axis,
        source_book=LegBook(),
    )
    return ArmMeasurement(
        strategy_id=strategy_id,
        strategy_version="source-v1",
        ambiguity_arm=cast(AmbiguityArm, ambiguity),
        quarantine_arm=quarantine,  # type: ignore[arg-type]
        namespaces={"in_sample": namespace},
        holdout_positions_discarded=0,
        close_sources={},
        series_evaluated=1,
        elapsed_s=0.0,
    )


def _fan(strategy_id: str) -> tuple[ArmMeasurement, ...]:
    return tuple(_measurement(strategy_id, ambiguity, quarantine) for ambiguity, quarantine in _KEYS)


def test_missing_robustness_cell_refuses_before_any_book_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def forbidden_build(**_kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("book construction must not start")

    monkeypatch.setattr(runner, "build_mt1_four_arm_books", forbidden_build)
    with pytest.raises(runner.MT1RunnerRefused, match="robustness fan is incomplete"):
        runner.assemble_mt1_in_sample_bundle(
            mt1_source_measurements=_fan(runner.MT1_SOURCE_STRATEGY_ID)[:-1],
            s8_source_measurements=_fan(runner.S8_SOURCE_STRATEGY_ID),
        )
    assert called is False


def test_all_four_structural_cells_finish_before_the_first_outcome_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    build_order: list[int] = []
    evaluation_build_counts: list[int] = []

    def build(**_kwargs: object) -> object:
        build_order.append(len(build_order) + 1)
        arm = SimpleNamespace(scaled=object(), unscaled=object(), structural=object())
        return SimpleNamespace(mt1=arm, s8=arm)

    def evaluate(**_kwargs: object) -> object:
        evaluation_build_counts.append(len(build_order))
        return SimpleNamespace(historical_statistical_conjuncts_pass=True)

    monkeypatch.setattr(runner, "build_mt1_four_arm_books", build)
    monkeypatch.setattr(runner, "evaluate_mt1_trial", evaluate)
    bundle = runner.assemble_mt1_in_sample_bundle(
        mt1_source_measurements=_fan(runner.MT1_SOURCE_STRATEGY_ID),
        s8_source_measurements=_fan(runner.S8_SOURCE_STRATEGY_ID),
    )

    assert build_order == [1, 2, 3, 4]
    assert evaluation_build_counts == [4, 4, 4, 4]
    assert [(cell.ambiguity_arm, cell.quarantine_arm) for cell in bundle.cells] == list(_KEYS)
    assert bundle.historical_statistical_conjuncts_pass is True


def test_a_late_structural_refusal_exposes_no_earlier_cell_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    builds = 0
    evaluations = 0

    def build(**_kwargs: object) -> object:
        nonlocal builds
        builds += 1
        if builds == 4:
            raise ValueError("fourth structural cell refused")
        return object()

    def evaluate(**_kwargs: object) -> object:
        nonlocal evaluations
        evaluations += 1
        return object()

    monkeypatch.setattr(runner, "build_mt1_four_arm_books", build)
    monkeypatch.setattr(runner, "evaluate_mt1_trial", evaluate)
    with pytest.raises(ValueError, match="fourth structural cell refused"):
        runner.assemble_mt1_in_sample_bundle(
            mt1_source_measurements=_fan(runner.MT1_SOURCE_STRATEGY_ID),
            s8_source_measurements=_fan(runner.S8_SOURCE_STRATEGY_ID),
        )
    assert builds == 4
    assert evaluations == 0


def test_source_fans_must_share_the_exact_axis_and_opportunity_population() -> None:
    wrong_axis = list(_fan(runner.S8_SOURCE_STRATEGY_ID))
    wrong_axis[-1] = _measurement(
        runner.S8_SOURCE_STRATEGY_ID,
        "worst_case",
        "masked",
        axis=(date(2000, 1, 3), date(2000, 1, 5)),
    )
    with pytest.raises(runner.MT1RunnerRefused, match="one exact in-sample metric axis"):
        runner.assemble_mt1_in_sample_bundle(
            mt1_source_measurements=_fan(runner.MT1_SOURCE_STRATEGY_ID),
            s8_source_measurements=tuple(wrong_axis),
        )

    other = ResultUniverseRecord(
        universe_rule_version="test-universe-v1",
        evaluated_instrument_ids=frozenset({2}),
        validated_universe_ids=frozenset({2}),
    )
    wrong_population = list(_fan(runner.S8_SOURCE_STRATEGY_ID))
    wrong_population[-1] = _measurement(
        runner.S8_SOURCE_STRATEGY_ID,
        "worst_case",
        "masked",
        opportunity=other,
    )
    with pytest.raises(runner.MT1RunnerRefused, match="one pre-mask opportunity population"):
        runner.assemble_mt1_in_sample_bundle(
            mt1_source_measurements=_fan(runner.MT1_SOURCE_STRATEGY_ID),
            s8_source_measurements=tuple(wrong_population),
        )
