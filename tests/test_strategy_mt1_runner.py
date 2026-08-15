"""#2769 complete-fan and structural-first MT-1 assembly tests."""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from types import SimpleNamespace
from typing import Any, cast

import pytest

import app.services.strategy_mt1_runner as runner
from app.services.backtest_run import ArmMeasurement, NamespaceMeasurement
from app.services.equity_curve import LegBook
from app.services.result_ledger import FrozenPreregistration, HoldoutAccessCounts
from app.services.strategy_mt1_preregistration import build_declarations
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


def _frozen(index: int, *, declaration=None, digest: str | None = None) -> FrozenPreregistration:
    current = declaration or build_declarations()[index]
    return FrozenPreregistration(
        declaration_id=100 + index,
        declaration=current,
        declaration_sha256=current.sha256 if digest is None else digest,
        chain_declaration_ids=(8 + index, 100 + index),
        supersedes_declaration_id=8 + index,
        supersession_reason="structural_refusal_policy_superseded",
        supersession_attestation="no outcome access",
    )


def test_preregistration_authority_requires_both_current_exact_unexposed_declarations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = [_frozen(0), _frozen(1)]
    monkeypatch.setattr(
        runner,
        "load_preregistration",
        lambda _conn, strategy_id, _version: (
            frozen[0] if strategy_id == frozen[0].declaration.strategy_id else frozen[1]
        ),
    )
    monkeypatch.setattr(runner, "holdout_access_counts", lambda *_args: HoldoutAccessCounts(0, 0))

    authority = runner.validate_mt1_preregistrations(cast(object, object()))  # type: ignore[arg-type]

    assert tuple(item.declaration_id for item in authority) == (100, 101)
    assert tuple(item.declaration_sha256 for item in authority) == tuple(item.declaration_sha256 for item in frozen)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing", "preregistration_missing"),
        ("digest", "declaration_digest_mismatch"),
        ("terms", "declaration_terms_changed=forward_shadow"),
        ("policy", "structural_refusal_policy_superseded"),
        ("exposed", "holdout_evaluations_already_exist=1"),
    ),
)
def test_preregistration_authority_refuses_every_pre_outcome_boundary_breach(
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    declarations = build_declarations()
    first: FrozenPreregistration | None = _frozen(0)
    if mutation == "missing":
        first = None
    elif mutation == "digest":
        first = _frozen(0, digest="0" * 64)
    elif mutation == "terms":
        changed = replace(
            declarations[0],
            forward_shadow=replace(declarations[0].forward_shadow, min_calendar_weeks=158),
        )
        first = _frozen(0, declaration=changed)
    elif mutation == "policy":
        changed = replace(declarations[0], structural_refusal_policy_version="stale-policy")
        first = _frozen(0, declaration=changed)

    def load(_conn: object, strategy_id: str, _version: str) -> FrozenPreregistration | None:
        return first if strategy_id == declarations[0].strategy_id else _frozen(1)

    def counts(_conn: object, strategy_id: str, _version: str) -> HoldoutAccessCounts:
        return (
            HoldoutAccessCounts(1, 0)
            if mutation == "exposed" and strategy_id == declarations[0].strategy_id
            else HoldoutAccessCounts(0, 0)
        )

    monkeypatch.setattr(runner, "load_preregistration", load)
    monkeypatch.setattr(runner, "holdout_access_counts", counts)
    with pytest.raises(runner.MT1RunnerRefused, match=message):
        runner.validate_mt1_preregistrations(cast(object, object()))  # type: ignore[arg-type]


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


def test_paved_run_checks_authority_before_corpus_and_builds_only_the_complete_in_sample_fan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []
    authority = (
        runner.MT1PreregistrationAuthority("mt1", "v1", 10, "a" * 64),
        runner.MT1PreregistrationAuthority("s8", "v1", 11, "b" * 64),
    )

    def validate(_conn: object):
        events.append("authority")
        return authority

    def load_corpus(_conn: object, **kwargs: object):
        events.append(("corpus", kwargs))
        return SimpleNamespace(universe_basis="survivorship_free")

    def load_regime(_conn: object, *, through_date: date | None = None):
        events.append(("regime", through_date))
        return object()

    def evaluate(_conn: object, entry: object, **kwargs: object):
        strategy_id = cast(str, getattr(entry, "strategy_id"))
        quarantine = cast(str, kwargs["quarantine_arm"])
        events.append(("evaluate", strategy_id, quarantine, kwargs["namespaces"]))
        return tuple(_measurement(strategy_id, ambiguity, quarantine) for ambiguity in ("best_case", "worst_case"))

    prepared = cast(runner.MT1PreparedBundle, object())

    def prepare(**kwargs: object):
        events.append(
            (
                "prepare",
                len(cast(tuple[object, ...], kwargs["mt1_source_measurements"])),
                len(cast(tuple[object, ...], kwargs["s8_source_measurements"])),
            )
        )
        return prepared

    monkeypatch.setattr(runner, "validate_mt1_preregistrations", validate)
    monkeypatch.setattr(runner, "load_corpus", load_corpus)
    monkeypatch.setattr(runner.MarketRegimeProvider, "load_research", load_regime)
    monkeypatch.setattr(runner, "evaluate_level_arms", evaluate)
    monkeypatch.setattr(runner, "prepare_mt1_in_sample_bundle", prepare)
    monkeypatch.setattr(runner, "evaluate_mt1_prepared_bundle", lambda value: cast(runner.MT1HistoricalBundle, value))

    result = runner.run_mt1_in_sample_evaluation(  # type: ignore[arg-type]
        cast(Any, object()), runner_source_head="a" * 40
    )

    assert result.authorities == authority
    assert result.bundle is cast(runner.MT1HistoricalBundle, prepared)
    assert events[0] == "authority"
    assert events[1] == (
        "corpus",
        {"universe_basis": "survivorship_free", "evaluation_window": runner.MT1_IN_SAMPLE_WINDOW},
    )
    assert runner.MT1_IN_SAMPLE_WINDOW.end < runner.HOLDOUT_BOUNDARY
    assert events[2] == ("regime", runner.MT1_IN_SAMPLE_WINDOW.end)
    assert events[3:7] == [
        ("evaluate", runner.MT1_SOURCE_STRATEGY_ID, "admitted", ("in_sample",)),
        ("evaluate", runner.MT1_SOURCE_STRATEGY_ID, "masked", ("in_sample",)),
        ("evaluate", runner.S8_SOURCE_STRATEGY_ID, "admitted", ("in_sample",)),
        ("evaluate", runner.S8_SOURCE_STRATEGY_ID, "masked", ("in_sample",)),
    ]
    assert events[7] == ("prepare", 4, 4)


def test_paved_run_loads_no_corpus_when_preregistration_refuses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner,
        "validate_mt1_preregistrations",
        lambda _conn: (_ for _ in ()).throw(runner.MT1RunnerRefused("authority refused")),
    )
    monkeypatch.setattr(
        runner,
        "load_corpus",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("corpus must remain sealed")),
    )
    with pytest.raises(runner.MT1RunnerRefused, match="authority refused"):
        runner.run_mt1_in_sample_evaluation(  # type: ignore[arg-type]
            cast(Any, object()), runner_source_head="a" * 40
        )


def test_paved_run_refuses_unpinned_source_before_authority_or_corpus(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner,
        "validate_mt1_preregistrations",
        lambda _conn: (_ for _ in ()).throw(AssertionError("authority must remain unread")),
    )
    monkeypatch.setattr(
        runner,
        "load_corpus",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("corpus must remain sealed")),
    )
    with pytest.raises(runner.MT1RunnerRefused, match="exact lower-case Git object ID"):
        runner.run_mt1_in_sample_evaluation(cast(Any, object()), runner_source_head="not-a-git-head")
