from datetime import date, timedelta

import numpy as np
import pytest

from app.services.equity_curve import LegBook, build_equity_curve
from app.services.position_builder import Window
from app.services.strategy_result import HOLDOUT_BOUNDARY
from app.services.strategy_statistics import DatedEquityCurve, TradeReturns, compute_metrics
from app.services.synthetic_control_run import CohortCollector, SeriesPlacement
from scripts import verify_2697_metric_axis_ab
from scripts.verify_2697_metric_axis_ab import (
    _IN_SAMPLE_WINDOW,
    _exact_candidate_head,
    _legacy_cohort_control,
    _load_sealed_inputs,
    _population_label,
)


def test_acceptance_corpus_stops_before_the_first_holdout_bar() -> None:
    assert _IN_SAMPLE_WINDOW.end < HOLDOUT_BOUNDARY


def test_acceptance_loads_both_price_and_regime_inputs_through_the_sealed_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    corpus = object()
    regime = object()

    def load_corpus(_conn: object, **kwargs: object) -> object:
        calls.append(("corpus", kwargs))
        return corpus

    def load_regime(_conn: object, *, through_date: date | None = None) -> object:
        calls.append(("regime", through_date))
        return regime

    monkeypatch.setattr(verify_2697_metric_axis_ab, "load_corpus", load_corpus)
    monkeypatch.setattr(
        verify_2697_metric_axis_ab.MarketRegimeProvider,
        "load_research",
        staticmethod(load_regime),
    )

    assert _load_sealed_inputs(object(), limit=50) == (corpus, regime)  # type: ignore[arg-type,comparison-overlap]
    assert calls == [
        (
            "corpus",
            {
                "universe_basis": verify_2697_metric_axis_ab.BACKTEST_UNIVERSE,
                "limit": 50,
                "evaluation_window": _IN_SAMPLE_WINDOW,
            },
        ),
        ("regime", _IN_SAMPLE_WINDOW.end),
    ]


def test_legacy_cohort_ab_arm_runs_the_declared_member_count() -> None:
    axis = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(8))
    prices = np.asarray([10.0 + index for index in range(len(axis))], dtype=np.float64)
    collector = CohortCollector(
        window=Window(axis[0], axis[-1]),
        placements=[
            SeriesPlacement(
                panel=np.arange(len(axis), dtype=np.int64),
                adjusted_open=prices,
                holds=np.asarray([2], dtype=np.int64),
                marks=prices,
                marks_first=0,
            )
        ],
    )
    strategy_metrics = compute_metrics(
        DatedEquityCurve(dates=axis, curve=build_equity_curve(LegBook(), date_count=len(axis))),
        trades=TradeReturns((), (), (), 0, 0),
        buy_and_hold=None,
        bootstrap_seed=None,
    )

    control = _legacy_cohort_control(
        collector,
        axis=axis,
        strategy_metrics=strategy_metrics,
        cohort_size=3,
        max_workers=1,
    )
    spawned = _legacy_cohort_control(
        collector,
        axis=axis,
        strategy_metrics=strategy_metrics,
        cohort_size=3,
        max_workers=2,
    )

    assert control.cohort_size == 3
    assert np.isfinite(control.cohort_sharpe_threshold)
    assert np.isfinite(control.cohort_return_threshold_pct)
    assert spawned == control


def test_a_strategy_subset_can_never_be_labelled_full_population() -> None:
    assert _population_label(limit=None, strategy=None) == "full"
    assert _population_label(limit=None, strategy="s1-time-series-momentum").startswith("smoke-")
    assert _population_label(limit=50, strategy=None).startswith("smoke-")


def test_exact_head_refuses_a_dirty_worktree(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Completed:
        stdout = " M app/services/backtest_run.py\n"

    monkeypatch.setattr(verify_2697_metric_axis_ab.subprocess, "run", lambda *_args, **_kwargs: _Completed())

    with pytest.raises(RuntimeError, match="clean worktree"):
        _exact_candidate_head()


def test_the_legacy_arm_refuses_a_member_that_drops_a_declared_trade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    axis = tuple(date(2020, 1, 1) + timedelta(days=index) for index in range(8))
    prices = np.asarray([10.0 + index for index in range(len(axis))], dtype=np.float64)
    collector = CohortCollector(
        window=Window(axis[0], axis[-1]),
        placements=[
            SeriesPlacement(
                panel=np.arange(len(axis), dtype=np.int64),
                adjusted_open=prices,
                holds=np.asarray([2], dtype=np.int64),
                marks=prices,
                marks_first=0,
            )
        ],
    )
    strategy_metrics = compute_metrics(
        DatedEquityCurve(dates=axis, curve=build_equity_curve(LegBook(), date_count=len(axis))),
        trades=TradeReturns((), (), (), 0, 0),
        buy_and_hold=None,
        bootstrap_seed=None,
    )
    monkeypatch.setattr(
        verify_2697_metric_axis_ab,
        "_place_member",
        lambda *_args, **_kwargs: (LegBook(), [], [], []),
    )

    with pytest.raises(RuntimeError, match="0 legs against the strategy's 1 matchable positions"):
        _legacy_cohort_control(
            collector,
            axis=axis,
            strategy_metrics=strategy_metrics,
            cohort_size=1,
            max_workers=1,
        )
