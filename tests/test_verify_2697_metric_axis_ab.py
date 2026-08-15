from datetime import date, timedelta

import numpy as np

from app.services.equity_curve import LegBook, build_equity_curve
from app.services.position_builder import Window
from app.services.strategy_statistics import DatedEquityCurve, TradeReturns, compute_metrics
from app.services.synthetic_control_run import CohortCollector, SeriesPlacement
from scripts.verify_2697_metric_axis_ab import _legacy_cohort_control


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
