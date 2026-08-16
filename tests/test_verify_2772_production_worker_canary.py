from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from app.services.random_entry_cohort import SPEC_COHORT_SIZE
from app.services.synthetic_control_run import CohortCollector, WorkerCanaryReport
from scripts.verify_2772_production_worker_canary import MAX_CANARY_SERIES, _canary_only, _CanaryComplete


def test_the_production_boundary_runs_only_the_fixed_canary_then_stops() -> None:
    collector = Mock(spec=CohortCollector)
    report = Mock(spec=WorkerCanaryReport)
    axis = (object(),)
    with (
        patch("scripts.verify_2772_production_worker_canary.run_worker_canary", return_value=report) as run,
        pytest.raises(_CanaryComplete) as stopped,
    ):
        _canary_only(collector, axis=axis, benchmark=None, cohort_size=SPEC_COHORT_SIZE)

    assert stopped.value.report is report
    run.assert_called_once_with(collector, axis=axis, benchmark=None)


def test_a_changed_production_cohort_size_refuses_before_the_canary() -> None:
    with (
        patch("scripts.verify_2772_production_worker_canary.run_worker_canary") as run,
        pytest.raises(RuntimeError, match="expected 1000"),
    ):
        _canary_only(Mock(spec=CohortCollector), axis=(), benchmark=None, cohort_size=999)
    run.assert_not_called()


def test_the_operator_series_cap_stays_small() -> None:
    assert MAX_CANARY_SERIES == 100
