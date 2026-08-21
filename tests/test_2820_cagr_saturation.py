"""#2820 part 1 — a wiped-out sleeve is a real outcome, not an invalid axis.

`compute_metrics` derives `total_return_pct` and `cagr_pct` from the SAME
`final_equity / starting_equity`, so they cannot genuinely disagree. But
`total_return_pct` is `(ratio - 1.0) * 100.0`, which saturates at exactly
`-100.0` for any ratio below `2**-54` — so reconstructing the ratio from it
gives `0.0`, and the axis rule used to demand a `-100%` CAGR.

Measured on backtest run 112188 (`s1-time-series-momentum in_sample/masked`):
CAGR `-63.08857514295101%` over `59.48528405201917` years is a final multiple of
`1.788e-26`. Correct, and refused — which killed the whole invocation at the
write phase and is what has been blocking the walk-forward evidence run.

⚠ The gate is narrowed, never skipped: a positive CAGR, or one too shallow for a
wipeout, is still refused. What it stops asserting is the one thing the stored
number provably cannot carry.

Pure — no DB.
"""

from __future__ import annotations

import math
from datetime import date
from types import SimpleNamespace
from typing import Any

import pytest

from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.strategy_result import (
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    LEGACY_RETURN_BASIS,
    METRIC_AXIS_RULE_VERSION,
    SATURATED_TOTAL_RETURN_MAX_MULTIPLE,
    ResultIdentity,
    metric_axis_invalid_reason,
    metric_axis_sha256,
    periods_per_year,
)

_AXIS = (EVALUATION_WINDOW_START, date(2021, 6, 28))
_PPY = periods_per_year(_AXIS)
_YEARS = (len(_AXIS) - 1) / _PPY


def _identity() -> ResultIdentity:
    return ResultIdentity(  # type: ignore[arg-type]
        strategy_id="s1-time-series-momentum",
        strategy_version="strategy-registry-v1+aaaaaaaaaaaa",
        result_scope="sleeve",
        namespace="in_sample",
        ambiguity_arm="worst_case",
        quarantine_arm="masked",
        sizing_rule=SIZING_RULE_ID,
        benchmark_rule=BENCHMARK_RULE_ID,
        cost_model_id="static-p75-insession-v1",
        corpus_version=CORPUS_VERSION,
        window_start=EVALUATION_WINDOW_START,
        window_end=EVALUATION_WINDOW_END,
        position_rule_set_version="position-builder-v1+bbbbbbbbbbbb",
        outcome_rule_set_version="outcome-resolver-v1+cccccccccccc",
        input_rule_set_version="price-quarantine-v1+dddddddddddd",
        return_basis=LEGACY_RETURN_BASIS,
        metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
        metric_axis_dates=_AXIS,
        metric_axis_start=_AXIS[0],
        metric_axis_end=_AXIS[-1],
        metric_axis_digest=metric_axis_sha256(_AXIS),
        opportunity_set_digest="a" * 64,
    )


def _metrics(*, cagr_pct: float, total_return_pct: float = -100.0) -> Any:
    return SimpleNamespace(periods_per_year=_PPY, total_return_pct=total_return_pct, cagr_pct=cagr_pct)


def _ceiling() -> float:
    return (SATURATED_TOTAL_RETURN_MAX_MULTIPLE ** (1.0 / _YEARS) - 1.0) * 100.0


def test_the_constant_is_the_actual_saturation_point_on_this_platform() -> None:
    """Re-derived, not trusted: the next double up must NOT saturate.

    Pins the constant to IEEE-754 rather than to a literal someone typed, which
    is what stops it drifting into an invented number.
    """
    r = SATURATED_TOTAL_RETURN_MAX_MULTIPLE

    assert (r - 1.0) * 100.0 == -100.0
    assert (math.nextafter(r, math.inf) - 1.0) * 100.0 != -100.0

    lo, hi = 0.0, 1e-10
    for _ in range(200):
        mid = (lo + hi) / 2
        if (mid - 1.0) * 100.0 == -100.0:
            lo = mid
        else:
            hi = mid
    assert lo == r


def test_the_run_112188_row_is_admitted() -> None:
    """The exact figures that killed the invocation, verbatim from the job row."""
    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-63.08857514295101))

    assert reason is None


def test_a_true_zero_wipeout_is_still_admitted() -> None:
    """The endpoint the old code special-cased must not regress."""
    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-100.0)) is None


@pytest.mark.parametrize("cagr_pct", [0.0, 5.0, -10.0, -101.0])
def test_a_cagr_the_saturated_bound_excludes_is_still_refused(cagr_pct: float) -> None:
    """Narrowed, not disabled — a wipeout cannot annualise to a shallow loss."""
    assert cagr_pct > _ceiling() or cagr_pct < -100.0, "test case must lie outside the admissible interval"

    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct))

    assert reason is not None
    assert reason.startswith("cagr_outside_saturated_total_return_bounds")


def test_the_bound_is_a_closed_interval_at_both_ends() -> None:
    ceiling = _ceiling()

    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=ceiling)) is None
    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=math.nextafter(ceiling, math.inf))) is not None
    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=math.nextafter(-100.0, -math.inf))) is not None


def test_an_unsaturated_total_return_still_reconciles_exactly() -> None:
    """The ordinary path keeps its 1e-10 equality — this change must not loosen it."""
    multiple = 1.21
    exact = (multiple ** (1.0 / _YEARS) - 1.0) * 100.0
    total_return_pct = (multiple - 1.0) * 100.0

    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=exact, total_return_pct=total_return_pct)) is None
    drifted = _metrics(cagr_pct=exact + 1e-6, total_return_pct=total_return_pct)
    reason = metric_axis_invalid_reason(_identity(), drifted)
    assert reason is not None
    assert reason.startswith("cagr_does_not_reconcile")
