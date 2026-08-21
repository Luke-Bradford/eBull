"""#2820 part 1 — a near-wiped-out sleeve is a real outcome, not an invalid axis.

`compute_metrics` derives `total_return_pct` and `cagr_pct` from the SAME
`final_equity / starting_equity`, so they cannot genuinely disagree. What failed
is the round trip: `total_return_pct` is `(ratio - 1.0) * 100.0`, which for a
near-wipeout loses almost every significant digit of `ratio` to cancellation,
and below `2**-54` loses all of them and rounds to exactly `-100.0`.

Measured on backtest run 112188 (`s1-time-series-momentum in_sample/masked`):
CAGR `-63.08857514295101%` over `59.48528405201917` years is a final multiple of
`1.788e-26`. Correct, and refused — which killed the invocation at the write
phase and has been blocking the walk-forward evidence run.

The rule now brackets the terminal multiples each stored number is consistent
with and asks whether the two brackets overlap. ⚠ That is STRICTER than the
`isclose(rel_tol=1e-10)` it replaces wherever the floats are well conditioned —
a 1e-12 CAGR drift is now refused — and widens only where a stored number
provably cannot distinguish its neighbours. Both sides are bracketed because
either can saturate: the total return over a long span, the CAGR over a sub-year
one.

Pure — no DB.
"""

from __future__ import annotations

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
    ResultIdentity,
    _ratio_from_cagr_pct,
    _ratio_from_total_return_pct,
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


def _honest(ratio: float, years: float = _YEARS) -> tuple[float, float]:
    """Exactly what ``compute_metrics`` stores for a sleeve ending at ``ratio``."""
    return ((ratio - 1.0) * 100.0, (ratio ** (1.0 / years) - 1.0) * 100.0)


def test_the_run_112188_row_is_admitted() -> None:
    """The exact figures that killed the invocation, verbatim from the job row."""
    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-63.08857514295101)) is None


@pytest.mark.parametrize("exponent", range(-30, 4))
def test_no_honest_sleeve_is_refused_across_thirty_four_orders_of_magnitude(exponent: int) -> None:
    """The whole reachable range of terminal equity, wipeout to 100x."""
    total_return_pct, cagr_pct = _honest(10.0**exponent)

    assert (
        metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct, total_return_pct=total_return_pct)) is None
    )


def test_a_true_zero_wipeout_is_admitted() -> None:
    """Both brackets reach zero, so they overlap there.

    ⚠ This is the row that breaks if `_ratio_from_cagr_pct` ever clamps its
    lower bound above zero: a true wipeout is the one outcome whose terminal
    multiple IS zero.
    """
    assert metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-100.0, total_return_pct=-100.0)) is None


@pytest.mark.parametrize("cagr_pct", [0.0, 5.0, -10.0, -30.0])
def test_a_cagr_inconsistent_with_a_wipeout_is_still_refused(cagr_pct: float) -> None:
    """Narrowed, not disabled: a wipeout cannot annualise to a shallow loss."""
    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct))

    assert reason is not None
    assert reason.startswith("cagr_does_not_reconcile")


def test_a_cagr_below_negative_100pct_is_refused_and_never_goes_complex() -> None:
    """A negative base under a fractional exponent returns a COMPLEX number."""
    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-150.0))

    assert reason is not None
    assert "implies None" in reason


def test_the_near_saturated_band_cannot_launder_an_inconsistent_cagr() -> None:
    """Codex review counterexample, kept as a regression test.

    Two earlier attempts at this fix passed the row below. `total_return_pct` of
    `-99.9999999999999` is a terminal multiple of 1e-15, whose true CAGR over
    this span is about -44.05% — but a comparison performed in PERCENTAGE space
    has roughly 1e-8 of absolute slack down there, which spans a decade of
    multiples. Bracketing the multiples instead is what closes it.
    """
    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=-34.0, total_return_pct=-99.9999999999999))

    assert reason is not None
    assert reason.startswith("cagr_does_not_reconcile")


def test_the_saturated_bracket_is_not_wider_than_saturation_actually_reaches() -> None:
    """Second Codex review counterexample, kept as a regression test.

    A terminal multiple of `2e-16` does NOT store a total return of `-100.0` —
    it stores `-99.99999999999997` — so its CAGR must not pair with `-100.0`.
    An earlier attempt widened the bracket by a full `ulp(1.0)` and admitted it.
    The slack is three units of `2**-54`, which is the smallest that rejects no
    honest row, and that is tight enough to catch this.
    """
    assert (2e-16 - 1.0) * 100.0 != -100.0, "premise: this multiple does not saturate"
    cagr_pct = (2e-16 ** (1.0 / _YEARS) - 1.0) * 100.0

    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct, total_return_pct=-100.0))

    assert reason is not None
    assert reason.startswith("cagr_does_not_reconcile")


@pytest.mark.parametrize(
    ("total_return_pct", "cagr_pct"),
    [
        (-100.0, float("nan")),
        (float("nan"), -63.0),
        (-100.0, float("inf")),
        (float("-inf"), -63.0),
    ],
)
def test_a_non_finite_metric_is_refused_before_any_interval_arithmetic(
    total_return_pct: float, cagr_pct: float
) -> None:
    """Third Codex review counterexample, kept as a regression test.

    Every NaN comparison is `False`, so `max(0.0, nan)` is `0.0` — a NaN CAGR
    would collapse its bracket to `(0.0, 0.0)`, which OVERLAPS a saturated total
    return's bracket and would promote a malformed row. The `math.isclose` rule
    this replaced refused NaN for free; interval arithmetic does not.
    """
    reason = metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct, total_return_pct=total_return_pct))

    assert reason is not None
    assert reason.startswith("metrics_not_finite")


def test_a_well_conditioned_row_is_reconciled_more_tightly_than_before() -> None:
    """⚠ The bracket rule is STRICTER here, not looser.

    The old rule compared with `isclose(rel_tol=1e-10)`, which admitted a 1e-12
    CAGR drift. The bracket collapses to near-nothing where the floats are well
    conditioned, so that drift is now refused.
    """
    total_return_pct, cagr_pct = _honest(1.21)

    assert (
        metric_axis_invalid_reason(_identity(), _metrics(cagr_pct=cagr_pct, total_return_pct=total_return_pct)) is None
    )
    for drift in (1e-12, 1e-10, 1e-6, 1e-3):
        drifted = _metrics(cagr_pct=cagr_pct + drift, total_return_pct=total_return_pct)
        reason = metric_axis_invalid_reason(_identity(), drifted)
        assert reason is not None, f"drift {drift!r} slipped through"
        assert reason.startswith("cagr_does_not_reconcile")


@pytest.mark.parametrize("years", [0.25, 0.5, 0.74, 0.9, 0.99, 1.0, 2.0, 5.0, 25.0, 100.0])
@pytest.mark.parametrize("exponent", [-30, -20, -15, -10, -5, -1, 0, 2, 10, 22, 30])
def test_either_side_may_saturate_depending_on_the_span(years: float, exponent: int) -> None:
    """⚠ Why both sides are bracketed rather than just the total return.

    Over 59 years a wipeout saturates `total_return_pct`; over a sub-year window
    the annualisation saturates `cagr_pct` instead. A rule that brackets one
    side only refuses honest rows on the other, so the grid crosses both.

    Exercised through the module-level helper rather than `metric_axis_invalid_reason`,
    because `years` is a function of the axis and the identity fixture pins one.
    """
    ratio = 10.0**exponent
    total_return_pct = (ratio - 1.0) * 100.0
    cagr_pct = (ratio ** (1.0 / years) - 1.0) * 100.0

    from_total_return = _ratio_from_total_return_pct(total_return_pct)
    from_cagr = _ratio_from_cagr_pct(cagr_pct, years)

    assert from_cagr is not None
    assert from_total_return[0] <= from_cagr[1] and from_cagr[0] <= from_total_return[1], (
        f"honest row rejected: ratio 1e{exponent} over {years}y"
    )
