"""#2778 — a scale-gate refusal costs one arm's control, not the whole run.

The gate's own projection is a 3-member extrapolation that swings by more than
2x in BOTH directions (#2778's measurements), so a refusal is partly noise. What
makes that expensive is not the estimate, it is the blast radius: on 2026-08-19
one cohort's estimate came in 45 seconds over the threshold, `ScaleBudgetExceeded`
propagated out of the arm constructor, and a full-set invocation lost a completed
1,000-member cohort and ~90 minutes of compute while persisting nothing.

These tests pin the two halves of the fix that must not drift apart:

* the pre-fan-out refusal is absorbed and named, and
* anything raised from INSIDE the fan-out is still fatal.

Pure — no DB, no fixtures. Deliberately a separate module from
``test_backtest_run.py``, which is fully ``db``-marked and therefore off the
pre-push gate.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app.services import backtest_run
from app.services.backtest_run import ArmMeasurement, BacktestRunReport, _arm_label, _run_cohort_for
from app.services.strategy_result import synthetic_control_promotion_refusals
from app.services.synthetic_control_run import CONTROL_NAMESPACE, ScaleBudgetExceeded

_REFUSAL = (
    "synthetic-control scale gate refused s1-time-series-momentum/worst_case/admitted: "
    "projected cohort wall time 1245.1s exceeds 1200.0s budget"
)


def _measured(trade_count: int = 5) -> dict[str, Any]:
    """The minimum ``_run_cohort_for`` reads off a namespace measurement."""
    return {CONTROL_NAMESPACE: SimpleNamespace(axis_dates=(), metrics=SimpleNamespace(trade_count=trade_count))}


def _call(monkeypatch: pytest.MonkeyPatch, raises: BaseException) -> tuple[Any, str | None]:
    seen: list[str] = []

    def _stub(_collector: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["label"])
        raise raises

    monkeypatch.setattr(backtest_run, "run_cohort", _stub)
    result = _run_cohort_for(
        SimpleNamespace(),  # type: ignore[arg-type]
        measured=_measured(),  # type: ignore[arg-type]
        corpus=SimpleNamespace(),  # type: ignore[arg-type]
        cohort_size=1000,
        label=_arm_label("s1-time-series-momentum", "worst_case", "admitted"),
        strategy_id="s1-time-series-momentum",
        quarantine_arm="admitted",
        ambiguity_arm="worst_case",
    )
    assert seen == ["s1-time-series-momentum/worst_case/admitted"]
    return result


def test_scale_refusal_is_absorbed_and_named(monkeypatch: pytest.MonkeyPatch) -> None:
    control, refusal = _call(monkeypatch, ScaleBudgetExceeded(_REFUSAL))

    assert control is None
    assert refusal == _REFUSAL


def test_a_refused_arm_carries_the_declared_not_run_code() -> None:
    """The absorbed refusal lands on an EXISTING refusal code, not a new one.

    ``synthetic_control_not_run`` is documented as *"no cohort exists for this
    result. The WRITER has not run §9's control"* — which is exactly true of a
    cohort the gate refused before fan-out, so nothing had to be invented.
    """
    assert synthetic_control_promotion_refusals(None) == ("synthetic_control_not_run",)


def test_a_failure_inside_the_fan_out_is_still_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    """The narrowness of the ``except`` is the safety property.

    ``reserve`` refuses before any member runs, so absorbing it discards nothing.
    A ``RuntimeError`` from the member pool means members WERE computed, and
    reporting that arm as a plain "control not run" would hide a partial cohort.
    """
    with pytest.raises(RuntimeError, match="cohort member set is incomplete"):
        _call(monkeypatch, RuntimeError("cohort member set is incomplete: missing=[7]"))


def test_no_cohort_requested_is_not_a_refusal(monkeypatch: pytest.MonkeyPatch) -> None:
    """``(None, None)`` and ``(None, message)`` are different states."""
    monkeypatch.setattr(backtest_run, "run_cohort", lambda *a, **k: pytest.fail("must not run"))

    assert _run_cohort_for(
        None,
        measured=_measured(),  # type: ignore[arg-type]
        corpus=SimpleNamespace(),  # type: ignore[arg-type]
        cohort_size=None,
        label="unused",
        strategy_id="s1-time-series-momentum",
        quarantine_arm="admitted",
    ) == (None, None)


def _arm(*, ambiguity_arm: str | None, refusal: str | None) -> ArmMeasurement:
    return ArmMeasurement(
        strategy_id="s1-time-series-momentum",
        strategy_version="strategy-registry-v1+deadbeef",
        quarantine_arm="admitted",
        namespaces={},
        holdout_positions_discarded=0,
        close_sources={},
        series_evaluated=1,
        elapsed_s=1.0,
        ambiguity_arm=ambiguity_arm,  # type: ignore[arg-type]
        cohort_refusal=refusal,
    )


def test_report_lists_only_refused_arms_keyed_by_the_gate_s_own_label() -> None:
    report = BacktestRunReport(
        runnable=("s1-time-series-momentum",),
        excluded=(),
        holdout_requested=False,
        arms=(
            _arm(ambiguity_arm="worst_case", refusal=_REFUSAL),
            _arm(ambiguity_arm="best_case", refusal=None),
            _arm(ambiguity_arm=None, refusal="refused/shared"),
        ),
    )

    assert report.control_refusals == {
        "s1-time-series-momentum/worst_case/admitted": _REFUSAL,
        "s1-time-series-momentum/shared/admitted": "refused/shared",
    }


def test_arm_label_is_the_string_the_gate_refuses_under() -> None:
    """One spelling, so a refusal can be looked up by the key it was filed under."""
    arm = _arm(ambiguity_arm="worst_case", refusal=_REFUSAL)

    assert arm.label == "s1-time-series-momentum/worst_case/admitted"
    assert arm.label in _REFUSAL
