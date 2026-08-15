from __future__ import annotations

import math
from datetime import date

import numpy as np
import pytest

from app.services.strategy_mt1_trial import (
    BLOCK_LENGTH_MONTHS,
    BOOTSTRAP_RESAMPLES,
    BOOTSTRAP_SEED,
    MIN_COMMON_MONTHS,
    TRIAL_EVALUATOR_VERSION,
    MonthlyReturn,
    MT1TrialRefused,
    PortfolioArm,
    ScaledBookStructuralAudit,
    certainty_equivalent,
    evaluate_mt1_trial,
    expected_shortfall_5,
    maximum_drawdown,
    structural_gate,
)


def _month(index: int) -> date:
    year, zero_month = divmod(index, 12)
    return date(2000 + year, zero_month + 1, 1)


def _arm(values: list[float], *, offset: int = 0) -> PortfolioArm:
    return PortfolioArm(tuple(MonthlyReturn(_month(index + offset), value) for index, value in enumerate(values)))


def _audit(*, turnover: float = 1.0, reconciled: bool = True) -> ScaledBookStructuralAudit:
    decisions = tuple(date(2010 + index // 12, index % 12 + 1, 28) for index in range(24))
    return ScaledBookStructuralAudit(decisions, decisions, turnover, 1_000_000.0, reconciled)


def _trial(**overrides: object):
    # Same 0.5% mean in both MT-1 arms; scaling removes enough dispersion to
    # improve CER, drawdown and the lower tail without manufacturing extra mean.
    mt1_scaled = [0.01 if index % 2 == 0 else 0.0 for index in range(MIN_COMMON_MONTHS)]
    mt1_unscaled = [0.04 if index % 2 == 0 else -0.03 for index in range(MIN_COMMON_MONTHS)]
    s8_unscaled = [0.004 if index % 2 == 0 else -0.004 for index in range(MIN_COMMON_MONTHS)]
    s8_scaled = list(s8_unscaled)
    arguments: dict[str, object] = {
        "mt1_scaled": _arm(mt1_scaled),
        "mt1_unscaled": _arm(mt1_unscaled),
        "s8_scaled": _arm(s8_scaled),
        "s8_unscaled": _arm(s8_unscaled),
        "mt1_structural": _audit(),
        "s8_structural": _audit(),
    }
    arguments.update(overrides)
    return evaluate_mt1_trial(**arguments)  # type: ignore[arg-type]


def test_frozen_four_arm_trial_is_deterministic_and_reports_provenance() -> None:
    first = _trial()
    second = _trial()

    assert first == second
    assert first.bootstrap_block_length == BLOCK_LENGTH_MONTHS == 12
    assert first.bootstrap_resamples == BOOTSTRAP_RESAMPLES == 10_000
    assert first.bootstrap_seed == BOOTSTRAP_SEED == 243_715_082_026
    assert first.evaluator_version == TRIAL_EVALUATOR_VERSION
    assert first.mt1_delta_interval.low > 0
    assert first.primary_interval.low > 0
    assert first.mt1_drawdown_improved
    assert first.mt1_expected_shortfall_improved
    assert first.historical_statistical_conjuncts_pass


def test_negative_control_is_subtracted_from_mt1_improvement() -> None:
    values = [0.01 if index % 2 == 0 else -0.01 for index in range(MIN_COMMON_MONTHS)]
    result = _trial(s8_scaled=_arm([value * 0.5 for value in values]), s8_unscaled=_arm(values))

    assert result.primary_difference_in_differences == pytest.approx(result.mt1_delta_cer - result.s8_delta_cer)
    assert result.primary_difference_in_differences < result.mt1_delta_cer


def test_identical_mt1_and_control_pairs_have_exactly_zero_paired_primary_interval() -> None:
    scaled = [0.01 if index % 3 else -0.005 for index in range(MIN_COMMON_MONTHS)]
    unscaled = [0.03 if index % 3 else -0.02 for index in range(MIN_COMMON_MONTHS)]
    result = _trial(
        mt1_scaled=_arm(scaled),
        mt1_unscaled=_arm(unscaled),
        s8_scaled=_arm(scaled),
        s8_unscaled=_arm(unscaled),
    )

    assert result.primary_interval.low == 0.0
    assert result.primary_interval.high == 0.0


def test_interval_regression_pins_seed_blocks_pairing_cer_and_percentile_tails() -> None:
    indexes = range(MIN_COMMON_MONTHS)
    mt1_scaled = [0.004 + 0.012 * math.sin(index / 5) + 0.002 * math.cos(index / 17) for index in indexes]
    mt1_unscaled = [0.004 + 0.025 * math.sin(index / 5) + 0.006 * math.cos(index / 11) for index in indexes]
    s8_scaled = [0.001 + 0.010 * math.sin(index / 7) for index in indexes]
    s8_unscaled = [0.001 + 0.012 * math.sin(index / 7) + 0.001 * math.cos(index / 13) for index in indexes]

    result = _trial(
        mt1_scaled=_arm(mt1_scaled),
        mt1_unscaled=_arm(mt1_unscaled),
        s8_scaled=_arm(s8_scaled),
        s8_unscaled=_arm(s8_unscaled),
    )

    assert result.mt1_delta_cer == pytest.approx(0.0010335601196231159, abs=1e-15)
    assert result.primary_difference_in_differences == pytest.approx(0.0011434851063477334, abs=1e-15)
    assert result.mt1_delta_interval.low == pytest.approx(-0.004435213432705545, abs=1e-15)
    assert result.mt1_delta_interval.high == pytest.approx(0.005599250327585452, abs=1e-15)
    assert result.primary_interval.low == pytest.approx(-0.004289905335013397, abs=1e-15)
    assert result.primary_interval.high == pytest.approx(0.0057473838633777296, abs=1e-15)


def test_months_are_intersected_and_exclusions_are_visible() -> None:
    extended = [0.01 if index % 2 == 0 else -0.01 for index in range(MIN_COMMON_MONTHS + 1)]
    result = _trial(
        mt1_scaled=_arm(extended),
        mt1_unscaled=_arm(extended[1:], offset=1),
        s8_scaled=_arm(extended),
        s8_unscaled=_arm(extended),
    )

    assert result.common_months[0] == _month(1)
    assert result.excluded_months_by_arm == (1, 0, 1, 1)


def test_fewer_than_120_common_months_refuses() -> None:
    short = _arm([0.01] * (MIN_COMMON_MONTHS - 1))
    with pytest.raises(MT1TrialRefused, match="120 common complete months"):
        _trial(mt1_scaled=short)


@pytest.mark.parametrize(
    ("audit", "message"),
    [
        (_audit(turnover=6.000001), "exceeds 600%"),
        (_audit(reconciled=False), "do not reconcile"),
        (
            ScaledBookStructuralAudit(
                _audit().decision_dates[:-1], _audit().expected_decision_dates, 1.0, 1_000_000.0, True
            ),
            "do not equal the frozen month-end clock",
        ),
    ],
)
def test_structural_gate_refuses_before_statistics(audit: ScaledBookStructuralAudit, message: str) -> None:
    with pytest.raises(MT1TrialRefused, match=message):
        _trial(mt1_structural=audit)


def test_structural_gate_reports_counts_not_returns() -> None:
    report = structural_gate(_audit(turnover=6.0), _audit(turnover=0.5))

    assert report.mt1_decision_dates == 24
    assert report.mt1_annualised_turnover == 6.0
    assert report.s8_annualised_turnover == 0.5
    assert not hasattr(report, "return")


def test_the_negative_control_must_share_mt1s_frozen_exposure_clock() -> None:
    shifted = _audit()
    shifted_dates = tuple(date(day.year, day.month, 27) for day in shifted.decision_dates)
    shifted = ScaledBookStructuralAudit(shifted_dates, shifted_dates, 1.0, 1_000_000.0, True)

    with pytest.raises(MT1TrialRefused, match="same frozen month-end exposure clock"):
        structural_gate(_audit(), shifted)


def test_risk_statistics_use_sample_variance_drawdown_magnitude_and_ceil_tail() -> None:
    values = np.asarray([0.10, -0.20, 0.05, -0.01, 0.02] + [0.03] * 15, dtype=np.float64)

    assert certainty_equivalent(values) == pytest.approx(np.mean(values) - 2.5 * np.var(values, ddof=1))
    assert maximum_drawdown(np.asarray([0.10, -0.20, 0.05])) == pytest.approx(0.20)
    assert expected_shortfall_5(values) == -0.20


def test_non_finite_derived_wealth_or_cer_refuses() -> None:
    enormous = np.asarray([1e308, 1e308], dtype=np.float64)

    with pytest.raises(MT1TrialRefused, match="certainty equivalent is not finite"):
        certainty_equivalent(enormous)
    with pytest.raises(MT1TrialRefused, match="non-finite wealth"):
        maximum_drawdown(enormous)


@pytest.mark.parametrize(
    "arm",
    [
        PortfolioArm((MonthlyReturn(date(2020, 1, 2), 0.01),)),
        PortfolioArm((MonthlyReturn(date(2020, 1, 1), float("nan")),)),
        PortfolioArm((MonthlyReturn(date(2020, 1, 1), -1.01),)),
        PortfolioArm((MonthlyReturn(date(2020, 2, 1), 0.01), MonthlyReturn(date(2020, 1, 1), 0.01))),
    ],
)
def test_invalid_arm_axes_and_values_refuse(arm: PortfolioArm) -> None:
    with pytest.raises(MT1TrialRefused):
        _trial(mt1_scaled=arm)


def test_passing_historical_statistic_does_not_claim_promotion() -> None:
    result = _trial()

    assert result.historical_statistical_conjuncts_pass
    assert not hasattr(result, "promotable")
    assert not hasattr(result, "capital_eligible")
