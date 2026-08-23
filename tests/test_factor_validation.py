from __future__ import annotations

import numpy as np
import pytest

from app.services.factor_validation import compare_factor_series


def _series(values: np.ndarray) -> dict[tuple[int, int], float]:
    return {(2020 + index // 12, index % 12 + 1): float(value) for index, value in enumerate(values)}


def test_matching_construction_passes_frozen_sign_and_alignment_rules() -> None:
    reference = np.random.default_rng(2912).normal(0, 0.04, 60)
    dependent = 0.001 + 1.4 * reference
    result = compare_factor_series(
        label="matching",
        dependent=_series(dependent),
        reference=_series(reference),
    )
    assert result.passed is True
    assert result.overlap_months == 60
    assert result.correlation == pytest.approx(1.0)
    assert result.alpha == pytest.approx(0.001)
    assert result.beta == pytest.approx(1.4)


def test_sign_inversion_is_a_failure_not_a_market_finding() -> None:
    reference = np.random.default_rng(2912).normal(0, 0.04, 60)
    result = compare_factor_series(
        label="inverted",
        dependent=_series(-reference),
        reference=_series(reference),
    )
    assert result.passed is False
    assert result.correlation == pytest.approx(-1.0)
    assert result.beta == pytest.approx(-1.0)
    assert any("correlation" in failure for failure in result.failures)
    assert any("beta" in failure for failure in result.failures)


def test_one_month_displacement_fails_alignment_rule() -> None:
    reference = np.random.default_rng(2912).normal(0, 0.04, 60)
    displaced = np.concatenate(([0.0], reference[:-1]))
    result = compare_factor_series(
        label="displaced",
        dependent=_series(displaced),
        reference=_series(reference),
    )
    assert result.passed is False
    assert result.reference_lag_one_correlation is not None
    assert result.correlation is not None
    assert abs(result.reference_lag_one_correlation) > abs(result.correlation)
    assert any("displaced" in failure for failure in result.failures)


def test_reference_control_uses_stricter_correlation_floor_without_displacement_gate() -> None:
    reference = np.random.default_rng(2912).normal(0, 0.04, 60)
    result = compare_factor_series(
        label="control",
        dependent=_series(reference),
        reference=_series(reference),
        kind="reference_control",
    )
    assert result.passed is True
    assert result.correlation == pytest.approx(1.0)


def test_sparse_construction_records_unavailable_correlations_as_failures() -> None:
    result = compare_factor_series(
        label="sparse",
        dependent={(2020, 1): 0.01},
        reference={(2020, 1): 0.02},
    )

    assert result.passed is False
    assert result.correlation is None
    assert result.reference_lag_one_correlation is None
    assert result.reference_lead_one_correlation is None
    assert "overlap 1 < 24" in result.failures
    assert "contemporaneous correlation is unavailable" in result.failures


def test_sparse_calendar_records_unavailable_displacements_as_failures() -> None:
    reference = {(2020 + index, 1): float(index) for index in range(24)}
    dependent = {month: value * 2 for month, value in reference.items()}

    result = compare_factor_series(label="annual-only", dependent=dependent, reference=reference)

    assert result.passed is False
    assert result.correlation == pytest.approx(1.0)
    assert result.reference_lag_one_correlation is None
    assert result.reference_lead_one_correlation is None
    assert "reference lag-one correlation is unavailable" in result.failures
    assert "reference lead-one correlation is unavailable" in result.failures
