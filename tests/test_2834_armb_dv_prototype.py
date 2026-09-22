"""#2834 ARM B stage (i): the pure logic of the frozen declaration.

Spec: ``docs/proposals/ta/2026-09-22-armb-dv-weighting-prototype.md`` §1, §2.4, §2.5
and §2.7. No database.
"""

from __future__ import annotations

import math

import pytest

from scripts.measure_2834_armb_dv_prototype import (
    DV_BARS,
    FAIL,
    PASS,
    UNMEASURABLE,
    Inference,
    cr1,
    dollar_volume,
    holding_return,
    normalised,
    profit_factor,
    verdict,
)


class TestDollarVolume:
    def test_means_only_the_last_window(self) -> None:
        products = [1000.0] * 5 + [10.0] * DV_BARS
        assert dollar_volume(products) == pytest.approx(10.0)

    def test_a_short_history_means_what_it_has(self) -> None:
        assert dollar_volume([4.0, 8.0]) == pytest.approx(6.0)

    def test_no_qualifying_bar_is_zero(self) -> None:
        assert dollar_volume([]) == 0.0

    def test_all_zero_volume_is_zero(self) -> None:
        assert dollar_volume([0.0] * DV_BARS) == 0.0


class TestNormalised:
    def test_weights_sum_to_one_and_keep_proportion(self) -> None:
        weights = normalised({1: 1.0, 2: 3.0, 3: 0.0})
        assert weights == pytest.approx({1: 0.25, 2: 0.75, 3: 0.0})

    def test_a_zero_denominator_is_refused_not_divided(self) -> None:
        assert normalised({1: 0.0, 2: 0.0}) is None
        assert normalised({}) is None


class TestHoldingReturn:
    def test_live_buy_and_hold(self) -> None:
        assert holding_return(100.0, 110.0) == pytest.approx(0.10)

    def test_termination_applies_the_fraction_to_the_last_mark(self) -> None:
        # §2.5: adj_close(u) × fraction / adj_close(t) − 1; proceeds held as cash.
        assert holding_return(100.0, 80.0, 0.7) == pytest.approx(-0.44)

    def test_a_non_positive_entry_is_refused(self) -> None:
        with pytest.raises(ValueError, match="not positive"):
            holding_return(0.0, 10.0)


class TestCr1:
    def test_matches_a_hand_computation(self) -> None:
        values = [0.01, 0.03, -0.02, 0.04]
        clusters = [2000, 2000, 2001, 2001]
        mean = 0.015
        residual_sums = [(0.01 - mean) + (0.03 - mean), (-0.02 - mean) + (0.04 - mean)]
        expected_se = math.sqrt(2 / 1 * sum(s * s for s in residual_sums) / 16)
        result = cr1(values, clusters)
        assert result.mean == pytest.approx(mean)
        assert result.se == pytest.approx(expected_se)
        assert result.t == pytest.approx(mean / expected_se)
        assert (result.n, result.clusters) == (4, 2)

    def test_one_cluster_has_no_inference(self) -> None:
        result = cr1([0.01, 0.02], [2000, 2000])
        assert result.mean == pytest.approx(0.015)
        assert result.se is None and result.t is None

    def test_zero_se_with_varying_values_is_invalid(self) -> None:
        # Per-cluster residual sums cancel to zero while the values vary: §2.7
        # judges validity on the SE, not on the variance of the values.
        result = cr1([0.25, 0.75, 0.75, 0.25], [2000, 2000, 2001, 2001])  # binary-exact
        assert result.se is None and result.t is None

    def test_empty_is_invalid(self) -> None:
        assert cr1([], []) == Inference(n=0, clusters=0, mean=None, se=None, t=None)


class TestProfitFactor:
    def test_ratio(self) -> None:
        assert profit_factor([0.02, -0.01, 0.01]) == pytest.approx(3.0)

    def test_no_losses_is_infinite(self) -> None:
        assert profit_factor([0.01]) == math.inf

    def test_empty_or_all_zero_is_undefined(self) -> None:
        assert profit_factor([]) is None
        assert profit_factor([0.0, 0.0]) is None


def _inf(mean: float | None, t: float | None) -> Inference:
    return Inference(n=10, clusters=5, mean=mean, se=None if t is None else 0.01, t=t)


class TestVerdict:
    """§1's frozen table: both arms must pass; an invalid inference is not a FAIL."""

    def test_both_arms_pass(self) -> None:
        assert verdict({"worst_case": _inf(0.01, 2.0), "best_case": _inf(0.02, 3.0)}, dropped_formations=0) == PASS

    def test_one_arm_missing_the_bar_fails(self) -> None:
        assert verdict({"worst_case": _inf(0.01, 1.99), "best_case": _inf(0.02, 3.0)}, dropped_formations=0) == FAIL

    def test_a_negative_mean_fails_even_with_a_large_t(self) -> None:
        assert verdict({"worst_case": _inf(-0.01, 2.5), "best_case": _inf(0.02, 3.0)}, dropped_formations=0) == FAIL

    def test_an_invalid_inference_is_unmeasurable(self) -> None:
        assert (
            verdict({"worst_case": _inf(0.01, None), "best_case": _inf(0.02, 3.0)}, dropped_formations=0)
            == UNMEASURABLE
        )

    def test_a_dropped_formation_is_unmeasurable(self) -> None:
        assert (
            verdict({"worst_case": _inf(0.01, 3.0), "best_case": _inf(0.02, 3.0)}, dropped_formations=1) == UNMEASURABLE
        )

    def test_no_arms_is_unmeasurable(self) -> None:
        assert verdict({}, dropped_formations=0) == UNMEASURABLE
