"""The calibration arm's statistics, as pure logic (#2598 scope 5, step 1).

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.1. The band
table itself is pinned in ``tests/test_cost_model.py``; what is pinned HERE is
the arithmetic that would produce the next one — the discrete-percentile rule
and the freeze quantisation.

⚠ Worth its own file because the numbers this arm prints are candidates for a
frozen literal. An off-by-one in the percentile index does not fail anything: it
selects the neighbouring observation, prints a plausible number, and that number
becomes a model somebody charges. There is no downstream assertion that would
catch it.

⚠ DB-free by design. Both functions are pure over a sequence of ``Decimal``.
"""

from __future__ import annotations

import math
from decimal import Decimal

import pytest

from scripts.verify_2240_cost_model import _freeze_ready, _percentile_disc


def _decimals(*values: str) -> list[Decimal]:
    return [Decimal(value) for value in values]


class TestTheDiscretePercentile:
    """``percentile_disc``: the smallest OBSERVED value reaching the share."""

    def test_it_returns_an_observed_value_and_never_an_interpolation(self) -> None:
        """The model claims to charge a spread somebody was actually quoted."""
        sample = _decimals("1.0", "2.0", "3.0", "4.0")
        for percentile in range(1, 101):
            assert _percentile_disc(sample, percentile=percentile) in sample

    @pytest.mark.parametrize(
        ("percentile", "expected"),
        [
            (25, "1.0"),  # ceil(4 × 0.25) = 1 -> the 1st smallest
            (50, "2.0"),
            (75, "3.0"),
            (100, "4.0"),
        ],
    )
    def test_the_index_is_ceil_n_times_p(self, percentile: int, expected: str) -> None:
        assert _percentile_disc(_decimals("4.0", "1.0", "3.0", "2.0"), percentile=percentile) == Decimal(expected)

    def test_p100_is_the_sample_maximum(self) -> None:
        """The same rule at its limit, not a special case — so the bounding
        table's ``max`` column cannot drift away from its percentile siblings."""
        sample = _decimals("0.5", "9.25", "3.0")
        assert _percentile_disc(sample, percentile=100) == max(sample)

    def test_the_index_is_exact_where_the_float_form_is_off_by_one(self) -> None:
        """⚠ THE REGRESSION THIS FILE EXISTS FOR.

        ``0.07 * 100`` is ``7.000000000000001`` in binary floating point, so
        ``ceil`` of it is 8 and the float form selects the WRONG observation —
        silently, and only for the percentiles where the product lands a ulp
        above an integer. Integer arithmetic has no such case.
        """
        assert math.ceil(0.07 * 100) == 8, "the float hazard this test pins has changed"
        sample = [Decimal(n) for n in range(1, 101)]
        assert _percentile_disc(sample, percentile=7) == Decimal(7)

    def test_a_single_observation_is_its_own_every_percentile(self) -> None:
        assert _percentile_disc(_decimals("1.45"), percentile=95) == Decimal("1.45")

    def test_an_empty_sample_is_refused_rather_than_wrapping_to_the_last_element(self) -> None:
        with pytest.raises(ValueError, match="empty sample"):
            _percentile_disc([], percentile=95)

    @pytest.mark.parametrize("percentile", [0, -1, 101])
    def test_a_percentile_outside_one_to_a_hundred_is_refused(self, percentile: int) -> None:
        with pytest.raises(ValueError, match="percentile must be in 1..100"):
            _percentile_disc(_decimals("1.0"), percentile=percentile)


class TestTheFreezeQuantisation:
    """ROUND_CEILING to 0.001 pp — the frozen model is never CHEAPER."""

    def test_it_never_rounds_a_cost_down(self) -> None:
        """The one direction that flatters a result. Criterion 2 puts the model
        deliberately at the pessimistic end."""
        for raw in _decimals("2.461538", "6.863905", "0.0000001", "12.421053", "0.399838"):
            assert _freeze_ready(raw) >= raw

    def test_it_lands_on_the_declared_quantum(self) -> None:
        assert _freeze_ready(Decimal("2.461538")) == Decimal("2.462")
        assert _freeze_ready(Decimal("0.871489")) == Decimal("0.872")

    def test_an_already_quantised_value_is_unchanged(self) -> None:
        """A re-freeze of the current table must not inflate it by a quantum."""
        assert _freeze_ready(Decimal("1.450")) == Decimal("1.450")
