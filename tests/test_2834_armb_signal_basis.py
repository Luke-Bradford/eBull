"""Pure-logic tests for #2834 ARM B blocker 3's displacement arithmetic.

No DB. The measurement is a full-population read whose evidence is the script's
own output on the corpus; what is pinned here is the arithmetic that decides
PASS vs FAIL, because both of its failure directions are silent:

* a divide-by-zero guard returning 100% would FAIL an empty formation;
* averaging the per-formation percentages instead of pooling would weight the
  1990s (mean cross-section ~2,088) the same as the 2020s (~7,983).
"""

from __future__ import annotations

import io
from datetime import date

import pytest

from scripts.measure_2834_armb_signal_basis import (
    _DISAGREEMENT_BAR_PCT,
    Formation,
    _report,
    expected_months,
)


class TestExpectedMonths:
    """The denominator the formation count is reported against.

    312 formations means nothing without it; 312 of 329 says 17 months produced
    no usable cross-section, which is the conditioning "full population" would
    otherwise conceal.
    """

    def test_counts_month_starts_in_a_half_open_range(self) -> None:
        assert expected_months(date(1994, 1, 1), date(2021, 6, 29)) == 329

    def test_same_month_is_zero(self) -> None:
        assert expected_months(date(2020, 3, 1), date(2020, 3, 31)) == 0

    def test_crosses_a_year_boundary(self) -> None:
        assert expected_months(date(2019, 11, 1), date(2020, 2, 1)) == 3


def _formation(**overrides: object) -> Formation:
    base: dict[str, object] = {
        "bar_date": date(2015, 6, 1),
        "cross_section": 1000,
        "decile_size": 100,
        "shared": 90,
        "close_only": 10,
        "adj_only": 10,
    }
    base.update(overrides)
    return Formation(**base)  # type: ignore[arg-type]


class TestDisagreementPct:
    def test_ordinary_ratio(self) -> None:
        assert _formation().disagreement_pct == pytest.approx(10.0)

    def test_empty_decile_is_zero_not_an_error(self) -> None:
        """A formation whose decile cut is empty displaced nothing.

        Zero is also the conservative direction: an unmeasurable formation must
        not push the pooled figure toward the FAIL bar.
        """
        assert _formation(decile_size=0, close_only=0).disagreement_pct == 0.0


class TestReport:
    def test_pooled_is_decile_weighted_not_a_mean_of_percentages(self) -> None:
        """The trap this measurement is most exposed to.

        A small early formation at 50% and a large late one at 10% pool to
        (5 + 50) / (10 + 500) = 10.78%, not to the 30.0% a mean of the two
        percentages would report. The corpus grows ~4x across the window, so
        the two disagree on real data and only the pooled figure answers
        "how much of the decile is wrong".
        """
        formations = [
            _formation(bar_date=date(1995, 1, 3), decile_size=10, close_only=5, adj_only=5),
            _formation(bar_date=date(2019, 1, 2), decile_size=500, close_only=50, adj_only=50),
        ]
        pooled = _report(formations, stream=io.StringIO())
        assert pooled == pytest.approx(100.0 * 55 / 510)
        assert pooled != pytest.approx(30.0)

    def test_no_formations_reports_zero_rather_than_dividing(self) -> None:
        assert _report([], stream=io.StringIO()) == 0.0

    def test_asymmetry_is_reported_so_a_broken_invariant_is_visible(self) -> None:
        """Both deciles are cut at ``n // 10`` from ONE shared cross-section, so
        ``close_only`` and ``adj_only`` are equal by construction. The report
        prints their absolute difference precisely so that if the shared-panel
        invariant ever breaks, the reader sees it instead of reading a
        displacement figure that has quietly changed meaning.
        """
        stream = io.StringIO()
        _report([_formation(close_only=10, adj_only=7)], stream=stream)
        assert "arm asymmetry (must be 0)   3" in stream.getvalue()

    def test_decade_rows_partition_the_formations(self) -> None:
        stream = io.StringIO()
        _report(
            [
                _formation(bar_date=date(1995, 1, 3)),
                _formation(bar_date=date(1999, 1, 4)),
                _formation(bar_date=date(2005, 1, 3)),
            ],
            stream=stream,
        )
        output = stream.getvalue()
        assert "  1990" in output
        assert "  2000" in output
        assert "  2010" not in output


class TestBar:
    def test_bar_is_low_because_the_arms_differ_only_by_corporate_actions(self) -> None:
        """Guards the constant against a later loosening that would hide the finding.

        The two arms score the same rule over the same names on two price bases,
        so any large disagreement IS the result. A bar above a few percent would
        let the measured 14.94% report as a PASS.
        """
        assert _DISAGREEMENT_BAR_PCT <= 2.0
