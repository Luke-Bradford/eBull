"""Pure-logic tests for #2834 ARM B step 0's coverage arithmetic.

No DB. The measurement itself is a full-population read whose evidence is the
script's own output on the corpus; what is pinned here is the arithmetic that
decides PASS vs FAIL, because a silent divide-by-zero would report 0% coverage
(a FAIL) and a silent 100% would report a PASS on an empty month.
"""

from __future__ import annotations

from datetime import date

import pytest

from scripts.measure_2834_armb_weighting_basis import (
    COVERAGE_BAR_PCT,
    FormationCoverage,
    pct,
)


class TestPct:
    def test_ordinary_ratio(self) -> None:
        assert pct(25.0, 100.0) == pytest.approx(25.0)

    @pytest.mark.parametrize("denominator", [0.0, -1.0])
    def test_non_positive_denominator_is_zero_not_an_error(self, denominator: float) -> None:
        """A formation month with no dollar volume is coverage of nothing.

        Reporting 0.0 makes it FAIL the bar, which is the conservative direction:
        an unmeasurable month must not be counted as covered.
        """
        assert pct(5.0, denominator) == 0.0


def _coverage(**overrides: object) -> FormationCoverage:
    base: dict[str, object] = {
        "month_start": date(2015, 6, 1),
        "formation_date": date(2015, 6, 30),
        "series_alive": 100,
        "series_any": 40,
        "series_fresh": 20,
        "dv_total": 1000.0,
        "dv_any": 400.0,
        "dv_fresh": 200.0,
    }
    base.update(overrides)
    return FormationCoverage(**base)  # type: ignore[arg-type]


class TestFormationCoverage:
    def test_percentages_use_their_own_denominators(self) -> None:
        cov = _coverage()
        assert cov.dv_any_pct == pytest.approx(40.0)
        assert cov.dv_fresh_pct == pytest.approx(20.0)
        assert cov.series_fresh_pct == pytest.approx(20.0)

    def test_fresh_never_exceeds_any_for_a_well_formed_row(self) -> None:
        """The fresh arm is a strict subset of the any arm by construction —
        the SQL's fresh predicate conjoins the any predicate with a lower bound.
        A row violating this would mean the two EXISTS clauses had drifted apart."""
        cov = _coverage()
        assert cov.dv_fresh_pct <= cov.dv_any_pct

    def test_empty_month_reports_zero_rather_than_dividing_by_zero(self) -> None:
        cov = _coverage(series_alive=0, series_any=0, series_fresh=0, dv_total=0.0, dv_any=0.0, dv_fresh=0.0)
        assert cov.dv_any_pct == 0.0
        assert cov.dv_fresh_pct == 0.0
        assert cov.series_fresh_pct == 0.0

    def test_bar_is_2834s_declared_eighty_percent(self) -> None:
        """The bar is read off the ticket, not chosen here. Pinned so a later
        edit that loosened it would have to change a test that says why."""
        assert COVERAGE_BAR_PCT == 80.0
