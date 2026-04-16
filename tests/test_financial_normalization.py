"""Tests for financial normalization -- facts_raw -> periods_raw -> canonical."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.financial_normalization import (
    FactRow,
    _derive_periods_from_facts,
)


def _fact(
    *,
    concept: str = "Revenues",
    val: Decimal = Decimal("50000000"),
    period_end: str = "2024-03-31",
    period_start: str | None = "2024-01-01",
    frame: str | None = "CY2024Q1",
    form_type: str = "10-Q",
    fiscal_year: int = 2024,
    fiscal_period: str = "Q1",
    accession_number: str = "accn-q1",
    filed_date: str = "2024-05-01",
    unit: str = "USD",
) -> FactRow:
    return FactRow(
        concept=concept,
        unit=unit,
        period_start=date.fromisoformat(period_start) if period_start else None,
        period_end=date.fromisoformat(period_end),
        val=val,
        frame=frame,
        form_type=form_type,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period,
        accession_number=accession_number,
        filed_date=date.fromisoformat(filed_date),
    )


class TestDerivePeriodsFromFacts:
    def test_single_quarter_revenue(self) -> None:
        """A single Q1 revenue fact produces one period row with revenue populated."""
        facts = [_fact()]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.period_type == "Q1"
        assert p.fiscal_year == 2024
        assert p.fiscal_quarter == 1
        assert p.revenue == Decimal("50000000")
        assert p.period_end_date == date(2024, 3, 31)
        assert p.period_start_date == date(2024, 1, 1)
        assert p.months_covered == 3
        assert p.source == "sec_edgar"
        assert not p.is_derived

    def test_multiple_concepts_same_period(self) -> None:
        """Multiple concepts for the same period merge into one period row."""
        facts = [
            _fact(concept="Revenues", val=Decimal("100000000")),
            _fact(concept="GrossProfit", val=Decimal("40000000")),
            _fact(concept="NetIncomeLoss", val=Decimal("20000000")),
            _fact(
                concept="Assets",
                val=Decimal("500000000"),
                period_start=None,
                frame=None,
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.revenue == Decimal("100000000")
        assert p.gross_profit == Decimal("40000000")
        assert p.net_income == Decimal("20000000")
        assert p.total_assets == Decimal("500000000")

    def test_fy_period_type(self) -> None:
        facts = [
            _fact(
                fiscal_period="FY",
                fiscal_year=2024,
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="accn-fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        assert periods[0].period_type == "FY"
        assert periods[0].months_covered == 12

    def test_derives_q4_from_fy_minus_quarters(self) -> None:
        """Q4 = FY - Q1 - Q2 - Q3 when Q4 not directly filed."""
        facts = [
            _fact(
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                fiscal_period="Q2",
                val=Decimal("120"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                fiscal_period="Q3",
                val=Decimal("110"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("500"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 1
        q4 = q4_periods[0]
        assert q4.revenue == Decimal("170")  # 500 - 100 - 120 - 110
        assert q4.is_derived is True
        assert q4.fiscal_year == 2024
        assert q4.fiscal_quarter == 4

    def test_skips_ytd_entries(self) -> None:
        """Entries without frame (YTD cumulative) are excluded -- we only want
        standalone quarterly or annual values identified by frame."""
        facts = [
            _fact(frame="CY2024Q1"),  # standalone quarter -- include
            _fact(
                frame=None,
                period_end="2024-06-30",
                period_start="2024-01-01",
                fiscal_period="Q2",
                accession_number="ytd-q2",
            ),  # YTD Q1+Q2 cumulative -- exclude
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        # Only the framed Q1 should produce a period
        assert len(periods) == 1
        assert periods[0].period_type == "Q1"

    def test_tag_priority_picks_first_match(self) -> None:
        """When multiple tags map to the same concept (e.g. revenue),
        the highest-priority tag's value is used."""
        facts = [
            _fact(
                concept="RevenueFromContractWithCustomerExcludingAssessedTax",
                val=Decimal("100"),
            ),
            _fact(concept="Revenues", val=Decimal("95")),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        # ASC 606 tag has priority
        assert periods[0].revenue == Decimal("100")


class TestDeriveQ4EdgeCases:
    def test_no_q4_derivation_without_all_three_quarters(self) -> None:
        """If Q1+Q2 exist but Q3 is missing, no Q4 is derived."""
        facts = [
            _fact(
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                fiscal_period="Q2",
                val=Decimal("120"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("500"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 0

    def test_no_q4_derivation_when_q4_exists(self) -> None:
        """If Q4 is directly filed, no derivation needed."""
        facts = [
            _fact(
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                fiscal_period="Q2",
                val=Decimal("120"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                fiscal_period="Q3",
                val=Decimal("110"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                fiscal_period="Q4",
                val=Decimal("170"),
                period_end="2024-12-31",
                period_start="2024-10-01",
                frame="CY2024Q4",
                accession_number="q4",
            ),
            _fact(
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("500"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4_periods = [p for p in periods if p.period_type == "Q4"]
        assert len(q4_periods) == 1
        assert q4_periods[0].revenue == Decimal("170")
        assert q4_periods[0].is_derived is False

    def test_derived_q4_balance_sheet_uses_fy(self) -> None:
        """Derived Q4 balance sheet = FY balance sheet (same point-in-time)."""
        facts = [
            _fact(
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                fiscal_period="Q2",
                val=Decimal("120"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                fiscal_period="Q3",
                val=Decimal("110"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("500"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
            ),
            # Balance sheet fact on FY
            _fact(
                concept="Assets",
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("999"),
                period_end="2024-12-31",
                period_start=None,
                frame=None,
                form_type="10-K",
                accession_number="fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4 = next(p for p in periods if p.period_type == "Q4")
        assert q4.total_assets == Decimal("999")


class TestMultiYearNormalization:
    def test_multiple_fiscal_years(self) -> None:
        """Facts from FY2023 and FY2024 produce separate periods."""
        facts = [
            _fact(
                fiscal_period="Q1",
                fiscal_year=2023,
                val=Decimal("80"),
                period_end="2023-03-31",
                period_start="2023-01-01",
                frame="CY2023Q1",
                accession_number="q1-2023",
            ),
            _fact(
                fiscal_period="Q1",
                fiscal_year=2024,
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1-2024",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 2
        years = {p.fiscal_year for p in periods}
        assert years == {2023, 2024}
