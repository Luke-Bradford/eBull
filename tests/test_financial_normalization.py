"""Tests for financial normalization -- facts_raw -> periods_raw -> canonical."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.fundamentals import (
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

    def test_dei_fact_does_not_pollute_period_end(self) -> None:
        """Regression for #558.

        DEI facts (e.g. dei:EntityCommonStockSharesOutstanding) carry an
        "as-of" context endDate equal to the filing date — typically
        ~6 weeks AFTER the real fiscal period end. Previously
        ``_derive_periods_from_facts`` did
        ``period_end = max(f.period_end for f in period_facts)``, which
        let a DEI fact lift period_end to the filing date and produced
        a duplicate row in financial_periods on subsequent runs. The
        fix restricts boundary derivation to facts whose concept maps
        to a canonical column.
        """
        gaap_fact = _fact(
            concept="Revenues",
            val=Decimal("100"),
            period_end="2026-01-31",  # real Q4 end
            period_start="2025-11-01",
            fiscal_period="Q4",
            fiscal_year=2025,
            frame="CY2025Q4",
            accession_number="0001326380-26-000013",
            filed_date="2026-03-19",
        )
        dei_fact = _fact(
            concept="EntityCommonStockSharesOutstanding",  # not in _TAG_TO_COLUMN
            val=Decimal("268000000"),
            period_end="2026-03-18",  # filing-date pollution
            period_start=None,
            fiscal_period="Q4",
            fiscal_year=2025,
            frame=None,  # would be filtered as YTD-duration, but it's instant (no start)
            accession_number="0001326380-26-000013",
            filed_date="2026-03-19",
        )
        periods = _derive_periods_from_facts([gaap_fact, dei_fact], reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        # Real fiscal end, NOT 2026-03-18.
        assert p.period_end_date == date(2026, 1, 31)
        # Real period start preserved.
        assert p.period_start_date == date(2025, 11, 1)
        # Mapped column populated.
        assert p.revenue == Decimal("100")

    def test_group_with_only_unmapped_facts_is_skipped(self) -> None:
        """If a (fy, fp) group contains no facts mapped to a canonical
        column, it must NOT produce a row anchored on filing-date
        metadata. Skipping prevents spurious rows like
        ``period_end_date = filing date`` from appearing in
        financial_periods (#558).
        """
        only_dei = _fact(
            concept="EntityCommonStockSharesOutstanding",
            val=Decimal("1"),
            period_end="2026-03-18",
            period_start=None,
            fiscal_period="Q4",
            fiscal_year=2025,
            frame=None,
        )
        periods = _derive_periods_from_facts([only_dei], reported_currency="USD")
        assert periods == []

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

    def test_q4_derivation_skips_column_when_quarter_missing(self) -> None:
        """If Q2 lacks revenue but Q1+Q3+FY have it, Q4 revenue is NOT derived
        (it would be overstated if we treated missing as zero)."""
        facts = [
            _fact(
                concept="Revenues",
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            # Q2 has gross_profit but NOT revenue
            _fact(
                concept="GrossProfit",
                fiscal_period="Q2",
                val=Decimal("50"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                concept="Revenues",
                fiscal_period="Q3",
                val=Decimal("110"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                concept="Revenues",
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
        q4 = next(p for p in periods if p.period_type == "Q4")
        # Revenue should NOT be derived — Q2 is missing
        assert q4.revenue is None

    def test_q4_eps_derived_via_subtraction(self) -> None:
        """EPS Q4 = FY - Q1 - Q2 - Q3 (subtraction, not FY copy).
        FY copy would overstate TTM since TTM = Q1+Q2+Q3+Q4."""
        facts = [
            _fact(
                concept="EarningsPerShareDiluted",
                fiscal_period="Q1",
                val=Decimal("1.50"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
                unit="USD/shares",
            ),
            _fact(
                concept="EarningsPerShareDiluted",
                fiscal_period="Q2",
                val=Decimal("1.60"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
                unit="USD/shares",
            ),
            _fact(
                concept="EarningsPerShareDiluted",
                fiscal_period="Q3",
                val=Decimal("1.70"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
                unit="USD/shares",
            ),
            _fact(
                concept="EarningsPerShareDiluted",
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("6.50"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
                unit="USD/shares",
            ),
            # Need revenue facts so Q4 derivation triggers
            _fact(
                concept="Revenues",
                fiscal_period="Q1",
                val=Decimal("100"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                concept="Revenues",
                fiscal_period="Q2",
                val=Decimal("120"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                concept="Revenues",
                fiscal_period="Q3",
                val=Decimal("110"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                concept="Revenues",
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
        q4 = next(p for p in periods if p.period_type == "Q4")
        # EPS derived via subtraction: 6.50 - 1.50 - 1.60 - 1.70 = 1.70
        # (not FY copy which would make TTM = Q1+Q2+Q3+FY = 11.30)
        assert q4.eps_diluted == Decimal("1.70")


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


# ---------------------------------------------------------------------------
# #682: SEC re-stamps prior-year comparative XBRL facts under the FILING's
# (fiscal_year, fiscal_period) context. Pre-fix the normaliser collapsed all
# three years' rows into one ``(fy, fp)`` group and the iteration order
# picked the EARLIEST period_end's value as canonical — IEP's 2023 $6.00
# row landed as FY2025 dps_declared, which then drove a wrong Q4 = FY −
# YTD = $4.50 via _canonical_merge. The fix filters value attribution to
# facts whose ``period_end`` matches the canonical max for the group, and
# prefers the latest ``filed_date`` on restatement ties.
# ---------------------------------------------------------------------------


class TestPriorYearComparativeMisattribution:
    def _ten_k_with_three_comparative_years(self) -> list[FactRow]:
        """Mirrors the IEP CIK 0000813762 case from issue #682: a 10-K
        filed 2026-02-26 emits the same XBRL concept three times under
        ``fy=2025/fp=FY``, one for each comparative year.
        """
        return [
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("6.00"),  # comparative two years prior
                period_end="2023-12-31",
                period_start="2023-01-01",
                frame=None,  # SEC frame is missing on prior-year comparatives
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                filed_date="2026-02-26",
                accession_number="0001104659-26-019821",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("3.50"),  # comparative one year prior
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame=None,
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                filed_date="2026-02-26",
                accession_number="0001104659-26-019821",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("2.00"),  # actual current FY value
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                filed_date="2026-02-26",
                accession_number="0001104659-26-019821",
            ),
        ]

    def test_canonical_fy_value_comes_from_max_period_end(self) -> None:
        """Acceptance criterion from issue #682: only the
        ``period_end=2025-12-31`` row drives the canonical FY 2025 row.
        """
        periods = _derive_periods_from_facts(
            self._ten_k_with_three_comparative_years(),
            reported_currency="USD",
        )

        fy_rows = [p for p in periods if p.period_type == "FY"]
        assert len(fy_rows) == 1
        fy = fy_rows[0]
        assert fy.fiscal_year == 2025
        assert fy.period_end_date == date(2025, 12, 31)
        assert fy.period_start_date == date(2025, 1, 1)
        assert fy.dps_declared == Decimal("2.00")
        assert fy.months_covered == 12

    def test_comparative_year_facts_do_not_pollute_source_ref(self) -> None:
        """Provenance for the FY row comes only from the accession that
        actually contributed values — the comparative rows' accession
        does not leak into ``source_ref`` for the canonical row (in
        this fixture all three rows are from the same accession, so
        the dedup yields a single accession either way; this test
        guards against future fixtures where comparatives come from a
        prior filing's accession).
        """
        facts = self._ten_k_with_three_comparative_years()
        # Rewrite the comparative rows to a different (older) accession
        # so a leak would show up in source_ref.
        facts[0] = FactRow(
            concept=facts[0].concept,
            unit=facts[0].unit,
            period_start=facts[0].period_start,
            period_end=facts[0].period_end,
            val=facts[0].val,
            frame=facts[0].frame,
            form_type=facts[0].form_type,
            fiscal_year=facts[0].fiscal_year,
            fiscal_period=facts[0].fiscal_period,
            accession_number="prior-10k-accn",
            filed_date=facts[0].filed_date,
        )

        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        assert "prior-10k-accn" not in fy.source_ref
        assert fy.source_ref == "0001104659-26-019821"


class TestRestatementPicksLatestFiledDate:
    def test_two_filings_same_period_end_latest_wins(self) -> None:
        """When two facts share ``(period_end, concept)`` from
        different accessions / filed_dates (a 10-K and a later 10-K/A
        amendment), the canonical row uses the value from the LATEST
        ``filed_date`` — restatement contract from issue #682.
        """
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("100"),
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="orig-10k",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="Revenues",
                val=Decimal("110"),  # restated
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K/A",
                accession_number="amend-10k-a",
                filed_date="2026-04-15",
            ),
        ]

        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        assert fy.revenue == Decimal("110")
        assert fy.form_type == "10-K/A"
        assert fy.filed_date == date(2026, 4, 15)


class TestPriorYearComparativeWithFrame:
    """Codex pre-flight: the previous fixture's comparative rows had
    ``frame=None``, which the YTD-disambiguation prefilter at line ~777
    drops before grouping — so ``canonical_facts = period_end == max(...)``
    was never actually exercised. This class covers the case where
    SEC restamps comparative rows WITH ``frame`` populated, so they
    survive the prefilter and reach the new filter."""

    def test_framed_comparatives_under_same_fy_fp_filtered_out(self) -> None:
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("1000"),  # comparative
                period_end="2023-12-31",
                period_start="2023-01-01",
                frame="CY2023",  # framed → survives YTD prefilter
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="Revenues",
                val=Decimal("2000"),  # comparative
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="Revenues",
                val=Decimal("3000"),  # current FY
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
        ]

        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        assert fy.fiscal_year == 2025
        assert fy.period_end_date == date(2025, 12, 31)
        assert fy.revenue == Decimal("3000")  # NOT 1000 (would be the bug)


class TestRestatementSameFiledDateTieBreaker:
    """Codex pre-flight: when two filings restate the same period
    but happen to share ``filed_date`` (rare but possible — a
    same-day 10-K and 10-K/A correction), the tiebreak should be
    deterministic. Sorting by ``(filed_date, accession_number) DESC``
    breaks ties on accession_number, which is the only other
    deterministic identifier available at fact level."""

    def test_same_filed_date_picks_higher_accession(self) -> None:
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("100"),
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="0000000000-26-000001",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="Revenues",
                val=Decimal("110"),
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K/A",
                accession_number="0000000000-26-000099",  # higher accession
                filed_date="2026-02-26",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        # Higher accession_number wins on tied filed_date — deterministic.
        assert fy.revenue == Decimal("110")
        assert fy.form_type == "10-K/A"


class TestQ4DerivationAfterCanonicalFix:
    def test_iep_shape_q4_dps_derives_to_correct_value(self) -> None:
        """End-to-end IEP-shape regression: with the canonical FY
        value at $2.00 (post-fix) and three quarterly $0.50 facts,
        Q4 derivation produces $0.50, not $4.50.
        """
        facts: list[FactRow] = [
            # Three quarterly facts at $0.50 each.
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("0.50"),
                period_end="2025-03-31",
                period_start="2025-01-01",
                frame="CY2025Q1",
                fiscal_year=2025,
                fiscal_period="Q1",
                form_type="10-Q",
                accession_number="q1-2025",
                filed_date="2025-05-01",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("0.50"),
                period_end="2025-06-30",
                period_start="2025-04-01",
                frame="CY2025Q2",
                fiscal_year=2025,
                fiscal_period="Q2",
                form_type="10-Q",
                accession_number="q2-2025",
                filed_date="2025-08-01",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("0.50"),
                period_end="2025-09-30",
                period_start="2025-07-01",
                frame="CY2025Q3",
                fiscal_year=2025,
                fiscal_period="Q3",
                form_type="10-Q",
                accession_number="q3-2025",
                filed_date="2025-11-01",
            ),
            # FY 10-K with three comparative-year FY rows under fy=2025/fp=FY.
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("6.00"),
                period_end="2023-12-31",
                period_start="2023-01-01",
                frame=None,
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("3.50"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame=None,
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
            _fact(
                concept="CommonStockDividendsPerShareDeclared",
                val=Decimal("2.00"),
                period_end="2025-12-31",
                period_start="2025-01-01",
                frame="CY2025",
                fiscal_year=2025,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="fy-2025-10k",
                filed_date="2026-02-26",
            ),
        ]

        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods if p.fiscal_year == 2025}

        assert by_type["FY"].dps_declared == Decimal("2.00")
        assert by_type["Q1"].dps_declared == Decimal("0.50")
        assert by_type["Q2"].dps_declared == Decimal("0.50")
        assert by_type["Q3"].dps_declared == Decimal("0.50")
        # Q4 is derived: FY (2.00) - Q1+Q2+Q3 (1.50) = 0.50.
        assert "Q4" in by_type
        q4 = by_type["Q4"]
        assert q4.is_derived is True
        assert q4.dps_declared == Decimal("0.50")
