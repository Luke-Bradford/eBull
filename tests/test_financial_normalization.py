"""Tests for financial normalization -- facts_raw -> periods_raw -> canonical."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from app.services.fundamentals import (
    FactRow,
    _derive_periods_from_facts,
    _is_plausible_fiscal_year,
    _resolve_period_fiscal_year,
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
        """#1835 — YTD cumulatives are excluded by DURATION, not by frame. A
        6-month (Q1+Q2 cumulative) duration fact tagged fp=Q2 falls outside the
        quarter window [60,120] days and is dropped; the standalone 3-month Q1
        fact is kept."""
        facts = [
            _fact(frame="CY2024Q1"),  # standalone 3mo quarter (~90d) -- include
            _fact(
                frame="CY2024Q2",  # frame present, but 6mo duration -- still excluded
                period_end="2024-06-30",
                period_start="2024-01-01",
                fiscal_period="Q2",
                accession_number="ytd-q2",
            ),  # YTD Q1+Q2 cumulative (~181d) -- exclude by duration
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        # Only the 3-month Q1 should produce a period
        assert len(periods) == 1
        assert periods[0].period_type == "Q1"

    def test_fy_binds_annual_fact_without_frame(self) -> None:
        """#1835 regression — an annual (~12-month) flow fact with frame=None is
        bound to the FY row (the SEC Frames label lands on the next year's
        comparative re-stamp, so 43% of genuine annual facts carry frame=NULL
        and were previously dropped, leaving FY revenue NULL)."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("391035000000"),
                fiscal_period="FY",
                fiscal_year=2024,
                period_start="2023-10-01",
                period_end="2024-09-28",  # ~363 days
                frame=None,
                form_type="10-K",
                accession_number="fy2024",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        assert periods[0].period_type == "FY"
        assert periods[0].revenue == Decimal("391035000000")
        assert periods[0].months_covered == 12

    def test_fy_rejects_quarter_duration_mislabeled_fy(self) -> None:
        """#1835 regression (failure mode 2) — a 3-month flow fact mislabeled
        fp=FY (legacy 8-K facts carry a quarterly frame) must NOT populate the FY
        flow column. With no annual-duration fact in the group, no FY row with a
        bound revenue is produced."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("64698000000"),  # a single-quarter magnitude
                fiscal_period="FY",
                fiscal_year=2020,
                period_start="2020-06-28",
                period_end="2020-09-26",  # ~90 days, mislabeled FY
                frame="CY2020Q4",
                form_type="10-K",
                accession_number="q4-as-fy",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        # The lone quarter-duration fact is rejected for the FY group and there
        # is no other fact to anchor any row, so NO period is produced at all.
        # Assert the full emptiness (not a vacuous `all(... for p in periods)`
        # over an empty list, which would pass even if the quarter magnitude had
        # wrongly bound). This also proves the 64698000000 magnitude never
        # surfaces as revenue, since there is no period to carry it.
        assert periods == []

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
        """#682 invariant: the FY2025 row's value comes ONLY from its own
        ``period_end=2025-12-31`` ($2.00), never the earliest comparative.

        #1914: the comparative FY2023/FY2024 rows are now ALSO recovered as
        their own rows (their own period_end + own value), instead of being
        discarded — so multi-year views no longer show blank prior years.
        """
        periods = _derive_periods_from_facts(
            self._ten_k_with_three_comparative_years(),
            reported_currency="USD",
        )

        fy_rows = {p.fiscal_year: p for p in periods if p.period_type == "FY"}
        # #1914 — all three comparative years recovered from the one 10-K.
        assert set(fy_rows) == {2023, 2024, 2025}
        # #682 — the primary FY2025 row keeps its OWN value, not the earliest
        # comparative ($6.00 from 2023).
        fy25 = fy_rows[2025]
        assert fy25.period_end_date == date(2025, 12, 31)
        assert fy25.period_start_date == date(2025, 1, 1)
        assert fy25.dps_declared == Decimal("2.00")
        assert fy25.months_covered == 12
        # #1914 — each comparative carries its own year's value + period_end.
        assert fy_rows[2024].period_end_date == date(2024, 12, 31)
        assert fy_rows[2024].dps_declared == Decimal("3.50")
        assert fy_rows[2023].period_end_date == date(2023, 12, 31)
        assert fy_rows[2023].dps_declared == Decimal("6.00")

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
        # The PRIMARY FY row (period_end 2025-12-31) draws provenance only from the
        # accession that contributed its value — the comparative's accession never
        # leaks in. (#1914: the comparative is now its own row, so target the
        # primary end explicitly.)
        fy = next(p for p in periods if p.period_type == "FY" and p.period_end_date == date(2025, 12, 31))
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
        fy_rows = {p.fiscal_year: p for p in periods if p.period_type == "FY"}
        # #682 — the primary FY2025 row takes its OWN period_end's value (3000),
        # never the earliest framed comparative (1000).
        assert fy_rows[2025].period_end_date == date(2025, 12, 31)
        assert fy_rows[2025].revenue == Decimal("3000")  # NOT 1000 (would be the bug)
        # #1914 — the framed comparatives are recovered as their own FY rows.
        assert fy_rows[2024].revenue == Decimal("2000")
        assert fy_rows[2023].revenue == Decimal("1000")


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


# ---------------------------------------------------------------------------
# #731: project four us-gaap balance-sheet concepts (treasury_shares,
# shares_authorized, shares_issued, retained_earnings) into the canonical
# financial_periods table. Each test seeds a balance-sheet (instant) fact
# alongside a revenue fact so the period row anchors on the fiscal end and
# the new column populates via the existing _TAG_TO_COLUMN dispatch.
# ---------------------------------------------------------------------------


class TestOwnershipColumnProjection:
    def _balance_sheet_fact(
        self,
        *,
        concept: str,
        val: Decimal,
        period_end: str = "2024-12-31",
        fiscal_year: int = 2024,
        fiscal_period: str = "FY",
        accession_number: str = "fy",
        unit: str = "shares",
    ) -> FactRow:
        """Balance-sheet items are point-in-time facts: instant context
        (period_start IS NULL), no frame, on the fiscal year-end."""
        return _fact(
            concept=concept,
            val=val,
            period_end=period_end,
            period_start=None,
            frame=None,
            fiscal_period=fiscal_period,
            fiscal_year=fiscal_year,
            accession_number=accession_number,
            form_type="10-K",
            unit=unit,
        )

    def _anchor_fact(self, *, fiscal_year: int = 2024, period_end: str = "2024-12-31") -> FactRow:
        """A revenue fact ensures the (fy, fp) group has a flow-item
        anchor on the fiscal year-end. Without it the balance-sheet
        fact alone could anchor the period."""
        return _fact(
            concept="Revenues",
            val=Decimal("1000000"),
            period_end=period_end,
            period_start=f"{fiscal_year}-01-01",
            frame=f"CY{fiscal_year}",
            fiscal_period="FY",
            fiscal_year=fiscal_year,
            accession_number="fy",
            form_type="10-K",
        )

    def test_treasury_stock_shares_alias(self) -> None:
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="TreasuryStockShares",
                val=Decimal("12500000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        assert periods[0].treasury_shares == Decimal("12500000")

    def test_treasury_stock_common_shares_fallback(self) -> None:
        """``TreasuryStockCommonShares`` is the second-priority alias —
        it must map when ``TreasuryStockShares`` is absent."""
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="TreasuryStockCommonShares",
                val=Decimal("8000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].treasury_shares == Decimal("8000000")

    def test_treasury_priority_order(self) -> None:
        """``TreasuryStockShares`` outranks ``TreasuryStockCommonShares``."""
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="TreasuryStockShares",
                val=Decimal("12500000"),
            ),
            self._balance_sheet_fact(
                concept="TreasuryStockCommonShares",
                val=Decimal("8000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].treasury_shares == Decimal("12500000")

    def test_shares_authorized_alias(self) -> None:
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="CommonStockSharesAuthorized",
                val=Decimal("5000000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].shares_authorized == Decimal("5000000000")

    def test_shares_issued_alias(self) -> None:
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="CommonStockSharesIssued",
                val=Decimal("1750000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].shares_issued == Decimal("1750000000")

    def test_retained_earnings_alias(self) -> None:
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="RetainedEarningsAccumulatedDeficit",
                val=Decimal("180000000000"),
                unit="USD",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].retained_earnings == Decimal("180000000000")

    def test_all_four_columns_populate_together(self) -> None:
        """End-to-end smoke: a single FY group with all four ownership
        facts populates every new column on the canonical row."""
        facts = [
            self._anchor_fact(),
            self._balance_sheet_fact(
                concept="TreasuryStockShares",
                val=Decimal("12500000"),
            ),
            self._balance_sheet_fact(
                concept="CommonStockSharesAuthorized",
                val=Decimal("5000000000"),
            ),
            self._balance_sheet_fact(
                concept="CommonStockSharesIssued",
                val=Decimal("1750000000"),
            ),
            self._balance_sheet_fact(
                concept="RetainedEarningsAccumulatedDeficit",
                val=Decimal("180000000000"),
                unit="USD",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.treasury_shares == Decimal("12500000")
        assert p.shares_authorized == Decimal("5000000000")
        assert p.shares_issued == Decimal("1750000000")
        assert p.retained_earnings == Decimal("180000000000")

    def test_q4_balance_sheet_inherits_fy_ownership_columns(self) -> None:
        """Derived Q4 balance sheet copies FY values for all
        ``_BALANCE_SHEET_COLUMNS`` entries (point-in-time = same
        fiscal year-end). Confirms the four new columns participate
        in the Q4 derivation copy loop."""
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
            self._balance_sheet_fact(
                concept="TreasuryStockShares",
                val=Decimal("12500000"),
            ),
            self._balance_sheet_fact(
                concept="RetainedEarningsAccumulatedDeficit",
                val=Decimal("180000000000"),
                unit="USD",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4 = next(p for p in periods if p.period_type == "Q4")
        assert q4.is_derived is True
        assert q4.treasury_shares == Decimal("12500000")
        assert q4.retained_earnings == Decimal("180000000000")


# ---------------------------------------------------------------------------
# #732: Tier 1 + Tier 2 allowlist expansion. Ten new aliases project into
# financial_periods. Four flow items (comprehensive_income,
# intangible_amortization, deferred_income_tax, other_nonoperating_income),
# and six point-in-time items (assets_current, liabilities_current,
# cash_restricted, additional_paid_in_capital, accumulated_oci,
# antidilutive_securities — the last is a weighted-average share count
# kept as point-in-time so the Q4 derivation copies the FY value forward
# instead of subtracting Q1+Q2+Q3 from FY).
# ---------------------------------------------------------------------------


class TestTier1Tier2AllowlistProjection:
    def _bs_fact(
        self,
        *,
        concept: str,
        val: Decimal,
        period_end: str = "2024-12-31",
        fiscal_year: int = 2024,
        fiscal_period: str = "FY",
        accession_number: str = "fy",
        unit: str = "USD",
    ) -> FactRow:
        return _fact(
            concept=concept,
            val=val,
            period_end=period_end,
            period_start=None,
            frame=None,
            fiscal_period=fiscal_period,
            fiscal_year=fiscal_year,
            accession_number=accession_number,
            form_type="10-K",
            unit=unit,
        )

    def _fy_anchor(self, fiscal_year: int = 2024) -> FactRow:
        return _fact(
            concept="Revenues",
            val=Decimal("1000000"),
            period_end=f"{fiscal_year}-12-31",
            period_start=f"{fiscal_year}-01-01",
            frame=f"CY{fiscal_year}",
            fiscal_period="FY",
            fiscal_year=fiscal_year,
            accession_number="fy",
            form_type="10-K",
        )

    def test_tier1_balance_sheet_aliases(self) -> None:
        """AssetsCurrent / LiabilitiesCurrent / CashCashEquivalentsRestrictedCash
        all project to their canonical columns from a single FY balance
        sheet."""
        facts = [
            self._fy_anchor(),
            self._bs_fact(concept="AssetsCurrent", val=Decimal("450000000")),
            self._bs_fact(concept="LiabilitiesCurrent", val=Decimal("320000000")),
            self._bs_fact(
                concept="CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
                val=Decimal("85000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert len(periods) == 1
        p = periods[0]
        assert p.assets_current == Decimal("450000000")
        assert p.liabilities_current == Decimal("320000000")
        assert p.cash_restricted == Decimal("85000000")

    def test_cash_restricted_separate_from_cash(self) -> None:
        """CashAndCashEquivalentsAtCarryingValue → cash; the FASB
        ASU 2016-18 concept → cash_restricted. The two columns must
        not collide — they are distinct concepts and an issuer can
        emit both."""
        facts = [
            self._fy_anchor(),
            self._bs_fact(
                concept="CashAndCashEquivalentsAtCarryingValue",
                val=Decimal("60000000"),
            ),
            self._bs_fact(
                concept="CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
                val=Decimal("85000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        p = periods[0]
        assert p.cash == Decimal("60000000")
        assert p.cash_restricted == Decimal("85000000")

    def test_tier1_flow_aliases(self) -> None:
        """ComprehensiveIncomeNetOfTax + AmortizationOfIntangibleAssets
        project into the FY row alongside the revenue anchor."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("1000000"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
            _fact(
                concept="ComprehensiveIncomeNetOfTax",
                val=Decimal("220000"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
            _fact(
                concept="AmortizationOfIntangibleAssets",
                val=Decimal("18000"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        p = periods[0]
        assert p.comprehensive_income == Decimal("220000")
        assert p.intangible_amortization == Decimal("18000")

    def test_tier2_flow_aliases(self) -> None:
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("1000000"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
            _fact(
                concept="DeferredIncomeTaxExpenseBenefit",
                val=Decimal("12000"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
            _fact(
                concept="OtherNonoperatingIncomeExpense",
                val=Decimal("-3500"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                fiscal_period="FY",
                fiscal_year=2024,
                accession_number="fy",
                form_type="10-K",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        p = periods[0]
        assert p.deferred_income_tax == Decimal("12000")
        assert p.other_nonoperating_income == Decimal("-3500")

    def test_tier2_balance_sheet_aliases(self) -> None:
        facts = [
            self._fy_anchor(),
            self._bs_fact(
                concept="AdditionalPaidInCapital",
                val=Decimal("250000000"),
            ),
            self._bs_fact(
                concept="AccumulatedOtherComprehensiveIncomeLossNetOfTax",
                val=Decimal("-15000000"),
            ),
            self._bs_fact(
                concept="AntidilutiveSecuritiesExcludedFromComputationOfEarningsPerShareAmount",
                val=Decimal("4500000"),
                unit="shares",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        p = periods[0]
        assert p.additional_paid_in_capital == Decimal("250000000")
        assert p.accumulated_oci == Decimal("-15000000")
        assert p.antidilutive_securities == Decimal("4500000")

    def test_q4_derivation_subtracts_flow_columns(self) -> None:
        """Tier 1 + Tier 2 flow items participate in the Q4 = FY -
        Q1+Q2+Q3 subtraction. comprehensive_income is the canonical
        one to pin (most-frequent flow concept in the Tier 1 audit)."""
        facts = [
            _fact(
                concept="ComprehensiveIncomeNetOfTax",
                fiscal_period="Q1",
                val=Decimal("50"),
                period_end="2024-03-31",
                period_start="2024-01-01",
                frame="CY2024Q1",
                accession_number="q1",
            ),
            _fact(
                concept="ComprehensiveIncomeNetOfTax",
                fiscal_period="Q2",
                val=Decimal("60"),
                period_end="2024-06-30",
                period_start="2024-04-01",
                frame="CY2024Q2",
                accession_number="q2",
            ),
            _fact(
                concept="ComprehensiveIncomeNetOfTax",
                fiscal_period="Q3",
                val=Decimal("55"),
                period_end="2024-09-30",
                period_start="2024-07-01",
                frame="CY2024Q3",
                accession_number="q3",
            ),
            _fact(
                concept="ComprehensiveIncomeNetOfTax",
                fiscal_period="FY",
                fiscal_year=2024,
                val=Decimal("250"),
                period_end="2024-12-31",
                period_start="2024-01-01",
                frame="CY2024",
                form_type="10-K",
                accession_number="fy",
            ),
            # Revenue facts so Q4 derivation triggers (driven by FY +
            # all three quarters present for the canonical row).
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
        assert q4.is_derived is True
        # 250 - 50 - 60 - 55 = 85
        assert q4.comprehensive_income == Decimal("85")

    def test_q4_balance_sheet_inherits_fy_for_tier1_tier2(self) -> None:
        """Tier 1 / Tier 2 point-in-time columns (assets_current,
        liabilities_current, additional_paid_in_capital, etc.) inherit
        the FY value on Q4 derivation — same handling as total_assets."""
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
            self._bs_fact(
                concept="AssetsCurrent",
                val=Decimal("450000000"),
            ),
            self._bs_fact(
                concept="LiabilitiesCurrent",
                val=Decimal("320000000"),
            ),
            self._bs_fact(
                concept="AdditionalPaidInCapital",
                val=Decimal("250000000"),
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q4 = next(p for p in periods if p.period_type == "Q4")
        assert q4.is_derived is True
        assert q4.assets_current == Decimal("450000000")
        assert q4.liabilities_current == Decimal("320000000")
        assert q4.additional_paid_in_capital == Decimal("250000000")


class TestPublicFloatOverlay735:
    """#735 — DEI EntityPublicFloat (10-K cover-page, period_end = issuer
    Q2-end, stamped fiscal_period='FY') is projected onto the FY row via an
    overlay, WITHOUT lifting the FY anchor (#558) and only on FY rows."""

    def _fy_gaap(self) -> FactRow:
        # us-gaap FY anchor fact (AAPL FY2025 ends 2025-09-27).
        return _fact(
            concept="Revenues",
            val=Decimal("391035000000"),
            period_start="2024-09-29",
            period_end="2025-09-27",
            frame="CY2025",
            form_type="10-K",
            fiscal_year=2025,
            fiscal_period="FY",
            accession_number="0000320193-25-000079",
            filed_date="2025-11-01",
        )

    def _float(
        self,
        *,
        val: Decimal = Decimal("3253431000000"),
        period_end: str = "2025-03-28",  # issuer Q2-end, NOT the FY anchor
        filed_date: str = "2025-11-01",
        accession_number: str = "0000320193-25-000079",
        unit: str = "USD",
    ) -> FactRow:
        return _fact(
            concept="EntityPublicFloat",
            val=val,
            period_start=None,  # instant
            period_end=period_end,
            frame="CY2025Q1I",
            form_type="10-K",
            fiscal_year=2025,
            fiscal_period="FY",
            accession_number=accession_number,
            filed_date=filed_date,
            unit=unit,
        )

    def test_float_overlaid_on_fy_row_without_lifting_anchor(self) -> None:
        periods = _derive_periods_from_facts([self._fy_gaap(), self._float()], reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        assert fy.public_float_usd == Decimal("3253431000000")
        # Anchor stays the real FY-end, NOT the float's Q2-end (#558).
        assert fy.period_end_date == date(2025, 9, 27)
        assert fy.revenue == Decimal("391035000000")

    def test_float_not_applied_to_quarter_rows(self) -> None:
        q1_gaap = _fact(
            concept="Revenues",
            val=Decimal("95000000000"),
            period_start="2024-09-29",
            period_end="2024-12-28",
            frame="CY2024Q4",
            fiscal_year=2025,
            fiscal_period="Q1",
            accession_number="q1",
            filed_date="2025-02-01",
        )
        periods = _derive_periods_from_facts([self._fy_gaap(), self._float(), q1_gaap], reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        q1 = next(p for p in periods if p.period_type == "Q1")
        assert fy.public_float_usd == Decimal("3253431000000")
        assert q1.public_float_usd is None

    def test_float_picks_current_over_comparative(self) -> None:
        # A comparative-year float re-stamped under the same fy must lose to
        # the current float (max period_end), and a later amendment wins on
        # filed_date at equal period_end.
        comparative = self._float(val=Decimal("2628553000000"), period_end="2024-03-29")
        current = self._float(val=Decimal("3253431000000"), period_end="2025-03-28", filed_date="2025-11-01")
        amendment = self._float(
            val=Decimal("3253431999999"),
            period_end="2025-03-28",
            filed_date="2025-12-15",
            accession_number="0000320193-25-000079-a",
        )
        periods = _derive_periods_from_facts(
            [self._fy_gaap(), comparative, current, amendment], reported_currency="USD"
        )
        fy = next(p for p in periods if p.period_type == "FY")
        assert fy.public_float_usd == Decimal("3253431999999")  # newest period_end, latest filed

    def test_non_usd_float_ignored(self) -> None:
        periods = _derive_periods_from_facts([self._fy_gaap(), self._float(unit="shares")], reported_currency="USD")
        fy = next(p for p in periods if p.period_type == "FY")
        assert fy.public_float_usd is None


class TestYtdDecumulation2036:
    """#2036 — YTD de-cumulation + D&A component-sum fallback.

    Interim cash-flow statements are YTD-only (17 CFR 210.10-01(c)(3));
    the discrete quarter is recovered as YTD_n - YTD_{n-1}. Spec:
    docs/proposals/etl/2026-07-15-fundamentals-dna-ytd-decumulation.md.
    """

    @staticmethod
    def _year_facts() -> list[FactRow]:
        """AAPL-shaped calendar year: discrete op income anchors Q1-Q3 + FY;
        D&A and operating_cf exist ONLY as Q1-discrete + Q2/Q3 YTD + FY."""
        rows: list[FactRow] = []
        anchors = [
            ("Q1", "2024-01-01", "2024-03-31"),
            ("Q2", "2024-04-01", "2024-06-30"),
            ("Q3", "2024-07-01", "2024-09-30"),
            ("FY", "2024-01-01", "2024-12-31"),
        ]
        for fp, start, end in anchors:
            rows.append(
                _fact(
                    concept="OperatingIncomeLoss",
                    val=Decimal("1000"),
                    period_start=start,
                    period_end=end,
                    fiscal_period=fp,
                    accession_number=f"op-{fp}",
                    filed_date="2025-02-01",
                )
            )
        cumulative = [
            ("Q1", "2024-03-31", "3080", "2024-05-01"),
            ("Q2", "2024-06-30", "5741", "2024-08-01"),
            ("Q3", "2024-09-30", "8571", "2024-11-01"),
            ("FY", "2024-12-31", "11698", "2025-02-01"),
        ]
        for fp, end, val, filed in cumulative:
            rows.append(
                _fact(
                    concept="DepreciationDepletionAndAmortization",
                    val=Decimal(val),
                    period_start="2024-01-01",
                    period_end=end,
                    fiscal_period=fp,
                    accession_number=f"da-{fp}",
                    filed_date=filed,
                )
            )
        for fp, end, val, filed in [
            ("Q1", "2024-03-31", "1000", "2024-05-01"),
            ("Q2", "2024-06-30", "2500", "2024-08-01"),
            ("Q3", "2024-09-30", "4500", "2024-11-01"),
            ("FY", "2024-12-31", "7000", "2025-02-01"),
        ]:
            rows.append(
                _fact(
                    concept="NetCashProvidedByUsedInOperatingActivities",
                    val=Decimal(val),
                    period_start="2024-01-01",
                    period_end=end,
                    fiscal_period=fp,
                    accession_number=f"ocf-{fp}",
                    filed_date=filed,
                )
            )
        return rows

    def test_decumulates_q2_q3_and_derives_q4(self) -> None:
        periods = _derive_periods_from_facts(self._year_facts(), reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q1"].depreciation_amort == Decimal("3080")
        assert by_type["Q2"].depreciation_amort == Decimal("2661")
        assert by_type["Q3"].depreciation_amort == Decimal("2830")
        assert by_type["Q4"].depreciation_amort == Decimal("3127")
        assert by_type["Q4"].is_derived
        assert by_type["Q2"].operating_cf == Decimal("1500")
        assert by_type["Q3"].operating_cf == Decimal("2000")
        assert by_type["Q4"].operating_cf == Decimal("2500")
        assert by_type["FY"].depreciation_amort == Decimal("11698")

    def test_broken_chain_no_fill(self) -> None:
        """Q3 YTD present but no Q2 cumulative -> Q3 stays None (no fabrication)."""
        facts = [f for f in self._year_facts() if f.accession_number not in ("da-Q2",)]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q2"].depreciation_amort is None
        assert by_type["Q3"].depreciation_amort is None

    def test_anchor_mismatch_no_fill(self) -> None:
        """A prior cumulative with a different period_start (not the same FY
        anchor) must not participate in the subtraction."""
        facts = [f for f in self._year_facts() if not f.accession_number.startswith("da-")]
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("5741"),
                period_start="2024-01-01",
                period_end="2024-06-30",
                fiscal_period="Q2",
                accession_number="da-ytd2",
            )
        )
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("3080"),
                period_start="2023-12-15",  # mismatched anchor
                period_end="2024-03-31",
                fiscal_period="Q1",
                accession_number="da-q1-off",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q2"].depreciation_amort is None

    def test_discrete_fact_not_overwritten(self) -> None:
        """Fill-only: a reported discrete Q2 fact wins over de-cumulation."""
        facts = self._year_facts()
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("9999"),
                period_start="2024-04-01",
                period_end="2024-06-30",
                fiscal_period="Q2",
                accession_number="da-q2-discrete",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q2"].depreciation_amort == Decimal("9999")

    def test_unit_mismatch_no_fill(self) -> None:
        facts = [f for f in self._year_facts() if not f.accession_number.startswith("da-")]
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("5741"),
                period_start="2024-01-01",
                period_end="2024-06-30",
                fiscal_period="Q2",
                accession_number="da-ytd2",
                unit="USD",
            )
        )
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("3080"),
                period_start="2024-01-01",
                period_end="2024-03-31",
                fiscal_period="Q1",
                accession_number="da-q1-eur",
                unit="EUR",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q2"].depreciation_amort is None

    def test_comparative_restamp_uses_canonical_end(self) -> None:
        """#682 class — a prior-year comparative YTD fact re-stamped under the
        current (fy, fp) context must not feed the fill; only the fact ending
        at the row's canonical period_end does."""
        facts = self._year_facts()
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("4444"),
                period_start="2023-01-01",
                period_end="2023-06-30",  # prior-year comparative span
                fiscal_period="Q2",
                fiscal_year=2024,
                accession_number="da-q2-comparative",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods if p.fiscal_year == 2024}
        assert by_type["Q2"].depreciation_amort == Decimal("2661")


class TestDaComponentSum2036:
    """#2036 §3.3 — D&A = Depreciation + intangible_amortization when no
    total-semantics concept is tagged (rule calibrated full-pop, spec §2.3)."""

    @staticmethod
    def _component_facts(*, with_ami: bool = True, with_dep: bool = True) -> list[FactRow]:
        rows: list[FactRow] = []
        for fp, start, end in [
            ("Q1", "2024-01-01", "2024-03-31"),
            ("Q2", "2024-04-01", "2024-06-30"),
        ]:
            rows.append(
                _fact(
                    concept="OperatingIncomeLoss",
                    val=Decimal("1000"),
                    period_start=start,
                    period_end=end,
                    fiscal_period=fp,
                    accession_number=f"op-{fp}",
                )
            )
        if with_dep:
            for fp, end, val in [("Q1", "2024-03-31", "100"), ("Q2", "2024-06-30", "230")]:
                rows.append(
                    _fact(
                        concept="Depreciation",
                        val=Decimal(val),
                        period_start="2024-01-01",  # cash-flow YTD anchor
                        period_end=end,
                        fiscal_period=fp,
                        accession_number=f"dep-{fp}",
                    )
                )
        if with_ami:
            for fp, start, end, val in [
                ("Q1", "2024-01-01", "2024-03-31", "40"),
                ("Q2", "2024-04-01", "2024-06-30", "45"),
            ]:
                rows.append(
                    _fact(
                        concept="AmortizationOfIntangibleAssets",
                        val=Decimal(val),
                        period_start=start,
                        period_end=end,
                        fiscal_period=fp,
                        accession_number=f"ami-{fp}",
                    )
                )
        return rows

    def test_component_sum_with_ytd_depreciation(self) -> None:
        periods = _derive_periods_from_facts(self._component_facts(), reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        # Q1: dep 100 direct + ami 40; Q2: dep de-cumulated 130 + ami 45.
        assert by_type["Q1"].depreciation_amort == Decimal("140")
        assert by_type["Q2"].depreciation_amort == Decimal("175")

    def test_depreciation_only(self) -> None:
        periods = _derive_periods_from_facts(self._component_facts(with_ami=False), reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q1"].depreciation_amort == Decimal("100")
        assert by_type["Q2"].depreciation_amort == Decimal("130")

    def test_ami_only_no_sum(self) -> None:
        """AmI without Depreciation would omit ALL depreciation -> no fill."""
        periods = _derive_periods_from_facts(self._component_facts(with_dep=False), reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q1"].depreciation_amort is None
        assert by_type["Q1"].intangible_amortization == Decimal("40")

    def test_total_concept_beats_component_sum(self) -> None:
        facts = self._component_facts()
        facts.append(
            _fact(
                concept="DepreciationDepletionAndAmortization",
                val=Decimal("150"),
                period_start="2024-01-01",
                period_end="2024-03-31",
                fiscal_period="Q1",
                accession_number="dda-q1",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["Q1"].depreciation_amort == Decimal("150")

    def test_accretion_net_alias_maps(self) -> None:
        facts = [
            _fact(
                concept="DepreciationAmortizationAndAccretionNet",
                val=Decimal("77"),
                accession_number="daan-q1",
            )
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods[0].depreciation_amort == Decimal("77")

    def test_depreciation_alone_does_not_anchor_row(self) -> None:
        """Raw-only component facts must never anchor a PeriodRow."""
        facts = [
            _fact(
                concept="Depreciation",
                val=Decimal("100"),
                fiscal_period="Q3",
                period_start="2024-01-01",
                period_end="2024-09-30",
                accession_number="dep-only",
            )
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        assert periods == []

    def test_component_sum_applies_to_fy_row(self) -> None:
        """FY rows get the component sum too — the Q4 = FY - sum(Q) derivation
        needs the FY row's depreciation_amort (bot NITPICK, PR #2040)."""
        facts = self._component_facts()
        facts.append(
            _fact(
                concept="OperatingIncomeLoss",
                val=Decimal("4000"),
                period_start="2024-01-01",
                period_end="2024-12-31",
                fiscal_period="FY",
                accession_number="op-FY",
            )
        )
        facts.append(
            _fact(
                concept="Depreciation",
                val=Decimal("500"),
                period_start="2024-01-01",
                period_end="2024-12-31",
                fiscal_period="FY",
                accession_number="dep-FY",
            )
        )
        facts.append(
            _fact(
                concept="AmortizationOfIntangibleAssets",
                val=Decimal("170"),
                period_start="2024-01-01",
                period_end="2024-12-31",
                fiscal_period="FY",
                accession_number="ami-FY",
            )
        )
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        by_type = {p.period_type: p for p in periods}
        assert by_type["FY"].depreciation_amort == Decimal("670")

    def test_depreciation_is_raw_only(self) -> None:
        """The load-bearing split (spec §3.3): Depreciation is captured into
        financial_facts_raw but must never enter the column priority pick."""
        from app.providers.implementations.sec_fundamentals import (
            _ALL_TRACKED_TAGS,
            RAW_ONLY_CONCEPTS,
        )
        from app.services.fundamentals import _TAG_TO_COLUMN

        assert "Depreciation" in RAW_ONLY_CONCEPTS
        assert "Depreciation" in _ALL_TRACKED_TAGS
        assert "Depreciation" not in _TAG_TO_COLUMN


class TestFiscalYearGapFill1914:
    """#1914 — comparative prior-year annual periods are recovered as their own
    rows, with fiscal_year re-derived from each period's own primary filing."""

    def test_resolve_exact_anchor(self) -> None:
        anchor = {("FY", date(2023, 9, 30)): 2023, ("FY", date(2024, 9, 28)): 2024}
        assert _resolve_period_fiscal_year(anchor, "FY", date(2023, 9, 30), 9999) == 2023

    def test_resolve_year_delta_fallback_sept_ender(self) -> None:
        # 2021-09-25 is a comparative-only end (no anchor); nearest anchor is
        # 2023-09-30 (fy 2023) → 2023 - (2023 - 2021) = 2021.
        anchor = {("FY", date(2023, 9, 30)): 2023}
        assert _resolve_period_fiscal_year(anchor, "FY", date(2021, 9, 25), 2023) == 2021

    def test_resolve_year_delta_fallback_feb_ender(self) -> None:
        # Off-December fiscal-year-end: anchor 2025-02-01 → fy 2024.
        anchor = {("FY", date(2025, 2, 1)): 2024}
        assert _resolve_period_fiscal_year(anchor, "FY", date(2024, 2, 3), 2025) == 2023
        assert _resolve_period_fiscal_year(anchor, "FY", date(2023, 1, 28), 2025) == 2022

    def test_resolve_no_anchor_falls_back_to_stamp(self) -> None:
        assert _resolve_period_fiscal_year({}, "FY", date(2024, 12, 31), 2024) == 2024

    def _fy_fact(self, *, val: str, end: str, start: str, fy: int, acc: str, filed: str) -> FactRow:
        return _fact(
            concept="Revenues",
            val=Decimal(val),
            period_end=end,
            period_start=start,
            frame=f"CY{end[:4]}",
            fiscal_year=fy,
            fiscal_period="FY",
            form_type="10-K",
            accession_number=acc,
            filed_date=filed,
        )

    def test_comparatives_recovered_across_two_filings(self) -> None:
        """Two 10-Ks (FY2024 + FY2025), each carrying two comparatives. The
        FY2023 end is a comparative in BOTH filings (its own 10-K aged out), so it
        is recovered; FY2024 is a primary of its own filing, not duplicated."""
        facts = [
            # FY2025 10-K: comparatives 2023, 2024 + primary 2025.
            self._fy_fact(val="300", end="2023-09-30", start="2022-10-01", fy=2025, acc="A", filed="2025-11-01"),
            self._fy_fact(val="391", end="2024-09-28", start="2023-10-01", fy=2025, acc="A", filed="2025-11-01"),
            self._fy_fact(val="416", end="2025-09-27", start="2024-09-29", fy=2025, acc="A", filed="2025-11-01"),
            # FY2024 10-K: comparatives 2022, 2023 + primary 2024.
            self._fy_fact(val="274", end="2022-09-24", start="2021-09-26", fy=2024, acc="B", filed="2024-11-01"),
            self._fy_fact(val="300", end="2023-09-30", start="2022-10-01", fy=2024, acc="B", filed="2024-11-01"),
            self._fy_fact(val="391", end="2024-09-28", start="2023-10-01", fy=2024, acc="B", filed="2024-11-01"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        # Every distinct FY end is emitted (from each filing carrying it); the SQL
        # best-source merge keeps the latest-filed row per fiscal_year. Replicate
        # that winner order (filed_date DESC, period_end DESC) to check the
        # canonical outcome.
        canon: dict[int, object] = {}
        for p in periods:
            if p.period_type != "FY":
                continue
            key = (p.filed_date, p.period_end_date)
            if p.fiscal_year not in canon or key > canon[p.fiscal_year][0]:  # type: ignore[index]
                canon[p.fiscal_year] = (key, p)
        fy_rows = {y: v[1] for y, v in canon.items()}  # type: ignore[index]
        # Continuous FY2022–FY2025 recovered from two filings.
        assert set(fy_rows) == {2022, 2023, 2024, 2025}
        assert fy_rows[2022].revenue == Decimal("274")
        assert fy_rows[2023].revenue == Decimal("300")
        assert fy_rows[2024].revenue == Decimal("391")
        assert fy_rows[2025].revenue == Decimal("416")
        # FY2024 (2024-09-28) is reported by BOTH filings (primary in B, comparative
        # in A) — the row merges them, with A's later filing driving provenance
        # (#682 restatement priority) and both accessions cited.
        assert fy_rows[2024].period_end_date == date(2024, 9, 28)
        assert fy_rows[2024].filed_date == date(2025, 11, 1)
        assert "A" in fy_rows[2024].source_ref and "B" in fy_rows[2024].source_ref

    def test_anchor_conflict_resolves_to_latest_filed(self) -> None:
        """When two filings each treat the same period_end as their primary but
        stamp it with different fy (a re-label / source error), the latest-filed
        accession's stamp wins — deterministic, not DB read-order dependent."""
        facts = [
            # Original stamps 2024-06-30 as fy=2024.
            self._fy_fact(val="100", end="2024-06-30", start="2023-07-01", fy=2024, acc="OLD", filed="2024-08-01"),
            # Later re-label stamps the SAME period_end as fy=2025.
            self._fy_fact(val="105", end="2024-06-30", start="2023-07-01", fy=2025, acc="NEW", filed="2025-08-01"),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy_rows = [p for p in periods if p.period_type == "FY"]
        # One merged FY row for the shared period_end, labelled by the latest filing.
        assert len(fy_rows) == 1
        assert fy_rows[0].fiscal_year == 2025
        assert fy_rows[0].period_end_date == date(2024, 6, 30)

    def test_collision_is_logged_not_silent(self, caplog) -> None:
        """A fiscal-year-end change makes two distinct annual ends both anchor to
        fy=2024 (both are their own filing's primary). Part-1 keeps one via the SQL
        merge; the drop must be logged (#1914/#541), never silent."""
        import logging

        facts = [
            # Old June fiscal-year-end 10-K stamped fy=2024 (full year).
            self._fy_fact(val="100", end="2024-06-30", start="2023-07-01", fy=2024, acc="OLD", filed="2024-08-01"),
            # New Dec fiscal-year-end 10-K also stamped fy=2024 (full year) — a
            # genuine collision: two full annual periods on one integer fy label.
            self._fy_fact(val="120", end="2024-12-31", start="2024-01-01", fy=2024, acc="NEW", filed="2025-02-01"),
        ]
        with caplog.at_level(logging.WARNING, logger="app.services.fundamentals"):
            periods = _derive_periods_from_facts(facts, reported_currency="USD")
        collided = [p for p in periods if p.period_type == "FY" and p.fiscal_year == 2024]
        assert {p.period_end_date for p in collided} == {date(2024, 6, 30), date(2024, 12, 31)}
        assert any("fiscal_year collision" in r.message for r in caplog.records)


class TestFiscalYearRangeGuard2192:
    """#2192 — a filer's mis-tagged DocumentFiscalYearFocus must never become
    a fiscal-year label or an anchor.

    Source rule: SEC companyfacts ``fy`` is the filing's DEI focus (#682) and
    SEC republishes it verbatim, errors included — data.sec.gov itself serves
    PRTH ``fy=43830`` (the Excel serial for that filing's own 2019-12-31
    period end) and WTBA ``fy=2107``. The values below are those real ones.
    """

    def test_plausibility_bounds(self) -> None:
        assert _is_plausible_fiscal_year(2019) is True
        assert _is_plausible_fiscal_year(1995) is True
        assert _is_plausible_fiscal_year(2100) is True
        # SEC's own `fy: 0` for facts from filings with no fiscal-period focus.
        assert _is_plausible_fiscal_year(0) is False
        # Excel serial for 2019-12-31, as published by SEC for PRTH.
        assert _is_plausible_fiscal_year(43830) is False
        # WTBA's digit transposition of 2017.
        assert _is_plausible_fiscal_year(2107) is False
        assert _is_plausible_fiscal_year(None) is False

    def test_no_anchor_implausible_stamp_degrades_to_period_end_year(self) -> None:
        # Previously returned the stamp verbatim, which sql/243's CHECK now
        # rejects outright — so the row would fail to store at all.
        assert _resolve_period_fiscal_year({}, "FY", date(2019, 12, 31), 43830) == 2019

    def test_no_anchor_plausible_stamp_still_wins_over_period_end_year(self) -> None:
        # An off-December filer labels 2025-02-01 as FY2024; the stamp must
        # still beat the calendar year when it is usable.
        assert _resolve_period_fiscal_year({}, "FY", date(2025, 2, 1), 2024) == 2024

    def test_implausible_stamp_never_becomes_an_anchor(self) -> None:
        """The PRTH shape: a 10-K whose OWN primary period is stamped with an
        Excel serial. The exact-anchor path would have returned it verbatim,
        and the anchor would then have poisoned the comparative by calendar
        delta (43830 - 1) rather than labelling it 2018."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("400"),
                period_end="2018-12-31",
                period_start="2018-01-01",
                frame="CY2018",
                fiscal_year=43830,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="P1",
                filed_date="2020-03-16",
            ),
            _fact(
                concept="Revenues",
                val=Decimal("500"),
                period_end="2019-12-31",
                period_start="2019-01-01",
                frame="CY2019",
                fiscal_year=43830,
                fiscal_period="FY",
                form_type="10-K",
                accession_number="P1",
                filed_date="2020-03-16",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        fy_labels = sorted(p.fiscal_year for p in periods if p.period_type == "FY")
        assert fy_labels == [2018, 2019]

    def test_implausible_stamp_on_a_quarter_is_derived_not_stored(self) -> None:
        """8 of the 12 damaged rows were quarters: #1914 confined re-derivation
        to FY, so the quarterly path passed the stamp straight through."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("100"),
                period_end="2019-03-31",
                period_start="2019-01-01",
                frame="CY2019Q1",
                fiscal_year=43555,
                fiscal_period="Q1",
                form_type="10-Q",
                accession_number="Q1",
                filed_date="2019-05-10",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q1 = [p for p in periods if p.period_type == "Q1"]
        assert len(q1) == 1
        assert q1[0].fiscal_year == 2019

    def test_plausible_quarter_stamp_is_passed_through_byte_identical(self) -> None:
        """The #1914 invariant: the quarter SET must not move. A sane stamp
        still wins even where it disagrees with the calendar year (an
        off-December filer's Q1 ending Feb 2024 is fiscal 2023)."""
        facts = [
            _fact(
                concept="Revenues",
                val=Decimal("100"),
                period_end="2024-02-03",
                period_start="2023-11-01",
                frame=None,
                fiscal_year=2023,
                fiscal_period="Q1",
                form_type="10-Q",
                accession_number="Q1",
                filed_date="2024-03-10",
            ),
        ]
        periods = _derive_periods_from_facts(facts, reported_currency="USD")
        q1 = [p for p in periods if p.period_type == "Q1"]
        assert len(q1) == 1
        assert q1[0].fiscal_year == 2023
