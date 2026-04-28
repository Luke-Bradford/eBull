"""
Fundamentals provider interface.

Domain code imports this interface only — never the concrete provider.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class FundamentalsSnapshot:
    """
    Normalised quarterly fundamentals for a single company.

    All monetary fields are in the company's reporting currency.
    None means the data was not available from the provider for this period.
    """

    symbol: str
    as_of_date: date  # period end date
    revenue_ttm: Decimal | None
    gross_margin: Decimal | None  # 0–1 ratio
    operating_margin: Decimal | None  # 0–1 ratio
    fcf: Decimal | None  # free cash flow, TTM
    cash: Decimal | None
    debt: Decimal | None  # total debt
    net_debt: Decimal | None
    shares_outstanding: int | None  # DB column must be BIGINT — large-caps exceed 2^31 (e.g. AAPL ~15bn)
    book_value: Decimal | None  # per share
    eps: Decimal | None  # diluted EPS, TTM


@dataclass(frozen=True)
class XbrlFact:
    """Single XBRL fact extracted from SEC companyfacts response."""

    concept: str
    taxonomy: str
    unit: str
    period_start: date | None
    period_end: date
    val: Decimal
    frame: str | None
    accession_number: str
    form_type: str
    filed_date: date
    fiscal_year: int | None
    fiscal_period: str | None
    decimals: str | None  # XBRL allows non-integer values like "INF"


@dataclass(frozen=True)
class XbrlConceptCatalogEntry:
    """Per-concept metadata extracted from the SEC companyfacts JSON.

    The companyfacts response carries a ``label`` + ``description``
    alongside each concept's ``units`` block. Capturing these into
    ``sec_facts_concept_catalog`` (#451) lets the UI and analysis
    queries render a human-readable concept name without hard-coding
    a Python alias map for the entire XBRL taxonomy.
    """

    taxonomy: str
    concept: str
    label: str | None
    description: str | None
    # Unit types observed for this concept in the current response.
    # ``sec_facts_concept_catalog.units_seen`` accumulates the union
    # across every ingest pass so a concept reporting in multiple
    # units over time is represented truthfully.
    units_seen: tuple[str, ...]


class FundamentalsProvider(ABC):
    """
    Interface for normalised company fundamentals: income, balance sheet, cash flow.

    No active implementation in v1 — fundamentals are sourced directly
    from SEC XBRL via ``app/services/fundamentals.py`` and stored in
    ``financial_periods`` / ``financial_facts_raw``. The interface is
    retained for a possible non-US (Companies House / EDINET) provider.
    """

    @abstractmethod
    def get_latest_snapshot(self, symbol: str) -> FundamentalsSnapshot | None:
        """
        Return the most recent fundamentals snapshot for a symbol.
        Returns None if the provider has no data for this symbol.
        """

    @abstractmethod
    def get_snapshot_history(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        limit: int = 40,
    ) -> list[FundamentalsSnapshot]:
        """
        Return fundamentals snapshots for a symbol within the date range,
        oldest first, up to limit entries.

        limit defaults to 40 (10 years of quarterly data).
        """
