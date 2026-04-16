"""
Fundamentals provider interface.

FMP is the v1 implementation. All domain code imports this interface only —
never the concrete provider.
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
    decimals: int | None


class FundamentalsProvider(ABC):
    """
    Interface for normalised company fundamentals: income, balance sheet, cash flow.

    v1 implementation: FmpFundamentalsProvider
    If official filing data disagrees with provider data, prefer the filing.
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
