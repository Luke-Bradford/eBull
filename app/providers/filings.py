"""
Filings provider interface.

Two v1 implementations:
  SecFilingsProvider          — US-listed companies (SEC EDGAR)
  CompaniesHouseFilingsProvider — UK-listed companies (Companies House)

All domain code imports this interface only. When a symbol has filings from
both sources (e.g. a dual-listed company), the caller decides which provider
to use — the interface is the same either way.

Rule: official filing text and events take precedence over normalised
provider data where they disagree.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class FilingEvent:
    """
    A single filed document as returned by the provider.

    raw_text is the full extracted text of the filing (may be large).
    source_url is the canonical permalink to the filing on the provider's system.
    """

    provider_id: str  # provider-native filing ID
    symbol: str
    filed_at: datetime
    filing_type: str  # e.g. "10-K", "10-Q", "8-K", "Annual Report", "CH-CS01"
    period_of_report: date | None
    source_url: str
    raw_text: str | None  # None if the provider only returns metadata


@dataclass(frozen=True)
class FilingSearchResult:
    """Lightweight listing returned by search — no raw_text."""

    provider_id: str
    symbol: str
    filed_at: datetime
    filing_type: str
    period_of_report: date | None
    source_url: str


class FilingsProvider(ABC):
    """
    Interface for official company filing documents and events.

    v1 implementations: SecFilingsProvider, CompaniesHouseFilingsProvider
    """

    @abstractmethod
    def list_filings(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        """
        Return filing metadata for a symbol over the date range.
        Optionally filter to specific filing types (e.g. ["10-K", "10-Q"]).
        Results are returned oldest-first.
        """

    @abstractmethod
    def get_filing(self, provider_id: str) -> FilingEvent:
        """
        Fetch a single filing by its provider-native ID, including raw text.
        Raises KeyError if not found.
        """
