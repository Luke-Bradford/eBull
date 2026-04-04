"""
Filings provider interface.

Two v1 implementations:
  SecFilingsProvider            — US-listed companies (SEC EDGAR)
  CompaniesHouseFilingsProvider — UK-listed companies (Companies House)

Providers are pure HTTP clients. They do not hold a database connection and do
not resolve instrument_id or symbol to provider-native identifiers. That
resolution is the service layer's responsibility.

Rule: official filing text and events take precedence over normalised
provider data where they disagree.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime


class FilingNotFound(Exception):
    """Raised by FilingsProvider.get_filing when the requested ID does not exist."""


@dataclass(frozen=True)
class FilingSearchResult:
    """
    Lightweight filing listing returned by search — no document text.

    provider_filing_id is the provider-native unique identifier used for
    idempotent upserts and as the key for get_filing.
    """

    provider_filing_id: str
    symbol: str
    filed_at: datetime
    filing_type: str
    period_of_report: date | None
    primary_document_url: str | None


@dataclass(frozen=True)
class FilingEvent:
    """
    A single filed document including extracted metadata.

    raw_text is not stored in v1. full-text persistence, if needed later,
    will use a separate filing_documents table rather than this model.

    extracted_summary and red_flag_score are populated by the service layer
    after ingestion, not by the provider.
    """

    provider_filing_id: str
    symbol: str
    filed_at: datetime
    filing_type: str
    period_of_report: date | None
    primary_document_url: str | None
    extracted_summary: str | None
    red_flag_score: float | None
    raw_payload: object  # provider response payload, persisted as JSONB


class FilingsProvider(ABC):
    """
    Interface for official company filing documents and events.

    Providers receive pre-resolved provider-native identifiers from the
    service layer. They do not perform symbol lookups or DB queries.

    v1 implementations: SecFilingsProvider, CompaniesHouseFilingsProvider
    """

    @abstractmethod
    def list_filings_by_identifier(
        self,
        identifier_type: str,
        identifier_value: str,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        """
        Return filing metadata for a pre-resolved provider-native identifier.

        identifier_type: e.g. 'cik' for SEC, 'company_number' for Companies House.
        identifier_value: the resolved identifier value for this instrument.

        Returns results oldest-first.
        Raises nothing for empty results — returns an empty list.
        """

    @abstractmethod
    def get_filing(self, provider_filing_id: str) -> FilingEvent:
        """
        Fetch a single filing by its provider-native ID.
        Raises FilingNotFound if the ID does not exist on the provider.
        """
