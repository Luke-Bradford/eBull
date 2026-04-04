"""
SEC EDGAR filings provider.

Implements FilingsProvider against the SEC EDGAR public API (no key required).
Used for US-listed companies.
Full implementation is built in issue #4 (filings and fundamentals).
"""

from datetime import date

from app.providers.filings import FilingEvent, FilingSearchResult, FilingsProvider


class SecFilingsProvider(FilingsProvider):
    """
    Fetches filing metadata and documents from SEC EDGAR.

    No API key required. Rate limit: 10 requests/second per SEC fair-use policy.
    Covers US-listed companies only.
    """

    BASE_URL = "https://data.sec.gov"

    def list_filings(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        raise NotImplementedError("Implemented in issue #4")

    def get_filing(self, provider_id: str) -> FilingEvent:
        raise NotImplementedError("Implemented in issue #4")
