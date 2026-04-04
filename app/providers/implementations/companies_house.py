"""
Companies House filings provider.

Implements FilingsProvider against the Companies House API.
Used for UK-listed companies.
Full implementation is built in issue #4 (filings and fundamentals).
"""

from datetime import date

from app.providers.filings import FilingEvent, FilingSearchResult, FilingsProvider


class CompaniesHouseFilingsProvider(FilingsProvider):
    """
    Fetches filing metadata and documents from Companies House.

    Requires COMPANIES_HOUSE_API_KEY in environment settings.
    Covers UK-registered companies only.
    """

    BASE_URL = "https://api.company-information.service.gov.uk"

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

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
