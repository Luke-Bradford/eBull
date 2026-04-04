"""
Companies House filings provider.

Implements FilingsProvider against the Companies House API.
Used for UK-registered companies.

Provider contract:
  - Requires a pre-resolved company_number from the service layer.
  - Does NOT accept a ticker symbol and fuzzy-search on every call.
  - The service layer resolves instrument_id → company_number via
    external_identifiers before calling this provider.
  - If no company_number exists, the service skips this provider and
    records the reason — this class is never called with an empty identifier.

Auth:
  - HTTP Basic auth with API key as the username, empty password.
  - Requires COMPANIES_HOUSE_API_KEY in settings.

Endpoints used:
  GET /company/{company_number}/filing-history
  GET /document/{document_id}/content  (for get_filing)

Raw responses are persisted before normalisation.
"""

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType

import httpx

from app.providers.filings import FilingEvent, FilingNotFound, FilingSearchResult, FilingsProvider

logger = logging.getLogger(__name__)

BASE_URL = "https://api.company-information.service.gov.uk"
_RAW_PAYLOAD_DIR = Path("data/raw/companies_house")
_PAGE_SIZE = 100


def _persist_raw(tag: str, payload: object) -> None:
    try:
        _RAW_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = _RAW_PAYLOAD_DIR / f"{tag}_{ts}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        logger.warning("Failed to persist raw CH payload for tag=%s", tag, exc_info=True)


class CompaniesHouseFilingsProvider(FilingsProvider):
    """
    Fetches filing metadata from Companies House.

    Requires a company_number (pre-resolved by the service layer).
    identifier_type must be 'company_number'.

    Use as a context manager:

        with CompaniesHouseFilingsProvider(api_key=...) as provider:
            results = provider.list_filings_by_identifier("company_number", "00102498")
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=BASE_URL,
            auth=(api_key, ""),  # CH uses API key as HTTP Basic username
            timeout=30.0,
        )

    def __enter__(self) -> "CompaniesHouseFilingsProvider":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def list_filings_by_identifier(
        self,
        identifier_type: str,
        identifier_value: str,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        """
        Return filing metadata for a company number.

        identifier_type must be 'company_number'.
        Results are returned oldest-first.
        """
        if identifier_type != "company_number":
            raise ValueError(
                f"CompaniesHouseFilingsProvider only supports identifier_type='company_number', got '{identifier_type}'"
            )

        company_number = identifier_value
        raw = self._fetch_filing_history(company_number)
        if raw is None:
            return []

        return _normalise_filings(company_number, raw, start_date, end_date, filing_types)

    def get_filing(self, provider_filing_id: str) -> FilingEvent:
        """
        Fetch a filing by its Companies House transaction ID.
        Raises FilingNotFound if not found (404).

        Note: Companies House filing-history items do not have a stable
        single-document URL in the same way as EDGAR. This method fetches
        the filing history item metadata only — full document download
        requires a separate document API call outside v1 scope.
        """
        # provider_filing_id format: "{company_number}/{transaction_id}"
        parts = provider_filing_id.split("/", 1)
        if len(parts) != 2:
            raise FilingNotFound(
                f"Invalid Companies House provider_filing_id format "
                f"(expected 'company_number/transaction_id'): {provider_filing_id}"
            )
        company_number, transaction_id = parts

        resp = self._client.get(f"/company/{company_number}/filing-history/{transaction_id}")
        if resp.status_code == 404:
            raise FilingNotFound(f"Filing not found: {provider_filing_id}")
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"ch_filing_{company_number}_{transaction_id}", raw)

        return _normalise_filing_event(provider_filing_id, company_number, raw)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_filing_history(self, company_number: str) -> list[dict[str, object]] | None:
        """Fetch filing history, returning a flat list of filing items."""
        resp = self._client.get(
            f"/company/{company_number}/filing-history",
            params={"items_per_page": _PAGE_SIZE, "start_index": 0},
        )
        if resp.status_code == 404:
            logger.warning("Companies House: no company found for number %s", company_number)
            return None
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"ch_filings_{company_number}", raw)

        items = raw.get("items")
        if not isinstance(items, list):
            return []
        return items  # type: ignore[return-value]


# ------------------------------------------------------------------
# Normalisers — pure functions, unit tested with fixture data
# ------------------------------------------------------------------


def _normalise_filings(
    company_number: str,
    items: list[dict[str, object]],
    start_date: date | None,
    end_date: date | None,
    filing_types: list[str] | None,
) -> list[FilingSearchResult]:
    results: list[FilingSearchResult] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        form_type = str(item.get("type", ""))
        if filing_types and form_type not in filing_types:
            continue

        raw_date = item.get("date")
        if not raw_date:
            continue
        try:
            filed_at = datetime.fromisoformat(str(raw_date)[:10]).replace(tzinfo=UTC)
        except ValueError:
            continue

        if start_date and filed_at.date() < start_date:
            continue
        if end_date and filed_at.date() > end_date:
            continue

        transaction_id = str(item.get("transaction_id", ""))
        if not transaction_id:
            logger.warning("Companies House: filing item missing transaction_id for company %s", company_number)
            continue

        # provider_filing_id encodes both company and transaction for get_filing
        provider_filing_id = f"{company_number}/{transaction_id}"

        # Links block: {"filing": {"href": "/filing/..."}}}
        links = item.get("links")
        doc_url: str | None = None
        if isinstance(links, dict):
            filing_link = links.get("filing")
            if isinstance(filing_link, dict):
                href = filing_link.get("href")
                if href:
                    doc_url = f"https://find-and-update.company-information.service.gov.uk{href}"

        results.append(
            FilingSearchResult(
                provider_filing_id=provider_filing_id,
                symbol=company_number,  # no ticker available at this layer
                filed_at=filed_at,
                filing_type=form_type,
                period_of_report=None,  # CH filing history does not reliably expose period
                primary_document_url=doc_url,
            )
        )

    results.sort(key=lambda r: r.filed_at)
    return results


def _normalise_filing_event(
    provider_filing_id: str,
    company_number: str,
    raw: dict[str, object],
) -> FilingEvent:
    raw_date = raw.get("date")
    try:
        filed_at = datetime.fromisoformat(str(raw_date)[:10]).replace(tzinfo=UTC) if raw_date else datetime.now(UTC)
    except ValueError:
        filed_at = datetime.now(UTC)

    links = raw.get("links")
    doc_url: str | None = None
    if isinstance(links, dict):
        filing_link = links.get("filing")
        if isinstance(filing_link, dict):
            href = filing_link.get("href")
            if href:
                doc_url = f"https://find-and-update.company-information.service.gov.uk{href}"

    return FilingEvent(
        provider_filing_id=provider_filing_id,
        symbol=company_number,
        filed_at=filed_at,
        filing_type=str(raw.get("type", "")),
        period_of_report=None,
        primary_document_url=doc_url,
        extracted_summary=None,
        red_flag_score=None,
        raw_payload=raw,
    )
