"""
SEC EDGAR filings provider.

Implements FilingsProvider against the SEC EDGAR public API (no key required).
Used for US-listed companies.

Symbol → CIK resolution:
  1. Fetch and cache https://www.sec.gov/files/company_tickers.json
  2. Build a ticker → CIK mapping from that file
  3. Use CIK to fetch filings from data.sec.gov/submissions/CIK##########.json

The ticker mapping is cached in memory for the lifetime of the provider
instance. Callers should refresh the provider instance daily.

Fair-use constraints (SEC policy):
  - 10 requests/second maximum
  - Declared User-Agent header required (set via sec_user_agent config value)

Provider contract:
  - Providers are pure HTTP clients — no DB access.
  - The service layer resolves instrument_id → CIK via external_identifiers
    and passes the CIK as identifier_value. If no CIK is available, the
    service skips SEC entirely and records the reason.
  - This provider also exposes build_cik_mapping() so the service layer can
    populate external_identifiers during daily CIK refresh.
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import TracebackType

import httpx

from app.providers.filings import FilingEvent, FilingNotFound, FilingSearchResult, FilingsProvider
from app.providers.resilient_client import ResilientClient

logger = logging.getLogger(__name__)

BASE_URL = "https://data.sec.gov"
_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_RAW_PAYLOAD_DIR = Path("data/raw/sec")

# SEC rate-limit: 10 req/s. We use a conservative inter-request floor.
_MIN_REQUEST_INTERVAL_S = 0.11


def _persist_raw(tag: str, payload: object) -> None:
    try:
        _RAW_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = _RAW_PAYLOAD_DIR / f"{tag}_{ts}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        logger.warning("Failed to persist raw SEC payload for tag=%s", tag, exc_info=True)


def _zero_pad_cik(cik: str | int) -> str:
    """Return a 10-digit zero-padded CIK string."""
    return str(int(cik)).zfill(10)


@dataclass(frozen=True)
class CikMappingResult:
    """Result of a conditional-GET fetch of company_tickers.json.

    - ``mapping`` — parsed {TICKER: zero-padded-CIK} dict.
    - ``body_hash`` — sha256 hex of the raw response bytes. Callers
      persist this as the watermark's ``response_hash`` so a subsequent
      run that fetches the same body (without a 304) can still no-op
      without reparsing.
    - ``last_modified`` — the server's ``Last-Modified`` header value,
      to be persisted as the watermark and sent as ``If-Modified-Since``
      on the next run. ``None`` if the header is absent.
    """

    mapping: dict[str, str]
    body_hash: str
    last_modified: str | None


class SecFilingsProvider(FilingsProvider):
    """
    Fetches filing metadata from SEC EDGAR.

    Requires a User-Agent header per SEC fair-use policy (set via config).
    Use as a context manager:

        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            results = provider.list_filings_by_identifier("cik", "0000320193")
    """

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._client = httpx.Client(
            base_url=BASE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=30.0,
        )
        # Separate client for the tickers file (different host)
        self._tickers_client = httpx.Client(
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=30.0,
        )
        # Both clients share the same SEC rate limit (10 req/s).
        # Shared timestamp ensures interleaved calls to different hosts
        # don't exceed the combined limit.
        shared_ts: list[float] = [0.0]
        self._http = ResilientClient(
            self._client,
            min_request_interval_s=_MIN_REQUEST_INTERVAL_S,
            shared_last_request=shared_ts,
        )
        self._http_tickers = ResilientClient(
            self._tickers_client,
            min_request_interval_s=_MIN_REQUEST_INTERVAL_S,
            shared_last_request=shared_ts,
        )

    def __enter__(self) -> SecFilingsProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()
        self._tickers_client.close()

    # ------------------------------------------------------------------
    # FilingsProvider interface
    # ------------------------------------------------------------------

    def list_filings_by_identifier(
        self,
        identifier_type: str,
        identifier_value: str,
        start_date: date | None = None,
        end_date: date | None = None,
        filing_types: list[str] | None = None,
    ) -> list[FilingSearchResult]:
        """
        Return filing metadata for a CIK.

        identifier_type must be 'cik'.
        identifier_value is the CIK (zero-padding is applied automatically).
        Results are returned oldest-first.
        """
        if identifier_type != "cik":
            raise ValueError(f"SecFilingsProvider only supports identifier_type='cik', got '{identifier_type}'")

        cik_padded = _zero_pad_cik(identifier_value)
        raw = self._fetch_submissions(cik_padded)
        if raw is None:
            return []

        return _normalise_filings(raw, cik_padded, start_date, end_date, filing_types)

    def get_filing(self, provider_filing_id: str) -> FilingEvent:
        """
        Fetch metadata for a single filing by accession number.

        provider_filing_id is the accession number, e.g. '0000320193-24-000001'.
        Raises FilingNotFound if the accession number cannot be resolved.
        """
        # Format: XXXXXXXXXX-YY-NNNNNN (18 chars without dashes); first 10 digits are the CIK
        raw_id = provider_filing_id.replace("-", "")
        if len(raw_id) != 18:
            raise FilingNotFound(f"Invalid accession number format: {provider_filing_id}")

        cik_padded = raw_id[:10]
        accession_no_dashes = raw_id

        # Fetch the filing index JSON
        path = f"/Archives/edgar/data/{int(cik_padded)}/{accession_no_dashes}/{accession_no_dashes}-index.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            raise FilingNotFound(f"Filing not found: {provider_filing_id}")
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"sec_filing_{provider_filing_id.replace('/', '_')}", raw)

        return _normalise_filing_event(provider_filing_id, raw)

    # ------------------------------------------------------------------
    # CIK mapping (for service layer to populate external_identifiers)
    # ------------------------------------------------------------------

    def build_cik_mapping(self) -> dict[str, str]:
        """
        Fetch SEC's company_tickers.json and return a dict of upper-cased
        ticker → zero-padded CIK string.

        Called by the service layer during the daily CIK refresh job.
        Unconditional path — retained for callers that don't care about
        deltas. Prefer ``build_cik_mapping_conditional`` for scheduled
        refreshes.
        """
        resp = self._http_tickers.get(_TICKERS_URL)
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw("sec_tickers", raw)
        return _parse_cik_mapping(raw)

    def build_cik_mapping_conditional(
        self,
        *,
        if_modified_since: str | None = None,
    ) -> CikMappingResult | None:
        """
        Conditional-GET variant of ``build_cik_mapping``.

        Sends ``If-Modified-Since: <if_modified_since>`` when a prior
        ``Last-Modified`` value is available. Returns:

        - ``None`` when the server responds 304 Not Modified.
        - ``CikMappingResult`` otherwise, carrying the parsed mapping,
          the raw body hash (sha256 hex) for secondary dedup, and the
          response ``Last-Modified`` header (may be None).

        www.sec.gov honours conditional requests on this endpoint
        (observed 2026-04-17). Per the SEC developer guidelines the
        User-Agent must identify the caller with an email — set
        via ``settings.sec_user_agent`` at client construction.
        """
        headers: dict[str, str] = {}
        if if_modified_since:
            headers["If-Modified-Since"] = if_modified_since

        resp = self._http_tickers.get(_TICKERS_URL, headers=headers)
        if resp.status_code == 304:
            logger.info("SEC tickers: 304 Not Modified")
            return None

        resp.raise_for_status()
        raw = resp.json()
        _persist_raw("sec_tickers", raw)
        body_hash = hashlib.sha256(resp.content).hexdigest()
        return CikMappingResult(
            mapping=_parse_cik_mapping(raw),
            body_hash=body_hash,
            last_modified=resp.headers.get("Last-Modified"),
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_submissions(self, cik_padded: str) -> dict[str, object] | None:
        path = f"/submissions/CIK{cik_padded}.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.warning("SEC: no submissions found for CIK %s", cik_padded)
            return None
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"sec_submissions_{cik_padded}", raw)
        return raw  # type: ignore[return-value]


# ------------------------------------------------------------------
# Normalisers — pure functions, unit tested with fixture data
# ------------------------------------------------------------------


def _parse_cik_mapping(raw: object) -> dict[str, str]:
    """
    Parse company_tickers.json into {TICKER: zero-padded-CIK}.

    SEC format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."}, ...}
    """
    if not isinstance(raw, dict):
        return {}
    mapping: dict[str, str] = {}
    for entry in raw.values():
        if not isinstance(entry, dict):
            continue
        ticker = entry.get("ticker")
        cik = entry.get("cik_str")
        if ticker and cik is not None:
            mapping[str(ticker).upper()] = _zero_pad_cik(cik)
    return mapping


def _normalise_filings(
    raw: dict[str, object],
    cik_padded: str,
    start_date: date | None,
    end_date: date | None,
    filing_types: list[str] | None,
) -> list[FilingSearchResult]:
    """
    Normalise an EDGAR submissions JSON response into FilingSearchResult list.

    The submissions JSON has recent filings inline under raw["filings"]["recent"]
    and may reference additional filing pages under raw["filings"]["files"].
    V1 only processes the inline "recent" block.
    """
    filings_block = raw.get("filings")
    if not isinstance(filings_block, dict):
        return []

    recent = filings_block.get("recent")
    if not isinstance(recent, dict):
        return []

    accession_numbers: list[str] = recent.get("accessionNumber") or []  # type: ignore[assignment]
    filing_dates: list[str] = recent.get("filingDate") or []  # type: ignore[assignment]
    form_types: list[str] = recent.get("form") or []  # type: ignore[assignment]
    primary_docs: list[str] = recent.get("primaryDocument") or []  # type: ignore[assignment]
    report_dates: list[str | None] = recent.get("reportDate") or []  # type: ignore[assignment]

    results: list[FilingSearchResult] = []
    for i, accession in enumerate(accession_numbers):
        form = form_types[i] if i < len(form_types) else ""
        # Skip entries with no form type rather than storing an empty string
        if not form:
            continue
        if filing_types and form not in filing_types:
            continue

        raw_date = filing_dates[i] if i < len(filing_dates) else None
        if not raw_date:
            continue
        try:
            filed_at = datetime.fromisoformat(raw_date).replace(tzinfo=UTC)
        except ValueError:
            continue

        if start_date and filed_at.date() < start_date:
            continue
        if end_date and filed_at.date() > end_date:
            continue

        raw_report = report_dates[i] if i < len(report_dates) else None
        period_of_report: date | None = None
        if raw_report:
            try:
                period_of_report = date.fromisoformat(str(raw_report)[:10])
            except ValueError:
                pass

        primary_doc = primary_docs[i] if i < len(primary_docs) else None
        accession_no_dashes = accession.replace("-", "")
        doc_url: str | None = None
        if primary_doc:
            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik_padded)}/{accession_no_dashes}/{primary_doc}"

        results.append(
            FilingSearchResult(
                provider_filing_id=accession,
                symbol=raw.get("tickers", [accession])[0] if isinstance(raw.get("tickers"), list) else accession,  # type: ignore[arg-type]
                filed_at=filed_at,
                filing_type=form,
                period_of_report=period_of_report,
                primary_document_url=doc_url,
            )
        )

    # Return oldest-first
    results.sort(key=lambda r: r.filed_at)
    return results


def _normalise_filing_event(provider_filing_id: str, raw: dict[str, object]) -> FilingEvent:
    """
    Normalise an EDGAR filing index JSON into a FilingEvent.
    extracted_summary and red_flag_score are not set here — the service layer
    populates those after ingestion.
    """
    raw_date = raw.get("filingDate") or raw.get("dateFiled")
    try:
        filed_at = datetime.fromisoformat(str(raw_date)[:10]).replace(tzinfo=UTC) if raw_date else datetime.now(UTC)
    except ValueError:
        filed_at = datetime.now(UTC)

    raw_period = raw.get("reportDate") or raw.get("periodOfReport")
    period_of_report: date | None = None
    if raw_period:
        try:
            period_of_report = date.fromisoformat(str(raw_period)[:10])
        except ValueError:
            pass

    return FilingEvent(
        provider_filing_id=provider_filing_id,
        symbol=str(raw.get("entityName", "")),
        filed_at=filed_at,
        filing_type=str(raw.get("form", "")),
        period_of_report=period_of_report,
        primary_document_url=None,  # index JSON does not include a direct doc URL
        extracted_summary=None,
        red_flag_score=None,
        raw_payload=raw,
    )
