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
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from types import TracebackType
from zoneinfo import ZoneInfo

import httpx

from app.providers.filings import FilingEvent, FilingNotFound, FilingSearchResult, FilingsProvider
from app.providers.resilient_client import ResilientClient
from app.services import raw_persistence

_ET = ZoneInfo("America/New_York")

# SEC publishes the daily master-index ~22:00 ET on the same business
# day. Before that moment a 403 on the current day is the "not yet
# published" signal. After it, a 403 means SEC is actively blocking us
# (UA/rate-limit/WAF) and must surface.
_MASTER_INDEX_PUBLISH_HOUR_ET = 22

logger = logging.getLogger(__name__)

BASE_URL = "https://data.sec.gov"
_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# SEC rate-limit: 10 req/s. We use a conservative inter-request floor.
_MIN_REQUEST_INTERVAL_S = 0.11


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


@dataclass(frozen=True)
class MasterIndexEntry:
    """One row from SEC's daily master-index file.

    - ``cik`` — 10-digit zero-padded CIK string.
    - ``accession_number`` — canonical dashed form like
      ``0000320193-26-000042``, extracted from ``Filename``.
    """

    cik: str
    company_name: str
    form_type: str
    date_filed: str
    accession_number: str


@dataclass(frozen=True)
class MasterIndexFetchResult:
    """Result of a conditional-GET fetch of master.YYYYMMDD.idx.

    Callers parse ``body`` via ``parse_master_index`` and persist
    ``body_hash`` + ``last_modified`` as watermark fields.
    """

    body: bytes
    body_hash: str
    last_modified: str | None


def parse_master_index(body: bytes) -> list[MasterIndexEntry]:
    """Parse SEC daily master-index bytes into entries.

    Format: header lines, a ``CIK|...|Filename`` column row, a dashed
    separator line, then pipe-delimited data rows. Malformed rows are
    skipped silently — the provider contract is best-effort parsing.
    """
    entries: list[MasterIndexEntry] = []
    text = body.decode("utf-8", errors="replace")
    in_data = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Data rows start after a line of dashes
        if set(line) == {"-"}:
            in_data = True
            continue
        if not in_data:
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik_raw, company, form, filed, filename = parts
        try:
            cik = _zero_pad_cik(cik_raw.strip())
        except ValueError:
            continue
        # Filename: edgar/data/<cik>/<accession-no-dashes>.txt
        stem = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        # Normalize to canonical dashed accession (0000320193-26-000042)
        # regardless of whether the filename stem supplied it dashed or
        # as 18 straight digits.
        digits_only = stem.replace("-", "")
        if len(digits_only) != 18 or not digits_only.isdigit():
            continue
        accession = f"{digits_only[:10]}-{digits_only[10:12]}-{digits_only[12:]}"
        entries.append(
            MasterIndexEntry(
                cik=cik,
                company_name=company.strip(),
                form_type=form.strip(),
                date_filed=filed.strip(),
                accession_number=accession,
            )
        )
    return entries


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
        raw_persistence.persist_raw_if_new("sec", f"sec_filing_{provider_filing_id.replace('/', '_')}", raw)

        return _normalise_filing_event(provider_filing_id, raw)

    def fetch_document_text(self, absolute_url: str) -> str | None:
        """Fetch the raw text of a filing's primary document.

        ``absolute_url`` is the fully-qualified ``https://www.sec.gov/
        Archives/edgar/data/...`` URL stored in
        ``filing_events.primary_document_url``. Returns the decoded
        body on 2xx, ``None`` on 404 / 410 (filing withdrawn), and
        re-raises on other HTTP errors so the caller can decide
        whether to retry or skip.

        Shares the tickers client's rate-limiter because both target
        ``www.sec.gov`` — ``data.sec.gov`` uses a separate host that
        counts under the same 10 req/s fair-use pool but is served
        by ``self._http``. Using ``_http_tickers`` here keeps the
        host-to-client mapping obvious (www.sec.gov ⇒ tickers client).

        **Contract (#448 / #453):** the body returned by this method
        MUST be routed through a service-layer ingester that
        normalises every structured field into SQL before the
        transaction commits. Disk-only persistence (writing the body
        to ``data/raw/*`` without a matching SQL row) is forbidden.

        Allowed callers are pinned by
        ``tests/test_fetch_document_text_callers.py``. Adding a new
        caller requires the test to be updated alongside a documented
        normalisation path into SQL. If all you need is the raw body
        for ad-hoc inspection, use a one-off script — don't add a
        service-layer caller that writes to disk and leaves the
        normalisation for "later".
        """
        resp = self._http_tickers.get(absolute_url)
        if resp.status_code in (404, 410):
            return None
        resp.raise_for_status()
        return resp.text

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
        raw_persistence.persist_raw_if_new("sec", "sec_tickers", raw)
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
        # Hash the raw bytes BEFORE decoding so body_hash always
        # reflects exactly what arrived over the wire — independent of
        # downstream JSON parsing or any future HTTP client swap that
        # might mutate the content view.
        body_hash = hashlib.sha256(resp.content).hexdigest()
        raw = resp.json()
        raw_persistence.persist_raw_if_new("sec", "sec_tickers", raw)
        return CikMappingResult(
            mapping=_parse_cik_mapping(raw),
            body_hash=body_hash,
            last_modified=resp.headers.get("Last-Modified"),
        )

    def fetch_master_index(
        self,
        target_date: date,
        *,
        if_modified_since: str | None = None,
    ) -> MasterIndexFetchResult | None:
        """Conditional-GET the SEC daily master-index for a given date.

        URL shape: ``https://www.sec.gov/Archives/edgar/daily-index/
        YYYY/QTR{1..4}/master.YYYYMMDD.idx``. Returns ``None`` on 304
        (or 404 — weekends and holidays have no file). Otherwise returns
        body bytes + sha256 hash + Last-Modified header for the caller
        to persist in the watermark row.

        SEC's Archives host serves 403 (not 404) for files that do not
        yet exist. Two tolerated 403 classes:
          1. Weekend (Sat/Sun) target_date — SEC never publishes on
             weekends, and observed behaviour shows 403 rather than 404.
          2. Not-yet-published — current-day before the ~22:00-ET
             publish cutoff, or future-dated.
        Any other 403 (past weekday, or current weekday after the
        publish cutoff) raises — that's SEC refusing us
        (UA/rate-limit/etc.), not awaiting publication.

        US federal holidays also yield 403/404 on SEC side but are not
        enumerated here — a same-day retry on the next business day
        catches them via the per-day watermark path.

        Rate-limited alongside the other SEC clients via the shared
        timestamp list, so a burst of N calls respects the 10 rps cap.
        """
        quarter = (target_date.month - 1) // 3 + 1
        url = (
            f"https://www.sec.gov/Archives/edgar/daily-index/"
            f"{target_date.year}/QTR{quarter}/master.{target_date.strftime('%Y%m%d')}.idx"
        )
        headers: dict[str, str] = {}
        if if_modified_since:
            headers["If-Modified-Since"] = if_modified_since

        resp = self._http_tickers.get(url, headers=headers)
        if resp.status_code in (304, 404):
            return None
        if resp.status_code == 403:
            # SEC is inconsistent about 404 vs 403 for non-existent files.
            # Two tolerated classes:
            #   1. Weekend: file never publishes on Sat/Sun, ever.
            #   2. Not-yet-published: current-day before the ~22:00-ET
            #      publish cutoff.
            # Anything else is a real block (UA/rate-limit/WAF) and must
            # surface.
            if target_date.weekday() >= 5:  # 5=Sat, 6=Sun
                logger.info(
                    "SEC master-index: 403 on %s treated as weekend (no publish)",
                    target_date.isoformat(),
                )
                return None
            now_et = datetime.now(_ET)
            publish_due = datetime.combine(
                target_date,
                time(_MASTER_INDEX_PUBLISH_HOUR_ET, 0),
                tzinfo=_ET,
            )
            if now_et < publish_due:
                logger.info(
                    "SEC master-index: 403 on %s treated as not-yet-published (now_et=%s publish_due=%s)",
                    target_date.isoformat(),
                    now_et.isoformat(timespec="minutes"),
                    publish_due.isoformat(timespec="minutes"),
                )
                return None
        resp.raise_for_status()
        body_hash = hashlib.sha256(resp.content).hexdigest()
        return MasterIndexFetchResult(
            body=resp.content,
            body_hash=body_hash,
            last_modified=resp.headers.get("Last-Modified"),
        )

    def fetch_submissions(self, cik: str) -> dict[str, object] | None:
        """Fetch ``data.sec.gov/submissions/CIKNNNNNNNNNN.json`` for a CIK.

        Returns the parsed JSON dict or ``None`` on 404. Callers must pass
        a 10-digit zero-padded CIK or a numeric string that zero-pads
        correctly; the helper normalizes via ``_zero_pad_cik``.
        """
        cik_padded = _zero_pad_cik(cik)
        return self._fetch_submissions(cik_padded)

    def fetch_submissions_page(self, name: str) -> dict[str, object] | None:
        """Fetch a secondary submissions page named in
        ``filings.files[].name`` (e.g. ``CIK0000320193-submissions-001.json``).

        Used by Chunk E's pagination loop (#268). Returns the parsed
        JSON dict or ``None`` on 404. Uses the same rate-limited HTTP
        client as ``fetch_submissions`` so the 10 req/s SEC cap is
        respected across the combined call pattern.
        """
        path = f"/submissions/{name}"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.warning("SEC: submissions page not found: %s", name)
            return None
        resp.raise_for_status()
        raw = resp.json()
        raw_persistence.persist_raw_if_new("sec", f"sec_submissions_page_{name}", raw)
        return raw  # type: ignore[return-value]

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
        raw_persistence.persist_raw_if_new("sec", f"sec_submissions_{cik_padded}", raw)
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


def _normalise_submissions_block(
    block: dict[str, object],
    cik_padded: str,
    start_date: date | None = None,
    end_date: date | None = None,
    filing_types: list[str] | None = None,
    symbol: str | None = None,
) -> list[FilingSearchResult]:
    """Pure normalisation of one submissions page (either the
    inline ``filings.recent`` sub-dict or a ``files[]`` secondary
    page JSON).

    Both shapes carry the same parallel arrays —
    ``{accessionNumber, filingDate, form, primaryDocument, reportDate}``
    — so Chunk E's pagination loop (#268) can call this per page
    without re-fetching the primary ``submissions.json``.

    ``symbol`` is used to populate ``FilingSearchResult.symbol``; if
    omitted the accession number is used as a fallback (matches the
    pre-refactor behaviour on empty ``tickers``).
    """
    accession_numbers: list[str] = block.get("accessionNumber") or []  # type: ignore[assignment]
    filing_dates: list[str] = block.get("filingDate") or []  # type: ignore[assignment]
    form_types: list[str] = block.get("form") or []  # type: ignore[assignment]
    primary_docs: list[str] = block.get("primaryDocument") or []  # type: ignore[assignment]
    report_dates: list[str | None] = block.get("reportDate") or []  # type: ignore[assignment]

    results: list[FilingSearchResult] = []
    for i, accession in enumerate(accession_numbers):
        form = form_types[i] if i < len(form_types) else ""
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
                symbol=symbol if symbol else accession,
                filed_at=filed_at,
                filing_type=form,
                period_of_report=period_of_report,
                primary_document_url=doc_url,
            )
        )

    # Oldest-first (matches pre-refactor contract).
    results.sort(key=lambda r: r.filed_at)
    return results


def _normalise_filings(
    raw: dict[str, object],
    cik_padded: str,
    start_date: date | None,
    end_date: date | None,
    filing_types: list[str] | None,
) -> list[FilingSearchResult]:
    """Normalise an EDGAR submissions JSON response into
    ``FilingSearchResult`` list.

    Extracts the inline ``filings.recent`` block and delegates to
    ``_normalise_submissions_block``. V1 of this function only
    processes the inline block; secondary ``filings.files[]`` pages
    are handled by Chunk E's backfill flow (#268) via explicit
    per-page calls to ``_normalise_submissions_block``.
    """
    filings_block = raw.get("filings")
    if not isinstance(filings_block, dict):
        return []

    recent = filings_block.get("recent")
    if not isinstance(recent, dict):
        return []

    tickers = raw.get("tickers")
    symbol = str(tickers[0]) if isinstance(tickers, list) and tickers else None
    return _normalise_submissions_block(recent, cik_padded, start_date, end_date, filing_types, symbol=symbol)


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
