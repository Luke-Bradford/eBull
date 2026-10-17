"""#3714: EDGAR acceptance time from the archived submission header ``<accession>.hdr.sgml``.

Source rule: ``<ACCEPTANCE-DATETIME>YYYYMMDDHHMMSS`` inside the ``<SEC-HEADER>`` block. The EDGAR PDS Technical
Specification (rev. 2013-07-22) defines the acceptance field as "Time (EST) at which the submission was accepted by
EDGAR"; that it is Eastern local time WITH DST is an empirical reading (Atom ``<updated>`` matched on 50 of 50 summer
entries, spec ``docs/proposals/etl/2026-10-17-3714-sec-acceptance-authority.md``). A wall time with no single UTC
instant (spring-forward gap, fall-back fold) is ``invalid_value``, never guessed.

The block is ``<SEC-HEADER>`` or, on 1993–96 filings, ``<IMS-HEADER>`` (``0000003453-95-000005``). The PDS specification
documents only the first; the second is accepted by construction, because it is the same envelope with the same
``<ACCESSION-NUMBER>`` line and is the archive's own header for that accession. The IMS headers observed carry no
acceptance tag, so they classify ``absent`` (``FILING-DATE`` is a date, never a time); a tag inside one would be read
like any other.

Every fetch is classified, never collapsed to "absent":

* final, the header itself answered: ``found`` (one valid tag in one header block for exactly that accession),
  ``absent`` (a valid block without the tag), ``invalid_value`` (tag present, value not 14 digits, not a calendar
  time, or a gap/fold wall time);
* retryable, only our observation failed: ``transport_error``, ``http_error`` (any non-200, 429 and 503 included),
  ``bad_response`` (a 200 that is not exactly one header block for that accession, or carries a duplicated tag).

``POLICY_VERSION`` bumps whenever a change could alter a FINAL outcome or the conversion; a bump makes every older final
check eligible for a re-fetch (``app.services.sec_acceptance_refill``). A change that only turns a retryable outcome
final needs no bump, because a retryable row is re-fetched on its own backoff anyway. Accepting ``<IMS-HEADER>`` was
that kind (every v1 final check began ``<SEC-HEADER>``, which classifies exactly as before), so it stayed at 1.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final, Literal
from zoneinfo import ZoneInfo

import httpx

from app.providers.implementations.sec_edgar import SecFilingsProvider

POLICY_VERSION: Final = 1

HeaderOutcome = Literal["found", "absent", "invalid_value", "transport_error", "http_error", "bad_response"]
FINAL_OUTCOMES: Final[frozenset[HeaderOutcome]] = frozenset({"found", "absent", "invalid_value"})
RETRYABLE_OUTCOMES: Final[frozenset[HeaderOutcome]] = frozenset({"transport_error", "http_error", "bad_response"})

NEW_YORK: Final = ZoneInfo("America/New_York")
_TAG_LINE: Final = re.compile(r"^<ACCEPTANCE-DATETIME>(.*)$", re.M)
_ACCESSION_LINE: Final = re.compile(r"^<ACCESSION-NUMBER>(.*)$", re.M)
_DIGITS14: Final = re.compile(r"[0-9]{14}")
# (open, close) per header envelope; a response must carry exactly one close tag across all of them.
_BLOCKS: Final = (("<SEC-HEADER>", "</SEC-HEADER>"), ("<IMS-HEADER>", "</IMS-HEADER>"))


@dataclass(frozen=True)
class HeaderCheck:
    """One classified header fetch. ``raw_value`` is the tag's text (whitespace-stripped) whenever a tag was read,
    valid or not; ``acceptance_utc`` is set exactly when ``outcome == 'found'``."""

    outcome: HeaderOutcome
    url: str
    raw_value: str | None = None
    acceptance_utc: datetime | None = None
    http_status: int | None = None
    diagnostic: str | None = None


def header_url(cik: str, accession: str) -> str:
    """The archived header URL, built from the manifest CIK (``primary_document_url`` may be NULL)."""
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{accession}.hdr.sgml"


def eastern_to_utc(raw: str) -> datetime | None:
    """14-digit Eastern wall clock -> aware UTC, or None for a non-calendar value or a DST gap / fold wall time."""
    if not _DIGITS14.fullmatch(raw):
        return None
    try:
        wall = datetime.strptime(raw, "%Y%m%d%H%M%S")
    except ValueError:
        return None
    first, second = wall.replace(tzinfo=NEW_YORK, fold=0), wall.replace(tzinfo=NEW_YORK, fold=1)
    if first.utcoffset() != second.utcoffset():
        return None  # fall-back fold: two instants share this wall time
    utc = first.astimezone(UTC)
    if utc.astimezone(NEW_YORK).replace(tzinfo=None) != wall:
        return None  # spring-forward gap: this wall time never happened
    return utc


def classify_header(accession: str, url: str, status: int, text: str) -> HeaderCheck:
    """Classify one HTTP response for ``accession``'s header. Pure; the fetch is ``fetch_header_acceptance``."""
    if status != 200:
        return HeaderCheck("http_error", url, http_status=status)
    close = next((c for o, c in _BLOCKS if text.startswith(o)), None)
    if close is None or sum(text.count(c) for _, c in _BLOCKS) != 1 or close not in text:
        return HeaderCheck("bad_response", url, http_status=status, diagnostic="not exactly one header block")
    block = text[: text.index(close)]
    accessions = [m.strip() for m in _ACCESSION_LINE.findall(block)]
    if accessions != [accession]:
        return HeaderCheck("bad_response", url, http_status=status, diagnostic=f"accession lines {accessions[:3]}")
    tags = [m.strip() for m in _TAG_LINE.findall(block)]
    if len(tags) > 1:
        return HeaderCheck("bad_response", url, http_status=status, diagnostic=f"{len(tags)} acceptance tags")
    if not tags:
        return HeaderCheck("absent", url, http_status=status)
    utc = eastern_to_utc(tags[0])
    if utc is None:
        return HeaderCheck("invalid_value", url, raw_value=tags[0], http_status=status)
    return HeaderCheck("found", url, raw_value=tags[0], acceptance_utc=utc, http_status=status)


def fetch_header_acceptance(provider: SecFilingsProvider, cik: str, accession: str) -> HeaderCheck:
    """One wire request (no client retry) through the provider's shared SEC throttle, classified."""
    url = header_url(cik, accession)
    try:
        status, text = provider.fetch_submission_header(url)
    except httpx.HTTPStatusError as exc:
        # The no-retry client raises a 429 / 5xx instead of returning it.
        return HeaderCheck("http_error", url, http_status=exc.response.status_code)
    except httpx.HTTPError as exc:
        return HeaderCheck("transport_error", url, diagnostic=type(exc).__name__)
    return classify_header(accession, url, status, text)
