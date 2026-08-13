"""SEC ``getcurrent`` Atom feed reader (#867).

Issue #867 / spec §"Layer 1 — getcurrent Atom feed (every 5 min)".

The getcurrent endpoint
(``https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent``)
emits every just-accepted SEC filing as Atom entries. One HTTP request
per cycle covers the entire SEC universe — the right shape for the
fast lane.

Pure provider: takes an HTTP getter, returns FilingIndexRow rows. The
caller (``app/jobs/sec_atom_fast_lane.py``) filters to
``cik IN universe`` + UPSERTs ``sec_filing_manifest`` for matches.

Atom XML shape (truncated):

    <feed xmlns="http://www.w3.org/2005/Atom">
      <updated>2026-04-30T16:30:00-04:00</updated>
      <entry>
        <title>4 - Apple Inc. (0000320193) (Filer)</title>
        <updated>2026-04-30T16:30:00-04:00</updated>
        <link rel="alternate" href=".../Archives/edgar/data/320193/0000320193-26-000042-index.htm"/>
        <category term="4"/>
        <id>urn:tag:sec.gov,2008:accession-number=0000320193-26-000042</id>
      </entry>
      ...
    </feed>

The CIK is parenthesised in the title; the accession is in the ``id``
URN; the form is in the ``category`` term.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from datetime import UTC, datetime

from app.providers.implementations.sec_submissions import FilingIndexRow
from app.services.sec_manifest import is_amendment_form, map_form_to_source

logger = logging.getLogger(__name__)


HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]

_GETCURRENT_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
    "&type=&company=&dateb=&owner=include&count=40&output=atom"
)

_NS = {"atom": "http://www.w3.org/2005/Atom"}

# CIK appears in the title in parens: ``Apple Inc. (0000320193) (Filer)``.
_CIK_PATTERN = re.compile(r"\((\d{4,10})\)")
# Accession from the URN id field.
_ACCESSION_PATTERN = re.compile(r"accession-number=(\d{10}-\d{2}-\d{6})")


def parse_getcurrent_atom(body: bytes) -> Iterator[FilingIndexRow]:
    """Parse a getcurrent Atom payload into FilingIndexRow rows.

    Skips entries we can't extract a CIK / accession / form from
    (defensive — SEC has occasionally emitted entries with
    edge-case characters that break our regex; loud failure here
    would block the whole feed for a single bad row).
    """
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        logger.warning("getcurrent atom: parse error %s; payload size=%d", exc, len(body))
        return

    for entry in root.findall("atom:entry", _NS):
        title_el = entry.find("atom:title", _NS)
        id_el = entry.find("atom:id", _NS)
        updated_el = entry.find("atom:updated", _NS)
        category_el = entry.find("atom:category", _NS)
        link_el = entry.find("atom:link", _NS)

        if title_el is None or title_el.text is None:
            continue
        if id_el is None or id_el.text is None:
            continue

        cik_match = _CIK_PATTERN.search(title_el.text)
        if not cik_match:
            continue
        cik_padded = cik_match.group(1).zfill(10)

        accession_match = _ACCESSION_PATTERN.search(id_el.text)
        if not accession_match:
            continue
        accession = accession_match.group(1)

        form: str | None = None
        if category_el is not None:
            form = category_el.attrib.get("term")
        if not form:
            continue

        # ``updated`` carries the SEC-accepted timestamp.
        accepted_at: datetime | None = None
        filed_at: datetime
        if updated_el is not None and updated_el.text:
            try:
                accepted_at = datetime.fromisoformat(updated_el.text)
                if accepted_at.tzinfo is None:
                    accepted_at = accepted_at.replace(tzinfo=UTC)
                else:
                    accepted_at = accepted_at.astimezone(UTC)
            except ValueError:
                accepted_at = None
        filed_at = accepted_at if accepted_at is not None else datetime.now(tz=UTC)

        primary_url: str | None = None
        if link_el is not None:
            primary_url = link_el.attrib.get("href") or None

        yield FilingIndexRow(
            accession_number=accession,
            cik=cik_padded,
            form=form.strip(),
            source=map_form_to_source(form),
            filed_at=filed_at,
            accepted_at=accepted_at,
            primary_document_url=primary_url,
            is_amendment=is_amendment_form(form),
        )


def read_getcurrent(
    http_get: HttpGet,
    *,
    user_agent: str = "eBull research/1.0 contact@example.com",
) -> Iterator[FilingIndexRow]:
    """Fetch + parse the getcurrent Atom feed.

    Empty iterator on 404 / parse error. The reconcile-via-daily-
    index job (#868) is the safety net for any feed-level loss.
    """
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/atom+xml",
        "Accept-Encoding": "gzip, deflate",
    }
    status, body = http_get(_GETCURRENT_URL, headers)
    if status == 404:
        logger.info("getcurrent atom: 404 (feed empty?)")
        return
    if status != 200:
        raise RuntimeError(f"getcurrent atom fetch failed: status={status}")

    yield from parse_getcurrent_atom(body)
