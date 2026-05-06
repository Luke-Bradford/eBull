"""Pure SEC daily-index reader for the freshness scheduler (#866).

Issue #866 / spec §"#865 — submissions.json + daily-index readers".

Daily-index files at
``https://www.sec.gov/Archives/edgar/daily-index/{YYYY}/QTR{q}/master.{YYYYMMDD}.idx``
list every filing accepted that day across the entire SEC universe.

Used by the daily-index reconciliation job (#868) as a SAFETY NET on
top of the Atom feed (#867). One ~1 MB download covers all CIKs +
all forms; we filter to (cik IN universe) + (source IN our set) and
UPSERT manifest rows the Atom feed missed.

Format (pipe-delimited after header):

    Description: Master Index of EDGAR Dissemination Feed
    Last Data Received: April 30, 2026
    Comments: webmaster@sec.gov
    Anonymous FTP: ftp://ftp.sec.gov/edgar/

    CIK|Company Name|Form Type|Date Filed|Filename
    --------------------------------------------------------------------------------
    320193|Apple Inc.|8-K|2026-04-30|edgar/data/320193/0000320193-26-000042.txt
    ...

The dashed separator line marks the start of data rows.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime

from app.providers.implementations.sec_submissions import FilingIndexRow
from app.services.sec_manifest import is_amendment_form, map_form_to_source

logger = logging.getLogger(__name__)


HttpGet = Callable[[str, dict[str, str]], tuple[int, bytes]]


def _zero_pad_cik(cik: str) -> str:
    return cik.lstrip().zfill(10)


def _quarter_for(when: date) -> int:
    """SEC organises daily indexes under year/QTRn directories."""
    return (when.month - 1) // 3 + 1


def _build_url(when: date) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/daily-index/"
        f"{when.year}/QTR{_quarter_for(when)}/"
        f"master.{when.strftime('%Y%m%d')}.idx"
    )


def _accession_from_filename(filename: str) -> str | None:
    """Extract dashed accession (``NNNNNNNNNN-NN-NNNNNN``) from the
    daily-index ``Filename`` column.

    SEC publishes each row as
    ``edgar/data/{cik_int}/{accession_no_dashes}.txt``. We rebuild the
    canonical dashed form. Returns ``None`` if the path doesn't match
    that shape (defensive — SEC has emitted the occasional malformed
    row historically).
    """
    if not filename:
        return None
    base = filename.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    digits = base.replace("-", "")
    if len(digits) != 18 or not digits.isdigit():
        return None
    return f"{digits[:10]}-{digits[10:12]}-{digits[12:]}"


def parse_daily_index(body: bytes, *, default_filed_at: date) -> Iterator[FilingIndexRow]:
    """Stream-parse the daily-index body into FilingIndexRow.

    ``default_filed_at`` is used when the row's date column is missing
    or unparseable — the request URL already carries the date, so the
    body row is just confirming.
    """
    text = body.decode("utf-8", errors="replace")
    in_data = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not in_data:
            if line.startswith("---"):
                in_data = True
            continue

        parts = line.split("|")
        if len(parts) < 5:
            continue
        cik_raw, _company, form_raw, date_raw, filename = parts[0], parts[1], parts[2], parts[3], parts[4]
        accession = _accession_from_filename(filename)
        if accession is None:
            continue

        cik_padded = _zero_pad_cik(cik_raw)
        form = form_raw.strip()
        try:
            filed_at = datetime.fromisoformat(date_raw.strip()).replace(tzinfo=UTC)
        except ValueError:
            filed_at = datetime(default_filed_at.year, default_filed_at.month, default_filed_at.day, tzinfo=UTC)

        primary_url: str | None = None
        if filename:
            primary_url = f"https://www.sec.gov/Archives/{filename.lstrip('/')}"

        yield FilingIndexRow(
            accession_number=accession,
            cik=cik_padded,
            form=form,
            source=map_form_to_source(form),
            filed_at=filed_at,
            accepted_at=None,
            primary_document_url=primary_url,
            is_amendment=is_amendment_form(form),
        )


def read_daily_index(
    http_get: HttpGet,
    when: date,
    *,
    user_agent: str = "eBull research/1.0 contact@example.com",
) -> Iterator[FilingIndexRow]:
    """Fetch + parse the SEC daily-index for one calendar day.

    Returns an iterator over FilingIndexRow. Empty iterator on 404
    (date not yet published / weekend / holiday).

    Caller filters by (cik IN universe) + (source IN our set) and
    feeds matching rows into ``record_manifest_entry``.
    """
    url = _build_url(when)
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
    }
    status, body = http_get(url, headers)
    if status == 404:
        logger.info("daily-index not published yet for %s (404)", when.isoformat())
        return
        yield  # pragma: no cover — keeps signature as Iterator
    if status != 200:
        raise RuntimeError(f"daily-index fetch failed: status={status} when={when.isoformat()}")

    yield from parse_daily_index(body, default_filed_at=when)
