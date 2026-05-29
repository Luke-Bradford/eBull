"""Generic 8-K structured-event parser + ingester (#450).

Complements the 8-K-specific Item 8.01 dividend parser (#434) by
capturing the full 8-K structure — filing header, per-item bodies,
exhibits list — into SQL so operators can query every 8-K event, not
just the dividend subset.

Pure/impure split:

- :func:`parse_8k_filing` is a pure function over raw HTML + the
  ``items[]`` list from submissions.json. Returns a
  :class:`Parsed8KFiling` with filing header fields, per-item bodies,
  and exhibits.
- :func:`ingest_8k_events` is the DB path — scans 8-K filings
  missing an ``eight_k_filings`` row, fetches HTML, parses, upserts
  across the four tables. Dividend-specific extraction (#434)
  continues to run alongside as a separate concern keyed on the same
  accession.

Tombstoning lives on ``eight_k_filings.is_tombstone`` so fetch
errors and parse misses don't re-hit SEC every tick.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Protocol

import psycopg

from app.services.bootstrap_state import resolve_progress_context
from app.services.filings import bootstrap_filings_recency_floor
from app.services.sec_manifest import is_amendment_form

logger = logging.getLogger(__name__)


_PARSER_VERSION = 1


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class Parsed8KItem:
    """One item block extracted from an 8-K filing."""

    item_code: str
    item_order: int
    body: str


@dataclass(frozen=True)
class Parsed8KExhibit:
    """One entry from the 8-K Item 9.01 exhibits list."""

    exhibit_number: str
    description: str | None


@dataclass(frozen=True)
class Parsed8KFiling:
    """Structured capture of one 8-K primary document."""

    document_type: str  # "8-K" or "8-K/A"
    is_amendment: bool
    date_of_report: date | None
    reporting_party: str | None
    signature_name: str | None
    signature_title: str | None
    signature_date: date | None
    remarks: str | None
    items: tuple[Parsed8KItem, ...]
    exhibits: tuple[Parsed8KExhibit, ...]


# ---------------------------------------------------------------------
# HTML → text
# ---------------------------------------------------------------------


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
_NBSP_RE = re.compile(r"&nbsp;|&#160;|&#xa0;| ", re.IGNORECASE)
_AMP_RE = re.compile(r"&amp;", re.IGNORECASE)


def _strip_html(text: str) -> str:
    no_nbsp = _NBSP_RE.sub(" ", text)
    no_amp = _AMP_RE.sub("&", no_nbsp)
    no_tags = _HTML_TAG_RE.sub(" ", no_amp)
    return _WHITESPACE_RE.sub(" ", no_tags).strip()


# ---------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

_MONTH_NAME_ALT = "|".join(sorted(_MONTHS.keys(), key=len, reverse=True))
_DATE_RE = re.compile(
    rf"(?:(?P<m1>{_MONTH_NAME_ALT})\s+(?P<d1>\d{{1,2}}),?\s+(?P<y1>\d{{4}}))"
    rf"|(?:(?P<m2>\d{{1,2}})/(?P<d2>\d{{1,2}})/(?P<y2>\d{{4}}))"
    rf"|(?:(?P<y3>\d{{4}})-(?P<m3>\d{{1,2}})-(?P<d3>\d{{1,2}}))",
    re.IGNORECASE,
)


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    m = _DATE_RE.search(raw)
    if m is None:
        return None
    try:
        if m.group("m1"):
            month = _MONTHS[m.group("m1").lower()]
            day = int(m.group("d1"))
            year = int(m.group("y1"))
        elif m.group("m2"):
            month = int(m.group("m2"))
            day = int(m.group("d2"))
            year = int(m.group("y2"))
        else:
            year = int(m.group("y3"))
            month = int(m.group("m3"))
            day = int(m.group("d3"))
        return date(year, month, day)
    except KeyError, ValueError:
        return None


# ---------------------------------------------------------------------
# Header extraction
# ---------------------------------------------------------------------


# Document type. Explicit "8-K/A" beats "8-K" when both appear.
_DOC_TYPE_RE = re.compile(r"\b(8-K/A|8-K)\b")

# "Date of Report (Date of earliest event reported)" — canonical
# SEC header phrase, followed by the report date.
_DATE_OF_REPORT_RE = re.compile(
    r"Date\s+of\s+[Rr]eport[^\n\r]{0,200}?(?P<when>" + _DATE_RE.pattern + r")",
    re.IGNORECASE,
)

# Signature block: "By: /s/ Name\nTitle: CFO\nDate: …"
_SIG_NAME_RE = re.compile(
    r"(?:By:|/s/)\s*(?P<name>[A-Z][A-Za-z.\-'\s]{2,60}?)(?:\s{2,}|\s+Title:|\s+Name:|\s*$|\s+Date:)",
)
_SIG_TITLE_RE = re.compile(
    r"\bTitle:\s*(?P<title>[A-Z][A-Za-z0-9,.\-&/\s]{2,80}?)(?:\s{2,}|\s+Date:|\s*$)",
)
_SIG_DATE_RE = re.compile(
    r"(?:Signature\s+Date|Signed|Date:)\s*(?P<when>" + _DATE_RE.pattern + r")",
    re.IGNORECASE,
)

# Exhibits list — a line like "99.1  Press Release dated ..."
# The description runs up to the next exhibit-number token or end of
# text, so consecutive exhibits don't swallow one another's
# descriptions.
_EXHIBIT_LINE_RE = re.compile(
    r"(?P<num>\d{1,3}\.\d{1,3})\s+(?P<desc>[A-Z][^\n]{5,300}?)"
    r"(?=\s+\d{1,3}\.\d{1,3}\s|\s*SIGNATURE|\s*$)",
    re.IGNORECASE,
)

# Item heading detection. SEC requires "Item X.XX" to head each item
# block. We split the filing body on these heading markers and take
# the text between consecutive headings as the item body. Matching
# just ``Item N.NN[.:]`` (without a rest-of-line capture) keeps the
# match tight so consecutive headings don't overlap — a rest-of-line
# capture previously ate into the next heading and corrupted body
# slicing across items.
_ITEM_HEADING_RE = re.compile(
    r"Item\s+(?P<code>\d{1,2}\.\d{1,2})\s*[\.:]",
    re.IGNORECASE,
)


def parse_8k_filing(
    raw_html: str,
    *,
    known_items: tuple[str, ...] = (),
    item_labels: dict[str, tuple[str, str | None]] | None = None,
) -> Parsed8KFiling | None:
    """Extract header + per-item bodies + exhibits from an 8-K filing.

    ``known_items`` is the ``filing_events.items[]`` list from
    submissions.json — a source-of-truth set of item codes the filing
    declared. When the HTML item-heading regex finds fewer items than
    the ``known_items`` list (e.g. the body uses a non-standard
    heading shape), we synthesise empty-body rows for the missing
    codes so every declared item still lands in SQL.

    ``item_labels`` maps item_code -> (label, severity) from
    ``sec_8k_item_codes``. When absent, labels fall back to the raw
    code and severity to ``None``.

    Returns ``None`` when the HTML is empty / not plausibly an 8-K.
    Otherwise returns a :class:`Parsed8KFiling` with at least an
    empty items tuple — callers distinguish "no body" from
    "unreachable" via the presence of a parent row.
    """
    if not raw_html:
        return None

    text = _strip_html(raw_html)
    if not text:
        return None

    # Document type — prefer 8-K/A when the header states an amendment.
    # Route through ``is_amendment_form`` so every SEC amendment
    # detection path uses the same canonical helper (#939).
    doc_match = _DOC_TYPE_RE.search(text)
    if doc_match is None:
        return None
    document_type = doc_match.group(1).upper()
    is_amendment = is_amendment_form(document_type)

    date_of_report_m = _DATE_OF_REPORT_RE.search(text)
    date_of_report = _parse_date(date_of_report_m.group("when")) if date_of_report_m else None

    # Reporting party — the registrant name usually sits between the
    # "Date of Report" line and the first "Item" heading. Best-effort
    # capture: take the first all-caps / title-case phrase between the
    # two markers.
    reporting_party = _extract_reporting_party(text)

    # Items
    items = _extract_items(text, known_items=known_items, item_labels=item_labels)

    # Exhibits — only when Item 9.01 is present.
    exhibits: tuple[Parsed8KExhibit, ...] = ()
    item_901 = next((it for it in items if it.item_code == "9.01"), None)
    if item_901 is not None:
        exhibits = _extract_exhibits(item_901.body)

    # Signature block
    sig_name_m = _SIG_NAME_RE.search(text)
    sig_title_m = _SIG_TITLE_RE.search(text)
    sig_date_m = _SIG_DATE_RE.search(text)
    signature_name = sig_name_m.group("name").strip() if sig_name_m else None
    signature_title = sig_title_m.group("title").strip() if sig_title_m else None
    signature_date = _parse_date(sig_date_m.group("when")) if sig_date_m else None

    # Remarks: text between the last item body and the signature
    # block. Rare; captured opportunistically.
    remarks: str | None = None

    return Parsed8KFiling(
        document_type=document_type,
        is_amendment=is_amendment,
        date_of_report=date_of_report,
        reporting_party=reporting_party,
        signature_name=signature_name,
        signature_title=signature_title,
        signature_date=signature_date,
        remarks=remarks,
        items=items,
        exhibits=exhibits,
    )


def _extract_reporting_party(text: str) -> str | None:
    """Pick the registrant name from the filing cover page.

    8-K cover pages carry the registrant name between ``Commission
    File Number`` and ``(State of Incorporation)`` or between
    ``(Exact name of registrant ...)`` lines. Best-effort regex —
    when nothing matches we return None rather than guessing.
    """
    m = re.search(
        r"\(\s*Exact\s+name\s+of\s+registrant[^)]*\)\s*(?P<name>[A-Z][A-Za-z0-9.,&\-\s]{2,120}?)"
        r"(?:\s*\(|\s+Commission)",
        text,
    )
    if m:
        return m.group("name").strip()
    return None


def _extract_items(
    text: str,
    *,
    known_items: tuple[str, ...],
    item_labels: dict[str, tuple[str, str | None]] | None,
) -> tuple[Parsed8KItem, ...]:
    """Walk the filing text, splitting on ``Item X.XX`` headings."""
    labels = item_labels or {}
    matches = list(_ITEM_HEADING_RE.finditer(text))
    if not matches:
        # Nothing parsed. Still synthesise empty-body rows for every
        # code ``known_items`` declared so the item appears in SQL.
        return tuple(Parsed8KItem(item_code=code, item_order=idx, body="") for idx, code in enumerate(known_items))

    items: list[Parsed8KItem] = []
    seen_codes: set[str] = set()
    for idx, m in enumerate(matches):
        code = m.group("code")
        if code in seen_codes:
            continue
        start = m.end()
        next_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        # Exclude the signature region when it sits inside the last
        # item's body (can't cleanly disambiguate in regex-only
        # pipeline; operator reader renders the raw body).
        body = text[start:next_start].strip()
        if len(body) > 20 * 1024:
            body = body[: 20 * 1024]
        seen_codes.add(code)
        items.append(Parsed8KItem(item_code=code, item_order=idx, body=body))

    # Backfill any code from ``known_items`` that the heading regex
    # didn't catch (odd heading shapes, OCR artefacts). Empty body so
    # the reader knows the item was declared but couldn't be parsed.
    for code in known_items:
        if code not in seen_codes:
            items.append(
                Parsed8KItem(
                    item_code=code,
                    item_order=len(items),
                    body="",
                )
            )

    # Re-sort items by SEC code for deterministic storage order. The
    # code is "X.YY"; parse numerically so 10.01 sorts after 9.99.
    def _sort_key(it: Parsed8KItem) -> tuple[int, int]:
        try:
            major, minor = it.item_code.split(".")
            return (int(major), int(minor))
        except ValueError:
            return (999, 999)

    items.sort(key=_sort_key)
    # Re-number order to match sort.
    items = [Parsed8KItem(item_code=it.item_code, item_order=idx, body=it.body) for idx, it in enumerate(items)]
    # Preserve the labels mapping (if provided) — annotating unused
    # here since the ingester applies it at upsert time; keeping the
    # param on the public signature so a future call site can request
    # labelled parse output without another round-trip.
    _ = labels
    return tuple(items)


def _extract_exhibits(item_901_body: str) -> tuple[Parsed8KExhibit, ...]:
    exhibits: list[Parsed8KExhibit] = []
    seen: set[str] = set()
    for m in _EXHIBIT_LINE_RE.finditer(item_901_body):
        num = m.group("num")
        if num in seen:
            continue
        desc = m.group("desc").strip()
        # Cap description length defensively.
        if len(desc) > 500:
            desc = desc[:500]
        seen.add(num)
        exhibits.append(Parsed8KExhibit(exhibit_number=num, description=desc))
    return tuple(exhibits)


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_8k_filing(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    parsed: Parsed8KFiling,
    item_labels: dict[str, tuple[str, str | None]] | None = None,
) -> None:
    """Insert / refresh the filing header + items + exhibits for one
    8-K accession.

    The items + exhibits snapshot is replaced atomically inside a
    savepoint so a failure mid-loop rolls back the DELETE too and
    the prior snapshot survives (``docs/review-prevention-log.md``:
    "DELETE-then-INSERT helper without a savepoint").
    """
    labels = item_labels or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eight_k_filings (
                accession_number, instrument_id, document_type,
                is_amendment, date_of_report, reporting_party,
                signature_name, signature_title, signature_date,
                remarks, primary_document_url, parser_version,
                is_tombstone, body_deferred
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, FALSE)
            ON CONFLICT (accession_number) DO UPDATE SET
                document_type        = EXCLUDED.document_type,
                is_amendment         = EXCLUDED.is_amendment,
                date_of_report       = EXCLUDED.date_of_report,
                reporting_party      = EXCLUDED.reporting_party,
                signature_name       = EXCLUDED.signature_name,
                signature_title      = EXCLUDED.signature_title,
                signature_date       = EXCLUDED.signature_date,
                remarks              = EXCLUDED.remarks,
                primary_document_url = EXCLUDED.primary_document_url,
                parser_version       = EXCLUDED.parser_version,
                is_tombstone         = FALSE,
                body_deferred        = FALSE,
                fetched_at           = NOW()
            """,
            (
                accession_number,
                instrument_id,
                parsed.document_type,
                parsed.is_amendment,
                parsed.date_of_report,
                parsed.reporting_party,
                parsed.signature_name,
                parsed.signature_title,
                parsed.signature_date,
                parsed.remarks,
                primary_document_url,
                _PARSER_VERSION,
            ),
        )

    # Items + exhibits snapshot: clear-and-repopulate inside a
    # savepoint so a failure mid-loop rolls back the DELETE too.
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM eight_k_items WHERE accession_number = %s",
                (accession_number,),
            )
            for item in parsed.items:
                label, severity = labels.get(item.item_code, (item.item_code, None))
                cur.execute(
                    """
                    INSERT INTO eight_k_items
                        (accession_number, item_code, item_label,
                         severity, item_order, body)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        accession_number,
                        item.item_code,
                        label,
                        severity,
                        item.item_order,
                        item.body,
                    ),
                )
            cur.execute(
                "DELETE FROM eight_k_exhibits WHERE accession_number = %s",
                (accession_number,),
            )
            for ex in parsed.exhibits:
                cur.execute(
                    """
                    INSERT INTO eight_k_exhibits
                        (accession_number, exhibit_number, description)
                    VALUES (%s, %s, %s)
                    """,
                    (accession_number, ex.exhibit_number, ex.description),
                )


def seed_eight_k_metadata(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    document_type: str,
    is_amendment: bool,
    date_of_report: date | None,
    primary_document_url: str,
    known_items: tuple[str, ...],
    item_labels: dict[str, tuple[str, str | None]] | None = None,
) -> bool:
    """Seed a deferred 8-K metadata row (#1343 bootstrap path).

    Writes the filing header (``document_type`` / ``is_amendment`` /
    ``date_of_report`` from ``filing_events`` metadata — NO body fetch)
    plus one ``eight_k_items`` row per ``known_items`` code (label /
    severity from ``sec_8k_item_codes``, ``body=''``), with
    ``body_deferred=TRUE`` and ``is_tombstone=FALSE``. The events rail
    renders from this metadata; item bodies + exhibits + signature
    fields fill lazily on first 8-K detail open (the
    ``/eight_k_filings/{accession}/body`` endpoint).

    ``date_of_report`` is sourced from ``filing_events.report_date``
    (submissions.json reportDate) so the rail orders correctly with no
    fetch. ``ON CONFLICT DO NOTHING`` — never clobbers a fetched filing
    or an existing deferred row. Returns True iff the filing row was
    inserted (its items are seeded in the same call). Caller owns the
    transaction boundary.
    """
    labels = item_labels or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eight_k_filings (
                accession_number, instrument_id, document_type,
                is_amendment, date_of_report, primary_document_url,
                parser_version, is_tombstone, body_deferred
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, FALSE, TRUE)
            ON CONFLICT (accession_number) DO NOTHING
            RETURNING accession_number
            """,
            (
                accession_number,
                instrument_id,
                document_type,
                is_amendment,
                date_of_report,
                primary_document_url,
                _PARSER_VERSION,
            ),
        )
        if cur.fetchone() is None:
            return False
        # The filing row is brand new (ON CONFLICT DO NOTHING returned a
        # row), so no eight_k_items exist yet for it — plain INSERT is
        # safe. Dedup codes defensively (filing_events.items can repeat).
        for order, code in enumerate(dict.fromkeys(known_items)):
            label, severity = labels.get(code, (code, None))
            cur.execute(
                """
                INSERT INTO eight_k_items
                    (accession_number, item_code, item_label,
                     severity, item_order, body)
                VALUES (%s, %s, %s, %s, %s, '')
                """,
                (accession_number, code, label, severity, order),
            )
    return True


def fetch_eight_k_body_now(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    accession_number: str,
) -> Literal["filled", "already", "not_deferred", "no_source", "failed"]:
    """Lazily fetch + cache a deferred 8-K filing body (#1343).

    Called by the 8-K body API when the rail detail for a filing whose
    ``body_deferred=TRUE`` is opened. Fetches the primary document,
    parses items + exhibits + signature, upserts (which clears
    ``body_deferred`` + fills item bodies/exhibits), stores raw, and
    transitions the manifest ``'deferred'→'parsed'`` (#938 raw-before-
    parsed honoured here — the worker guard does not cover this path).

    A blocking per-accession advisory lock collapses the concurrent
    double-open herd (advisory ``hashtext`` namespace, matching
    ``app/jobs/locks.py``; never blocks the manifest worker). Determin-
    istic failure converts the deferred row to a tombstone (so the rail
    drops it — matching the eager path) + manifest ``→'tombstoned'`` and
    returns ``'failed'`` (API → 2xx empty). A transient error RAISES
    (API → 503). Caller owns the request; this commits its own units.
    """
    from app.services.raw_filings import store_raw
    from app.services.sec_manifest import transition_status

    lock_key = f"sec_lazy_body_8k:{accession_number}"

    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id, primary_document_url, body_deferred "
            "FROM eight_k_filings WHERE accession_number = %s",
            (accession_number,),
        )
        row = cur.fetchone()
    conn.commit()
    if row is None or not bool(row[2]):
        return "not_deferred"
    instrument_id = int(row[0])
    url = row[1]

    def _tombstone(reason: str) -> None:
        with conn.cursor() as cur:
            # Drop the seeded metadata item rows so the detail returns a
            # clean empty state, not placeholder labels with empty bodies
            # (Codex ckpt2). The filing row stays tombstoned → the rail
            # excludes it and it isn't re-fetched.
            cur.execute("DELETE FROM eight_k_items WHERE accession_number = %s", (accession_number,))
            cur.execute(
                "UPDATE eight_k_filings SET is_tombstone = TRUE, body_deferred = FALSE, "
                "fetched_at = NOW() WHERE accession_number = %s",
                (accession_number,),
            )
        try:
            transition_status(conn, accession_number, ingest_status="tombstoned", error=reason)
        except Exception:
            logger.warning(
                "fetch_eight_k_body_now: manifest tombstone transition skipped accession=%s",
                accession_number,
                exc_info=True,
            )
        conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtext(%s)::int)", (lock_key,))
    conn.commit()
    try:
        # Re-read under the lock — a concurrent holder may have just filled it.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT body_deferred FROM eight_k_filings WHERE accession_number = %s",
                (accession_number,),
            )
            r2 = cur.fetchone()
        conn.commit()
        if r2 is None or not bool(r2[0]):
            return "already"

        if not url:
            # No fetchable URL (malformed seed) — tombstone so the row
            # EXITS the deferred state rather than being re-attempted on
            # every click (bot review BLOCKING: no_source infinite-defer).
            _tombstone("no primary_document_url")
            return "no_source"

        labels = _load_item_labels(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(items, ARRAY[]::TEXT[]) FROM filing_events "
                "WHERE provider = 'sec' AND provider_filing_id = %s LIMIT 1",
                (accession_number,),
            )
            irow = cur.fetchone()
        conn.commit()
        known_items = tuple(str(c) for c in (irow[0] if irow and irow[0] else []))

        try:
            html = fetcher.fetch_document_text(str(url))
        except Exception:
            logger.warning("fetch_eight_k_body_now: fetch failed accession=%s", accession_number, exc_info=True)
            raise  # transient → API 503

        if html is None:
            _tombstone("lazy fetch: empty or non-200")
            return "failed"

        parsed = parse_8k_filing(html, known_items=known_items, item_labels=labels)
        if parsed is None:
            _tombstone("lazy fetch: parse miss")
            return "failed"

        upsert_8k_filing(
            conn,
            instrument_id=instrument_id,
            accession_number=accession_number,
            primary_document_url=str(url),
            parsed=parsed,
            item_labels=labels,
        )
        try:
            store_raw(
                conn,
                accession_number=accession_number,
                document_kind="primary_doc",
                payload=html,
                source_url=str(url),
            )
            transition_status(conn, accession_number, ingest_status="parsed", raw_status="stored")
        except Exception:
            logger.warning(
                "fetch_eight_k_body_now: manifest deferred→parsed transition skipped "
                "accession=%s (body cached; split non-fatal)",
                accession_number,
                exc_info=True,
            )
        conn.commit()
        return "filled"
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s)::int)", (lock_key,))
        conn.commit()


def _write_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    primary_document_url: str,
    document_type: str,
) -> None:
    """Mark an accession as unfetchable / unparseable at the filing
    level so the next ingester pass skips it."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO eight_k_filings (
                accession_number, instrument_id, document_type,
                primary_document_url, parser_version, is_tombstone
            ) VALUES (%s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (accession_number) DO NOTHING
            """,
            (
                accession_number,
                instrument_id,
                document_type,
                primary_document_url,
                _PARSER_VERSION,
            ),
        )


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class EightKItemRow:
    item_code: str
    item_label: str
    severity: str | None
    body: str


@dataclass(frozen=True)
class EightKExhibitRow:
    exhibit_number: str
    description: str | None


@dataclass(frozen=True)
class EightKFilingRow:
    accession_number: str
    document_type: str
    is_amendment: bool
    date_of_report: date | None
    reporting_party: str | None
    signature_name: str | None
    signature_title: str | None
    signature_date: date | None
    primary_document_url: str | None
    body_deferred: bool
    items: tuple[EightKItemRow, ...]
    exhibits: tuple[EightKExhibitRow, ...]


def list_8k_filings(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    limit: int = 50,
) -> list[EightKFilingRow]:
    """Return recent 8-K filings for an instrument with items +
    exhibits attached. Tombstoned filings excluded."""
    with conn.cursor() as cur:
        # Per-share-class read bridge (#1117 PR-B): eight_k_filings is
        # entity-level (PK accession, child FKs anchor on it). Reads
        # for share-class siblings route through filing_events as the
        # per-instrument bridge so both GOOG and GOOGL render the
        # same 8-K event without duplicating eight_k_filings rows.
        cur.execute(
            """
            SELECT
                f.accession_number, f.document_type, f.is_amendment,
                f.date_of_report, f.reporting_party,
                f.signature_name, f.signature_title, f.signature_date,
                f.primary_document_url, f.body_deferred,
                COALESCE(f.date_of_report, fe.filing_date) AS effective_date
            FROM eight_k_filings f
            JOIN LATERAL (
                -- Per-share-class bridge (#1117 PR-B) + #1343 effective
                -- date. LATERAL + LIMIT 1 avoids row fan-out when an
                -- accession maps to multiple filing_events siblings, and
                -- exposes filing_date so a deferred row (date_of_report
                -- NULL until lazy fill) orders by its filing date instead
                -- of sinking to the bottom of the rail.
                SELECT fe.filing_date
                FROM filing_events fe
                WHERE fe.provider_filing_id = f.accession_number
                  AND fe.provider = 'sec'
                  AND fe.instrument_id = %s
                  AND fe.filing_type IN ('8-K', '8-K/A')
                ORDER BY fe.filing_event_id
                LIMIT 1
            ) fe ON TRUE
            WHERE f.is_tombstone = FALSE
            ORDER BY effective_date DESC NULLS LAST, f.fetched_at DESC
            LIMIT %s
            """,
            (instrument_id, limit),
        )
        raw_filings = cur.fetchall()
        accessions = [str(r[0]) for r in raw_filings]

        items_by_acc: dict[str, list[EightKItemRow]] = {a: [] for a in accessions}
        exhibits_by_acc: dict[str, list[EightKExhibitRow]] = {a: [] for a in accessions}
        if accessions:
            cur.execute(
                """
                SELECT accession_number, item_code, item_label,
                       severity, body
                FROM eight_k_items
                WHERE accession_number = ANY(%s)
                ORDER BY accession_number, item_order
                """,
                (accessions,),
            )
            for acc, code, label, severity, body in cur.fetchall():
                items_by_acc[str(acc)].append(
                    EightKItemRow(
                        item_code=str(code),
                        item_label=str(label),
                        severity=severity,
                        body=str(body),
                    )
                )
            cur.execute(
                """
                SELECT accession_number, exhibit_number, description
                FROM eight_k_exhibits
                WHERE accession_number = ANY(%s)
                ORDER BY accession_number, exhibit_number
                """,
                (accessions,),
            )
            for acc, num, desc in cur.fetchall():
                exhibits_by_acc[str(acc)].append(
                    EightKExhibitRow(
                        exhibit_number=str(num),
                        description=desc,
                    )
                )
    rows: list[EightKFilingRow] = []
    for r in raw_filings:
        acc = str(r[0])
        rows.append(
            EightKFilingRow(
                accession_number=acc,
                document_type=str(r[1]),
                is_amendment=bool(r[2]),
                date_of_report=r[3],
                reporting_party=r[4],
                signature_name=r[5],
                signature_title=r[6],
                signature_date=r[7],
                primary_document_url=r[8],
                body_deferred=bool(r[9]),
                items=tuple(items_by_acc.get(acc, [])),
                exhibits=tuple(exhibits_by_acc.get(acc, [])),
            )
        )
    return rows


def get_8k_filing(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
) -> EightKFilingRow | None:
    """Read one 8-K filing (header + items + exhibits) by accession.

    Used by the lazy-body API after :func:`fetch_eight_k_body_now` fills
    a deferred filing, to return the now-complete row. Returns None if
    the accession has no row. (Unlike :func:`list_8k_filings`, this does
    not filter tombstones — the caller inspects the row.)"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, document_type, is_amendment,
                   date_of_report, reporting_party,
                   signature_name, signature_title, signature_date,
                   primary_document_url, body_deferred
            FROM eight_k_filings
            WHERE accession_number = %s
            """,
            (accession_number,),
        )
        frow = cur.fetchone()
        if frow is None:
            return None
        cur.execute(
            """
            SELECT item_code, item_label, severity, body
            FROM eight_k_items WHERE accession_number = %s
            ORDER BY item_order
            """,
            (accession_number,),
        )
        items = tuple(
            EightKItemRow(item_code=str(c), item_label=str(lbl), severity=sev, body=str(b))
            for c, lbl, sev, b in cur.fetchall()
        )
        cur.execute(
            """
            SELECT exhibit_number, description
            FROM eight_k_exhibits WHERE accession_number = %s
            ORDER BY exhibit_number
            """,
            (accession_number,),
        )
        exhibits = tuple(EightKExhibitRow(exhibit_number=str(n), description=d) for n, d in cur.fetchall())
    return EightKFilingRow(
        accession_number=str(frow[0]),
        document_type=str(frow[1]),
        is_amendment=bool(frow[2]),
        date_of_report=frow[3],
        reporting_party=frow[4],
        signature_name=frow[5],
        signature_title=frow[6],
        signature_date=frow[7],
        primary_document_url=frow[8],
        body_deferred=bool(frow[9]),
        items=items,
        exhibits=exhibits,
    )


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class IngestResult:
    filings_scanned: int
    filings_parsed: int
    items_inserted: int
    fetch_errors: int
    parse_misses: int


def _load_item_labels(
    conn: psycopg.Connection[Any],
) -> dict[str, tuple[str, str | None]]:
    """Load sec_8k_item_codes into a (code → (label, severity)) map.

    ``severity`` is preserved as-is from the row — ``None`` passes
    through as ``None`` rather than being coerced to the string
    "None" by ``str()``. The schema today is NOT NULL but widening
    the reader keeps the helper safe against a future migration
    that relaxes that constraint.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT code, label, severity FROM sec_8k_item_codes")
        result: dict[str, tuple[str, str | None]] = {}
        for r in cur.fetchall():
            severity_raw = r[2]
            severity = str(severity_raw) if severity_raw is not None else None
            result[str(r[0])] = (str(r[1]), severity)
        return result


def ingest_8k_events(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 200,
    prefetch_urls: bool = False,
    prefetch_user_agent: str | None = None,
) -> IngestResult:
    """Scan 8-K filings lacking an ``eight_k_filings`` row, fetch the
    primary document, parse, upsert.

    Candidate selector:

    1. ``fe.filing_type IN ('8-K', '8-K/A')``.
    2. ``fe.primary_document_url IS NOT NULL``.
    3. No existing ``eight_k_filings`` row (tombstones live in the
       same table so a failed filing isn't re-fetched every tick).
    4. Ordered by filing_date DESC so fresh filings always get budget.
    """
    conn.commit()
    labels = _load_item_labels(conn)

    # #1343 — under an orchestrated bootstrap, seed 8-K METADATA only
    # (header + item codes/dates from filing_events; no body fetch),
    # recency-bounded like S18. The weekly safety-net / manual POST
    # (progress_ctx None) keep the eager fetch+parse path. Bodies fill
    # lazily on first 8-K detail open.
    progress_ctx = resolve_progress_context()
    metadata_only = progress_ctx is not None
    min_filing_date = bootstrap_filings_recency_floor() if metadata_only else None

    candidates: list[tuple[int, str, str, tuple[str, ...], str, date | None, date]] = []
    with conn.cursor() as cur:
        # Per-#1117 PR-B: filing_events fans out per share-class
        # sibling under sql/144. Universe-wide candidate selectors
        # must dedup by accession (inner CTE) before applying the
        # priority LIMIT, otherwise N siblings consume N slots of
        # the LIMIT budget for the same accession AND the parser
        # runs twice with different canonical instrument_ids
        # (entity-level rows flap on each second pass).
        cur.execute(
            """
            WITH per_accession AS (
                SELECT DISTINCT ON (fe.provider_filing_id)
                    fe.instrument_id, fe.provider_filing_id,
                    fe.primary_document_url, fe.items, fe.filing_date,
                    fe.filing_type, fe.report_date,
                    fe.filing_event_id
                FROM filing_events fe
                LEFT JOIN eight_k_filings ekf
                    ON ekf.accession_number = fe.provider_filing_id
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('8-K', '8-K/A')
                  AND fe.primary_document_url IS NOT NULL
                  AND ekf.accession_number IS NULL
                  AND (%(min_filing_date)s::date IS NULL
                       OR fe.filing_date >= %(min_filing_date)s)
                ORDER BY fe.provider_filing_id, fe.instrument_id
            )
            SELECT instrument_id, provider_filing_id, primary_document_url,
                   COALESCE(items, ARRAY[]::TEXT[]),
                   filing_type, report_date, filing_date
            FROM per_accession
            ORDER BY filing_date DESC, filing_event_id DESC
            LIMIT %(limit)s
            """,
            {"limit": (None if metadata_only else limit), "min_filing_date": min_filing_date},
        )
        for row in cur.fetchall():
            candidates.append(
                (
                    int(row[0]),
                    str(row[1]),
                    str(row[2]),
                    tuple(str(c) for c in (row[3] or [])),
                    str(row[4]),
                    row[5],
                    row[6],
                )
            )
    conn.commit()

    filings_parsed = 0
    items_inserted = 0
    fetch_errors = 0
    parse_misses = 0

    # #1045 fast path: prefetch the cohort's primary documents via the
    # pipelined fetcher so per-filing fetch_document_text reads from
    # cache. Misses fall back to the underlying sync fetcher.
    if prefetch_urls and candidates and not metadata_only:
        from app.services.sec_pipelined_fetcher import _CachedDocFetcher, prefetch_document_texts

        urls = [c[2] for c in candidates]
        ua = prefetch_user_agent or "eBull research/1.0"
        cache = prefetch_document_texts(urls, user_agent=ua)
        fetcher = _CachedDocFetcher(fetcher, cache)  # type: ignore[assignment]

    seeded = 0
    for instrument_id, accession, url, known_items, filing_type, report_date, filing_date in candidates:
        if metadata_only:
            # #1343 — bootstrap metadata-only seed: header + item codes
            # from filing_events (no HTTP, no exhibits/bodies/signatures).
            # date_of_report from report_date (submissions reportDate),
            # falling back to filing_date so the rail still orders right.
            if seed_eight_k_metadata(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                document_type=filing_type,
                is_amendment=is_amendment_form(filing_type),
                date_of_report=report_date or filing_date,
                primary_document_url=url,
                known_items=known_items,
                item_labels=labels,
            ):
                seeded += 1
                filings_parsed += 1
                items_inserted += len(dict.fromkeys(known_items))
            # DB-only seed: commit per chunk (no per-row HTTP crash-resume
            # need) so a 30-80k-row recency cohort doesn't pay a round-trip
            # per row.
            if seeded % 500 == 0:
                conn.commit()
            continue
        try:
            html = fetcher.fetch_document_text(url)
        except Exception:
            logger.warning(
                "ingest_8k_events: fetch failed accession=%s url=%s",
                accession,
                url,
                exc_info=True,
            )
            fetch_errors += 1
            _write_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                document_type="8-K",
            )
            conn.commit()
            continue
        if html is None:
            fetch_errors += 1
            _write_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                document_type="8-K",
            )
            conn.commit()
            continue

        parsed = parse_8k_filing(html, known_items=known_items, item_labels=labels)
        if parsed is None:
            parse_misses += 1
            _write_tombstone(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                document_type="8-K",
            )
            conn.commit()
            continue

        try:
            upsert_8k_filing(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                primary_document_url=url,
                parsed=parsed,
                item_labels=labels,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_8k_events: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            continue

        filings_parsed += 1
        items_inserted += len(parsed.items)

    if metadata_only:
        conn.commit()
        logger.info(
            "ingest_8k_events: metadata-only seed — cohort=%d seeded=%d "
            "min_filing_date=%s (bodies deferred to first view, #1343)",
            len(candidates),
            seeded,
            min_filing_date.isoformat() if min_filing_date is not None else "none",
        )

    return IngestResult(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        items_inserted=items_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )
