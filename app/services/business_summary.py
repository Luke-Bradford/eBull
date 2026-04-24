"""10-K Item 1 "Business" narrative extractor + ingester (#428).

Replaces the Yahoo ``longBusinessSummary`` blurb with the
authoritative multi-page description every SEC 10-K carries under
Item 1. Free, official, bounded per issuer to ~quarterly cadence
(10-K + 10-K/A amendments).

Shape mirrors :mod:`app.services.dividend_calendar` (#434):

- :func:`extract_business_section` is a pure function over raw HTML.
- :func:`ingest_business_summaries` is the DB path, bounded per run
  with a 7-day TTL on ``last_parsed_at`` so repeat fetches don't
  consume SEC rate-limit budget.

Acceptance bar from issue #428: instrument page shows an authentic
10-K business description for US tickers with a 10-K on file;
yfinance fallback only when absent.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# HTML stripping — shared with dividend_calendar in spirit, duplicated
# here so the two parsers don't couple on each other.
# ---------------------------------------------------------------------


_IXBRL_TAG_RE = re.compile(r"<ix:[^>]*>|</ix:[^>]*>", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_NBSP_RE = re.compile(r"&nbsp;|&#160;|&#xa0;| ", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def _strip_html(raw: str) -> str:
    """Strip HTML + iXBRL to a whitespace-collapsed plain-text stream.

    iXBRL tags are stripped independently first because their
    attribute content (``contextref``, ``unitref``, ...) otherwise
    leaks through a naive ``<[^>]+>`` pass when the browser-tolerant
    markup has nested/unbalanced tags that confuse the simpler
    regex. Attribute content is never user-facing narrative."""
    # Strip iXBRL element wrappers; the inner text survives.
    no_ix = _IXBRL_TAG_RE.sub(" ", raw)
    no_tags = _HTML_TAG_RE.sub(" ", no_ix)
    no_nbsp = _NBSP_RE.sub(" ", no_tags)
    return _WHITESPACE_RE.sub(" ", no_nbsp).strip()


# ---------------------------------------------------------------------
# Section extraction
# ---------------------------------------------------------------------


# Byte cap on the stored body. 10 KB is large enough for multi-page
# Item 1 bodies (empirical: biggest Aristocrats 10-Ks run 6–8 KB after
# whitespace collapse) with headroom for future "more…" expanders,
# and small enough that TOASTing stays cheap.
MAX_BODY_BYTES = 10 * 1024

# "Item 1. Business" — case-insensitive, tolerant of extra
# whitespace. The dot after the 1 is mandatory — 10-Ks consistently
# use the dotted form, and a dot-less match picked up false positives
# mid-sentence ("item 1 cause of action" etc.) in pilot runs.
_ITEM_1_RE = re.compile(r"\bItem\s+1\.\s*Business\b", re.IGNORECASE)

# "Item 1A. Risk Factors" is the universal end marker. If absent we
# fall back to a byte-cap slice from the Item 1 position.
_ITEM_1A_RE = re.compile(r"\bItem\s+1A\.\s*Risk\s+Factors\b", re.IGNORECASE)


def extract_business_section(raw_html: str) -> str | None:
    """Return the "Item 1. Business" narrative as plain text.

    Returns ``None`` when the Item 1 heading is absent. Otherwise
    returns the slice between the last Item 1 heading preceding the
    Item 1A marker and the Item 1A marker itself. When Item 1A is
    missing (malformed 10-K), takes a byte-bounded tail after Item 1.

    The "last before 1A" choice is deliberate: 10-Ks place a
    table-of-contents entry with the same "Item 1. Business" text
    earlier in the document, followed by the actual narrative
    heading. Picking the last occurrence before Item 1A skips the
    TOC entry and lands on the real section header.
    """
    if not raw_html:
        return None
    text = _strip_html(raw_html)

    matches_1 = list(_ITEM_1_RE.finditer(text))
    if not matches_1:
        return None

    # Take the LAST Item 1A occurrence, not the first. The table of
    # contents at the top of a 10-K lists "Item 1A. Risk Factors"
    # once as a link target — the real heading appears again later.
    # Using the last occurrence ensures the body region we slice
    # actually contains the Item 1 narrative between the real Item
    # 1 heading (also the last occurrence) and the real Item 1A
    # heading. Same logic applies to Item 1.
    matches_1a = list(_ITEM_1A_RE.finditer(text))
    end = matches_1a[-1].start() if matches_1a else len(text)

    # Pick the last Item 1 marker that precedes Item 1A (or EOF).
    # Filings that have only a TOC mention fall into the first
    # match, which is a tight slice — callers should enforce a
    # minimum body length before storing (done in the ingester).
    candidates = [m for m in matches_1 if m.start() < end]
    if not candidates:
        return None
    start = candidates[-1].end()

    body = text[start:end].strip()
    if not body:
        return None

    # Byte-cap on plain text. UTF-8 encoding is ASCII-dominated for
    # English 10-Ks so .encode() is cheap.
    encoded = body.encode("utf-8")
    if len(encoded) > MAX_BODY_BYTES:
        # Decode-safe truncation: step back to a valid codepoint if
        # the cap landed mid-byte-sequence.
        truncated = encoded[:MAX_BODY_BYTES]
        body = truncated.decode("utf-8", errors="ignore")

    return body


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_business_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    body: str,
    source_accession: str,
) -> bool:
    """Insert or update one ``instrument_business_summary`` row.

    Returns ``True`` on INSERT, ``False`` on UPDATE. The UPDATE path
    overwrites the body + source_accession + timestamps so a later
    10-K supersedes an older one cleanly."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_business_summary
                (instrument_id, body, source_accession)
            VALUES (%s, %s, %s)
            ON CONFLICT (instrument_id) DO UPDATE SET
                body             = EXCLUDED.body,
                source_accession = EXCLUDED.source_accession,
                fetched_at       = NOW(),
                last_parsed_at   = NOW()
            RETURNING (xmax = 0) AS inserted
            """,
            (instrument_id, body, source_accession),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False


def record_parse_attempt(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
) -> None:
    """Stamp a parse attempt without ever overwriting a real body.

    INSERT path (first-time failure): writes a tombstone row with
    ``body = ''`` and the failing ``source_accession`` so the next
    ingester pass sees a row with ``source_accession`` == the same
    accession and ``last_parsed_at`` < 7 days — and skips it.

    UPDATE path (prior row exists, failed retry): only bumps
    ``last_parsed_at`` + ``source_accession``. Preserves any real
    ``body`` from an earlier successful parse so a transient error
    on a later 10-K can never destroy the extracted narrative
    (Codex #434 / #446 BLOCKING pattern applied to #428).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_business_summary
                (instrument_id, body, source_accession)
            VALUES (%s, '', %s)
            ON CONFLICT (instrument_id) DO UPDATE SET
                source_accession = EXCLUDED.source_accession,
                last_parsed_at   = NOW()
            """,
            (instrument_id, source_accession),
        )


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


def get_business_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> str | None:
    """Return the stored Item 1 body, or None when no body exists.

    A row with ``body = ''`` is a tombstone (the ingester tried and
    failed to extract) — treated as "no body available" by callers
    so the SEC-profile endpoint still falls through to the yfinance
    description fallback. Read-only."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT body FROM instrument_business_summary WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    body = str(row[0])
    return body if body else None


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one ``ingest_business_summaries`` call."""

    filings_scanned: int
    rows_inserted: int
    rows_updated: int
    fetch_errors: int
    parse_misses: int


# Soft minimum body length below which the extractor is treated as a
# parse miss. Tuned to exclude TOC-only fragments ("Item 1. Business
# ... 3") while keeping short-but-real business descriptions.
_MIN_BODY_LEN = 120


def ingest_business_summaries(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 200,
) -> IngestResult:
    """Scan 10-K filings, fetch primary doc, extract Item 1, upsert.

    Candidate selector (shape addresses Codex #428 findings):

    1. ``fe.filing_type IN ('10-K', '10-K/A')`` — amendments retain
       their ``/A`` suffix through the SEC pipeline (see
       ``app/services/fundamentals.py``); narrowing to plain ``'10-K'``
       misses restated annual reports and pins ``source_accession`` to
       a stale pre-amendment filing.
    2. ``fe.primary_document_url IS NOT NULL`` — unparseable without.
    3. No ``instrument_business_summary`` row, OR the stored
       ``source_accession`` differs from this filing's accession
       (later 10-K supersedes), OR the existing row is older than 7
       days since last parse (TTL-gated retry).
    4. Newest filing wins per instrument (``DISTINCT ON`` resolves to
       the latest filing_date, tie-break on filing_event_id); the
       outer query then sorts GLOBALLY newest-first so a backlog
       doesn't delay fresh filings for higher instrument_ids
       indefinitely.

    Bounded per run (``limit=200``). 10-Ks are quarterly so the
    steady-state backlog is small; a large limit protects against a
    catch-up after a scheduler outage without starving other SEC
    calls on the same rate-limit pool.
    """
    conn.commit()

    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH latest_per_instrument AS (
                SELECT DISTINCT ON (fe.instrument_id)
                       fe.instrument_id,
                       fe.provider_filing_id,
                       fe.primary_document_url,
                       fe.filing_date,
                       fe.filing_event_id
                FROM filing_events fe
                WHERE fe.provider = 'sec'
                  AND fe.filing_type IN ('10-K', '10-K/A')
                  AND fe.primary_document_url IS NOT NULL
                ORDER BY fe.instrument_id, fe.filing_date DESC, fe.filing_event_id DESC
            )
            SELECT lpi.instrument_id,
                   lpi.provider_filing_id,
                   lpi.primary_document_url
            FROM latest_per_instrument lpi
            LEFT JOIN instrument_business_summary bs
                   ON bs.instrument_id = lpi.instrument_id
            WHERE bs.instrument_id IS NULL
               OR bs.source_accession <> lpi.provider_filing_id
               OR bs.last_parsed_at < NOW() - INTERVAL '7 days'
            ORDER BY lpi.filing_date DESC, lpi.filing_event_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), str(row[2])))
    conn.commit()

    inserted = 0
    updated = 0
    fetch_errors = 0
    parse_misses = 0

    for instrument_id, accession, url in candidates:
        try:
            html = fetcher.fetch_document_text(url)
        except Exception:
            logger.warning(
                "ingest_business_summaries: fetch failed accession=%s url=%s",
                accession,
                url,
                exc_info=True,
            )
            fetch_errors += 1
            record_parse_attempt(conn, instrument_id=instrument_id, source_accession=accession)
            conn.commit()
            continue
        if html is None:
            fetch_errors += 1
            record_parse_attempt(conn, instrument_id=instrument_id, source_accession=accession)
            conn.commit()
            continue

        body = extract_business_section(html)
        if body is None or len(body) < _MIN_BODY_LEN:
            parse_misses += 1
            record_parse_attempt(conn, instrument_id=instrument_id, source_accession=accession)
            conn.commit()
            continue

        try:
            did_insert = upsert_business_summary(
                conn,
                instrument_id=instrument_id,
                body=body,
                source_accession=accession,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_business_summaries: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            continue

        if did_insert:
            inserted += 1
        else:
            updated += 1

    return IngestResult(
        filings_scanned=len(candidates),
        rows_inserted=inserted,
        rows_updated=updated,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )
