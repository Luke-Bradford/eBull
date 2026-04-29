"""Dividend-calendar parser + ingester for 8-K Item 8.01 (#434).

SEC XBRL (``financial_periods.dps_declared``) captures per-period
declared amounts but NOT the ex-date / record-date / pay-date
calendar. That calendar lives only in the free-form announcement
text of 8-Ks carrying Item 8.01 "Other Events". This module:

1. Parses a single 8-K primary-document text into a
   :class:`DividendAnnouncement` — a pure function, heavily unit-
   tested against real Aristocrats shapes.
2. Ingests unparsed 8.01 filings into ``dividend_events`` — pulls
   the primary document via the SEC provider, parses, upserts.

Acceptance bar (issue #434): ≥80% of Dividend Aristocrats extract
cleanly. The parser is intentionally regex-only — LLM-based fallback
is phase-2 if coverage proves insufficient.

Shape: ``parse_dividend_announcement`` is the one public pure
function. The DB path is :func:`ingest_dividend_events` which owns
the filing fetch + upsert, commits per filing so a bad one doesn't
roll back the batch.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import psycopg

logger = logging.getLogger(__name__)


class _DocFetcher(Protocol):
    """Minimal subset of ``SecFilingsProvider`` the ingester needs.

    Narrow Protocol (not the full provider) so tests can substitute
    a plain callable-shaped stub without implementing the full
    provider interface."""

    def fetch_document_text(self, absolute_url: str) -> str | None: ...


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class DividendAnnouncement:
    """Structured extraction from one 8-K Item 8.01 primary document.

    All date fields + the amount are individually nullable so a
    partially-parsed announcement still yields a useful row (e.g. an
    amount with no dates, or dates without an amount). ``None`` as
    the parser's overall return value means "not a dividend
    announcement" — a separate signal from "dividend announcement
    with missing fields".

    ``dps_declared`` is a string to preserve the exact decimal
    precision that appears in the filing. Casting to Decimal happens
    at the DB boundary via psycopg's NUMERIC adapter; string avoids
    introducing float drift in transit.
    """

    declaration_date: date | None = None
    ex_date: date | None = None
    record_date: date | None = None
    pay_date: date | None = None
    dps_declared: str | None = None


# ---------------------------------------------------------------------
# HTML → text
# ---------------------------------------------------------------------


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")
# &nbsp; / numeric entities appear everywhere in EDGAR HTML; normalise
# to plain spaces before the label regexes run so "\s+" buffers work.
_NBSP_RE = re.compile(r"&nbsp;|&#160;|&#xa0;| ", re.IGNORECASE)


def _strip_html(text: str) -> str:
    """Collapse HTML tags + entities to a single flat whitespace
    stream. Deliberately crude — the point is making the regexes
    below whitespace-tolerant, not preserving semantic structure."""
    no_nbsp = _NBSP_RE.sub(" ", text)
    no_tags = _HTML_TAG_RE.sub(" ", no_nbsp)
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
    # Three-letter abbreviations occur in press-release-style 8-Ks.
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
# Two shapes we accept:
#   (a) "Month DD, YYYY" — SEC canonical press-release form
#   (b) "MM/DD/YYYY" or "M/D/YYYY" — some smaller filers
_DATE_RE = re.compile(
    rf"(?:(?P<m1>{_MONTH_NAME_ALT})\s+(?P<d1>\d{{1,2}}),?\s+(?P<y1>\d{{4}}))"
    rf"|(?:(?P<m2>\d{{1,2}})/(?P<d2>\d{{1,2}})/(?P<y2>\d{{4}}))",
    re.IGNORECASE,
)


def _parse_date(raw: str | None) -> date | None:
    """Parse a single date token (already known to match ``_DATE_RE``).

    Returns ``None`` for out-of-range month/day or any parse failure
    — the parser never raises for bad dates, since filings carry
    typos often enough that aborting the whole announcement would
    drop real rows."""
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
        else:
            month = int(m.group("m2"))
            day = int(m.group("d2"))
            year = int(m.group("y2"))
        return date(year, month, day)
    except KeyError, ValueError:
        # `_MONTHS[name]` may KeyError on an unrecognised month
        # token; `int(...)` may ValueError on a token that is not a
        # parseable integer. Either path means the regex matched
        # something that does not actually represent a date — return
        # None and let the caller fall back to a different label.
        # Pre-#644 this was Python 2 syntax (`except KeyError, ValueError:`)
        # which is a SyntaxError in Python 3; the file imported
        # successfully only because the path was unreachable on the
        # adapter's failing flow at the time.
        return None


# ---------------------------------------------------------------------
# Label-oriented extraction
# ---------------------------------------------------------------------


# Dividend-language gate: the text MUST contain an explicit "dividend"
# reference near a dollar-per-share shape, otherwise it's not a
# dividend announcement (Item 8.01 also covers buybacks, litigation,
# JV news, etc.). The regex is intentionally broad on both sides so
# real announcements match regardless of word order.
_DIVIDEND_CONTEXT_RE = re.compile(
    r"dividend[\s\S]{0,120}?\$\s*\d+\.\d+[\s\S]{0,20}?per\s+share"
    r"|\$\s*\d+\.\d+[\s\S]{0,40}?per\s+share[\s\S]{0,120}?dividend",
    re.IGNORECASE,
)

_AMOUNT_RE = re.compile(
    r"\$\s*(?P<amt>\d+\.\d+)\s*(?:per\s+share|a\s+share|/\s*share)",
    re.IGNORECASE,
)

# Each calendar-date label below is anchored to a known phrase then a
# bounded lookahead for the date. The bound is tight enough that a
# later mention of a different date doesn't bleed across labels.
_DECLARATION_RE = re.compile(
    r"on\s+(?P<when>" + _DATE_RE.pattern + r")[^.]{0,160}?"
    r"(?:board|directors)[^.]{0,60}?declared",
    re.IGNORECASE,
)
_PAY_RE = re.compile(
    r"payable\s+(?:on\s+)?(?P<when>" + _DATE_RE.pattern + r")",
    re.IGNORECASE,
)
_RECORD_RE = re.compile(
    r"(?:shareholders?|stockholders?|holders?)\s+of\s+record"
    r"(?:\s+(?:as\s+of|on|at(?:\s+the\s+close\s+of\s+business\s+on)?))?"
    r"\s+(?P<when>" + _DATE_RE.pattern + r")",
    re.IGNORECASE,
)
_EX_RE = re.compile(
    r"ex[-\s]?dividend\s+date[^.]{0,40}?"
    r"(?P<when>" + _DATE_RE.pattern + r")",
    re.IGNORECASE,
)


def parse_dividend_announcement(raw_text: str) -> DividendAnnouncement | None:
    """Extract dividend calendar + amount from one 8-K Item 8.01 text.

    Returns ``None`` when the document does not look like a dividend
    announcement (no ``$N.NN per share`` + ``dividend`` co-occurring).

    Returns a :class:`DividendAnnouncement` otherwise — any
    individually-missing field is ``None``. Callers treat the return
    as advisory: a parsed row carrying only an amount still beats
    silently dropping a filing where the date language was in a
    shape the regex doesn't cover yet.
    """
    if not raw_text:
        return None
    text = _strip_html(raw_text)
    if not _DIVIDEND_CONTEXT_RE.search(text):
        return None

    amt_m = _AMOUNT_RE.search(text)
    dps = amt_m.group("amt") if amt_m else None

    declaration = _parse_date(_extract(_DECLARATION_RE, text))
    pay = _parse_date(_extract(_PAY_RE, text))
    record = _parse_date(_extract(_RECORD_RE, text))
    ex = _parse_date(_extract(_EX_RE, text))

    if not any((declaration, pay, record, ex, dps)):
        # Dividend context matched but nothing we can commit — skip.
        return None

    return DividendAnnouncement(
        declaration_date=declaration,
        ex_date=ex,
        record_date=record,
        pay_date=pay,
        dps_declared=dps,
    )


def _extract(pattern: re.Pattern[str], text: str) -> str | None:
    m = pattern.search(text)
    return m.group("when") if m else None


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one ``ingest_dividend_events`` call."""

    filings_scanned: int
    rows_inserted: int
    rows_updated: int
    fetch_errors: int
    parse_misses: int


def ingest_dividend_events(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 500,
) -> IngestResult:
    """Find unprocessed 8-K Item 8.01 filings, fetch + parse + upsert.

    Query shape: filings where ``provider='sec'``, ``filing_type='8-K'``,
    ``items`` contains ``'8.01'``, ``primary_document_url IS NOT NULL``,
    AND there is no existing ``dividend_events`` row for
    ``(instrument_id, provider_filing_id)``. Oldest-first so a stop
    mid-batch resumes cleanly.

    Commits after each filing. A fetch failure or a parse miss on one
    filing never rolls back siblings in the batch — per-row
    durability over throughput since the ingester runs daily and
    filings arrive in tens, not millions.

    Returns an :class:`IngestResult` with counters; callers log it.
    """
    conn.commit()  # settle any open read tx (service-wide durability invariant).

    # Candidate selector rules (addresses Codex #434 review):
    #
    # 1. Re-parse partial rows so a future regex improvement can backfill
    #    missing dates. A row is "partial" when any of the three calendar
    #    dates (ex/record/pay) is NULL. Upsert is idempotent so a stable
    #    full row never produces a duplicate.
    # 2. Gate the re-parse with a 7-day TTL (``created_at``) so stable
    #    partial rows don't pound SEC every run.
    # 3. Order by ``filing_date DESC`` so fresh filings always get budget
    #    — prevents a backlog of permanent parse-misses at the oldest end
    #    from starving newer, parseable announcements.
    candidates: list[tuple[int, int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.filing_event_id,
                   fe.instrument_id,
                   fe.provider_filing_id,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN dividend_events de
              ON de.instrument_id = fe.instrument_id
             AND de.source_accession = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type = '8-K'
              AND fe.items IS NOT NULL
              AND '8.01' = ANY(fe.items)
              AND fe.primary_document_url IS NOT NULL
              AND (
                    de.id IS NULL
                 OR (
                        (de.ex_date IS NULL OR de.record_date IS NULL OR de.pay_date IS NULL)
                        AND de.last_parsed_at < NOW() - INTERVAL '7 days'
                    )
              )
            ORDER BY fe.filing_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), int(row[1]), str(row[2]), str(row[3])))
    conn.commit()

    inserted = 0
    updated = 0
    fetch_errors = 0
    parse_misses = 0

    # Tombstone sentinel: written on fetch-errors + parse-misses so
    # the JOIN's 7-day TTL bounds re-fetches to weekly rather than
    # daily. On first-time: inserts a row with all-NULL date/amount
    # fields. On re-attempt (row already exists from a prior run
    # that parsed partial data): ONLY bumps ``last_parsed_at`` and
    # preserves the existing dates/amount — must not clobber
    # previously-extracted values with NULLs when a transient fetch
    # error hits between TTL windows (Codex PR #446 BLOCKING fix).
    def _write_tombstone(instrument_id: int, accession: str) -> None:
        try:
            _upsert_tombstone(conn, instrument_id=instrument_id, source_accession=accession)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_dividend_events: tombstone upsert failed accession=%s",
                accession,
                exc_info=True,
            )

    for _filing_id, instrument_id, accession, url in candidates:
        try:
            body = fetcher.fetch_document_text(url)
        except Exception:
            logger.warning(
                "ingest_dividend_events: fetch failed accession=%s url=%s",
                accession,
                url,
                exc_info=True,
            )
            fetch_errors += 1
            _write_tombstone(instrument_id, accession)
            continue
        if body is None:
            # 404 / 410 — filing withdrawn or URL moved. Write a
            # tombstone so the 7-day TTL on last_parsed_at caps retry
            # cadence at weekly rather than daily.
            fetch_errors += 1
            _write_tombstone(instrument_id, accession)
            continue

        announcement = parse_dividend_announcement(body)
        if announcement is None:
            # Non-dividend 8.01 (buyback, JV, litigation) — same
            # tombstone treatment. The parser may improve later and
            # weekly re-parse will pick it up.
            parse_misses += 1
            _write_tombstone(instrument_id, accession)
            continue

        try:
            did_insert = upsert_dividend_event(
                conn,
                instrument_id=instrument_id,
                source_accession=accession,
                announcement=announcement,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_dividend_events: upsert failed accession=%s",
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


def _upsert_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
) -> None:
    """Insert an all-NULL tombstone OR bump ``last_parsed_at`` on an
    existing row — never overwrites non-NULL date/amount fields.

    Called when the ingester records a fetch error or parse miss for
    a filing. The idempotency contract of the partial-row TTL rule
    (re-parse after 7 days) combined with a fetch failure between
    TTL windows must NOT destroy the previously-parsed dates. A
    prior run's real data wins over a later transient-failure
    tombstone.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dividend_events
                (instrument_id, source_accession, declaration_date,
                 ex_date, record_date, pay_date, dps_declared, currency)
            VALUES (%s, %s, NULL, NULL, NULL, NULL, NULL, 'USD')
            ON CONFLICT (instrument_id, source_accession) DO UPDATE SET
                last_parsed_at = NOW()
            """,
            (instrument_id, source_accession),
        )


def upsert_dividend_event(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    source_accession: str,
    announcement: DividendAnnouncement,
    currency: str = "USD",
) -> bool:
    """Insert or update one ``dividend_events`` row.

    Returns ``True`` on INSERT, ``False`` on UPDATE. Idempotency key
    is ``(instrument_id, source_accession)`` per the migration's
    UNIQUE constraint — re-running the ingester on the same filing
    rewrites dates/amount if the parser now recognises shapes it
    previously missed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dividend_events
                (instrument_id, source_accession, declaration_date,
                 ex_date, record_date, pay_date, dps_declared, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id, source_accession) DO UPDATE SET
                declaration_date = EXCLUDED.declaration_date,
                ex_date          = EXCLUDED.ex_date,
                record_date      = EXCLUDED.record_date,
                pay_date         = EXCLUDED.pay_date,
                dps_declared     = EXCLUDED.dps_declared,
                currency         = EXCLUDED.currency,
                last_parsed_at   = NOW()
            RETURNING (xmax = 0) AS inserted
            """,
            (
                instrument_id,
                source_accession,
                announcement.declaration_date,
                announcement.ex_date,
                announcement.record_date,
                announcement.pay_date,
                announcement.dps_declared,
                currency,
            ),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False
