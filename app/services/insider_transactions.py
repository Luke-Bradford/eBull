"""Form 4 insider-transactions parser + ingester (#429).

SEC Form 4 is filed by directors, officers, and ≥10% holders within
two business days of any trade in their company's stock. Free,
timely, strong sentiment signal. The submissions.json feed flags
``insiderTransactionForIssuerExists: 1`` on issuers that have ever
filed a Form 4; the transaction rows themselves live in each Form 4
XML primary document.

Shape mirrors :mod:`app.services.business_summary` (#428):

- :func:`parse_form_4_xml` is a pure function over raw XML.
- :func:`ingest_insider_transactions` is the DB path — queries
  filing_events for Form 4 filings lacking matching insider rows,
  fetches the XML, parses, upserts.

No raw-file persistence (per issue scope): parsed rows are the
durable artifact. The accession_number links back to the source
filing for audit.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET  # noqa: S405 — Form 4 source is SEC EDGAR, trusted.
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

import psycopg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedTransaction:
    """One row extracted from a Form 4 transaction table."""

    txn_row_num: int
    txn_date: date
    txn_code: str
    shares: Decimal | None
    price: Decimal | None
    direct_indirect: str | None
    is_derivative: bool


@dataclass(frozen=True)
class ParsedFiling:
    """Structured extraction of one Form 4 XML primary document."""

    filer_name: str
    filer_role: str | None
    transactions: tuple[ParsedTransaction, ...]


# ---------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------


def parse_form_4_xml(raw_xml: str) -> ParsedFiling | None:
    """Extract filer + transactions from a Form 4 primary document.

    Returns ``None`` when:

    - The input is empty or fails to parse as XML.
    - The document root is not ``ownershipDocument`` (wrong document
      at the URL — URL rot or operator pointed us at a cover page).
    - Zero transactions are present under either the non-derivative
      or derivative table. A filer-only row is not a useful
      observation; drop it rather than insert a phantom record.

    Otherwise returns a :class:`ParsedFiling` with zero-or-more
    :class:`ParsedTransaction` rows. Row indices (``txn_row_num``)
    are source-order across both tables: non-derivative first, then
    derivative — this makes the ``(accession, row_num)`` UNIQUE key
    stable across re-parses."""
    if not raw_xml:
        return None
    # Strip any XML default namespace before parsing. SEC's canonical
    # Form 4 XSD declares a namespace and real filings sometimes inline
    # it as ``xmlns="..."`` at the root, which turns every subsequent
    # namespace-blind ``.find()`` into a miss. Regex pre-strip keeps
    # the rest of the parser simple without requiring every ``.find()``
    # site to carry ``{ns}`` braces.
    cleaned_xml = _XMLNS_RE.sub("", raw_xml)
    try:
        root = ET.fromstring(cleaned_xml)
    except ET.ParseError:
        return None

    # Form 4 root is ``ownershipDocument``; if anything else is at the
    # root, we have the wrong document.
    if _localname(root.tag) != "ownershipDocument":
        return None

    filer_name = _text(root.find(".//reportingOwnerId/rptOwnerName"))
    if filer_name is None:
        return None

    filer_role = _extract_role(root.find(".//reportingOwnerRelationship"))
    transactions = _extract_transactions(root)

    if not transactions:
        return None

    return ParsedFiling(
        filer_name=filer_name,
        filer_role=filer_role,
        transactions=tuple(transactions),
    )


# Matches the ``xmlns="..."`` attribute on any element. We strip
# these before parsing so element tag lookups don't have to be
# namespace-aware — cheap trade vs threading ``{ns}`` braces through
# every ``.find()`` path below.
_XMLNS_RE = re.compile(r'\sxmlns="[^"]*"')


def _localname(tag: str) -> str:
    """Strip the XML namespace prefix from an element tag. Form 4 uses
    no namespace in practice, but defensive against future schema
    changes."""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _text(el: ET.Element | None) -> str | None:
    if el is None or el.text is None:
        return None
    stripped = el.text.strip()
    return stripped or None


def _child_text(el: ET.Element | None, path: str) -> str | None:
    """Convenience for the common ``transactionShares/value`` shape
    where a leaf node wraps its value in a ``<value>`` child."""
    if el is None:
        return None
    target = el.find(path)
    return _text(target)


def _extract_role(rel: ET.Element | None) -> str | None:
    """Pipe-join the relationship flags into a single role string.

    The Form 4 ``reportingOwnerRelationship`` block carries four
    mutually-non-exclusive boolean flags (isDirector, isOfficer,
    isTenPercentOwner, isOther). Joining them preserves every signal
    a later sentiment tag might want to weight (e.g. CEO buys vs
    10%-holder buys) without exploding the column count.

    Returns ``None`` when the block is absent entirely — the filer
    still counts as an "insider" by virtue of having filed a Form 4,
    just with unknown specifics."""
    if rel is None:
        return None
    parts: list[str] = []
    if _child_text(rel, "isDirector") == "1":
        parts.append("director")
    if _child_text(rel, "isOfficer") == "1":
        title = _child_text(rel, "officerTitle")
        parts.append(f"officer:{title}" if title else "officer")
    if _child_text(rel, "isTenPercentOwner") == "1":
        parts.append("ten_percent_owner")
    if _child_text(rel, "isOther") == "1":
        other_text = _child_text(rel, "otherText")
        parts.append(f"other:{other_text}" if other_text else "other")
    return "|".join(parts) if parts else None


def _extract_transactions(root: ET.Element) -> list[ParsedTransaction]:
    """Walk both transaction tables in source order.

    ``txn_row_num`` is the SOURCE POSITION across both tables (non-
    derivative first, then derivative) so the UNIQUE
    ``(accession, row_num)`` key is stable across parser revisions.
    Row 0 that fails to parse today and succeeds after a regex fix
    tomorrow still lands at row 0; parseable siblings keep their
    indices. Increment the counter for every source element, not
    only the ones that parse successfully."""
    rows: list[ParsedTransaction] = []
    row_num = 0
    for table_tag, is_deriv in (
        ("nonDerivativeTable/nonDerivativeTransaction", False),
        ("derivativeTable/derivativeTransaction", True),
    ):
        for txn_el in root.findall(table_tag):
            parsed = _parse_one_transaction(txn_el, row_num, is_deriv)
            row_num += 1  # always advance, even when _parse_one_transaction returns None
            if parsed is not None:
                rows.append(parsed)
    return rows


_VALID_DIRECT_INDIRECT = {"D", "I"}


def _parse_one_transaction(
    txn: ET.Element,
    row_num: int,
    is_derivative: bool,
) -> ParsedTransaction | None:
    """Parse one ``(nonD|D)erivativeTransaction`` element.

    Returns ``None`` when the date or code can't be extracted — these
    are the two fields the ``insider_transactions`` schema marks
    NOT NULL, so a row missing either is unusable. Shares, price,
    and direct_indirect are all nullable and pass through as None
    when absent/malformed."""
    txn_date_s = _child_text(txn, "transactionDate/value")
    txn_code = _child_text(txn, "transactionCoding/transactionCode")
    if not txn_date_s or not txn_code:
        return None
    try:
        txn_date = date.fromisoformat(txn_date_s[:10])
    except ValueError:
        return None

    shares = _safe_decimal(
        _child_text(txn, "transactionAmounts/transactionShares/value"),
        max_value=_MAX_SHARES,
    )
    price = _safe_decimal(
        _child_text(txn, "transactionAmounts/transactionPricePerShare/value"),
        max_value=_MAX_PRICE,
    )
    # Grants often lack a price or carry "0"; normalise "0" / "0.00"
    # to None so the column semantically means "unpriced" rather
    # than "priced at zero".
    if price is not None and price == Decimal(0):
        price = None

    direct_raw = _child_text(txn, "ownershipNature/directOrIndirectOwnership/value")
    direct_indirect = direct_raw if direct_raw in _VALID_DIRECT_INDIRECT else None

    return ParsedTransaction(
        txn_row_num=row_num,
        txn_date=txn_date,
        txn_code=txn_code,
        shares=shares,
        price=price,
        direct_indirect=direct_indirect,
        is_derivative=is_derivative,
    )


# Shares precision in the schema is NUMERIC(20,4) → max ~10^16.
# Price is NUMERIC(18,6) → max ~10^12. Real Form 4 values never
# approach these — largest insider trades on record sit comfortably
# under 10^9 shares at under 10^6 per share. Reject values above
# these caps so a malformed filing (decimal-separator bug, a
# scientific-notation overflow) fails parsing rather than rolling
# back the whole transaction at INSERT time (Codex #429 M3).
_MAX_SHARES = Decimal("1e10")
_MAX_PRICE = Decimal("1e9")


def _safe_decimal(raw: str | None, *, max_value: Decimal) -> Decimal | None:
    """Parse a numeric value with strict Form 4 semantics.

    Returns ``None`` for malformed strings, NaN / Infinity, negative
    values, and values exceeding ``max_value``. The schema columns
    are NUMERIC(precision, scale) and the ``get_insider_summary``
    signed aggregation assumes non-negative inputs — a negative
    shares value would double-sign on a sell (code=S) and produce a
    phantom net buy. Defensive parse is cheaper than a ruined
    aggregate."""
    if raw is None:
        return None
    try:
        parsed = Decimal(raw)
    except InvalidOperation, ValueError:
        return None
    if not parsed.is_finite():  # NaN, Infinity, -Infinity
        return None
    if parsed < Decimal(0):
        return None
    if parsed > max_value:
        return None
    return parsed


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_transactions(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
    parsed: ParsedFiling,
) -> int:
    """Insert parsed Form 4 rows. Returns the number of rows inserted.

    Idempotency: the ``(accession_number, txn_row_num)`` UNIQUE key
    means re-running the ingester on the same filing is a no-op via
    ``ON CONFLICT DO NOTHING``. We do NOT update existing rows — a
    Form 4's transaction content is immutable; a correction is
    filed as a Form 4/A under a new accession."""
    inserted = 0
    with conn.cursor() as cur:
        for txn in parsed.transactions:
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_name, filer_role, txn_date, txn_code,
                     shares, price, direct_indirect, is_derivative)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (accession_number, txn_row_num) DO NOTHING
                """,
                (
                    instrument_id,
                    accession_number,
                    txn.txn_row_num,
                    parsed.filer_name,
                    parsed.filer_role,
                    txn.txn_date,
                    txn.txn_code,
                    txn.shares,
                    txn.price,
                    txn.direct_indirect,
                    txn.is_derivative,
                ),
            )
            inserted += cur.rowcount
    return inserted


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _DocFetcher(Protocol):
    def fetch_document_text(self, absolute_url: str) -> str | None: ...


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one ``ingest_insider_transactions`` call."""

    filings_scanned: int
    filings_parsed: int
    rows_inserted: int
    fetch_errors: int
    parse_misses: int


# Tombstone sentinels written on fetch error / parse miss so the
# ingester's LEFT JOIN excludes that accession on subsequent runs.
# Without this the hourly schedule would re-fetch every dead URL on
# every run (Codex #429 H1). The reader filters these out by
# ``filer_name = _TOMBSTONE_FILER`` so they never leak into summaries.
_TOMBSTONE_FILER = "__TOMBSTONE__"
_TOMBSTONE_CODE = "TOMBSTONE"


def _write_tombstone(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession_number: str,
) -> None:
    """Insert a sentinel row so the next ingester pass skips this
    accession. Uses row_num=-1 + a dedicated filer_name so the row
    is unambiguous to readers. Inserted with is_derivative=TRUE so
    even a missed reader filter wouldn't pollute the non-derivative
    90-day net-buy aggregate.

    Idempotent on re-run: the (accession, row_num) UNIQUE key makes
    a second tombstone attempt a no-op via ON CONFLICT DO NOTHING."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO insider_transactions
                (instrument_id, accession_number, txn_row_num,
                 filer_name, txn_date, txn_code, is_derivative)
            VALUES (%s, %s, -1, %s, CURRENT_DATE, %s, TRUE)
            ON CONFLICT (accession_number, txn_row_num) DO NOTHING
            """,
            (instrument_id, accession_number, _TOMBSTONE_FILER, _TOMBSTONE_CODE),
        )


def ingest_insider_transactions(
    conn: psycopg.Connection[Any],
    fetcher: _DocFetcher,
    *,
    limit: int = 500,
) -> IngestResult:
    """Scan Form 4 filings, fetch XML, parse, upsert.

    Candidate selector:

    1. ``fe.filing_type IN ('4', '4/A')`` — amendments carry the same
       shape and are authoritative replacements.
    2. ``fe.primary_document_url IS NOT NULL``.
    3. No existing ``insider_transactions`` row for the accession.
       Form 4 content is immutable per accession so a single success
       is enough. On fetch error / parse miss we write a tombstone
       (``filer_name = __TOMBSTONE__``, ``row_num = -1``) so the
       LEFT JOIN excludes the accession on subsequent runs — the
       hourly schedule must not re-fetch dead URLs every hour
       (Codex #429 H1). Tombstones are filtered by the reader so
       they don't leak into the summary.
    4. Ordered by filing_date DESC so fresh filings always get
       budget.

    Bounded per run (``limit=500``) to match the expected daily
    Form 4 volume across the universe.
    """
    conn.commit()

    candidates: list[tuple[int, str, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.instrument_id,
                   fe.provider_filing_id,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN (
                SELECT DISTINCT accession_number
                FROM insider_transactions
            ) it ON it.accession_number = fe.provider_filing_id
            WHERE fe.provider = 'sec'
              AND fe.filing_type IN ('4', '4/A')
              AND fe.primary_document_url IS NOT NULL
              AND it.accession_number IS NULL
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1]), str(row[2])))
    conn.commit()

    filings_parsed = 0
    rows_inserted = 0
    fetch_errors = 0
    parse_misses = 0

    for instrument_id, accession, url in candidates:
        try:
            xml = fetcher.fetch_document_text(url)
        except Exception:
            logger.warning(
                "ingest_insider_transactions: fetch failed accession=%s url=%s",
                accession,
                url,
                exc_info=True,
            )
            fetch_errors += 1
            _write_tombstone(conn, instrument_id=instrument_id, accession_number=accession)
            conn.commit()
            continue
        if xml is None:
            fetch_errors += 1
            _write_tombstone(conn, instrument_id=instrument_id, accession_number=accession)
            conn.commit()
            continue

        parsed = parse_form_4_xml(xml)
        if parsed is None:
            parse_misses += 1
            _write_tombstone(conn, instrument_id=instrument_id, accession_number=accession)
            conn.commit()
            continue

        try:
            inserted = upsert_transactions(
                conn,
                instrument_id=instrument_id,
                accession_number=accession,
                parsed=parsed,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_insider_transactions: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            continue

        filings_parsed += 1
        rows_inserted += inserted

    return IngestResult(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        rows_inserted=rows_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class InsiderSummary:
    """Aggregate view of recent insider activity for an instrument.

    ``net_shares_90d`` is positive when insiders net-bought over the
    window, negative when they net-sold. Only non-derivative
    transactions contribute — options/RSU grants are weaker signals
    and the issue explicitly wants buy/sell directionality."""

    net_shares_90d: Decimal
    buy_count_90d: int
    sell_count_90d: int
    unique_filers_90d: int
    latest_txn_date: date | None


def get_insider_summary(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> InsiderSummary:
    """Return the 90-day insider summary. Always returns a summary
    object (zero-counters when no activity) so callers don't have to
    branch on None."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(
                    CASE
                        WHEN txn_code = 'P' THEN shares
                        WHEN txn_code = 'S' THEN -shares
                        ELSE 0
                    END
                ), 0) AS net_shares,
                COUNT(*) FILTER (WHERE txn_code = 'P') AS buys,
                COUNT(*) FILTER (WHERE txn_code = 'S') AS sells,
                COUNT(DISTINCT filer_name) AS filers,
                MAX(txn_date) AS latest
            FROM insider_transactions
            WHERE instrument_id = %s
              AND is_derivative = FALSE
              AND txn_date >= CURRENT_DATE - INTERVAL '90 days'
              -- Tombstone sentinels (fetch error / parse miss) carry
              -- the dedicated filer_name marker; filter them out so
              -- the aggregate only reflects real transactions.
              AND filer_name <> %s
            """,
            (instrument_id, _TOMBSTONE_FILER),
        )
        row = cur.fetchone()
    if row is None:
        return InsiderSummary(Decimal(0), 0, 0, 0, None)
    return InsiderSummary(
        net_shares_90d=Decimal(row[0] or 0),
        buy_count_90d=int(row[1] or 0),
        sell_count_90d=int(row[2] or 0),
        unique_filers_90d=int(row[3] or 0),
        latest_txn_date=row[4],
    )
