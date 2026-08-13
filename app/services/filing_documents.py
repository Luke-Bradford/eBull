"""SEC filing-document manifest parser + ingester (#452 / #723).

Every SEC filing's archive directory exposes a JSON listing at
``/Archives/edgar/data/{cik}/{accession_no_dashes}/index.json`` —
one entry per file in the submission (primary doc, exhibits, XBRL
files, graphics, cover page). Migration 062 added ``filing_documents``
to capture the manifest as SQL rows. This module fetches the listing
JSON and populates the table.

Pure/impure split mirrors the other services in this family:

- :func:`parse_filing_index` is a pure function over raw index JSON
  returning a tuple of :class:`ParsedFilingDocument`.
- :func:`ingest_filing_documents` is the DB path — walks
  ``filing_events`` missing any ``filing_documents`` children,
  fetches the index JSON via the provider, parses, upserts.

Retires the ``data/raw/sec/sec_filing_*.json`` disk dump now that
every structured field lands in SQL (#453 contract).

#723: this module's pre-rewrite implementation targeted a
``{accession}-index.json`` URL that does not exist on SEC EDGAR and
parsed a hypothetical top-level ``items: [...]`` shape that SEC has
never returned. Both bugs are fixed here. ``document_type`` and
``description`` columns stay NULL on this code path because SEC's
``index.json`` ``type`` field is the content-type icon name
(``text.gif``, ``compressed.gif``) — the rich SEC type labels
(``EX-99.1``, ``GRAPHIC``, ``XBRL INSTANCE DOCUMENT``) live only
in the ``-index.html`` rendering and parsing that requires HTML. A
follow-up can layer the HTML parse on top if the cross-issuer
type-scoped queries the schema mentions become an active need.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg

logger = logging.getLogger(__name__)


_PARSER_VERSION = 2  # bumped: rewrite for real SEC directory.item shape


# ---------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedFilingDocument:
    """One document entry from a filing's index manifest."""

    document_name: str
    document_type: str | None
    description: str | None
    size_bytes: int | None
    is_primary: bool
    document_url: str


# ---------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------


def _coerce_int_size(raw: object) -> int | None:
    """Parse SEC's ``size`` string to an int.

    SEC sometimes emits an empty string for index/header entries
    (e.g. ``0000320193-26-000011-index-headers.html``) — those map
    to ``None``. Numeric-looking strings parse normally; anything
    else maps to ``None``.
    """
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        if not raw.strip():
            return None
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def parse_filing_index(
    raw_index: Mapping[str, object],
    *,
    accession_number: str,
    cik: str | int,
    primary_document_name: str | None = None,
) -> tuple[ParsedFilingDocument, ...]:
    """Walk a SEC filing's ``/index.json`` payload and emit one
    :class:`ParsedFilingDocument` per file in the submission.

    SEC's actual response shape (verified against live archive,
    documented at #723):

    .. code:: json

        {
          "directory": {
            "name": "/Archives/edgar/data/320193/000032019326000011",
            "parent-dir": "/Archives/edgar/data/320193/",
            "item": [
              {"last-modified": "2026-04-30 16:30:41",
               "name": "0000320193-26-000011-index-headers.html",
               "type": "text.gif",
               "size": ""},
              {"last-modified": "2026-04-30 16:30:41",
               "name": "aapl-20260430.htm",
               "type": "text.gif",
               "size": "37639"},
              ...
            ]
          }
        }

    The ``type`` field is a content-type icon (e.g. ``text.gif``,
    ``compressed.gif``) and is NOT the SEC document-type label
    (``EX-99.1``, ``GRAPHIC``, etc.) — those live only in the
    ``-index.html`` rendering. This parser leaves ``document_type``
    and ``description`` NULL; a future enhancement can layer in HTML
    parsing if cross-issuer type-scoped queries become a need.

    ``primary_document_name`` is supplied by the caller from
    ``filing_events.primary_document_url`` (the submission-level
    primary, which the archive listing does not flag). When
    ``None``, no row is marked primary.

    Returns an empty tuple when the payload is malformed (missing
    ``directory.item``, wrong types) — callers treat that as a
    parse miss, NOT a fetch error.
    """
    directory = raw_index.get("directory")
    if not isinstance(directory, dict):
        return ()
    items = directory.get("item")
    if not isinstance(items, list):
        return ()

    try:
        cik_int = int(cik)
    except TypeError, ValueError:
        return ()

    accession_no_dashes = accession_number.replace("-", "")

    docs: list[ParsedFilingDocument] = []
    seen_names: set[str] = set()
    for entry in items:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name or name in seen_names:
            continue
        seen_names.add(name)

        size_bytes = _coerce_int_size(entry.get("size"))
        document_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no_dashes}/{name}"
        is_primary = primary_document_name is not None and name == primary_document_name

        docs.append(
            ParsedFilingDocument(
                document_name=name,
                document_type=None,  # see docstring — needs HTML parse
                description=None,
                size_bytes=size_bytes,
                is_primary=is_primary,
                document_url=document_url,
            )
        )
    return tuple(docs)


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


def upsert_filing_documents(
    conn: psycopg.Connection[Any],
    *,
    filing_event_id: int,
    accession_number: str,
    documents: tuple[ParsedFilingDocument, ...],
) -> int:
    """Replace the document snapshot for ``(filing_event_id)`` with
    the parsed list.

    DELETE + INSERT wrapped in a savepoint so a mid-loop INSERT
    failure rolls back the DELETE atomically and the prior snapshot
    survives (see docs/review-prevention-log.md "DELETE-then-INSERT
    helper without a savepoint").
    """
    if not documents:
        return 0
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM filing_documents WHERE filing_event_id = %s",
                (filing_event_id,),
            )
            inserted = 0
            for doc in documents:
                cur.execute(
                    """
                    INSERT INTO filing_documents
                        (filing_event_id, accession_number, document_name,
                         document_type, description, size_bytes,
                         is_primary, document_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        filing_event_id,
                        accession_number,
                        doc.document_name,
                        doc.document_type,
                        doc.description,
                        doc.size_bytes,
                        doc.is_primary,
                        doc.document_url,
                    ),
                )
                inserted += 1
            return inserted


# ---------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------


class _IndexFetcher(Protocol):
    """Minimal Protocol the ingester needs from the provider.

    Narrow shape so tests can substitute a dict-stub without
    implementing the full provider interface.
    """

    def fetch_filing_index(
        self,
        provider_filing_id: str,
        *,
        issuer_cik: str | None = None,
    ) -> dict[str, object] | None: ...


@dataclass(frozen=True)
class IngestResult:
    filings_scanned: int
    filings_parsed: int
    documents_inserted: int
    fetch_errors: int
    parse_misses: int


def ingest_filing_documents(
    conn: psycopg.Connection[Any],
    fetcher: _IndexFetcher,
    *,
    limit: int = 500,
) -> IngestResult:
    """Scan ``filing_events`` for accessions missing any
    ``filing_documents`` children, fetch the index JSON, upsert.

    Candidate selector:

    1. ``fe.provider = 'sec'`` — only SEC filings carry an index
       JSON in this shape.
    2. No existing ``filing_documents`` row for the filing_event_id.
    3. ``primary_document_url`` available — needed to flag the
       submission-level primary (the archive listing does not).
       Filings missing a primary URL skip silently; those rows
       cannot anchor an ``is_primary=TRUE`` row and would produce
       a misleading "all rows is_primary=FALSE" listing.
    4. Ordered by filing_date DESC so fresh filings always get
       budget; historical backlog drains via the scheduler's
       continuous tick.

    Bounded per run (``limit=500``). The index JSON is small (~2 KB
    typical) so the rate-limit cost is modest even on a large
    backlog tick.
    """
    conn.commit()

    candidates: list[tuple[int, str, str, str | None]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.filing_event_id,
                   fe.provider_filing_id,
                   ei.identifier_value AS cik,
                   fe.primary_document_url
            FROM filing_events fe
            LEFT JOIN filing_documents fd
                ON fd.filing_event_id = fe.filing_event_id
            JOIN external_identifiers ei
                ON ei.instrument_id = fe.instrument_id
                AND ei.provider = 'sec'
                AND ei.identifier_type = 'cik'
                AND ei.is_primary = TRUE
            WHERE fe.provider = 'sec'
              AND fd.id IS NULL
              AND fe.primary_document_url IS NOT NULL
            GROUP BY fe.filing_event_id, fe.provider_filing_id,
                     ei.identifier_value, fe.primary_document_url, fe.filing_date
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append(
                (
                    int(row[0]),
                    str(row[1]),
                    str(row[2]),
                    str(row[3]) if row[3] is not None else None,
                )
            )
    conn.commit()

    filings_parsed = 0
    documents_inserted = 0
    fetch_errors = 0
    parse_misses = 0

    for filing_event_id, accession, cik, primary_url in candidates:
        try:
            # Pass the issuer CIK explicitly so the archive URL
            # routes under the issuer-not-filer path (#736).
            raw = fetcher.fetch_filing_index(accession, issuer_cik=cik)
        except Exception:
            logger.warning(
                "ingest_filing_documents: fetch failed accession=%s",
                accession,
                exc_info=True,
            )
            fetch_errors += 1
            continue
        if raw is None:
            fetch_errors += 1
            continue

        # Derive the primary document filename from the stored URL —
        # the archive listing has no flag for it.
        primary_name: str | None = None
        if primary_url:
            primary_name = primary_url.rsplit("/", 1)[-1] or None

        docs = parse_filing_index(
            raw,
            accession_number=accession,
            cik=cik,
            primary_document_name=primary_name,
        )
        if not docs:
            parse_misses += 1
            continue

        try:
            upsert_filing_documents(
                conn,
                filing_event_id=filing_event_id,
                accession_number=accession,
                documents=docs,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            logger.warning(
                "ingest_filing_documents: upsert failed accession=%s",
                accession,
                exc_info=True,
            )
            continue

        filings_parsed += 1
        documents_inserted += len(docs)

    logger.info(
        "ingest_filing_documents: parser_version=%d scanned=%d parsed=%d docs=%d fetch_errors=%d parse_misses=%d",
        _PARSER_VERSION,
        len(candidates),
        filings_parsed,
        documents_inserted,
        fetch_errors,
        parse_misses,
    )

    return IngestResult(
        filings_scanned=len(candidates),
        filings_parsed=filings_parsed,
        documents_inserted=documents_inserted,
        fetch_errors=fetch_errors,
        parse_misses=parse_misses,
    )


# ---------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class FilingDocumentRow:
    document_name: str
    document_type: str | None
    description: str | None
    size_bytes: int | None
    is_primary: bool
    document_url: str


def list_filing_documents(
    conn: psycopg.Connection[Any],
    *,
    filing_event_id: int,
) -> tuple[FilingDocumentRow, ...]:
    """Return the document manifest for one filing, primary first.

    Empty tuple when no documents on file (either the ingester hasn't
    touched this filing yet, or the filing carries no index JSON).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT document_name, document_type, description,
                   size_bytes, is_primary, document_url
            FROM filing_documents
            WHERE filing_event_id = %s
            ORDER BY is_primary DESC, document_name ASC
            """,
            (filing_event_id,),
        )
        rows = cur.fetchall()
    return tuple(
        FilingDocumentRow(
            document_name=str(r[0]),
            document_type=r[1],
            description=r[2],
            size_bytes=r[3],
            is_primary=bool(r[4]),
            document_url=str(r[5]),
        )
        for r in rows
    )
