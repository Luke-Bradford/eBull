"""SEC filing-document manifest parser + ingester (#452 Phase A).

Every SEC filing has an ``{accession}-index.json`` listing every
document in the submission — primary doc, exhibits, XBRL files,
graphics, cover page. Migration 062 added ``filing_documents`` to
capture the manifest as SQL rows. This module parses the index
JSON and populates the table.

Pure/impure split mirrors the other services in this family:

- :func:`parse_filing_index` is a pure function over raw index JSON
  returning a tuple of :class:`ParsedFilingDocument`.
- :func:`ingest_filing_documents` is the DB path — walks
  ``filing_events`` missing any ``filing_documents`` children,
  fetches the index JSON via the provider, parses, upserts.

Retires the ``data/raw/sec/sec_filing_*.json`` disk dump now that
every structured field lands in SQL (#453 contract).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

import psycopg

logger = logging.getLogger(__name__)


_PARSER_VERSION = 1


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


def parse_filing_index(
    raw_index: dict[str, object],
    *,
    accession_number: str,
) -> tuple[ParsedFilingDocument, ...]:
    """Walk the filing-index JSON and emit one ``ParsedFilingDocument``
    per document entry.

    SEC's index JSON shape:

    .. code:: json

        {
          "cik": "320193",
          "form": "10-K",
          "primaryDocument": "aapl-20240930.htm",
          "filingDate": "2024-11-01",
          ...
          "items": [
            {"name": "aapl-20240930.htm", "type": "10-K",
             "description": "10-K", "size": 1258402},
            {"name": "ex-21.htm", "type": "EX-21",
             "description": "Subsidiaries of the Registrant", "size": 1892},
            ...
          ]
        }

    The ``items`` list is the authoritative manifest. When a field is
    absent in a row we preserve ``None`` rather than fabricating a
    default — downstream renderers can distinguish "no description"
    from "empty description".
    """
    items = raw_index.get("items")
    if not isinstance(items, list):
        return ()
    cik_raw = raw_index.get("cik")
    primary_name = raw_index.get("primaryDocument")
    if not isinstance(primary_name, str):
        primary_name = None

    # CIK as an integer drops any leading zeroes, matching the SEC
    # archive path shape (``/edgar/data/<int_cik>/<accession>/...``).
    cik_int: int | None
    try:
        cik_int = int(str(cik_raw)) if cik_raw is not None else None
    except TypeError, ValueError:
        cik_int = None

    acc_no_dashes = accession_number.replace("-", "")

    docs: list[ParsedFilingDocument] = []
    seen_names: set[str] = set()
    for entry in items:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str) or not name or name in seen_names:
            continue
        seen_names.add(name)

        doc_type_raw = entry.get("type")
        doc_type = str(doc_type_raw) if isinstance(doc_type_raw, str) and doc_type_raw else None
        desc_raw = entry.get("description")
        description = str(desc_raw) if isinstance(desc_raw, str) and desc_raw else None
        size_raw = entry.get("size")
        size_bytes: int | None
        try:
            size_bytes = int(size_raw) if size_raw is not None else None
        except TypeError, ValueError:
            size_bytes = None

        if cik_int is not None:
            document_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no_dashes}/{name}"
        else:
            document_url = name  # best effort when CIK missing from index

        docs.append(
            ParsedFilingDocument(
                document_name=name,
                document_type=doc_type,
                description=description,
                size_bytes=size_bytes,
                is_primary=(name == primary_name),
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

    def fetch_filing_index(self, provider_filing_id: str) -> dict[str, object] | None: ...


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
    3. Ordered by filing_date DESC so fresh filings always get
       budget; historical backlog drains via the scheduler's
       continuous tick.

    Bounded per run (``limit=500``). The index JSON is small (~2 KB
    typical) so the rate-limit cost is modest even on a large
    backlog tick.
    """
    conn.commit()

    candidates: list[tuple[int, str]] = []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fe.filing_event_id, fe.provider_filing_id
            FROM filing_events fe
            LEFT JOIN filing_documents fd
                ON fd.filing_event_id = fe.filing_event_id
            WHERE fe.provider = 'sec'
              AND fd.id IS NULL
            GROUP BY fe.filing_event_id, fe.provider_filing_id, fe.filing_date
            ORDER BY fe.filing_date DESC, fe.filing_event_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        for row in cur.fetchall():
            candidates.append((int(row[0]), str(row[1])))
    conn.commit()

    filings_parsed = 0
    documents_inserted = 0
    fetch_errors = 0
    parse_misses = 0

    for filing_event_id, accession in candidates:
        try:
            raw = fetcher.fetch_filing_index(accession)
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

        docs = parse_filing_index(raw, accession_number=accession)
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

    # Reference the parser version so future refactors can gate a
    # re-parse by a version change. Currently unused in SQL — kept
    # as a module constant so the import doesn't feel dead.
    _ = _PARSER_VERSION

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
