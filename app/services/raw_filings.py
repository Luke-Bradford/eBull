"""Raw-filings document store.

Single-source-of-truth helper for the ``filing_raw_documents`` table
(migration 107). Every ownership-side ingester (13F, 13D/G, Form 4,
Form 3, DEF 14A) writes the source XML / HTML body here at fetch
time so re-wash workflows can run against stored bodies instead of
re-fetching from SEC.

Operator audit 2026-05-03 found the prior pattern dropped raw bodies
after parsing — meaning a parser bug discovered later forced a full
re-fetch from SEC at 10 req/sec. This module is the foundation for
the audit-and-rewash workflow.

The helper is deliberately small (UPSERT + read by accession +
size-aggregate) so per-ingester wiring is a 2-line change at each
fetch site:

    from app.services.raw_filings import store_raw

    body = provider.fetch_xml(accession)
    store_raw(
        conn,
        accession_number=accession,
        document_kind="form4_xml",
        payload=body,
        parser_version="form4-v1",
        source_url=primary_doc_url,
    )
    parsed = parse_form4(body)
    # ... rest of ingester writes parsed rows ...

Re-wash flow (separate module, future PR):

    for row in iter_raw(conn, document_kind="form4_xml"):
        if row.parser_version == CURRENT_PARSER_VERSION:
            continue  # already on latest parser
        re_parse_and_upsert(conn, row.accession_number, row.payload)

The contract is: ``store_raw`` is idempotent; calling it twice for
the same ``(accession, document_kind)`` overwrites the body and
refreshes ``fetched_at`` + ``parser_version``. The new body is
authoritative — re-fetches from SEC are exactly the case where
overwriting is the right behaviour (the document may have been
amended).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

DocumentKind = Literal[
    "primary_doc",
    "infotable_13f",
    "primary_doc_13dg",
    "form4_xml",
    "form3_xml",
    "form5_xml",
    "def14a_body",
    "nport_xml",
]
# submissions.json / companyfacts.json are keyed by CIK, not by SEC
# accession number — they belong in their own per-CIK store, not in
# this per-filing table. Claude PR 808 review (BLOCKING) caught the
# prior overload that smuggled CIKs into the accession_number column.
# Future PR adds a sibling ``cik_raw_documents`` table for those.


@dataclass(frozen=True)
class RawFilingDocument:
    accession_number: str
    document_kind: DocumentKind
    payload: str
    byte_count: int
    parser_version: str | None
    fetched_at: datetime
    source_url: str | None


def store_raw(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
    payload: str,
    parser_version: str | None = None,
    source_url: str | None = None,
) -> None:
    """Idempotent UPSERT into ``filing_raw_documents``.

    Re-calling for the same ``(accession_number, document_kind)``
    overwrites the body and refreshes ``fetched_at`` +
    ``parser_version`` + ``source_url``. The new body is treated as
    authoritative — re-fetches from SEC are exactly when
    overwriting is correct (the document may have been amended).
    """
    if not accession_number:
        raise ValueError("accession_number is required")
    if not payload:
        raise ValueError("payload is required (empty payload would defeat re-wash)")
    conn.execute(
        """
        INSERT INTO filing_raw_documents (
            accession_number, document_kind, payload, parser_version,
            source_url, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (accession_number, document_kind) DO UPDATE SET
            payload = EXCLUDED.payload,
            parser_version = EXCLUDED.parser_version,
            source_url = EXCLUDED.source_url,
            fetched_at = NOW()
        """,
        (accession_number, document_kind, payload, parser_version, source_url),
    )


def read_raw(
    conn: psycopg.Connection[Any],
    *,
    accession_number: str,
    document_kind: DocumentKind,
) -> RawFilingDocument | None:
    """Fetch the raw body for one (accession, kind) pair, or
    ``None`` when the row is missing. Read-only; safe to call
    inside a snapshot_read."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, document_kind, payload, byte_count,
                   parser_version, fetched_at, source_url
            FROM filing_raw_documents
            WHERE accession_number = %s AND document_kind = %s
            """,
            (accession_number, document_kind),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return RawFilingDocument(
        accession_number=str(row["accession_number"]),  # type: ignore[arg-type]
        document_kind=row["document_kind"],  # type: ignore[arg-type]
        payload=str(row["payload"]),  # type: ignore[arg-type]
        byte_count=int(row["byte_count"]),  # type: ignore[arg-type]
        parser_version=(str(row["parser_version"]) if row.get("parser_version") is not None else None),
        fetched_at=row["fetched_at"],  # type: ignore[arg-type]
        source_url=(str(row["source_url"]) if row.get("source_url") is not None else None),
    )


def iter_raw(
    conn: psycopg.Connection[Any],
    *,
    document_kind: DocumentKind,
    parser_version_not_in: tuple[str, ...] = (),
    batch_size: int = 100,
) -> Iterator[RawFilingDocument]:
    """Yield raw documents of a kind, optionally filtered to those
    NOT already on a current parser version. Used by re-wash
    workflows to walk only the rows that need re-parsing.

    Server-side cursor avoids loading the full set into Python — the
    body column compresses but uncompressed sizes can be hundreds of
    KB per row.
    """
    where = ["document_kind = %s"]
    params: list[Any] = [document_kind]
    if parser_version_not_in:
        # Cover both NULL parser_version and any other value not in
        # the list. Using NOT IN doesn't catch NULLs, so explicit OR.
        placeholders = ",".join(["%s"] * len(parser_version_not_in))
        where.append(f"(parser_version IS NULL OR parser_version NOT IN ({placeholders}))")
        params.extend(parser_version_not_in)
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT accession_number, document_kind, payload, byte_count,
               parser_version, fetched_at, source_url
        FROM filing_raw_documents
        WHERE {where_sql}
        ORDER BY fetched_at DESC, accession_number
    """  # noqa: S608 — where_sql built from hardcoded enum + placeholders
    # Server-side cursor names are session-scoped in psycopg v3, so
    # a hardcoded name would collide between concurrent ``iter_raw``
    # calls on the same connection. Generate a unique name per call.
    # Claude PR 808 review (BLOCKING) caught the prior literal name.
    cursor_name = f"iter_raw_{uuid.uuid4().hex}"
    with conn.cursor(row_factory=psycopg.rows.dict_row, name=cursor_name) as cur:
        cur.itersize = batch_size
        cur.execute(sql, params)  # type: ignore[arg-type]  # f-string composed from closed enum
        for row in cur:
            yield RawFilingDocument(
                accession_number=str(row["accession_number"]),  # type: ignore[arg-type]
                document_kind=row["document_kind"],  # type: ignore[arg-type]
                payload=str(row["payload"]),  # type: ignore[arg-type]
                byte_count=int(row["byte_count"]),  # type: ignore[arg-type]
                parser_version=(str(row["parser_version"]) if row.get("parser_version") is not None else None),
                fetched_at=row["fetched_at"],  # type: ignore[arg-type]
                source_url=(str(row["source_url"]) if row.get("source_url") is not None else None),
            )


@dataclass(frozen=True)
class StorageSummary:
    document_kind: DocumentKind
    row_count: int
    total_bytes: int
    avg_bytes: int


def storage_summary(conn: psycopg.Connection[Any]) -> list[StorageSummary]:
    """Per-kind row + byte summary. Drives the operator-visible
    storage chip on the ingest-health page."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT document_kind,
                   COUNT(*) AS row_count,
                   COALESCE(SUM(byte_count), 0) AS total_bytes,
                   COALESCE(AVG(byte_count)::int, 0) AS avg_bytes
            FROM filing_raw_documents
            GROUP BY document_kind
            ORDER BY total_bytes DESC
            """,
        )
        rows = cur.fetchall()
    return [
        StorageSummary(
            document_kind=row["document_kind"],  # type: ignore[arg-type]
            row_count=int(row["row_count"]),  # type: ignore[arg-type]
            total_bytes=int(row["total_bytes"]),  # type: ignore[arg-type]
            avg_bytes=int(row["avg_bytes"]),  # type: ignore[arg-type]
        )
        for row in rows
    ]
