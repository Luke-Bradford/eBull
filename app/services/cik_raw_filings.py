"""Per-CIK raw-document store helper.

Sibling of ``app.services.raw_filings`` (per-accession) for SEC
documents keyed by CIK rather than by accession number —
``submissions.json`` and ``companyfacts.json`` are rolling per-issuer
documents covering ALL their filings.

The contract mirrors ``raw_filings``:

  * ``store_cik_raw`` is idempotent — re-calling for the same
    ``(cik, document_kind)`` overwrites the body and refreshes
    ``fetched_at``. The new body is authoritative.
  * ``read_cik_raw`` returns the cached body or ``None``. Optional
    ``max_age`` parameter implements write-through cache semantics:
    rows older than ``max_age`` count as a miss so the caller
    re-fetches.

Operator audit 2026-05-03 motivated this: every reconciliation
spot-check was re-fetching ``companyfacts.json`` from SEC. A
short-TTL cache here turns repeated checks against the same CIK
into hot reads.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows

CikDocumentKind = Literal[
    "submissions_json",
    "companyfacts_json",
]


@dataclass(frozen=True)
class CikRawDocument:
    cik: str
    document_kind: CikDocumentKind
    payload: str
    byte_count: int
    fetched_at: datetime
    source_url: str | None


def store_cik_raw(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    document_kind: CikDocumentKind,
    payload: str,
    source_url: str | None = None,
) -> None:
    """Idempotent UPSERT into ``cik_raw_documents``.

    Re-calling for the same ``(cik, document_kind)`` overwrites the
    body and refreshes ``fetched_at``. The new body is treated as
    authoritative — re-fetches from SEC are exactly when overwriting
    is correct (issuer profile + facts payloads are amended over
    time as new filings land).
    """
    if not cik:
        raise ValueError("cik is required")
    if len(cik) != 10 or not cik.isdigit():
        # The 10-digit padding is the canonical form across the rest
        # of the codebase (external_identifiers.identifier_value,
        # SEC URL paths). Reject anything else at the boundary so a
        # caller passing a raw integer doesn't pollute the table.
        raise ValueError(f"cik must be 10-digit zero-padded, got {cik!r}")
    if not payload:
        raise ValueError("payload is required (empty payload would defeat the cache)")
    conn.execute(
        """
        INSERT INTO cik_raw_documents (
            cik, document_kind, payload, source_url, fetched_at
        ) VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (cik, document_kind) DO UPDATE SET
            payload = EXCLUDED.payload,
            source_url = EXCLUDED.source_url,
            fetched_at = NOW()
        """,
        (cik, document_kind, payload, source_url),
    )


def read_cik_raw(
    conn: psycopg.Connection[Any],
    *,
    cik: str,
    document_kind: CikDocumentKind,
    max_age: timedelta | None = None,
) -> CikRawDocument | None:
    """Return the cached body for ``(cik, document_kind)`` or
    ``None`` when missing.

    ``max_age`` is the write-through cache knob: rows older than
    ``max_age`` count as a miss (returns ``None``) so the caller
    re-fetches. Pass ``None`` (the default) to ignore freshness and
    always return whatever is on file.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cik, document_kind, payload, byte_count,
                   fetched_at, source_url
            FROM cik_raw_documents
            WHERE cik = %s AND document_kind = %s
            """,
            (cik, document_kind),
        )
        row = cur.fetchone()
    if row is None:
        return None
    fetched_at = row["fetched_at"]
    if max_age is not None and isinstance(fetched_at, datetime):
        # Compare against UTC now() — fetched_at is TIMESTAMPTZ so
        # it's already timezone-aware. A stale-cache hit returns None
        # so the caller re-fetches; the next ``store_cik_raw`` then
        # overwrites the row with the fresh body.
        if datetime.now(UTC) - fetched_at > max_age:
            return None
    return CikRawDocument(
        cik=str(row["cik"]),  # type: ignore[arg-type]
        document_kind=row["document_kind"],  # type: ignore[arg-type]
        payload=str(row["payload"]),  # type: ignore[arg-type]
        byte_count=int(row["byte_count"]),  # type: ignore[arg-type]
        fetched_at=fetched_at,  # type: ignore[arg-type]
        source_url=(str(row["source_url"]) if row.get("source_url") is not None else None),
    )


@dataclass(frozen=True)
class CikStorageSummary:
    document_kind: CikDocumentKind
    row_count: int
    total_bytes: int


def cik_storage_summary(conn: psycopg.Connection[Any]) -> list[CikStorageSummary]:
    """Per-kind row + byte summary. Drives the operator-visible
    storage chip on the ingest-health page."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT document_kind,
                   COUNT(*) AS row_count,
                   COALESCE(SUM(byte_count), 0) AS total_bytes
            FROM cik_raw_documents
            GROUP BY document_kind
            ORDER BY total_bytes DESC
            """,
        )
        rows = cur.fetchall()
    return [
        CikStorageSummary(
            document_kind=row["document_kind"],  # type: ignore[arg-type]
            row_count=int(row["row_count"]),  # type: ignore[arg-type]
            total_bytes=int(row["total_bytes"]),  # type: ignore[arg-type]
        )
        for row in rows
    ]
