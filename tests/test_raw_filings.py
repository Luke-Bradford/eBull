"""Tests for the raw-filings document store (migration 107).

Pins the contract: idempotent UPSERT, body fidelity round-trip,
iter filter on parser_version, byte-count generated column, kind
CHECK constraint.
"""

from __future__ import annotations

import hashlib

import psycopg
import pytest

from app.services import raw_filings
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_SAMPLE_FORM4 = (
    "<?xml version='1.0' encoding='UTF-8'?>\n"
    "<ownershipDocument>"
    "<periodOfReport>2026-04-15</periodOfReport>"
    "<reportingOwner><reportingOwnerId>"
    "<rptOwnerCik>0001767470</rptOwnerCik>"
    "<rptOwnerName>Cohen Ryan</rptOwnerName>"
    "</reportingOwnerId></reportingOwner>"
    "</ownershipDocument>"
)


def test_store_and_read_roundtrips_payload(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0001767470-26-000003",
        document_kind="form4_xml",
        payload=_SAMPLE_FORM4,
        parser_version="form4-v1",
        source_url="https://www.sec.gov/Archives/edgar/data/1767470/000176747026000003/",
    )
    conn.commit()
    doc = raw_filings.read_raw(
        conn,
        accession_number="0001767470-26-000003",
        document_kind="form4_xml",
    )
    assert doc is not None
    assert doc.payload == _SAMPLE_FORM4
    assert doc.byte_count == len(_SAMPLE_FORM4.encode("utf-8"))
    assert doc.parser_version == "form4-v1"
    assert doc.source_url is not None and doc.source_url.startswith("https://")


def test_store_is_idempotent_and_overwrites(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Re-fetching an amended document must overwrite the prior body
    + refresh fetched_at. Re-runs of the ingester on the same
    accession should not produce duplicate rows."""
    conn = ebull_test_conn
    accession = "0001767470-26-000004"
    raw_filings.store_raw(
        conn,
        accession_number=accession,
        document_kind="form4_xml",
        payload="<original/>",
        parser_version="form4-v1",
    )
    conn.commit()
    raw_filings.store_raw(
        conn,
        accession_number=accession,
        document_kind="form4_xml",
        payload="<amended>now with more data</amended>",
        parser_version="form4-v2",
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), payload, parser_version FROM filing_raw_documents "
            "WHERE accession_number = %s GROUP BY payload, parser_version",
            (accession,),
        )
        rows = cur.fetchall()
    # Exactly one row — UPSERT replaced.
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == "<amended>now with more data</amended>"
    assert rows[0][2] == "form4-v2"


def test_read_missing_returns_none(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    doc = raw_filings.read_raw(
        conn,
        accession_number="does-not-exist-26-000000",
        document_kind="form4_xml",
    )
    assert doc is None


def test_unknown_document_kind_rejected_at_check_constraint(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The CHECK constraint on ``document_kind`` blocks typos at
    write time. Codex pre-push prevention pattern: a parser typo
    cannot smuggle a quietly-mis-classified row into the store."""
    conn = ebull_test_conn
    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO filing_raw_documents (
                    accession_number, document_kind, payload
                ) VALUES (%s, %s, %s)
                """,
                ("0000000000-26-000001", "form4_typo", "<x/>"),
            )


def test_empty_payload_rejected_at_helper(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Empty payload defeats the purpose (no body to re-wash from);
    helper rejects rather than silently writing a useless row."""
    conn = ebull_test_conn
    with pytest.raises(ValueError):
        raw_filings.store_raw(
            conn,
            accession_number="0001767470-26-000005",
            document_kind="form4_xml",
            payload="",
        )


_SAMPLE_PRIMARY_DOC = "<edgarSubmission><headerData>primary doc body</headerData></edgarSubmission>"
_PRIMARY_URL = "https://www.sec.gov/Archives/edgar/data/1/000/primary_doc.html"


def test_store_primary_doc_is_born_compacted(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """#1615 — write-only kinds (primary_doc) are stored BORN-COMPACTED:
    payload NULL + recorded sha + swept_at, the bytes never persisted.
    The row still exists (#938 raw-before-parsed invariant) and is
    rehydratable from source_url. The recorded hash is byte-identical to
    the Python rehydrate verifier."""
    conn = ebull_test_conn
    accession = "0001767470-26-001615"
    raw_filings.store_raw(
        conn,
        accession_number=accession,
        document_kind="primary_doc",
        payload=_SAMPLE_PRIMARY_DOC,
        parser_version="primary-v1",
        source_url=_PRIMARY_URL,
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT payload, byte_count, payload_sha256, payload_swept_at, source_url, parser_version "
            "FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'primary_doc'",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    payload, byte_count, sha, swept_at, source_url, parser_version = row
    assert payload is None and byte_count is None
    assert swept_at is not None
    assert sha == hashlib.sha256(_SAMPLE_PRIMARY_DOC.encode("utf-8")).hexdigest()
    assert source_url == _PRIMARY_URL
    assert parser_version == "primary-v1"
    # read_raw surfaces it as a swept row — require_payload fails loud.
    doc = raw_filings.read_raw(conn, accession_number=accession, document_kind="primary_doc")
    assert doc is not None and doc.payload is None and doc.byte_count is None
    with pytest.raises(RuntimeError, match="swept"):
        doc.require_payload()


def test_store_primary_doc_requires_source_url(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A born-compacted row's only recovery path is source_url, so a
    write-only kind stored without one is rejected at the helper —
    never persist an unrecoverable payload-less row."""
    conn = ebull_test_conn
    with pytest.raises(ValueError, match="source_url is required"):
        raw_filings.store_raw(
            conn,
            accession_number="0001767470-26-001616",
            document_kind="primary_doc",
            payload=_SAMPLE_PRIMARY_DOC,
        )


def test_store_primary_doc_idempotent_stays_compacted(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Re-storing an amended primary_doc keeps it born-compacted (the
    bytes are never resurrected), one row, sha refreshed to the new body."""
    conn = ebull_test_conn
    accession = "0001767470-26-001617"
    amended = "<v2>amended</v2>"
    raw_filings.store_raw(
        conn, accession_number=accession, document_kind="primary_doc", payload="<v1/>", source_url=_PRIMARY_URL
    )
    conn.commit()
    raw_filings.store_raw(
        conn, accession_number=accession, document_kind="primary_doc", payload=amended, source_url=_PRIMARY_URL
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), bool_and(payload IS NULL), max(payload_sha256) "
            "FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'primary_doc'",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    n, all_null, sha = row
    assert n == 1 and all_null is True
    assert sha == hashlib.sha256(amended.encode("utf-8")).hexdigest()


def test_iter_raw_filters_by_kind_and_parser_version(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``iter_raw`` returns only rows for the requested kind, and
    optionally only those NOT on the current parser version."""
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0000000001-26-000001",
        document_kind="form4_xml",
        payload="<old/>",
        parser_version="form4-v1",
    )
    raw_filings.store_raw(
        conn,
        accession_number="0000000001-26-000002",
        document_kind="form4_xml",
        payload="<new/>",
        parser_version="form4-v2",
    )
    raw_filings.store_raw(
        conn,
        accession_number="0000000001-26-000003",
        document_kind="form3_xml",
        payload="<f3/>",
        parser_version="form3-v1",
    )
    conn.commit()

    form4_all = list(raw_filings.iter_raw(conn, document_kind="form4_xml"))
    assert len(form4_all) == 2

    # Filter to "needs re-parse" — anything NOT on form4-v2.
    needs_reparse = list(
        raw_filings.iter_raw(
            conn,
            document_kind="form4_xml",
            parser_version_not_in=("form4-v2",),
        )
    )
    assert len(needs_reparse) == 1
    assert needs_reparse[0].parser_version == "form4-v1"


def test_storage_summary_groups_by_kind(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0000000002-26-000001",
        document_kind="form4_xml",
        payload="x" * 100,
    )
    raw_filings.store_raw(
        conn,
        accession_number="0000000002-26-000002",
        document_kind="form4_xml",
        payload="y" * 200,
    )
    raw_filings.store_raw(
        conn,
        accession_number="0000000002-26-000003",
        document_kind="def14a_body",
        payload="z" * 5000,
    )
    conn.commit()
    summary = raw_filings.storage_summary(conn)
    by_kind = {s.document_kind: s for s in summary}
    assert by_kind["form4_xml"].row_count == 2
    assert by_kind["form4_xml"].total_bytes == 300
    assert by_kind["def14a_body"].row_count == 1
    assert by_kind["def14a_body"].total_bytes == 5000


def test_iter_raw_concurrent_calls_use_unique_cursor_names(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Server-side cursor names are session-scoped in psycopg v3.
    Two concurrent ``iter_raw`` calls on the same connection must
    not collide. Claude PR 808 review (BLOCKING) caught the prior
    literal cursor name. Pin the dynamic-name behaviour by opening
    two iterators side-by-side and walking both."""
    conn = ebull_test_conn
    for i in range(3):
        raw_filings.store_raw(
            conn,
            accession_number=f"0000000999-26-{i:06d}",
            document_kind="form4_xml",
            payload=f"<doc-{i}/>",
        )
    conn.commit()
    iter_a = raw_filings.iter_raw(conn, document_kind="form4_xml", batch_size=1)
    iter_b = raw_filings.iter_raw(conn, document_kind="form4_xml", batch_size=1)
    # Side-by-side advance — would have raised
    # ``ProgrammingError: cursor "iter_raw" already exists`` under
    # the prior static-name implementation.
    a1 = next(iter_a)
    b1 = next(iter_b)
    assert a1.require_payload().startswith("<doc-")
    assert b1.require_payload().startswith("<doc-")
    # Drain both so the server-side cursors close cleanly.
    list(iter_a)
    list(iter_b)


def test_byte_count_generated_from_octet_length(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``byte_count`` is GENERATED ALWAYS so a manual UPDATE on
    payload also refreshes it. Pin the invariant so a future schema
    change can't silently drop the GENERATED clause."""
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0000000003-26-000001",
        document_kind="form4_xml",
        payload="<short/>",
    )
    conn.commit()
    doc = raw_filings.read_raw(
        conn,
        accession_number="0000000003-26-000001",
        document_kind="form4_xml",
    )
    assert doc is not None
    assert doc.byte_count == len(b"<short/>")


# ---------------------------------------------------------------------------
# #1591 — stored_body: reuse a present body, else fall through to fetch
# ---------------------------------------------------------------------------


def test_stored_body_returns_payload_on_hit(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A retained kind with a present payload returns the body for reuse."""
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0001767470-26-000091",
        document_kind="form4_xml",
        payload=_SAMPLE_FORM4,
        parser_version="form4-v1",
    )
    conn.commit()
    assert (
        raw_filings.stored_body(conn, accession_number="0001767470-26-000091", document_kind="form4_xml")
        == _SAMPLE_FORM4
    )


def test_stored_body_returns_none_on_missing_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """No row → None → the caller fetches (first ingest)."""
    assert (
        raw_filings.stored_body(ebull_test_conn, accession_number="9999999999-26-000000", document_kind="form4_xml")
        is None
    )


def test_stored_body_returns_none_on_swept_payload(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A SWEPT kind (primary_doc) is born-compacted (payload NULL) by
    store_raw, so stored_body returns None and the caller rehydrates /
    re-fetches — reuse is impossible for a body that was never stored. This
    is exactly why #1591 excludes 10-K/8-K from the reuse retarget."""
    conn = ebull_test_conn
    raw_filings.store_raw(
        conn,
        accession_number="0000000009-26-000001",
        document_kind="primary_doc",
        payload="<html>10-K body</html>",
        parser_version="10k-v2",
        source_url="https://www.sec.gov/Archives/edgar/data/9/000/primary.htm",
    )
    conn.commit()
    # Sanity: the row exists but its payload was born-compacted to NULL.
    doc = raw_filings.read_raw(conn, accession_number="0000000009-26-000001", document_kind="primary_doc")
    assert doc is not None and doc.payload is None
    assert raw_filings.stored_body(conn, accession_number="0000000009-26-000001", document_kind="primary_doc") is None
