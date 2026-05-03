"""Tests for the raw-filings document store (migration 107).

Pins the contract: idempotent UPSERT, body fidelity round-trip,
iter filter on parser_version, byte-count generated column, kind
CHECK constraint.
"""

from __future__ import annotations

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
    assert a1.payload.startswith("<doc-")
    assert b1.payload.startswith("<doc-")
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
