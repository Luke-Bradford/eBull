"""Tests for the per-CIK raw-document store (migration 109).

Pins the contract: idempotent UPSERT, body fidelity round-trip,
``max_age`` cache-miss semantics, byte-count generated column,
CIK validation at the boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from app.services import cik_raw_filings
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_SAMPLE_FACTS = '{"cik": 320193, "facts": {"dei": {"EntityCommonStockSharesOutstanding": {}}}}'


def test_store_and_read_roundtrips_payload(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000320193",
        document_kind="companyfacts_json",
        payload=_SAMPLE_FACTS,
        source_url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json",
    )
    conn.commit()

    doc = cik_raw_filings.read_cik_raw(
        conn,
        cik="0000320193",
        document_kind="companyfacts_json",
    )
    assert doc is not None
    assert doc.payload == _SAMPLE_FACTS
    assert doc.byte_count == len(_SAMPLE_FACTS.encode("utf-8"))
    assert doc.source_url is not None and doc.source_url.startswith("https://")


def test_store_is_idempotent_and_overwrites(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Re-fetching an updated payload overwrites the prior body +
    refreshes ``fetched_at``."""
    conn = ebull_test_conn
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000320193",
        document_kind="companyfacts_json",
        payload='{"cik": 320193, "v": 1}',
    )
    conn.commit()
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000320193",
        document_kind="companyfacts_json",
        payload='{"cik": 320193, "v": 2}',
    )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), max(payload) FROM cik_raw_documents WHERE cik = %s",
            ("0000320193",),
        )
        row = cur.fetchone()
    assert row is not None
    count, latest = row
    assert count == 1  # idempotent — no duplicate row
    assert latest == '{"cik": 320193, "v": 2}'  # overwrite


def test_read_returns_none_when_max_age_exceeded(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``max_age`` makes stale rows count as a cache miss so the
    caller re-fetches. Drives the write-through cache pattern."""
    conn = ebull_test_conn
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000320194",
        document_kind="companyfacts_json",
        payload=_SAMPLE_FACTS,
    )
    # Backdate fetched_at so the row is well past the max_age window.
    conn.execute(
        """
        UPDATE cik_raw_documents
        SET fetched_at = %s
        WHERE cik = %s AND document_kind = %s
        """,
        (
            datetime.now(UTC) - timedelta(days=2),
            "0000320194",
            "companyfacts_json",
        ),
    )
    conn.commit()

    fresh = cik_raw_filings.read_cik_raw(
        conn,
        cik="0000320194",
        document_kind="companyfacts_json",
        max_age=timedelta(hours=24),
    )
    assert fresh is None  # stale → miss

    # Without max_age the row is returned regardless of age.
    any_age = cik_raw_filings.read_cik_raw(
        conn,
        cik="0000320194",
        document_kind="companyfacts_json",
    )
    assert any_age is not None


def test_read_returns_row_within_max_age(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000320195",
        document_kind="companyfacts_json",
        payload=_SAMPLE_FACTS,
    )
    conn.commit()

    fresh = cik_raw_filings.read_cik_raw(
        conn,
        cik="0000320195",
        document_kind="companyfacts_json",
        max_age=timedelta(hours=24),
    )
    assert fresh is not None
    assert fresh.payload == _SAMPLE_FACTS


def test_store_rejects_unpadded_cik(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Boundary validation — CIK must be 10-digit zero-padded so it
    matches ``external_identifiers.identifier_value``."""
    conn = ebull_test_conn
    with pytest.raises(ValueError, match="10-digit zero-padded"):
        cik_raw_filings.store_cik_raw(
            conn,
            cik="320193",  # missing zero-padding
            document_kind="companyfacts_json",
            payload=_SAMPLE_FACTS,
        )


def test_store_rejects_empty_payload(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    with pytest.raises(ValueError, match="payload is required"):
        cik_raw_filings.store_cik_raw(
            conn,
            cik="0000320193",
            document_kind="companyfacts_json",
            payload="",
        )


def test_db_check_constraint_blocks_unpadded_cik(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Defence in depth: even a direct-SQL writer (bypassing
    ``store_cik_raw`` validation) must not be able to create a
    cache split via an unpadded CIK. The CHECK constraint enforces
    the 10-digit invariant at the DB layer."""
    conn = ebull_test_conn
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO cik_raw_documents (cik, document_kind, payload)
            VALUES (%s, %s, %s)
            """,
            ("320193", "companyfacts_json", "x"),  # 6-digit, unpadded
        )
    conn.rollback()


def test_storage_summary_aggregates_per_kind(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000111100",
        document_kind="companyfacts_json",
        payload="x" * 50,
    )
    cik_raw_filings.store_cik_raw(
        conn,
        cik="0000111101",
        document_kind="submissions_json",
        payload="y" * 100,
    )
    conn.commit()

    summary = cik_raw_filings.cik_storage_summary(conn)
    by_kind = {s.document_kind: s for s in summary}
    assert by_kind["companyfacts_json"].row_count == 1
    assert by_kind["companyfacts_json"].total_bytes == 50
    assert by_kind["submissions_json"].row_count == 1
    assert by_kind["submissions_json"].total_bytes == 100
