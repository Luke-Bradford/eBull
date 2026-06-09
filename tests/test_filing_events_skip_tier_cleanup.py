"""DB integration test for the #1013 skip-tier filing_events cleanup.

Spec: docs/specs/ops/2026-06-09-filing-events-skip-tier-cleanup.md.

Exercises the single SQL mechanism (bounded skip-tier delete with scope
guards + cascade) end to end against a real Postgres. The keep-set
membership itself is covered by pure-logic tests in
``tests/test_filings_form_allowlist.py``.
"""

from __future__ import annotations

import psycopg
import pytest

from app.services.filing_events_cleanup import cleanup_skip_tier_filing_events
from tests.fixtures.ebull_test_db import ebull_test_conn, test_database_url  # noqa: F401

_IID = 561013


def _seed_instrument(conn: psycopg.Connection[tuple], instrument_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            (instrument_id, f"SKIP{instrument_id}", f"Skip-tier test {instrument_id}"),
        )


def _insert_event(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filing_type: str | None,
    provider: str = "sec",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider, provider_filing_id)
            VALUES (%s, DATE '2020-01-01', %s, %s, %s)
            RETURNING filing_event_id
            """,
            (_IID, filing_type, provider, accession),
        )
        row = cur.fetchone()
        assert row is not None
        return row[0]


def _insert_document(conn: psycopg.Connection[tuple], *, filing_event_id: int, accession: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_documents
                (filing_event_id, accession_number, document_name, document_url)
            VALUES (%s, %s, %s, %s)
            """,
            (filing_event_id, accession, "primary.htm", f"https://sec.gov/{accession}/primary.htm"),
        )


def _count_events(conn: psycopg.Connection[tuple], *, filing_type: str | None, provider: str = "sec") -> int:
    with conn.cursor() as cur:
        if filing_type is None:
            cur.execute(
                "SELECT count(*) FROM filing_events WHERE instrument_id = %s AND provider = %s AND filing_type IS NULL",
                (_IID, provider),
            )
        else:
            cur.execute(
                "SELECT count(*) FROM filing_events WHERE instrument_id = %s AND provider = %s AND filing_type = %s",
                (_IID, provider, filing_type),
            )
        row = cur.fetchone()
        assert row is not None
        return row[0]


def _count_documents(conn: psycopg.Connection[tuple], *, filing_event_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM filing_documents WHERE filing_event_id = %s",
            (filing_event_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return row[0]


@pytest.mark.integration
def test_skip_tier_cleanup_deletes_only_unkept_sec_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)

    # Keep-tier SEC (survive).
    _insert_event(conn, accession="keep-10k", filing_type="10-K")
    _insert_event(conn, accession="keep-4", filing_type="4")
    # Legacy short-form 13D/G alias (survive — the #1013 fix).
    _insert_event(conn, accession="keep-sc13g", filing_type="SC 13G/A")
    # provider != 'sec' carrying a skip-tier SEC form name (survive — keep-set is SEC-only).
    _insert_event(conn, accession="keep-uk-fwp", filing_type="FWP", provider="companies_house")
    # filing_type IS NULL SEC row (survive — never delete unclassifiable).
    _insert_event(conn, accession="keep-null", filing_type=None)
    # Skip-tier SEC (delete).
    fwp_id = _insert_event(conn, accession="drop-fwp", filing_type="FWP")
    _insert_event(conn, accession="drop-upload", filing_type="UPLOAD")
    # filing_documents child on a skip-tier row (must cascade-delete).
    _insert_document(conn, filing_event_id=fwp_id, accession="drop-fwp")
    conn.commit()

    assert _count_documents(conn, filing_event_id=fwp_id) == 1

    # batch_size=1 forces multiple batches over the 2 skip-tier SEC rows.
    summary = cleanup_skip_tier_filing_events(database_url=test_database_url(), batch_size=1)

    assert summary.total_deleted == 2
    assert summary.by_form_type == {"FWP": 1, "UPLOAD": 1}
    assert summary.batches == 2  # one row per batch

    conn.commit()  # end the idle tx so the next read sees the service's committed deletes

    # Survivors intact.
    assert _count_events(conn, filing_type="10-K") == 1
    assert _count_events(conn, filing_type="4") == 1
    assert _count_events(conn, filing_type="SC 13G/A") == 1
    assert _count_events(conn, filing_type="FWP", provider="companies_house") == 1
    assert _count_events(conn, filing_type=None) == 1
    # Skip-tier gone.
    assert _count_events(conn, filing_type="FWP") == 0
    assert _count_events(conn, filing_type="UPLOAD") == 0
    # Cascade removed the child.
    assert _count_documents(conn, filing_event_id=fwp_id) == 0


@pytest.mark.integration
def test_skip_tier_cleanup_is_idempotent(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, _IID)
    _insert_event(conn, accession="drop-fwp-idem", filing_type="FWP")
    conn.commit()

    first = cleanup_skip_tier_filing_events(database_url=test_database_url())
    assert first.total_deleted == 1

    second = cleanup_skip_tier_filing_events(database_url=test_database_url())
    assert second.total_deleted == 0
    assert second.batches == 0
    assert second.by_form_type == {}
