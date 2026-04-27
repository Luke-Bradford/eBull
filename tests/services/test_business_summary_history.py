"""list_10k_history filters to SEC provider only (#559)."""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.services.business_summary import list_10k_history

_INSERT_FILING_EVENT_SQL = """
INSERT INTO filing_events
    (instrument_id, provider, provider_filing_id, filing_type, filing_date,
     primary_document_url, source_url)
VALUES (%s, %s, %s, %s, %s, NULL, NULL)
"""


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 5590) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, "TEST559P2", "Test 559 P2"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


@pytest.mark.integration
def test_list_10k_history_filters_to_sec_provider(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """A non-SEC 10-K row in filing_events must NOT appear in the
    SEC 10-K history list."""
    iid = _seed_instrument(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            _INSERT_FILING_EVENT_SQL,
            (iid, "sec", "0001234567-26-000001", "10-K", date(2026, 3, 24)),
        )
        cur.execute(
            _INSERT_FILING_EVENT_SQL,
            (iid, "sec", "0001234567-25-000001", "10-K", date(2025, 3, 24)),
        )
        # Non-SEC row that must be filtered out.
        cur.execute(
            _INSERT_FILING_EVENT_SQL,
            (iid, "companies_house", "CH-000001", "10-K", date(2026, 3, 24)),
        )
    ebull_test_conn.commit()

    history = list_10k_history(ebull_test_conn, instrument_id=iid)
    assert len(history) == 2, (
        f"expected 2 SEC rows, got {len(history)} — "
        f"non-SEC rows: {[h.accession_number for h in history if not h.accession_number.startswith('00012')]}"
    )
    assert all(h.accession_number.startswith("00012345") for h in history), (
        f"non-SEC rows leaked into history: {[h.accession_number for h in history]}"
    )
    # Descending order
    assert history[0].filing_date == date(2026, 3, 24)
    assert history[1].filing_date == date(2025, 3, 24)
