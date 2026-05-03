"""Tests for the DEF 14A drillthrough + CSV export
(#788 Chain 2.7).

Pins: latest-filing holders only on the drill, header always
on the CSV, gates (404 unknown / no-SEC-CIK, 400 wrong provider).
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _seed_instrument_with_cik(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cik: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (iid, cik),
    )


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    from app.api import auth
    from app.db import get_conn

    def _override_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    def _override_auth() -> object:
        return object()

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[auth.require_session_or_service_token] = _override_auth
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(auth.require_session_or_service_token, None)


def test_drill_returns_latest_filing_holders_only(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When two DEF 14A filings exist, the drill returns ONLY the
    holders from the most recent (by as_of_date)."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_001, symbol="DRILL", cik="0000111100")
    # Older filing — should NOT appear in the response.
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, 'old-26-1', '0000111100',
                  'Old Holder', 'officer', 100, 5.0, '2024-03-01')
        """,
        (972_001,),
    )
    # Newer filing — these should appear.
    for name, shares in [("Latest A", 200), ("Latest B", 150)]:
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                instrument_id, accession_number, issuer_cik,
                holder_name, holder_role, shares, percent_of_class, as_of_date
            ) VALUES (%s, 'new-26-1', '0000111100',
                      %s, 'director', %s, 8.0, '2025-03-01')
            """,
            (972_001, name, shares),
        )
    conn.commit()

    resp = client.get("/instruments/DRILL/def14a_holdings/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "DRILL"
    holder_names = {h["holder_name"] for h in body["holders"]}
    # Old Holder (older as_of_date) must NOT be in the latest snapshot.
    assert holder_names == {"Latest A", "Latest B"}
    # Sorted by shares desc — Latest A (200) before Latest B (150).
    assert body["holders"][0]["holder_name"] == "Latest A"
    # Pipeline state counts ALL rows (3 across two filings).
    assert body["pipeline_typed_row_count"] == 3


def test_drill_404s_unknown_symbol(client: TestClient) -> None:
    resp = client.get("/instruments/NONEXISTENT/def14a_holdings/drill")
    assert resp.status_code == 404


def test_drill_404s_when_no_sec_cik(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (972_002, 'NOCIK14A', 'No CIK', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()
    resp = client.get("/instruments/NOCIK14A/def14a_holdings/drill")
    assert resp.status_code == 404


def test_drill_rejects_wrong_provider(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_003, symbol="WRONGP", cik="0000111101")
    conn.commit()
    resp = client.get("/instruments/WRONGP/def14a_holdings/drill?provider=sec_form4")
    assert resp.status_code == 400


def test_drill_no_holders_note_only_when_no_evidence(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """With zero typed rows + zero tombstones + zero raw bodies,
    'no DEF 14A holders' note fires. With evidence, the more
    specific notes carry the truth."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_004, symbol="EMPTY14A", cik="0000111102")
    conn.commit()
    resp = client.get("/instruments/EMPTY14A/def14a_holdings/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["holders"] == []
    assert any("no DEF 14A holders" in n for n in body["pipeline_notes"])


def test_csv_export_emits_header_even_on_empty(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_005, symbol="CSVD14", cik="0000111103")
    conn.commit()
    resp = client.get("/instruments/CSVD14/def14a_holdings/export.csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    rows = list(csv.reader(io.StringIO(resp.text)))
    assert rows[0] == [
        "accession_number",
        "issuer_cik",
        "holder_name",
        "holder_role",
        "shares",
        "percent_of_class",
        "as_of_date",
    ]
    assert len(rows) == 1


def test_csv_export_includes_all_filings(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """CSV is for historical analysis — includes EVERY filing,
    not just the latest. Distinguishes from the drill view which
    is latest-only."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_006, symbol="HIST14", cik="0000111104")
    for accn, name, year in [("h-24", "Past Holder", "2024"), ("h-25", "Now Holder", "2025")]:
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                instrument_id, accession_number, issuer_cik,
                holder_name, holder_role, shares, percent_of_class, as_of_date
            ) VALUES (%s, %s, '0000111104',
                      %s, 'officer', 100, 5.0, %s)
            """,
            (972_006, accn, name, f"{year}-03-01"),
        )
    conn.commit()
    resp = client.get("/instruments/HIST14/def14a_holdings/export.csv")
    assert resp.status_code == 200
    rows = list(csv.reader(io.StringIO(resp.text)))
    holder_names = {row[2] for row in rows[1:]}
    assert holder_names == {"Past Holder", "Now Holder"}


def test_drill_surfaces_newer_unparsed_filing_via_filing_events(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A 2026 DEF 14A failed to parse (only filing_events row +
    def14a_ingest_log status='failed'); 2025 holders exist in the
    typed table. Drill MUST surface the gap so the operator
    doesn't silently see stale 2025 holders. Regression for the
    high-severity Codex finding."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_010, symbol="STALE", cik="0000111110")
    # 2025 typed holders.
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, '2025-1', '0000111110', 'A', 'officer', 100, 5.0, '2025-03-01')
        """,
        (972_010,),
    )
    # 2026 filing in filing_events with no typed rows.
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type, source_url,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, '2026-03-15', 'DEF 14A', 'https://example.com/x',
                  'sec', '2026-1', 'https://example.com/x')
        """,
        (972_010,),
    )
    conn.commit()

    resp = client.get("/instruments/STALE/def14a_holdings/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["latest_known_filing_date"] == "2026-03-15"
    assert body["holders_as_of_date"] == "2025-03-01"
    notes = body["pipeline_notes"]
    assert any("newer DEF 14A 2026-1" in n and "2026-03-15" in n for n in notes), (
        f"expected stale-holders surface; got notes={notes}"
    )


def test_drill_no_stale_warning_on_healthy_single_filing(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A single DEF 14A with both filing_events row AND typed
    holders must NOT trip the stale-holders warning. The
    accession-comparison guards against false positives where
    filing_date differs from as_of_date for the SAME filing.
    Regression for the high-severity Codex finding."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_012, symbol="HEALTH", cik="0000111112")
    accession = "h-26-1"
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, %s, '0000111112', 'A', 'officer', 100, 5.0, '2026-03-01')
        """,
        (972_012, accession),
    )
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type, source_url,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, '2026-03-15', 'DEF 14A', 'https://example.com/h',
                  'sec', %s, 'https://example.com/h')
        """,
        (972_012, accession),
    )
    conn.commit()

    resp = client.get("/instruments/HEALTH/def14a_holdings/drill")
    assert resp.status_code == 200
    notes = resp.json()["pipeline_notes"]
    assert not any("missing typed rows" in n for n in notes), (
        f"unexpected stale warning on healthy filing; notes={notes}"
    )


def test_drill_no_holders_note_suppressed_when_filing_events_has_pending(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Discovered-but-unparsed: filing_events row exists but no
    typed rows / no ingest_log entry. 'no DEF 14A holders' note
    must NOT fire — the gap is queue-side, surface that explicitly.
    Regression for the medium-severity Codex finding."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_011, symbol="PEND", cik="0000111111")
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type, source_url,
            provider, provider_filing_id, primary_document_url
        ) VALUES (%s, '2025-03-01', 'DEF 14A', 'https://example.com/y',
                  'sec', 'pending-1', 'https://example.com/y')
        """,
        (972_011,),
    )
    conn.commit()

    resp = client.get("/instruments/PEND/def14a_holdings/drill")
    assert resp.status_code == 200
    body = resp.json()
    notes = body["pipeline_notes"]
    assert not any("no DEF 14A holders" in n for n in notes)
    assert any("not yet ingested" in n for n in notes)


def test_drill_same_day_filings_tie_break_on_accession_not_insert_order(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When two DEF 14A filings share the same filing_date, the
    'latest' must tie-break on accession (provider_filing_id),
    not on filing_event_id. Otherwise insertion order decides
    which accession wins, which can flip a healthy state into a
    false stale-holders warning. Regression for the medium
    severity Codex finding."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_013, symbol="TIE14", cik="0000111113")
    # Holders are on acc-2 (the lexicographically later
    # accession). Insert acc-2 to filing_events FIRST, so a
    # naive ``ORDER BY filing_event_id DESC`` picks acc-1
    # (inserted later, larger event_id).
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, 'acc-2', '0000111113', 'A', 'officer', 100, 5.0, '2026-03-01')
        """,
        (972_013,),
    )
    for accn in ("acc-2", "acc-1"):
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type, source_url,
                provider, provider_filing_id, primary_document_url
            ) VALUES (%s, '2026-03-15', 'DEF 14A', 'https://example.com/x',
                      'sec', %s, 'https://example.com/x')
            """,
            (972_013, accn),
        )
    conn.commit()

    resp = client.get("/instruments/TIE14/def14a_holdings/drill")
    assert resp.status_code == 200
    notes = resp.json()["pipeline_notes"]
    # acc-2 > acc-1 lexicographically; latest should be acc-2 =
    # holders' accession ⇒ NO stale warning.
    assert not any("missing typed rows" in n for n in notes), (
        f"unexpected stale warning when latest accession matches holders; notes={notes}"
    )


def test_drill_tombstone_query_is_filing_distinct(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """The tombstone query uses ``COUNT(DISTINCT log.accession_number)``
    so the count is filing-level even if the schema later drops the
    PK on accession_number (e.g. moves to a retry-attempt model).

    Today ``def14a_ingest_log`` has a PK on accession_number, so
    retries update in place via ON CONFLICT — bare COUNT(*) is
    equivalent here. The DISTINCT is defense-in-depth; this test
    pins the contract by seeding two accessions in different
    tombstone-states and asserting the count is 2 (not double-
    counted, not collapsed). Codex PR #833 review.
    """
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=972_020, symbol="TOMB14", cik="0000111110")
    # Two distinct tombstoned filings.
    for accn, status in [("tomb-26-1", "failed"), ("tomb-26-2", "partial")]:
        conn.execute(
            """
            INSERT INTO def14a_ingest_log (
                accession_number, issuer_cik, status, error, fetched_at
            ) VALUES (%s, '0000111110', %s, 'simulated', NOW())
            """,
            (accn, status),
        )
        conn.execute(
            """
            INSERT INTO filing_events (
                instrument_id, provider, provider_filing_id, filing_type,
                filing_date, primary_document_url, source_url
            ) VALUES (%s, 'sec', %s, 'DEF 14A', '2025-04-01',
                      'http://x', 'http://y')
            ON CONFLICT (provider, provider_filing_id) DO NOTHING
            """,
            (972_020, accn),
        )
    conn.commit()

    resp = client.get("/instruments/TOMB14/def14a_holdings/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pipeline_tombstone_count"] == 2
