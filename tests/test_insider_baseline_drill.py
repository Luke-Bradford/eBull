"""Tests for the Form 3 baseline drillthrough + CSV export
(#788 Chain 2.6).

Pins: drill endpoint composites baseline list + pipeline state,
CSV export emits header even on empty data, unknown / no-SEC
instruments 404.
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
    monkeypatch: pytest.MonkeyPatch,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    """Route the FastAPI app's get_conn dep at the test DB so the
    drill endpoint reads what we seed."""
    from app.api import auth
    from app.db import get_conn

    def _override_conn() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    def _override_auth() -> object:
        return object()  # any non-None marker

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[auth.require_session_or_service_token] = _override_auth
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(auth.require_session_or_service_token, None)


def test_drill_returns_pipeline_state_alongside_baseline_rows(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """An instrument with no Form 3 filings still resolves — drill
    returns rows=[] plus a pipeline state with the 'no Form 3
    baseline filings' note so the operator sees the gap explicitly."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=971_001, symbol="ABCD", cik="0000111000")
    conn.commit()

    resp = client.get("/instruments/ABCD/insider_baseline/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["symbol"] == "ABCD"
    assert body["instrument_id"] == 971_001
    assert body["rows"] == []
    assert body["pipeline_typed_row_count"] == 0
    assert body["pipeline_raw_body_count"] == 0
    assert body["pipeline_tombstone_count"] == 0
    assert any("Form 3" in n or "no Form 3" in n.lower() for n in body["pipeline_notes"])


def test_drill_404s_unknown_symbol(client: TestClient) -> None:
    resp = client.get("/instruments/NONEXISTENT/insider_baseline/drill")
    assert resp.status_code == 404


def test_drill_404s_when_no_sec_cik(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Instrument exists but has no SEC CIK → 404. Same gate as
    /insider_baseline."""
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (971_002, 'NOCIK', 'No CIK Inc', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
    )
    conn.commit()

    resp = client.get("/instruments/NOCIK/insider_baseline/drill")
    assert resp.status_code == 404


def test_csv_export_emits_header_even_on_empty(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Empty data still produces a valid CSV with header so
    automation scripts don't have to branch on the empty case."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=971_003, symbol="EXPT", cik="0000111002")
    conn.commit()

    resp = client.get("/instruments/EXPT/insider_baseline/export.csv")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers["content-disposition"]
    rows = list(csv.reader(io.StringIO(resp.text)))
    assert rows[0] == [
        "filer_cik",
        "filer_name",
        "filer_role",
        "security_title",
        "is_derivative",
        "direct_indirect",
        "shares",
        "value_owned",
        "as_of_date",
    ]
    assert len(rows) == 1  # header only


def test_csv_export_404s_unknown_symbol(client: TestClient) -> None:
    resp = client.get("/instruments/NONEXISTENT/insider_baseline/export.csv")
    assert resp.status_code == 404


def test_drill_rejects_wrong_provider(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """``?provider=sec_form4`` is invalid for the Form 3 baseline
    drill. Same gate as /insider_baseline. Codex pre-push review
    caught the missing validator."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=971_010, symbol="PROVDR", cik="0000111005")
    conn.commit()

    resp = client.get("/instruments/PROVDR/insider_baseline/drill?provider=sec_form4")
    assert resp.status_code == 400


def test_csv_rejects_wrong_provider(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Same provider gate on the CSV export."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=971_011, symbol="PROVCS", cik="0000111006")
    conn.commit()

    resp = client.get("/instruments/PROVCS/insider_baseline/export.csv?provider=sec_form4")
    assert resp.status_code == 400


def test_drill_no_filings_note_only_when_no_evidence_at_all(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When tombstones or raw bodies exist, the 'no Form 3 baseline
    filings' note must NOT fire — the more specific notes
    (tombstoned / rewash candidate) carry the truth. Codex pre-push
    review caught the prior mislabelling."""
    conn = ebull_test_conn
    _seed_instrument_with_cik(conn, iid=971_012, symbol="TOMB3", cik="0000111007")
    # Tombstoned filing only — no typed rows, no raw body.
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone,
            period_of_report
        ) VALUES ('t-26-1', 971_012, '3', 'u', 1, TRUE, '2025-01-15')
        """,
    )
    conn.commit()

    resp = client.get("/instruments/TOMB3/insider_baseline/drill")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pipeline_tombstone_count"] == 1
    notes = body["pipeline_notes"]
    # 'no Form 3 baseline filings' must NOT be in notes — there IS a
    # filing, it's tombstoned. The specific tombstone note carries
    # the actual state.
    assert not any("no Form 3 baseline filings" in n for n in notes)
    assert any("tombstoned" in n.lower() for n in notes)
