"""Tests for fund-metadata API endpoints (#1171, T9)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} fund"),
    )


def _seed_observation_and_current(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str = "0001-26-X",
    period_end: date = date(2025, 12, 31),
) -> None:
    conn.execute(
        """
        INSERT INTO fund_metadata_observations (
            instrument_id, source_accession, filed_at, period_end,
            document_type, amendment_flag, parser_version,
            trust_cik, class_id, expense_ratio_pct, net_assets_amt
        ) VALUES (
            %s, %s, NOW(), %s, 'N-CSR', FALSE, 'n-csr-fund-metadata-v1',
            '0000036405', 'C000010048', 0.0004, 1000000000
        )
        """,
        (instrument_id, accession, period_end),
    )


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
):
    """FastAPI test client wired to the per-test DB connection.

    Overrides three dependencies:
    - ``require_session_or_service_token`` + ``require_service_token`` —
      auth bypass so endpoint tests don't need full operator setup.
    - ``get_conn`` — route DB reads through ``ebull_test_conn`` so seeded
      rows are visible to TestClient requests (PR #1172 review WARNING:
      without this override, seed helpers are invisible to the endpoint
      and golden-response tests are unimplementable).
    """
    from app.api.auth import require_service_token, require_session_or_service_token
    from app.db import get_conn

    def _bypass_auth() -> None:
        return None

    def _conn_override():
        yield ebull_test_conn

    app.dependency_overrides[require_session_or_service_token] = _bypass_auth
    app.dependency_overrides[require_service_token] = _bypass_auth
    app.dependency_overrides[get_conn] = _conn_override
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_session_or_service_token, None)
        app.dependency_overrides.pop(require_service_token, None)
        app.dependency_overrides.pop(get_conn, None)


def test_get_fund_metadata_404_unknown_symbol(client: TestClient) -> None:
    resp = client.get("/instruments/UNKNOWN_SYMBOL_QQQ/fund-metadata")
    assert resp.status_code == 404


def test_coverage_endpoint_returns_counts(client: TestClient) -> None:
    resp = client.get("/coverage/fund-metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert "total_observations_current" in body
    assert "directory_class_count" in body
    assert "directory_pending_external_id" in body


def test_history_endpoint_404_unknown_symbol(client: TestClient) -> None:
    resp = client.get("/instruments/UNKNOWN_SYMBOL_ZZZ/fund-metadata/history")
    assert resp.status_code == 404


def test_history_endpoint_accepts_since_date(client: TestClient) -> None:
    resp = client.get("/instruments/UNKNOWN_SYMBOL_ZZZ/fund-metadata/history?since=2025-01-01")
    # Still 404 on unknown symbol but the `since` param parses correctly.
    assert resp.status_code == 404


def test_get_fund_metadata_returns_seeded_row(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """T10 golden-response: seed a (observation, current) row and assert the
    endpoint returns the expected fund-metadata payload."""
    _seed_instrument(ebull_test_conn, iid=9001, symbol="VFIAX_GOLD")
    _seed_observation_and_current(ebull_test_conn, instrument_id=9001)
    # The seed helper writes only to fund_metadata_observations; trigger a
    # refresh so fund_metadata_current is populated.
    from app.services.fund_metadata import refresh_fund_metadata_current

    refresh_fund_metadata_current(ebull_test_conn, instrument_id=9001)
    ebull_test_conn.commit()

    resp = client.get("/instruments/VFIAX_GOLD/fund-metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert body["instrument_id"] == 9001
    assert body["symbol"] == "VFIAX_GOLD"
    assert body["class_id"] == "C000010048"
    assert body["document_type"] == "N-CSR"
    assert body["trust_cik"] == "0000036405"
    assert Decimal(body["expense_ratio_pct"]) == Decimal("0.00040000")
    assert Decimal(body["net_assets_amt"]) == Decimal("1000000000")


def test_history_endpoint_returns_seeded_observations(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _seed_instrument(ebull_test_conn, iid=9002, symbol="VFIAX_HIST")
    _seed_observation_and_current(
        ebull_test_conn, instrument_id=9002, accession="0001-25-A", period_end=date(2024, 12, 31)
    )
    _seed_observation_and_current(
        ebull_test_conn, instrument_id=9002, accession="0001-25-B", period_end=date(2025, 12, 31)
    )
    ebull_test_conn.commit()

    resp = client.get("/instruments/VFIAX_HIST/fund-metadata/history")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 2
    # Newer period_end first.
    assert rows[0]["period_end"] == "2025-12-31"
    assert rows[1]["period_end"] == "2024-12-31"

    # since= filter narrows to one row.
    resp = client.get("/instruments/VFIAX_HIST/fund-metadata/history?since=2025-06-01")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 1
    assert rows[0]["period_end"] == "2025-12-31"
