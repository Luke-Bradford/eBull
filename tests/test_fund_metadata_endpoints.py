"""Tests for fund-metadata API endpoints (#1171, T9)."""

from __future__ import annotations

from datetime import date

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
    """FastAPI test client. Uses the ebull_test DB fixture so reads see the
    same per-test schema state.

    Override the ``require_session_or_service_token`` dependency so endpoint
    tests don't need full operator auth setup.
    """
    from app.api.auth import require_session_or_service_token

    def _bypass_auth() -> None:
        return None

    app.dependency_overrides[require_session_or_service_token] = _bypass_auth
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.pop(require_session_or_service_token, None)


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
