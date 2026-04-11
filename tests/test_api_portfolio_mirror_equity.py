"""§8.6 Test 1 — AUM delta test for the API path.

Uses TestClient + app.dependency_overrides[get_conn] to point
at ebull_test, seeds mirror_aum_fixture + one eBull position +
cash, and asserts (against GET /portfolio — the router mounts
with prefix=/portfolio in app/api/portfolio.py:38):
- PortfolioResponse.mirror_equity == _load_mirror_equity(conn)
- PortfolioResponse.total_aum == positions_mv + cash + mirror_equity
- soft-close baseline returns to positions + cash
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.portfolio import _load_mirror_equity
from tests.fixtures.copy_mirrors import (
    _NOW,
    mirror_aum_fixture,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB API mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


@pytest.fixture
def client(conn: psycopg.Connection[Any]) -> Iterator[TestClient]:
    """Overrides get_conn to reuse the test fixture connection, so
    assertions read the same DB state the endpoint sees.

    Auth (``require_session_or_service_token``) is already no-op'd
    globally by ``tests/conftest.py:21`` — do NOT touch that key here.
    A per-test pop would wipe the global override and poison every
    subsequent test file (test_api_recommendations, test_api_system,
    etc.) with 401s.
    """

    def _override_conn() -> Iterator[psycopg.Connection[Any]]:
        yield conn

    app.dependency_overrides[get_conn] = _override_conn
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _seed_ebull_position_and_cash(
    conn: psycopg.Connection[Any],
) -> tuple[float, float]:
    """Add one eBull position + one cash row on top of
    mirror_aum_fixture. Returns (positions_mv, cash).

    Schema references — same as the §8.5 helper: sql/001_init.sql:1-13
    (instruments), :159-168 (positions), :170-177 (cash_ledger),
    sql/021_positions_source.sql (positions.source allows only
    'ebull' | 'broker_sync'). No 'tier'/'created_at'/'updated_at' on
    instruments; no 'reason'/'recorded_at' on cash_ledger.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (770001, 'EBULL', 'eBull Position',
                    'healthcare', TRUE)
            """
        )
        cur.execute(
            """
            INSERT INTO positions (instrument_id, current_units,
                                   cost_basis, avg_cost, open_date,
                                   source, updated_at)
            VALUES (770001, 10.0, 200.0, 20.0,
                    %(today)s, 'broker_sync', %(now)s)
            """,
            {"today": _NOW.date(), "now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask, quoted_at)
            VALUES (770001, 25.0, 24.95, 25.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO cash_ledger (event_type, amount, currency)
            VALUES ('deposit', 100.0, 'GBP')
            """
        )
    return 250.0, 100.0  # mv = units * last = 10 * 25


def test_api_portfolio_mirror_equity_present_in_response(conn: psycopg.Connection[Any], client: TestClient) -> None:
    """§8.6 Test 1: GET /portfolio exposes mirror_equity and
    sums it into total_aum.
    """
    mirror_aum_fixture(conn)
    positions_mv, cash = _seed_ebull_position_and_cash(conn)
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    response = client.get("/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == pytest.approx(expected_mirror, abs=1e-6)
    assert body["total_aum"] == pytest.approx(positions_mv + cash + expected_mirror, abs=1e-6)


def test_api_portfolio_soft_close_baseline(conn: psycopg.Connection[Any], client: TestClient) -> None:
    """§8.6 Test 1 baseline: flip mirrors to active=FALSE →
    mirror_equity returns to 0.0 and total_aum to positions + cash.
    """
    mirror_aum_fixture(conn)
    positions_mv, cash = _seed_ebull_position_and_cash(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    response = client.get("/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == 0.0
    assert body["total_aum"] == pytest.approx(positions_mv + cash, abs=1e-6)


def test_api_portfolio_no_mirrors_field_default(conn: psycopg.Connection[Any], client: TestClient) -> None:
    """§6.4 contract: with no copy_mirrors rows at all,
    mirror_equity is the float 0.0 (not None, not absent).
    """
    _seed_ebull_position_and_cash(conn)
    conn.commit()

    response = client.get("/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == 0.0
    assert "mirror_equity" in body  # field is always present
