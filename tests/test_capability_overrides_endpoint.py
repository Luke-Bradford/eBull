"""DB-backed test for GET /admin/capability-overrides (#531).

Guards the SQL/schema wiring the mock-based pattern cannot: the handler
shipped selecting a nonexistent ``exchanges.name`` column (the table's
human label is ``description``), 500-ing in production because no test
ever ran the real query against the real schema. This exercises the
endpoint end-to-end against the migrated test DB.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from app.api.auth import require_service_token, require_session_or_service_token
from app.db import get_conn
from app.main import app
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    def _conn_override() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    def _bypass_auth() -> None:
        return None

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


def _insert_exchange(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    description: str,
    asset_class: str,
    capabilities: dict[str, list[str]],
) -> None:
    conn.execute(
        """
        INSERT INTO exchanges (exchange_id, description, asset_class, capabilities)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (exchange_id) DO UPDATE
          SET description = EXCLUDED.description,
              asset_class = EXCLUDED.asset_class,
              capabilities = EXCLUDED.capabilities
        """,
        (exchange_id, description, asset_class, Jsonb(capabilities)),
    )
    conn.commit()


def test_endpoint_runs_real_sql_and_surfaces_drift(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    # A us_equity exchange with wiped capabilities diverges from the seed.
    _insert_exchange(
        ebull_test_conn,
        exchange_id="ZZTESTX",
        description="Drift Test Exchange",
        asset_class="us_equity",
        capabilities={},
    )

    resp = client.get("/admin/capability-overrides")

    # The original bug 500'd here (SELECT name FROM exchanges).
    assert resp.status_code == 200
    body = resp.json()

    seeded = {row["exchange_id"]: row for row in body["rows"]}
    assert "ZZTESTX" in seeded, "wiped-capabilities exchange should surface as drift"
    row = seeded["ZZTESTX"]
    assert row["exchange_name"] == "Drift Test Exchange"
    assert row["asset_class"] == "us_equity"

    caps = {d["capability"]: d for d in row["diffs"]}
    # filings seed is ["sec_edgar"]; current wiped to [].
    assert "filings" in caps
    assert caps["filings"]["seed_providers"] == ["sec_edgar"]
    assert caps["filings"]["current_providers"] == []


def test_endpoint_excludes_exchange_at_seed_default(
    client: TestClient,
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    _insert_exchange(
        ebull_test_conn,
        exchange_id="ZZSEED",
        description="At-Seed Exchange",
        asset_class="us_equity",
        capabilities={
            "filings": ["sec_edgar"],
            "fundamentals": ["sec_xbrl"],
            "dividends": ["sec_dividend_summary"],
            "insider": ["sec_form4"],
            "analyst": [],
            "ratings": [],
            "esg": [],
            "ownership": ["sec_13f", "sec_13d_13g"],
            "corporate_events": ["sec_8k_events"],
            "business_summary": ["sec_10k_item1"],
            "officers": [],
        },
    )

    resp = client.get("/admin/capability-overrides")

    assert resp.status_code == 200
    ids = {row["exchange_id"] for row in resp.json()["rows"]}
    assert "ZZSEED" not in ids, "an exchange at seed default must not surface as drift"
