"""API tests for /instruments/{symbol}/institutional-holdings (#730 PR 4).

Pins:
  * 404 on unknown symbol.
  * Empty payload (200, totals=null, filers=[]) when no holdings on file.
  * Latest-quarter cohort selection (older quarters excluded from the
    response).
  * Per-slice totals split institutions vs ETFs by filer_type.
  * Option exposure (PUT / CALL) excluded from totals but included in
    the filer drilldown list.
  * limit + ordering — top-N by shares DESC.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.providers.implementations.sec_13f import ThirteenFFilerInfo, ThirteenFHolding
from app.services.institutional_holdings import (
    _upsert_filer,
    _upsert_holding,
    seed_etf_filer,
    seed_filer,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    """TestClient with get_conn overridden to yield the ebull_test
    connection. Restores the override on teardown so cross-test
    leaks can't poison subsequent suites."""

    def _dep() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _dep
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _make_filer_info(*, cik: str, name: str) -> ThirteenFFilerInfo:
    return ThirteenFFilerInfo(
        cik=cik,
        name=name,
        period_of_report=date(2024, 12, 31),
        filed_at=datetime(2025, 2, 14, tzinfo=UTC),
        table_value_total_usd=Decimal("1000000000"),
    )


def _make_holding(
    *,
    cusip: str = "037833100",
    shares: str,
    value: str = "100000000",
    put_call: str | None = None,
) -> ThirteenFHolding:
    return ThirteenFHolding(
        cusip=cusip,
        name_of_issuer="APPLE INC",
        title_of_class="COM",
        value_usd=Decimal(value),
        shares_or_principal=Decimal(shares),
        shares_or_principal_type="SH",
        put_call=put_call,  # type: ignore[arg-type]
        investment_discretion="SOLE",
        voting_sole=Decimal(shares),
        voting_shared=Decimal(0),
        voting_none=Decimal(0),
    )


class TestInstitutionalHoldingsEndpoint:
    def test_unknown_symbol_returns_404(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        # ebull_test_conn truncates the planner tables; symbol genuinely
        # not present.
        resp = client.get("/instruments/UNKNOWN_TEST_SYMBOL/institutional-holdings")
        assert resp.status_code == 404

    def test_known_symbol_with_no_holdings_returns_empty_payload(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=730_400, symbol="AAPL_T1")
        conn.commit()

        resp = client.get("/instruments/AAPL_T1/institutional-holdings")
        assert resp.status_code == 200
        data = resp.json()
        assert data["symbol"] == "AAPL_T1"
        assert data["totals"] is None
        assert data["filers"] == []

    def test_latest_quarter_cohort(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        """Two quarters of holdings present; the endpoint returns
        only the latest quarter's rows + the latest period in totals."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=730_401, symbol="AAPL_T2")
        # Vanguard is an ETF filer.
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD")
        seed_filer(conn, cik="0000102909", label="VANGUARD")
        seed_filer(conn, cik="0001067983", label="BERKSHIRE")
        conn.commit()

        # Older quarter — Q3 2024.
        old_filer_id = _upsert_filer(
            conn,
            ThirteenFFilerInfo(
                cik="0001067983",
                name="BERKSHIRE",
                period_of_report=date(2024, 9, 30),
                filed_at=datetime(2024, 11, 14, tzinfo=UTC),
                table_value_total_usd=Decimal("1"),
            ),
        )
        _upsert_holding(
            conn,
            filer_id=old_filer_id,
            instrument_id=730_401,
            accession_number="0001067983-24-Q3",
            period_of_report=date(2024, 9, 30),
            filed_at=datetime(2024, 11, 14, tzinfo=UTC),
            holding=_make_holding(shares="1000000"),
        )

        # Latest quarter — Q4 2024.
        new_filer_id = _upsert_filer(
            conn,
            _make_filer_info(cik="0001067983", name="BERKSHIRE"),
        )
        _upsert_holding(
            conn,
            filer_id=new_filer_id,
            instrument_id=730_401,
            accession_number="0001067983-25-Q4",
            period_of_report=date(2024, 12, 31),
            filed_at=datetime(2025, 2, 14, tzinfo=UTC),
            holding=_make_holding(shares="2000000"),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL_T2/institutional-holdings")
        assert resp.status_code == 200
        data = resp.json()

        # Only Q4 row in the response.
        assert len(data["filers"]) == 1
        assert data["filers"][0]["accession_number"] == "0001067983-25-Q4"
        assert Decimal(data["filers"][0]["shares"]) == Decimal("2000000")

        assert data["totals"]["period_of_report"] == "2024-12-31"
        assert Decimal(data["totals"]["institutions_shares"]) == Decimal("2000000")
        assert Decimal(data["totals"]["etfs_shares"]) == Decimal(0)
        assert data["totals"]["total_filers"] == 1

    def test_etf_vs_institutions_split(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        """Vanguard (ETF-seeded) and Berkshire (default INV) on the
        same instrument — totals split by filer_type."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=730_402, symbol="AAPL_T3")
        seed_etf_filer(conn, cik="0000102909", label="VANGUARD")
        conn.commit()

        # Vanguard — classified as ETF.
        van_id = _upsert_filer(conn, _make_filer_info(cik="0000102909", name="VANGUARD"))
        _upsert_holding(
            conn,
            filer_id=van_id,
            instrument_id=730_402,
            accession_number="0000102909-25-Q4",
            period_of_report=date(2024, 12, 31),
            filed_at=datetime(2025, 2, 14, tzinfo=UTC),
            holding=_make_holding(shares="5000000"),
        )

        # Berkshire — default INV.
        brk_id = _upsert_filer(conn, _make_filer_info(cik="0001067983", name="BERKSHIRE"))
        _upsert_holding(
            conn,
            filer_id=brk_id,
            instrument_id=730_402,
            accession_number="0001067983-25-Q4",
            period_of_report=date(2024, 12, 31),
            filed_at=datetime(2025, 2, 14, tzinfo=UTC),
            holding=_make_holding(shares="2000000"),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL_T3/institutional-holdings")
        assert resp.status_code == 200
        data = resp.json()

        assert Decimal(data["totals"]["etfs_shares"]) == Decimal("5000000")
        assert Decimal(data["totals"]["institutions_shares"]) == Decimal("2000000")
        assert data["totals"]["total_etfs_filers"] == 1
        assert data["totals"]["total_institutions_filers"] == 1
        assert data["totals"]["total_filers"] == 2

    def test_put_call_excluded_from_totals(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        """Option exposure (PUT / CALL) appears in the filer
        drilldown list but is excluded from the slice totals so a
        protective put is not counted as long ownership."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=730_403, symbol="AAPL_T4")
        conn.commit()

        filer_id = _upsert_filer(conn, _make_filer_info(cik="0001067983", name="BERKSHIRE"))

        # Equity exposure — counts toward institutions_shares.
        _upsert_holding(
            conn,
            filer_id=filer_id,
            instrument_id=730_403,
            accession_number="0001067983-25-Q4",
            period_of_report=date(2024, 12, 31),
            filed_at=datetime(2025, 2, 14, tzinfo=UTC),
            holding=_make_holding(shares="1000000", put_call=None),
        )
        # PUT exposure — drilldown only.
        _upsert_holding(
            conn,
            filer_id=filer_id,
            instrument_id=730_403,
            accession_number="0001067983-25-Q4",
            period_of_report=date(2024, 12, 31),
            filed_at=datetime(2025, 2, 14, tzinfo=UTC),
            holding=_make_holding(shares="500000", put_call="PUT"),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL_T4/institutional-holdings")
        assert resp.status_code == 200
        data = resp.json()

        # Slice total excludes the PUT exposure.
        assert Decimal(data["totals"]["institutions_shares"]) == Decimal("1000000")

        # Both rows surface in the filer list.
        put_calls = sorted([f["is_put_call"] for f in data["filers"]], key=lambda v: v or "")
        assert put_calls == [None, "PUT"]

    def test_limit_param_caps_filer_list(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=730_404, symbol="AAPL_T5")

        # Three filers, three holdings.
        for i, cik in enumerate(["0000000001", "0000000002", "0000000003"]):
            seed_filer(conn, cik=cik, label=f"FILER_{i}")
            filer_id = _upsert_filer(conn, _make_filer_info(cik=cik, name=f"FILER_{i}"))
            _upsert_holding(
                conn,
                filer_id=filer_id,
                instrument_id=730_404,
                accession_number=f"{cik}-25-Q4",
                period_of_report=date(2024, 12, 31),
                filed_at=datetime(2025, 2, 14, tzinfo=UTC),
                holding=_make_holding(shares=str(1_000_000 * (i + 1))),
            )
        conn.commit()

        resp = client.get("/instruments/AAPL_T5/institutional-holdings?limit=2")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["filers"]) == 2
        # Top-N by shares DESC.
        assert Decimal(data["filers"][0]["shares"]) == Decimal("3000000")
        assert Decimal(data["filers"][1]["shares"]) == Decimal("2000000")

    def test_limit_clamped_to_max(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
        client: TestClient,
    ) -> None:
        """limit=501 rejected (>= 500 max) — FastAPI Query validation
        returns 422."""
        # No seeding needed; the param validation fails before DB.
        resp = client.get("/instruments/AAPL_T6/institutional-holdings?limit=501")
        assert resp.status_code == 422
