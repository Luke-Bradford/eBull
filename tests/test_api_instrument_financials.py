"""Tests for GET /instruments/{symbol}/financials (Phase 2.3)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.api.instruments import get_yfinance_provider
from app.main import app
from app.providers.implementations.yfinance_provider import (
    YFinanceFinancialRow,
    YFinanceFinancials,
)


def _install_stub_provider(provider: object) -> None:
    app.dependency_overrides[get_yfinance_provider] = lambda: provider


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    app.dependency_overrides.pop(get_yfinance_provider, None)


def _install_db_conn(instrument_row: dict | None, periods_rows: list[dict] | None = None) -> None:
    """Stub DB that returns instrument_row on first execute, periods_rows on second."""

    def _conn():
        conn_mock = MagicMock()
        cur_mock = MagicMock()
        cur_mock.__enter__.return_value = cur_mock
        cur_mock.fetchone.return_value = instrument_row
        cur_mock.fetchall.return_value = periods_rows if periods_rows is not None else []
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _conn


def _clear_db_override() -> None:
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def test_financials_unknown_symbol_returns_404(client: TestClient) -> None:
    stub_provider = MagicMock()
    _install_stub_provider(stub_provider)
    _install_db_conn(None)
    try:
        resp = client.get("/instruments/NOTREAL/financials?statement=income")
    finally:
        _clear_db_override()
    assert resp.status_code == 404
    stub_provider.get_financials.assert_not_called()


def test_financials_empty_symbol_returns_400(client: TestClient) -> None:
    stub_provider = MagicMock()
    _install_stub_provider(stub_provider)
    _install_db_conn(None)
    try:
        resp = client.get("/instruments/%20/financials?statement=income")
    finally:
        _clear_db_override()
    assert resp.status_code == 400


def test_financials_invalid_statement_returns_422(client: TestClient) -> None:
    # Literal validation on the query param means FastAPI returns 422 before
    # the handler runs, without hitting the DB or yfinance.
    stub_provider = MagicMock()
    _install_stub_provider(stub_provider)
    _install_db_conn({"instrument_id": 1, "symbol": "AAPL"})
    try:
        resp = client.get("/instruments/AAPL/financials?statement=flufinomics")
    finally:
        _clear_db_override()
    assert resp.status_code == 422


def test_financials_local_data_wins(client: TestClient) -> None:
    """When financial_periods has rows, yfinance is not consulted."""
    stub_provider = MagicMock()
    _install_stub_provider(stub_provider)
    periods_rows = [
        {
            "period_end_date": date(2026, 3, 31),
            "period_type": "Q1",
            "reported_currency": "USD",
            "revenue": Decimal("90000000000"),
            "cost_of_revenue": Decimal("50000000000"),
            "gross_profit": Decimal("40000000000"),
            "operating_income": Decimal("30000000000"),
            "net_income": Decimal("25000000000"),
            "eps_basic": Decimal("1.55"),
            "eps_diluted": Decimal("1.52"),
            "research_and_dev": None,
            "sga_expense": None,
            "depreciation_amort": None,
            "interest_expense": None,
            "income_tax": None,
            "shares_basic": None,
            "shares_diluted": None,
            "sbc_expense": None,
        }
    ]
    _install_db_conn({"instrument_id": 1, "symbol": "AAPL"}, periods_rows)
    try:
        resp = client.get("/instruments/AAPL/financials?statement=income&period=quarterly")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "financial_periods"
    assert body["currency"] == "USD"
    assert len(body["rows"]) == 1
    row = body["rows"][0]
    assert row["period_end"] == "2026-03-31"
    assert row["period_type"] == "Q1"
    assert row["values"]["revenue"] == "90000000000"
    assert row["values"]["research_and_dev"] is None
    stub_provider.get_financials.assert_not_called()


def test_financials_yfinance_fallback(client: TestClient) -> None:
    """When financial_periods has no rows, yfinance is consulted."""
    stub_provider = MagicMock()
    stub_provider.get_financials.return_value = YFinanceFinancials(
        symbol="VOD.L",
        statement="income",
        period="quarterly",
        currency="GBP",
        rows=[
            YFinanceFinancialRow(
                period_end=date(2026, 3, 31),
                values={"Total Revenue": Decimal("9000000000")},
            )
        ],
    )
    _install_stub_provider(stub_provider)
    _install_db_conn({"instrument_id": 7, "symbol": "VOD.L"}, periods_rows=[])
    try:
        resp = client.get("/instruments/VOD.L/financials?statement=income&period=quarterly")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "yfinance"
    assert body["currency"] == "GBP"
    assert len(body["rows"]) == 1
    assert body["rows"][0]["values"]["Total Revenue"] == "9000000000"
    # period_type inferred from the fiscal-quarter-end month (Mar → Q1).
    assert body["rows"][0]["period_type"] == "Q1"
    stub_provider.get_financials.assert_called_once_with("VOD.L", statement="income", period="quarterly")


def test_financials_no_data_anywhere_returns_empty_rows(client: TestClient) -> None:
    """Neither financial_periods nor yfinance has data — return 200 with
    empty rows, NOT 404 or 500. UI shows 'no statement data'."""
    stub_provider = MagicMock()
    stub_provider.get_financials.return_value = None
    _install_stub_provider(stub_provider)
    _install_db_conn({"instrument_id": 99, "symbol": "WEIRD"}, periods_rows=[])
    try:
        resp = client.get("/instruments/WEIRD/financials?statement=balance")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["source"] == "yfinance"
    assert body["rows"] == []


def test_financials_balance_statement_uses_balance_columns(client: TestClient) -> None:
    stub_provider = MagicMock()
    _install_stub_provider(stub_provider)
    periods_rows = [
        {
            "period_end_date": date(2026, 3, 31),
            "period_type": "FY",
            "reported_currency": "USD",
            "total_assets": Decimal("400000000000"),
            "total_liabilities": Decimal("300000000000"),
            "shareholders_equity": Decimal("100000000000"),
            "cash": None,
            "long_term_debt": None,
            "short_term_debt": None,
            "shares_outstanding": None,
            "inventory": None,
            "receivables": None,
            "payables": None,
            "goodwill": None,
            "ppe_net": None,
        }
    ]
    _install_db_conn({"instrument_id": 1, "symbol": "AAPL"}, periods_rows)
    try:
        resp = client.get("/instruments/AAPL/financials?statement=balance&period=annual")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body["rows"][0]["values"].keys()) == {
        "total_assets",
        "total_liabilities",
        "shareholders_equity",
        "cash",
        "long_term_debt",
        "short_term_debt",
        "shares_outstanding",
        "inventory",
        "receivables",
        "payables",
        "goodwill",
        "ppe_net",
    }
    assert body["rows"][0]["values"]["total_assets"] == "400000000000"
