"""Tests for GET /instruments/{symbol}/financials.

After #498/#499 retired yfinance, the endpoint sources financial
statement rows exclusively from local ``financial_periods``
(SEC XBRL-derived). When SEC has no data for the instrument, the
response is an empty row list with ``source = "unavailable"`` —
no fallback to a non-canonical provider.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _install_db_conn(instrument_row: dict | None, periods_rows: list[dict] | None = None) -> None:
    """Stub DB: first cursor returns ``instrument_row`` on fetchone,
    second cursor returns ``periods_rows`` on fetchall."""

    def _conn() -> Iterator[MagicMock]:
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


def _install_sequenced_db_conn(
    fetchone_results: list[dict | None],
    periods_rows: list[dict] | None = None,
) -> MagicMock:
    """Stub DB where each ``fetchone`` returns the next item in
    ``fetchone_results``.

    Needed for the symbol-or-id resolver (#2184), which makes up to TWO
    resolution queries: the by-symbol attempt first, then the by-id
    attempt only if that missed. Returns the shared cursor mock so a
    caller can inspect ``execute`` calls.
    """
    cur_mock = MagicMock()
    cur_mock.__enter__.return_value = cur_mock
    cur_mock.fetchone.side_effect = list(fetchone_results)
    cur_mock.fetchall.return_value = periods_rows if periods_rows is not None else []

    def _conn() -> Iterator[MagicMock]:
        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _conn
    return cur_mock


def test_unknown_symbol_returns_404(client: TestClient) -> None:
    _install_db_conn(None)
    try:
        resp = client.get("/instruments/NOTREAL/financials?statement=income")
    finally:
        _clear_db_override()
    assert resp.status_code == 404


def test_empty_symbol_returns_400(client: TestClient) -> None:
    _install_db_conn(None)
    try:
        resp = client.get("/instruments/%20/financials?statement=income")
    finally:
        _clear_db_override()
    assert resp.status_code == 400


def test_invalid_statement_returns_422(client: TestClient) -> None:
    """Literal validation on the query param means FastAPI returns
    422 before the handler runs."""
    _install_db_conn({"instrument_id": 1, "symbol": "AAPL"})
    try:
        resp = client.get("/instruments/AAPL/financials?statement=flufinomics")
    finally:
        _clear_db_override()
    assert resp.status_code == 422


def test_local_sec_data_returned(client: TestClient) -> None:
    """When ``financial_periods`` has rows, the endpoint returns them
    with ``source = "financial_periods"``."""
    periods_rows = [
        {
            "period_end_date": date(2025, 12, 31),
            "period_type": "FY",
            "reported_currency": "USD",
            "revenue": Decimal("400000000000"),
            "net_income": Decimal("100000000000"),
            "operating_income": Decimal("130000000000"),
            "ebit": None,
            "ebitda": None,
            "interest_expense": None,
            "tax_expense": None,
            "gross_profit": None,
        },
    ]
    _install_db_conn(
        {"instrument_id": 1, "symbol": "AAPL"},
        periods_rows=periods_rows,
    )
    try:
        resp = client.get("/instruments/AAPL/financials?statement=income&period=annual")
    finally:
        _clear_db_override()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["statement"] == "income"
    assert body["period"] == "annual"
    assert body["currency"] == "USD"
    assert body["source"] == "financial_periods"
    assert len(body["rows"]) == 1
    assert body["rows"][0]["period_type"] == "FY"


def test_periods_query_orders_by_period_end_date_desc_first(client: TestClient) -> None:
    """#558/#613 review: ordering pins the chronological-DESC contract.

    ``period_end_date DESC`` is the primary key so the rendered
    columns walk backwards through real fiscal time. ``filed_date
    DESC NULLS LAST`` is the tie-breaker — it must NOT precede
    ``period_end_date`` (that would interleave restatement rows
    chronologically incorrect on the operator's Financials tab),
    and it must NOT include ``fiscal_quarter DESC NULLS FIRST``
    (which would push FY rows ahead of Q4 rows for the same
    fiscal year on the annual / mixed-period view).
    """

    cur_mock = MagicMock()
    cur_mock.__enter__.return_value = cur_mock
    cur_mock.fetchone.return_value = {"instrument_id": 1, "symbol": "AAPL"}
    cur_mock.fetchall.return_value = []

    def _conn() -> Iterator[MagicMock]:
        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cur_mock
        yield conn_mock

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _conn
    try:
        resp = client.get("/instruments/AAPL/financials?statement=income&period=quarterly")
        assert resp.status_code == 200
        executed_sql = "".join(str(call.args[0]) for call in cur_mock.execute.call_args_list if call.args)
        assert "ORDER BY period_end_date DESC" in executed_sql, executed_sql
        assert "filed_date DESC NULLS LAST" in executed_sql
        # NULLS FIRST on fiscal_quarter would re-order FY ahead of Q4 for the same year.
        assert "NULLS FIRST" not in executed_sql
    finally:
        _clear_db_override()


def test_no_local_data_returns_unavailable_empty(client: TestClient) -> None:
    """When SEC has no rows for the instrument, return empty payload
    with ``source = "unavailable"`` — no yfinance fallback."""
    _install_db_conn(
        {"instrument_id": 999, "symbol": "FOO"},
        periods_rows=[],
    )
    try:
        resp = client.get("/instruments/FOO/financials?statement=income&period=quarterly")
    finally:
        _clear_db_override()

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "FOO"
    assert body["source"] == "unavailable"
    assert body["rows"] == []
    assert body["currency"] is None


def test_balance_statement_dispatch(client: TestClient) -> None:
    """The handler dispatches on the ``statement`` query param to the
    right column whitelist. A broken column map for balance would
    surface as a 500 here. Asserts the rendered ``statement`` and
    ``period`` echo correctly + an empty payload doesn't 500."""
    _install_db_conn(
        {"instrument_id": 1, "symbol": "AAPL"},
        periods_rows=[],
    )
    try:
        resp = client.get("/instruments/AAPL/financials?statement=balance&period=annual")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["statement"] == "balance"
    assert body["period"] == "annual"
    assert body["source"] == "unavailable"


def test_cashflow_statement_dispatch(client: TestClient) -> None:
    """Same dispatch guard for the cashflow statement path."""
    _install_db_conn(
        {"instrument_id": 1, "symbol": "AAPL"},
        periods_rows=[],
    )
    try:
        resp = client.get("/instruments/AAPL/financials?statement=cashflow&period=quarterly")
    finally:
        _clear_db_override()
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["statement"] == "cashflow"
    assert body["period"] == "quarterly"


# ---------------------------------------------------------------------------
# #2184 — symbol-or-id path param. Spec §1.1's named acceptance test:
# "request each drill endpoint by id and by symbol; both must 200."
#
# These drive the real handler through `resolve_instrument_ref`, which the
# pure-logic table test in `tests/test_instrument_ref_resolution.py` does
# not execute. Limits, stated honestly: the cursor is a MagicMock, so these
# pin the routing, the attempt ORDER, and the SQL↔params binding — not that
# a column name exists in Postgres. Column correctness rests on the dev
# stack verification recorded in the PR.
# ---------------------------------------------------------------------------


def test_numeric_id_returns_200_with_the_resolved_symbol(client: TestClient) -> None:
    """`/instruments/1699/financials` — the shape that 404'd before #2184.

    The by-symbol attempt misses (no ticker "1699"), the by-id attempt
    hits, and the payload echoes the RESOLVED symbol so the FE heading can
    read "GME" rather than "1699".
    """
    cur_mock = _install_sequenced_db_conn([None, {"instrument_id": 1699, "symbol": "GME"}])
    try:
        resp = client.get("/instruments/1699/financials?statement=income&period=annual")
    finally:
        _clear_db_override()

    assert resp.status_code == 200, resp.text
    assert resp.json()["symbol"] == "GME"

    # Both resolution attempts ran, symbol first — and each bound its own
    # param key. A placeholder/param-key mismatch (`%(iid)s` bound with
    # `{"s": ...}`) is precisely what psycopg raises on at runtime and what
    # a fetchone-only assertion would miss.
    executed = [(str(c.args[0]), c.args[1]) for c in cur_mock.execute.call_args_list if c.args]
    resolution = [e for e in executed if "FROM instruments" in e[0]]
    assert len(resolution) == 2, executed
    assert "UPPER(symbol) = %(s)s" in resolution[0][0]
    assert resolution[0][1] == {"s": "1699"}
    assert "instrument_id = %(iid)s" in resolution[1][0]
    assert resolution[1][1] == {"iid": 1699}


def test_symbol_never_triggers_the_id_attempt(client: TestClient) -> None:
    """The shadowing guard, end to end.

    A ref that resolves as a symbol must stop there. If the id attempt
    ever ran first — or ran at all on a hit — an unrelated
    `instrument_id` could serve a DIFFERENT issuer's financials under a
    numeric-looking ticker.
    """
    cur_mock = _install_sequenced_db_conn([{"instrument_id": 1001, "symbol": "AAPL"}])
    try:
        resp = client.get("/instruments/AAPL/financials?statement=income&period=annual")
    finally:
        _clear_db_override()

    assert resp.status_code == 200, resp.text
    assert resp.json()["symbol"] == "AAPL"
    executed = [str(c.args[0]) for c in cur_mock.execute.call_args_list if c.args]
    resolution = [sql for sql in executed if "FROM instruments" in sql]
    assert len(resolution) == 1, resolution
    assert "instrument_id = %(iid)s" not in resolution[0]


def test_numeric_ref_matching_nothing_still_404s(client: TestClient) -> None:
    """Both attempts miss → 404, not a 500 and not a wrong instrument."""
    _install_sequenced_db_conn([None, None])
    try:
        resp = client.get("/instruments/999999999/financials?statement=income")
    finally:
        _clear_db_override()
    assert resp.status_code == 404
