"""
Tests for app.api.instruments — instrument list and detail endpoints.

Test strategy:
  Mock DB via FastAPI dependency override. The ``get_conn`` dependency is
  replaced with a mock connection that returns ``dict_row``-style dicts,
  matching ``row_factory=psycopg.rows.dict_row`` used in production.

  No ``psycopg.connect`` patching — the override injects the mock connection
  directly, proving that endpoints consume connections from the pool dependency.

  FastAPI ``TestClient`` drives requests through the real router, exercising
  Pydantic validation, query-parameter parsing, and response serialisation.

Structure:
  - TestListInstruments       — pagination, filters, empty/missing data
  - TestGetInstrumentDetail   — happy path, 404, missing optional data
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)


def _make_instrument_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    company_name: str = "Apple Inc",
    exchange: str | None = "NASDAQ",
    currency: str | None = "USD",
    sector: str | None = "Technology",
    industry: str | None = "Consumer Electronics",
    country: str | None = "US",
    is_tradable: bool = True,
    first_seen_at: datetime = _NOW,
    last_seen_at: datetime = _NOW,
    coverage_tier: int | None = 1,
    bid: float | None = 185.50,
    ask: float | None = 185.60,
    last: float | None = 185.55,
    spread_pct: float | None = 0.054,
    quoted_at: datetime | None = _NOW,
) -> dict[str, Any]:
    """Build a dict matching the joined instruments+quotes+coverage query shape."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "exchange": exchange,
        "currency": currency,
        "sector": sector,
        "industry": industry,
        "country": country,
        "is_tradable": is_tradable,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "coverage_tier": coverage_tier,
        "bid": bid,
        "ask": ask,
        "last": last,
        "spread_pct": spread_pct,
        "quoted_at": quoted_at,
    }


def _make_ext_id_row(
    provider: str = "sec",
    identifier_type: str = "cik",
    identifier_value: str = "0000320193",
) -> dict[str, Any]:
    return {
        "provider": provider,
        "identifier_type": identifier_type,
        "identifier_value": identifier_value,
    }


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection.

    ``cursor_results`` is a list of result sets, one per ``cur.execute()`` call.
    Each ``fetchone()`` returns the first row (or None), and ``fetchall()``
    returns the full list.
    """
    cur = MagicMock()
    result_iter = iter(cursor_results)

    def _on_execute(*_args: Any, **_kwargs: Any) -> None:
        rows = next(result_iter)
        cur.fetchone.return_value = rows[0] if rows else None
        cur.fetchall.return_value = rows

    cur.execute.side_effect = _on_execute
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Set up a mock connection as the get_conn dependency override.

    Returns the mock connection so tests can inspect ``cur.execute`` calls.
    """
    conn = _mock_conn(cursor_results)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _cleanup() -> None:
    """Restore the fallback dependency override after each test."""
    app.dependency_overrides[get_conn] = _fallback_conn


# Default override so validation-rejection tests (422 before DB access) don't
# crash on app.state.db_pool missing — the pool isn't created without lifespan.
def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestListInstruments
# ---------------------------------------------------------------------------


class TestListInstruments:
    """GET /instruments — paginated list with optional filters."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_returns_items(self) -> None:
        row = _make_instrument_row()
        _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments")

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["offset"] == 0
        assert body["limit"] == 50
        assert len(body["items"]) == 1

        item = body["items"][0]
        assert item["instrument_id"] == 1
        assert item["symbol"] == "AAPL"
        assert item["coverage_tier"] == 1
        assert item["latest_quote"]["bid"] == 185.50
        assert item["latest_quote"]["ask"] == 185.60

    def test_empty_table_returns_empty_list(self) -> None:
        _with_conn([[{"cnt": 0}], []])
        resp = client.get("/instruments")

        assert resp.status_code == 200
        body = resp.json()
        assert body["items"] == []
        assert body["total"] == 0

    def test_missing_quote_returns_null(self) -> None:
        row = _make_instrument_row(bid=None, ask=None, last=None, spread_pct=None, quoted_at=None)
        _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments")

        assert resp.status_code == 200
        assert resp.json()["items"][0]["latest_quote"] is None

    def test_missing_coverage_returns_null(self) -> None:
        row = _make_instrument_row(coverage_tier=None)
        _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments")

        assert resp.status_code == 200
        assert resp.json()["items"][0]["coverage_tier"] is None

    def test_partial_quote_row_returns_null(self) -> None:
        """quoted_at present but bid None → latest_quote should be null, not crash."""
        row = _make_instrument_row(bid=None, ask=None, last=None, spread_pct=None, quoted_at=_NOW)
        _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments")

        assert resp.status_code == 200
        assert resp.json()["items"][0]["latest_quote"] is None

    def test_filter_by_search(self) -> None:
        row = _make_instrument_row(symbol="AAPL", company_name="Apple Inc")
        conn = _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments", params={"search": "AAP"})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        # Both count and items queries should receive search params
        count_params = cur.execute.call_args_list[0][0][1]
        assert count_params["search_prefix"] == "AAP%"
        assert count_params["search_contains"] == "%AAP%"
        items_params = cur.execute.call_args_list[1][0][1]
        assert items_params["search_prefix"] == "AAP%"
        assert items_params["search_contains"] == "%AAP%"

    def test_search_whitespace_only_ignored(self) -> None:
        """A search string of only whitespace should be treated as no search."""
        conn = _with_conn([[{"cnt": 0}], []])
        resp = client.get("/instruments", params={"search": "   "})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert "search_prefix" not in count_params

    def test_search_max_length_rejected(self) -> None:
        resp = client.get("/instruments", params={"search": "a" * 101})
        assert resp.status_code == 422

    def test_filter_by_sector(self) -> None:
        row = _make_instrument_row(sector="Technology")
        conn = _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments", params={"sector": "Technology"})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        first_execute_params = cur.execute.call_args_list[0][0][1]
        assert first_execute_params["sector"] == "Technology"

    def test_filter_by_coverage_tier(self) -> None:
        row = _make_instrument_row(coverage_tier=1)
        conn = _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments", params={"coverage_tier": 1})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        first_execute_params = cur.execute.call_args_list[0][0][1]
        assert first_execute_params["coverage_tier"] == 1

    def test_filter_by_exchange(self) -> None:
        row = _make_instrument_row(exchange="NASDAQ")
        conn = _with_conn([[{"cnt": 1}], [row]])
        resp = client.get("/instruments", params={"exchange": "NASDAQ"})

        assert resp.status_code == 200
        cur = conn.cursor.return_value
        first_execute_params = cur.execute.call_args_list[0][0][1]
        assert first_execute_params["exchange"] == "NASDAQ"

    def test_pagination_offset_and_limit(self) -> None:
        row = _make_instrument_row()
        conn = _with_conn([[{"cnt": 100}], [row]])
        resp = client.get("/instruments", params={"offset": 10, "limit": 25})

        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 100
        assert body["offset"] == 10
        assert body["limit"] == 25

        cur = conn.cursor.return_value
        items_params = cur.execute.call_args_list[1][0][1]
        assert items_params["offset"] == 10
        assert items_params["limit"] == 25

    def test_limit_capped_at_max(self) -> None:
        """limit > MAX_PAGE_LIMIT is rejected by FastAPI validation."""
        resp = client.get("/instruments", params={"limit": 999})
        assert resp.status_code == 422

    def test_negative_offset_rejected(self) -> None:
        resp = client.get("/instruments", params={"offset": -1})
        assert resp.status_code == 422

    def test_zero_limit_rejected(self) -> None:
        resp = client.get("/instruments", params={"limit": 0})
        assert resp.status_code == 422

    def test_invalid_coverage_tier_rejected(self) -> None:
        resp = client.get("/instruments", params={"coverage_tier": 5})
        assert resp.status_code == 422

    def test_coverage_tier_zero_rejected(self) -> None:
        resp = client.get("/instruments", params={"coverage_tier": 0})
        assert resp.status_code == 422

    def test_count_query_receives_only_filter_params(self) -> None:
        """COUNT query must not receive limit/offset — only filter keys."""
        conn = _with_conn([[{"cnt": 5}], []])
        client.get("/instruments", params={"sector": "Tech", "offset": 10, "limit": 25})

        cur = conn.cursor.return_value
        count_params = cur.execute.call_args_list[0][0][1]
        assert "limit" not in count_params
        assert "offset" not in count_params
        assert count_params["sector"] == "Tech"

    def test_search_count_query_includes_where_clause(self) -> None:
        """COUNT query must include the ILIKE search filter when search is active."""
        conn = _with_conn([[{"cnt": 0}], []])
        client.get("/instruments", params={"search": "App"})

        cur = conn.cursor.return_value
        count_sql: str = cur.execute.call_args_list[0][0][0]
        assert "ILIKE" in count_sql

    def test_count_query_omits_coverage_join_when_no_tier_filter(self) -> None:
        """When coverage_tier filter is not active, COUNT query should not join coverage."""
        conn = _with_conn([[{"cnt": 0}], []])
        client.get("/instruments")

        cur = conn.cursor.return_value
        count_sql: str = cur.execute.call_args_list[0][0][0]
        assert "coverage" not in count_sql.lower()

    def test_count_query_joins_coverage_when_tier_filter_active(self) -> None:
        """When coverage_tier filter is active, COUNT query must join coverage."""
        conn = _with_conn([[{"cnt": 0}], []])
        client.get("/instruments", params={"coverage_tier": 1})

        cur = conn.cursor.return_value
        count_sql: str = cur.execute.call_args_list[0][0][0]
        assert "coverage" in count_sql.lower()


# ---------------------------------------------------------------------------
# TestGetInstrumentDetail
# ---------------------------------------------------------------------------


class TestGetInstrumentDetail:
    """GET /instruments/{instrument_id} — single instrument detail."""

    def teardown_method(self) -> None:
        _cleanup()

    def test_happy_path_full_detail(self) -> None:
        row = _make_instrument_row()
        ext_ids = [
            _make_ext_id_row("fmp", "symbol", "AAPL"),
            _make_ext_id_row("sec", "cik", "0000320193"),
        ]
        _with_conn([[row], ext_ids])
        resp = client.get("/instruments/1")

        assert resp.status_code == 200
        body = resp.json()
        assert body["instrument_id"] == 1
        assert body["symbol"] == "AAPL"
        assert body["industry"] == "Consumer Electronics"
        assert body["country"] == "US"
        assert body["coverage_tier"] == 1
        assert body["latest_quote"]["bid"] == 185.50
        assert len(body["external_identifiers"]) == 2
        assert body["external_identifiers"][0]["provider"] == "fmp"
        assert body["external_identifiers"][1]["provider"] == "sec"

    def test_not_found_returns_404(self) -> None:
        # Only one result set needed — 404 is raised before the identifiers query.
        _with_conn([[]])
        resp = client.get("/instruments/999")

        assert resp.status_code == 404
        assert "999" in resp.json()["detail"]

    def test_no_external_identifiers(self) -> None:
        row = _make_instrument_row()
        _with_conn([[row], []])
        resp = client.get("/instruments/1")

        assert resp.status_code == 200
        assert resp.json()["external_identifiers"] == []

    def test_no_quote_returns_null(self) -> None:
        row = _make_instrument_row(bid=None, ask=None, last=None, spread_pct=None, quoted_at=None)
        _with_conn([[row], []])
        resp = client.get("/instruments/1")

        assert resp.status_code == 200
        assert resp.json()["latest_quote"] is None

    def test_partial_quote_row_returns_null(self) -> None:
        """quoted_at present but bid/ask None → latest_quote should be null, not crash."""
        row = _make_instrument_row(bid=None, ask=None, last=None, spread_pct=None, quoted_at=_NOW)
        _with_conn([[row], []])
        resp = client.get("/instruments/1")

        assert resp.status_code == 200
        assert resp.json()["latest_quote"] is None

    def test_no_coverage_returns_null(self) -> None:
        row = _make_instrument_row(coverage_tier=None)
        _with_conn([[row], []])
        resp = client.get("/instruments/1")

        assert resp.status_code == 200
        assert resp.json()["coverage_tier"] is None

    def test_external_identifiers_ordered_deterministically(self) -> None:
        """Identifiers are ordered by provider, identifier_type, identifier_value."""
        row = _make_instrument_row()
        ext_ids = [
            _make_ext_id_row("fmp", "symbol", "AAPL"),
            _make_ext_id_row("sec", "cik", "0000320193"),
            _make_ext_id_row("sec", "ticker", "AAPL"),
        ]
        conn = _with_conn([[row], ext_ids])
        client.get("/instruments/1")

        cur = conn.cursor.return_value
        identifiers_sql: str = cur.execute.call_args_list[1][0][0]
        assert "ORDER BY provider, identifier_type, identifier_value" in identifiers_sql
