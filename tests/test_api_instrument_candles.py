"""Tests for GET /instruments/{symbol}/candles (Slice A of #316)."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


class FetchOne:
    """Cursor fixture: returns `row` from `fetchone`. `fetchall` returns []."""

    __slots__ = ("row",)

    def __init__(self, row: object) -> None:
        self.row = row


class FetchAll:
    """Cursor fixture: returns `rows` from `fetchall`. `fetchone` returns None."""

    __slots__ = ("rows",)

    def __init__(self, rows: list[object]) -> None:
        self.rows = rows


def _make_cursor_sequence(
    per_cursor: list[FetchOne | FetchAll],
) -> MagicMock:
    """Per-cursor isolation: each `cursor(...)` call returns a fresh
    mock whose `fetchone`/`fetchall` behaviour is determined by an
    explicit FetchOne / FetchAll tag (avoids the len-based heuristic
    that the Codex slice-A round-2 review flagged as fragile).

    The candles endpoint opens two cursors:
      1. Symbol → instrument_id lookup (FetchOne).
      2. Candle row fetch (FetchAll).
    """
    cursors = iter(per_cursor)

    def _next_cursor(*_args: object, **_kwargs: object) -> MagicMock:
        cur = MagicMock()
        cur.__enter__.return_value = cur
        cur.__exit__.return_value = None
        spec = next(cursors)
        if isinstance(spec, FetchOne):
            cur.fetchone.return_value = spec.row
            cur.fetchall.return_value = []
        else:
            cur.fetchone.return_value = None
            cur.fetchall.return_value = list(spec.rows)
        return cur

    conn = MagicMock()
    conn.cursor.side_effect = _next_cursor
    return conn


def test_candles_unknown_symbol_returns_404(client: TestClient) -> None:
    """No instrument row → 404 before the price_daily fetch ever runs."""
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _make_cursor_sequence([FetchOne(None)])

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/NOTREAL/candles?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 404


def test_candles_empty_symbol_returns_400(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/%20%20/candles?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 400


def test_candles_happy_path_returns_ohlcv_rows(client: TestClient) -> None:
    from datetime import date

    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _make_cursor_sequence(
            [
                FetchOne({"instrument_id": 42, "symbol": "AAPL"}),
                FetchAll(
                    [
                        {
                            "price_date": date(2026, 4, 10),
                            "open": Decimal("180.00"),
                            "high": Decimal("182.50"),
                            "low": Decimal("179.80"),
                            "close": Decimal("181.25"),
                            "volume": Decimal("1000000"),
                        },
                        {
                            "price_date": date(2026, 4, 11),
                            "open": Decimal("181.25"),
                            "high": Decimal("183.00"),
                            "low": Decimal("180.75"),
                            "close": Decimal("182.60"),
                            "volume": Decimal("950000"),
                        },
                    ]
                ),
            ]
        )

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/candles?range=1m")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["range"] == "1m"
    assert body["days"] == 30
    assert len(body["rows"]) == 2
    # Rows are returned in date-ascending order.
    assert body["rows"][0]["date"] == "2026-04-10"
    assert body["rows"][1]["date"] == "2026-04-11"
    assert body["rows"][0]["close"] == "181.25"


def test_candles_max_range_has_null_days(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _make_cursor_sequence(
            [
                FetchOne({"instrument_id": 42, "symbol": "AAPL"}),
                FetchAll([]),
            ]
        )

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/candles?range=max")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["range"] == "max"
    assert body["days"] is None
    assert body["rows"] == []


def test_candles_invalid_range_token_returns_422(client: TestClient) -> None:
    """FastAPI's Literal validation rejects unrecognised range tokens —
    ensures callers can't silently request a wrong lookback."""
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield MagicMock()

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/candles?range=banana")
    finally:
        app.dependency_overrides.pop(get_conn, None)
    assert resp.status_code == 422


def test_candles_default_range_is_1m(client: TestClient) -> None:
    from app.db import get_conn

    def _db_conn() -> Iterator[MagicMock]:
        yield _make_cursor_sequence(
            [
                FetchOne({"instrument_id": 42, "symbol": "AAPL"}),
                FetchAll([]),
            ]
        )

    app.dependency_overrides[get_conn] = _db_conn
    try:
        resp = client.get("/instruments/AAPL/candles")
    finally:
        app.dependency_overrides.pop(get_conn, None)

    assert resp.status_code == 200
    assert resp.json()["range"] == "1m"
    assert resp.json()["days"] == 30
