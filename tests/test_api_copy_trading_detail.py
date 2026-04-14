"""Tests for GET /portfolio/copy-trading/{mirror_id} — mirror detail endpoint (#221).

Test strategy:
  Mock DB via FastAPI dependency override (same pattern as test_api_portfolio).
  The ``get_conn`` dependency is replaced with a mock connection.

Structure:
  - TestMirrorDetail — happy path, 404, position rendering
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.runtime_config import RuntimeConfig

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)

_DEFAULT_CONFIG = RuntimeConfig(
    enable_auto_trading=False,
    enable_live_trading=False,
    display_currency="USD",
    updated_at=_NOW,
    updated_by="test",
    reason="test",
)


def _make_mirror_row(
    mirror_id: int = 1001,
    parent_username: str = "thomaspj",
    active: bool = True,
    initial_investment: float = 10000.0,
    deposit_summary: float = 0.0,
    withdrawal_summary: float = 0.0,
    available_amount: float = 500.0,
    closed_positions_net_profit: float = 100.0,
    started_copy_date: datetime = _NOW,
    closed_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "parent_username": parent_username,
        "mirror_id": mirror_id,
        "active": active,
        "initial_investment": initial_investment,
        "deposit_summary": deposit_summary,
        "withdrawal_summary": withdrawal_summary,
        "available_amount": available_amount,
        "closed_positions_net_profit": closed_positions_net_profit,
        "started_copy_date": started_copy_date,
        "closed_at": closed_at,
    }


def _make_position_row(
    mirror_id: int = 1001,
    position_id: int = 5001,
    instrument_id: int = 42,
    symbol: str | None = "AAPL",
    company_name: str | None = "Apple Inc.",
    is_buy: bool = True,
    units: float = 10.0,
    amount: float = 7000.0,
    open_rate: float = 150.0,
    open_conversion_rate: float = 1.0,
    open_date_time: datetime = _NOW,
    quote_last: float | None = 160.0,
    daily_close: float | None = None,
) -> dict[str, Any]:
    return {
        "mirror_id": mirror_id,
        "position_id": position_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "company_name": company_name,
        "is_buy": is_buy,
        "units": units,
        "amount": amount,
        "open_rate": open_rate,
        "open_conversion_rate": open_conversion_rate,
        "open_date_time": open_date_time,
        "quote_last": quote_last,
        "daily_close": daily_close,
    }


def _mock_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    """Build a mock psycopg.Connection supporting transaction context."""
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

    # Support `with conn.transaction():` context manager.
    tx = MagicMock()
    tx.__enter__ = MagicMock(return_value=tx)
    tx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = tx

    return conn


def _with_conn(cursor_results: list[list[dict[str, Any]]]) -> MagicMock:
    conn = _mock_conn(cursor_results)

    def _override() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _override
    return conn


def _cleanup() -> None:
    app.dependency_overrides[get_conn] = _fallback_conn


def _fallback_conn() -> Iterator[MagicMock]:
    yield _mock_conn([])


app.dependency_overrides.setdefault(get_conn, _fallback_conn)

client = TestClient(app)


# ---------------------------------------------------------------------------
# TestMirrorDetail
# ---------------------------------------------------------------------------


class TestMirrorDetail:
    """GET /portfolio/copy-trading/{mirror_id}."""

    def setup_method(self) -> None:
        self._patch_config = patch(
            "app.api.copy_trading.get_runtime_config",
            return_value=_DEFAULT_CONFIG,
        )
        self._patch_fx_meta = patch(
            "app.api.copy_trading.load_live_fx_rates_with_metadata",
            return_value={},
        )
        self._patch_config.start()
        self._patch_fx_meta.start()

    def teardown_method(self) -> None:
        self._patch_config.stop()
        self._patch_fx_meta.stop()
        _cleanup()

    def test_happy_path(self) -> None:
        """Returns mirror stats and positions for a valid mirror_id."""
        mirror = _make_mirror_row()
        position = _make_position_row()
        _with_conn([[mirror], [position]])

        resp = client.get("/portfolio/copy-trading/1001")
        assert resp.status_code == 200
        body = resp.json()

        assert body["parent_username"] == "thomaspj"
        assert body["mirror"]["mirror_id"] == 1001
        assert body["mirror"]["active"] is True
        assert len(body["mirror"]["positions"]) == 1
        assert body["mirror"]["positions"][0]["symbol"] == "AAPL"

    def test_404_for_unknown_mirror(self) -> None:
        """Returns 404 when mirror_id does not exist."""
        _with_conn([[]])  # empty result for mirror query

        resp = client.get("/portfolio/copy-trading/9999")
        assert resp.status_code == 404
        assert "9999" in resp.json()["detail"]

    def test_empty_positions(self) -> None:
        """Mirror with no positions returns empty positions list."""
        mirror = _make_mirror_row()
        _with_conn([[mirror], []])

        resp = client.get("/portfolio/copy-trading/1001")
        assert resp.status_code == 200
        body = resp.json()

        assert body["mirror"]["positions"] == []
        assert body["mirror"]["position_count"] == 0

    def test_mirror_equity_computed(self) -> None:
        """mirror_equity = available_amount (converted) + sum(position market_values)."""
        mirror = _make_mirror_row(available_amount=500.0)
        # Position: amount=7000, units=10, open_rate=150, quote_last=160
        # MTM: 7000 + 1 * 10 * (160 - 150) * 1.0 = 7100
        position = _make_position_row(
            amount=7000.0,
            units=10.0,
            open_rate=150.0,
            quote_last=160.0,
            open_conversion_rate=1.0,
        )
        _with_conn([[mirror], [position]])

        resp = client.get("/portfolio/copy-trading/1001")
        assert resp.status_code == 200
        body = resp.json()

        # 500 + 7100 = 7600
        assert body["mirror"]["mirror_equity"] == 7600.0
