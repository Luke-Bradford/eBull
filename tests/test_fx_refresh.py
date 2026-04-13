"""Tests for live FX rate refresh from eToro conversion rates."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

from app.services.fx import upsert_live_fx_rate


def test_upsert_live_fx_rate() -> None:
    conn = MagicMock()
    upsert_live_fx_rate(
        conn,
        from_currency="USD",
        to_currency="GBP",
        rate=Decimal("0.78"),
        quoted_at=datetime(2026, 4, 13, 14, 0, tzinfo=UTC),
    )
    conn.execute.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "ON CONFLICT" in sql
    assert "live_fx_rates" in sql
