"""Tests for app.services.position_monitor — intraday SL/TP/thesis-break detection.

Structure:
  - TestCheckPositionHealth — full check_position_health with mocked DB
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from app.services.position_monitor import (
    EXIT_RED_FLAG_THRESHOLD,
    check_position_health,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    """Build a mock cursor that returns dict rows from fetchall."""
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = rows[0] if rows else None
    cur.fetchall.return_value = rows
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Build a mock connection whose cursor() calls consume cursors in order."""
    conn = MagicMock()
    cursor_iter = iter(cursors)
    conn.cursor.side_effect = lambda **kwargs: next(cursor_iter)
    conn.execute.return_value = MagicMock()
    return conn


def _position_row(
    instrument_id: int = 1,
    symbol: str = "AAPL",
    stop_loss_rate: float | None = 140.0,
    take_profit_rate: float | None = 200.0,
    bid: float | None = 160.0,
    red_flag_score: float | None = 0.20,
) -> dict[str, Any]:
    """Build a synthetic position row matching the query projection."""
    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "stop_loss_rate": stop_loss_rate,
        "take_profit_rate": take_profit_rate,
        "bid": bid,
        "red_flag_score": red_flag_score,
    }


# ---------------------------------------------------------------------------
# TestCheckPositionHealth
# ---------------------------------------------------------------------------


class TestCheckPositionHealth:
    """Full check_position_health with mocked DB."""

    def test_no_open_positions_returns_empty(self) -> None:
        """Empty fetchall → 0 checked, no alerts."""
        conn = _make_conn([_make_cursor([])])
        result = check_position_health(conn)
        assert result.positions_checked == 0
        assert result.alerts == []

    def test_position_below_stop_loss_generates_alert(self) -> None:
        """bid < sl → sl_breach alert."""
        row = _position_row(bid=130.0, stop_loss_rate=140.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        sl_alerts = [a for a in result.alerts if a.alert_type == "sl_breach"]
        assert len(sl_alerts) == 1
        alert = sl_alerts[0]
        assert alert.instrument_id == 1
        assert alert.symbol == "AAPL"
        assert alert.current_bid == Decimal("130.0")
        assert "stop_loss" in alert.detail

    def test_position_above_take_profit_generates_alert(self) -> None:
        """bid >= tp → tp_breach alert."""
        row = _position_row(bid=200.0, take_profit_rate=200.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tp_alerts = [a for a in result.alerts if a.alert_type == "tp_breach"]
        assert len(tp_alerts) == 1
        alert = tp_alerts[0]
        assert alert.current_bid == Decimal("200.0")
        assert "take_profit" in alert.detail

    def test_position_strictly_above_take_profit_generates_alert(self) -> None:
        """bid > tp (strict) also triggers tp_breach."""
        row = _position_row(bid=210.0, take_profit_rate=200.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tp_alerts = [a for a in result.alerts if a.alert_type == "tp_breach"]
        assert len(tp_alerts) == 1

    def test_position_with_high_red_flag_generates_thesis_break_alert(self) -> None:
        """red_flag >= 0.80 → thesis_break alert."""
        row = _position_row(red_flag_score=0.85)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1
        alert = tb_alerts[0]
        assert "red_flag" in alert.detail
        assert "threshold" in alert.detail

    def test_red_flag_exactly_at_threshold_generates_alert(self) -> None:
        """red_flag == EXIT_RED_FLAG_THRESHOLD (0.80) → thesis_break (inclusive boundary)."""
        row = _position_row(red_flag_score=float(EXIT_RED_FLAG_THRESHOLD))
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1

    def test_red_flag_just_below_threshold_no_alert(self) -> None:
        """red_flag < EXIT_RED_FLAG_THRESHOLD → no thesis_break alert."""
        row = _position_row(red_flag_score=0.79)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 0

    def test_healthy_position_generates_no_alert(self) -> None:
        """Price in range, low red flag → no alerts."""
        row = _position_row(
            bid=160.0,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.20,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        assert result.alerts == []

    def test_null_sl_tp_does_not_crash(self) -> None:
        """NULL SL/TP/red_flag → no alerts, no crash."""
        row = _position_row(
            stop_loss_rate=None,
            take_profit_rate=None,
            red_flag_score=None,
            bid=160.0,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert result.positions_checked == 1
        assert result.alerts == []

    def test_null_bid_skips_sl_and_tp_checks(self) -> None:
        """NULL bid → sl_breach and tp_breach checks skipped; no alerts."""
        row = _position_row(
            bid=None,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.10,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        # red_flag is 0.10 < threshold so no thesis_break either
        assert result.alerts == []

    def test_multiple_positions_counted_correctly(self) -> None:
        """Multiple positions → positions_checked equals the row count."""
        rows = [
            _position_row(instrument_id=1, symbol="AAPL"),
            _position_row(instrument_id=2, symbol="TSLA"),
            _position_row(instrument_id=3, symbol="MSFT"),
        ]
        conn = _make_conn([_make_cursor(rows)])
        result = check_position_health(conn)
        assert result.positions_checked == 3

    def test_position_can_trigger_multiple_alert_types(self) -> None:
        """A single position can generate both sl_breach and thesis_break simultaneously."""
        row = _position_row(
            bid=130.0,
            stop_loss_rate=140.0,
            take_profit_rate=200.0,
            red_flag_score=0.90,
        )
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        alert_types = {a.alert_type for a in result.alerts}
        assert "sl_breach" in alert_types
        assert "thesis_break" in alert_types
        # bid=130 < tp=200 so no tp_breach
        assert "tp_breach" not in alert_types

    def test_alert_carries_correct_instrument_id_and_symbol(self) -> None:
        """Alert fields match the source position row."""
        row = _position_row(instrument_id=42, symbol="NVDA", bid=130.0, stop_loss_rate=140.0)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        assert len(result.alerts) == 1
        alert = result.alerts[0]
        assert alert.instrument_id == 42
        assert alert.symbol == "NVDA"

    def test_result_is_frozen_dataclass(self) -> None:
        """MonitorResult and MonitorAlert are frozen (immutable)."""
        conn = _make_conn([_make_cursor([])])
        result = check_position_health(conn)
        import dataclasses

        assert dataclasses.is_dataclass(result)
        # Frozen: assigning to a field raises FrozenInstanceError
        try:
            result.positions_checked = 999  # type: ignore[misc]
            assert False, "Expected FrozenInstanceError"
        except Exception as e:
            assert "FrozenInstanceError" in type(e).__name__ or "cannot assign" in str(e).lower()

    def test_alert_current_bid_is_none_when_bid_null(self) -> None:
        """When bid is NULL but red_flag triggers thesis_break, current_bid on alert is None."""
        row = _position_row(bid=None, red_flag_score=0.90)
        conn = _make_conn([_make_cursor([row])])
        result = check_position_health(conn)
        tb_alerts = [a for a in result.alerts if a.alert_type == "thesis_break"]
        assert len(tb_alerts) == 1
        assert tb_alerts[0].current_bid is None
