"""Tests for app.services.entry_timing — TA-informed entry condition evaluation.

Structure:
  - TestConditionEvaluators — individual TA condition checks
  - TestStopLossComputation — ATR-based SL with floor/ceiling clamps
  - TestTakeProfitComputation — base_value TP guard
  - TestEvaluateEntryConditions — full evaluate_entry_conditions with mocked DB
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from app.services.entry_timing import (
    RSI_OVERBOUGHT,
    _compute_stop_loss,
    _compute_take_profit,
    _eval_bollinger,
    _eval_macd,
    _eval_rsi,
    _eval_trend,
    evaluate_entry_conditions,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor_sequence: list[MagicMock]) -> MagicMock:
    conn = MagicMock()
    conn.cursor.side_effect = cursor_sequence
    conn.execute.return_value = MagicMock()
    return conn


def _rec_row(
    recommendation_id: int = 1,
    instrument_id: int = 42,
    action: str = "BUY",
    target_entry: float | None = 150.0,
    status: str = "proposed",
    base_value: float | None = 200.0,
    buy_zone_low: float | None = 140.0,
    buy_zone_high: float | None = 160.0,
    confidence_score: float | None = 0.8,
    suggested_size_pct: float | None = 0.05,
) -> dict[str, Any]:
    return {
        "recommendation_id": recommendation_id,
        "instrument_id": instrument_id,
        "action": action,
        "target_entry": target_entry,
        "suggested_size_pct": suggested_size_pct,
        "status": status,
        "base_value": base_value,
        "buy_zone_low": buy_zone_low,
        "buy_zone_high": buy_zone_high,
        "confidence_score": confidence_score,
    }


def _ta_row(
    close: float = 155.0,
    sma_200: float | None = 145.0,
    rsi_14: float | None = 55.0,
    macd_histogram: float | None = 0.5,
    bb_upper: float | None = 165.0,
    bb_lower: float | None = 135.0,
    atr_14: float | None = 3.5,
) -> dict[str, Any]:
    return {
        "close": close,
        "sma_200": sma_200,
        "rsi_14": rsi_14,
        "macd_histogram": macd_histogram,
        "bb_upper": bb_upper,
        "bb_lower": bb_lower,
        "atr_14": atr_14,
    }


# ---------------------------------------------------------------------------
# TestConditionEvaluators
# ---------------------------------------------------------------------------


class TestConditionEvaluators:
    """Individual TA condition checks."""

    def test_rsi_null_is_neutral(self) -> None:
        desc, ok = _eval_rsi(None)
        assert ok is True
        assert "neutral" in desc

    def test_rsi_below_threshold_is_ok(self) -> None:
        desc, ok = _eval_rsi(50.0)
        assert ok is True
        assert "ok" in desc

    def test_rsi_at_threshold_is_ok(self) -> None:
        desc, ok = _eval_rsi(RSI_OVERBOUGHT)
        assert ok is True

    def test_rsi_above_threshold_defers(self) -> None:
        desc, ok = _eval_rsi(80.0)
        assert ok is False
        assert "overbought" in desc

    def test_macd_null_is_neutral(self) -> None:
        desc, ok = _eval_macd(None)
        assert ok is True
        assert "neutral" in desc

    def test_macd_positive_is_favorable(self) -> None:
        desc, ok = _eval_macd(0.5)
        assert ok is True
        assert "favorable" in desc

    def test_macd_zero_is_favorable(self) -> None:
        desc, ok = _eval_macd(0.0)
        assert ok is True

    def test_macd_negative_defers(self) -> None:
        desc, ok = _eval_macd(-0.5)
        assert ok is False
        assert "weak momentum" in desc

    def test_bollinger_null_is_neutral(self) -> None:
        desc, ok = _eval_bollinger(155.0, None, None)
        assert ok is True
        assert "neutral" in desc

    def test_bollinger_within_band_is_ok(self) -> None:
        desc, ok = _eval_bollinger(150.0, bb_upper=165.0, bb_lower=135.0)
        assert ok is True
        assert "ok" in desc

    def test_bollinger_near_upper_defers(self) -> None:
        # position = (164 - 135) / (165 - 135) = 0.966 > 0.95
        desc, ok = _eval_bollinger(164.0, bb_upper=165.0, bb_lower=135.0)
        assert ok is False
        assert "overextended" in desc

    def test_bollinger_zero_width_is_neutral(self) -> None:
        desc, ok = _eval_bollinger(150.0, bb_upper=150.0, bb_lower=150.0)
        assert ok is True
        assert "neutral" in desc

    def test_trend_null_sma_is_neutral(self) -> None:
        desc, ok = _eval_trend(155.0, None)
        assert ok is True
        assert "neutral" in desc

    def test_trend_above_sma_favorable(self) -> None:
        desc, ok = _eval_trend(155.0, 145.0)
        assert ok is True
        assert "favorable" in desc

    def test_trend_below_sma_still_passes(self) -> None:
        # Below SMA-200 is informational, not a hard defer.
        desc, ok = _eval_trend(140.0, 145.0)
        assert ok is True
        assert "below trend" in desc


# ---------------------------------------------------------------------------
# TestStopLossComputation
# ---------------------------------------------------------------------------


class TestStopLossComputation:
    """ATR-based SL with floor and minimum distance clamps."""

    def test_normal_atr_stop_loss(self) -> None:
        # entry=150, ATR=3.5 => SL = 150 - 2*3.5 = 143
        # Floor = 150 * 0.95 = 142.5 => max(143, 142.5) = 143
        # MinDist = 150 * 0.98 = 147 => min(143, 147) = 143
        sl = _compute_stop_loss(Decimal("150"), Decimal("3.5"))
        assert sl == Decimal("143.0")

    def test_low_price_high_atr_uses_floor(self) -> None:
        # entry=3, ATR=2 => SL = 3 - 2*2 = -1
        # Floor = 3 * 0.95 = 2.85 => max(-1, 2.85) = 2.85
        # MinDist = 3 * 0.98 = 2.94 => min(2.85, 2.94) = 2.85
        sl = _compute_stop_loss(Decimal("3"), Decimal("2"))
        assert sl == Decimal("2.85")

    def test_low_volatility_uses_min_distance(self) -> None:
        # entry=150, ATR=0.5 => SL = 150 - 2*0.5 = 149
        # Floor = 150 * 0.95 = 142.5 => max(149, 142.5) = 149
        # MinDist = 150 * 0.98 = 147 => min(149, 147) = 147
        sl = _compute_stop_loss(Decimal("150"), Decimal("0.5"))
        assert sl == Decimal("147.0")

    def test_null_atr_uses_floor(self) -> None:
        sl = _compute_stop_loss(Decimal("100"), None)
        assert sl == Decimal("95.0")  # 100 * 0.95

    def test_zero_atr_uses_floor(self) -> None:
        sl = _compute_stop_loss(Decimal("100"), Decimal("0"))
        assert sl == Decimal("95.0")

    def test_sl_always_positive(self) -> None:
        # Even with extreme ATR, floor prevents negative SL.
        sl = _compute_stop_loss(Decimal("1"), Decimal("100"))
        assert sl > 0


# ---------------------------------------------------------------------------
# TestTakeProfitComputation
# ---------------------------------------------------------------------------


class TestTakeProfitComputation:
    """Take-profit from thesis base_value."""

    def test_base_value_above_entry(self) -> None:
        tp = _compute_take_profit(Decimal("150"), Decimal("200"))
        assert tp == Decimal("200")

    def test_base_value_at_entry_returns_none(self) -> None:
        tp = _compute_take_profit(Decimal("150"), Decimal("150"))
        assert tp is None

    def test_base_value_below_entry_returns_none(self) -> None:
        tp = _compute_take_profit(Decimal("150"), Decimal("100"))
        assert tp is None

    def test_null_base_value_returns_none(self) -> None:
        tp = _compute_take_profit(Decimal("150"), None)
        assert tp is None


# ---------------------------------------------------------------------------
# TestEvaluateEntryConditions
# ---------------------------------------------------------------------------


class TestEvaluateEntryConditions:
    """Full evaluate_entry_conditions with mocked DB."""

    def test_exit_rec_always_skips(self) -> None:
        rec = _rec_row(action="EXIT")
        conn = _make_conn([_make_cursor([rec])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "skip"
        assert result.stop_loss_rate is None

    def test_hold_rec_always_skips(self) -> None:
        rec = _rec_row(action="HOLD")
        conn = _make_conn([_make_cursor([rec])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "skip"

    def test_missing_rec_returns_skip(self) -> None:
        conn = _make_conn([_make_cursor([])])
        result = evaluate_entry_conditions(conn, 999)
        assert result.verdict == "skip"
        assert "not found" in result.rationale

    def test_all_favorable_passes(self) -> None:
        rec = _rec_row()
        ta = _ta_row()
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "pass"
        assert result.stop_loss_rate is not None
        assert result.take_profit_rate == Decimal("200")

    def test_overbought_rsi_defers(self) -> None:
        rec = _rec_row()
        ta = _ta_row(rsi_14=80.0)
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "defer"
        assert "overbought" in result.rationale

    def test_negative_macd_defers(self) -> None:
        rec = _rec_row()
        ta = _ta_row(macd_histogram=-1.0)
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "defer"
        assert "weak momentum" in result.rationale

    def test_overextended_bollinger_defers(self) -> None:
        rec = _rec_row()
        ta = _ta_row(close=164.5, bb_upper=165.0, bb_lower=135.0)
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "defer"
        assert "overextended" in result.rationale

    def test_all_null_ta_passes(self) -> None:
        """NULL indicators are neutral — should pass, not block."""
        rec = _rec_row()
        ta = _ta_row(
            sma_200=None,
            rsi_14=None,
            macd_histogram=None,
            bb_upper=None,
            bb_lower=None,
            atr_14=None,
        )
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "pass"

    def test_no_ta_row_passes(self) -> None:
        """No price_daily TA row at all — pass through (don't block on missing data)."""
        rec = _rec_row()
        conn = _make_conn([_make_cursor([rec]), _make_cursor([])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "pass"

    def test_null_target_entry_no_sl_tp(self) -> None:
        """If target_entry is NULL, SL/TP should be None."""
        rec = _rec_row(target_entry=None)
        ta = _ta_row()
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.stop_loss_rate is None
        assert result.take_profit_rate is None

    def test_base_value_below_entry_no_tp(self) -> None:
        """If base_value <= entry, TP should be None (guard against immediate trigger)."""
        rec = _rec_row(target_entry=200.0, base_value=180.0)
        ta = _ta_row()
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.take_profit_rate is None
        # SL should still be computed.
        assert result.stop_loss_rate is not None

    def test_sl_tp_computed_even_on_defer(self) -> None:
        """SL/TP should be computed and included even when verdict is defer (audit trail)."""
        rec = _rec_row()
        ta = _ta_row(rsi_14=80.0)  # trigger defer
        conn = _make_conn([_make_cursor([rec]), _make_cursor([ta])])
        result = evaluate_entry_conditions(conn, 1)
        assert result.verdict == "defer"
        assert result.stop_loss_rate is not None
        assert "SL=" in result.rationale
