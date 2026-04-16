"""Tests for the transaction cost model service."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.services.transaction_cost import (
    CostEstimate,
    TransactionCostConfigCorrupt,
    estimate_cost,
    get_transaction_cost_config,
    load_instrument_cost,
    record_actual_cost,
    record_estimated_cost,
)


class TestGetTransactionCostConfig:
    def test_returns_config_when_row_exists(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = {
            "max_total_cost_bps": Decimal("150"),
            "min_return_vs_cost_ratio": Decimal("3.0"),
            "default_hold_days": 90,
        }
        config = get_transaction_cost_config(conn)
        assert config["max_total_cost_bps"] == Decimal("150")
        assert config["default_hold_days"] == 90

    def test_raises_when_row_missing(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = None
        with pytest.raises(TransactionCostConfigCorrupt):
            get_transaction_cost_config(conn)


class TestEstimateCost:
    def test_spread_only_us_stock(self) -> None:
        """USD instrument with no overnight or FX costs."""
        result = estimate_cost(
            spread_bps=Decimal("50"),
            overnight_rate=Decimal("0"),
            fx_markup_bps=Decimal("0"),
            hold_days=90,
            max_total_cost_bps=Decimal("150"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=None,
        )
        assert result.spread_bps == Decimal("50")
        assert result.total_entry_cost_bps == Decimal("50")
        assert result.total_carry_cost_bps == Decimal("0")
        assert result.total_cost_bps == Decimal("50")
        assert result.is_cost_prohibitive is False

    def test_spread_plus_overnight(self) -> None:
        """CFD-like instrument with overnight fees."""
        result = estimate_cost(
            spread_bps=Decimal("50"),
            overnight_rate=Decimal("1.5"),
            fx_markup_bps=Decimal("0"),
            hold_days=90,
            max_total_cost_bps=Decimal("150"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=None,
        )
        assert result.overnight_bps_per_day == Decimal("1.5")
        assert result.total_carry_cost_bps == Decimal("135")
        assert result.total_cost_bps == Decimal("185")
        assert result.is_cost_prohibitive is True

    def test_spread_plus_fx(self) -> None:
        """Non-USD instrument with FX markup."""
        result = estimate_cost(
            spread_bps=Decimal("30"),
            overnight_rate=Decimal("0"),
            fx_markup_bps=Decimal("50"),
            hold_days=90,
            max_total_cost_bps=Decimal("200"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=None,
        )
        assert result.fx_markup_bps == Decimal("50")
        assert result.total_entry_cost_bps == Decimal("130")
        assert result.total_cost_bps == Decimal("130")
        assert result.is_cost_prohibitive is False

    def test_cost_prohibitive_by_ratio(self) -> None:
        """Cost is below threshold but return/cost ratio is too low."""
        result = estimate_cost(
            spread_bps=Decimal("80"),
            overnight_rate=Decimal("0"),
            fx_markup_bps=Decimal("0"),
            hold_days=90,
            max_total_cost_bps=Decimal("150"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=Decimal("2.0"),
        )
        assert result.is_cost_prohibitive is True
        assert "ratio" in (result.prohibitive_reason or "").lower()

    def test_no_return_estimate_skips_ratio_check(self) -> None:
        """When expected_return_pct is None, only absolute threshold applies."""
        result = estimate_cost(
            spread_bps=Decimal("80"),
            overnight_rate=Decimal("0"),
            fx_markup_bps=Decimal("0"),
            hold_days=90,
            max_total_cost_bps=Decimal("150"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=None,
        )
        assert result.is_cost_prohibitive is False

    def test_zero_spread_passes(self) -> None:
        """Commission-free trade with zero spread (unlikely but safe)."""
        result = estimate_cost(
            spread_bps=Decimal("0"),
            overnight_rate=Decimal("0"),
            fx_markup_bps=Decimal("0"),
            hold_days=90,
            max_total_cost_bps=Decimal("150"),
            min_return_vs_cost_ratio=Decimal("3.0"),
            expected_return_pct=None,
        )
        assert result.total_cost_bps == Decimal("0")
        assert result.is_cost_prohibitive is False


class TestLoadInstrumentCost:
    def test_returns_cost_model_row_when_exists(self) -> None:
        """Active cost_model row is preferred over computed spread."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = {
            "spread_bps": Decimal("45.5"),
            "overnight_rate": Decimal("1.2"),
            "fx_pair": "GBP/USD",
            "fx_markup_bps": Decimal("50"),
        }
        result = load_instrument_cost(conn, instrument_id=123)
        assert result is not None
        assert result["spread_bps"] == Decimal("45.5")
        assert result["overnight_rate"] == Decimal("1.2")
        assert result["fx_pair"] == "GBP/USD"

    def test_returns_none_when_no_cost_model(self) -> None:
        """No cost_model row — caller should fall back to quote spread."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = None
        result = load_instrument_cost(conn, instrument_id=123)
        assert result is None


class TestComputeSpreadFromQuote:
    def test_computes_bps_from_spread_pct(self) -> None:
        """spread_pct is in percent; convert to bps (* 100)."""
        from app.services.transaction_cost import spread_pct_to_bps

        assert spread_pct_to_bps(Decimal("0.45")) == Decimal("45")

    def test_none_returns_none(self) -> None:
        from app.services.transaction_cost import spread_pct_to_bps

        assert spread_pct_to_bps(None) is None


class TestRecordEstimatedCost:
    def test_inserts_cost_record(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        estimate = CostEstimate(
            spread_bps=Decimal("50"),
            overnight_bps_per_day=Decimal("0"),
            fx_markup_bps=Decimal("0"),
            estimated_hold_days=90,
            total_entry_cost_bps=Decimal("50"),
            total_carry_cost_bps=Decimal("0"),
            total_cost_bps=Decimal("50"),
            is_cost_prohibitive=False,
            prohibitive_reason=None,
        )
        record_estimated_cost(
            conn,
            order_id=1,
            recommendation_id=10,
            instrument_id=100,
            estimate=estimate,
        )
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "INSERT INTO trade_cost_record" in sql
        params = cursor.execute.call_args[0][1]
        assert params["estimated_total_bps"] == Decimal("50")


class TestRecordActualCost:
    def test_updates_actual_columns(self) -> None:
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        record_actual_cost(
            conn,
            order_id=1,
            actual_spread_bps=Decimal("48"),
            actual_total_bps=Decimal("48"),
        )
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "UPDATE trade_cost_record" in sql
        params = cursor.execute.call_args[0][1]
        assert params["actual_spread_bps"] == Decimal("48")
