"""Tests that EXIT fills trigger return attribution.

Verifies that _maybe_trigger_attribution is called correctly based on
the position's remaining units after an EXIT fill.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.services.return_attribution import AttributionResult

_ORDER_CLIENT = "app.services.order_client"


class TestMaybeTriggerAttribution:
    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution")
    def test_triggers_on_full_close(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """When current_units_after is 0, attribution should run."""
        from app.services.order_client import _maybe_trigger_attribution

        mock_compute.return_value = AttributionResult(
            instrument_id=42,
            hold_start=date(2025, 1, 15),
            hold_end=date(2025, 6, 15),
            hold_days=151,
            gross_return_pct=Decimal("0.20"),
            market_return_pct=Decimal("0.05"),
            sector_return_pct=Decimal("0.08"),
            model_alpha_pct=Decimal("0.12"),
            timing_alpha_pct=Decimal("0"),
            cost_drag_pct=Decimal("0.004"),
            residual_pct=Decimal("-0.004"),
            score_at_entry=Decimal("0.75"),
            score_components={"quality_score": 0.8},
            entry_fill_id=1,
            exit_fill_id=2,
            recommendation_id=50,
        )
        conn = MagicMock()

        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))

        mock_compute.assert_called_once_with(conn, 42)
        mock_persist.assert_called_once()

    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution")
    def test_not_triggered_when_position_open(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """If current_units > 0 after exit, no attribution."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("5"))

        mock_compute.assert_not_called()
        mock_persist.assert_not_called()

    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution", return_value=None)
    def test_none_result_skips_persist(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """If compute_attribution returns None (e.g. missing data), skip persist."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))

        mock_compute.assert_called_once()
        mock_persist.assert_not_called()

    @patch(f"{_ORDER_CLIENT}.compute_attribution", side_effect=Exception("DB error"))
    def test_error_does_not_propagate(
        self,
        mock_compute: MagicMock,
    ) -> None:
        """Attribution failure must not abort the order execution."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        # Should not raise
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))

    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution")
    def test_triggers_on_negative_units(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """Negative units (rounding overshoot) should also trigger attribution."""
        from app.services.order_client import _maybe_trigger_attribution

        mock_compute.return_value = MagicMock(spec=AttributionResult)
        conn = MagicMock()

        _maybe_trigger_attribution(
            conn,
            instrument_id=42,
            current_units_after=Decimal("-0.001"),
        )

        mock_compute.assert_called_once_with(conn, 42)
        mock_persist.assert_called_once()
