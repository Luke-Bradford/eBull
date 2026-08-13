"""Unit boundaries for immutable opportunity-forecast mathematics."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_opportunity_forecast import (
    ForecastCalibration,
    OpportunityForecast,
    OpportunityForecastError,
    record_opportunity_forecast,
    register_forecast_calibration,
)


def _forecast() -> OpportunityForecast:
    decided_at = datetime(2026, 8, 7, 15, tzinfo=UTC)
    return OpportunityForecast(
        signal_id=1,
        decided_at=decided_at,
        valid_through=decided_at + timedelta(days=5),
        horizon_market_days=5,
        target_barrier_pct=Decimal("4"),
        stop_barrier_pct=Decimal("2"),
        setup_version="setup-v1",
        exit_policy_version="exit-v1",
        calibration_id="calibration-v1",
        target_probability=Decimal("0.60"),
        stop_probability=Decimal("0.25"),
        timeout_probability=Decimal("0.15"),
        target_net_return_pct=Decimal("4"),
        stop_net_return_pct=Decimal("-2"),
        timeout_net_return_pct=Decimal("0"),
        expected_duration_hours=Decimal("24"),
        uncertainty_penalty_pct=Decimal("0.20"),
        tail_penalty_pct=Decimal("0.10"),
        correlation_penalty_pct=Decimal("0.10"),
        cost_stress_penalty_pct=Decimal("0.10"),
        conservative_net_expectancy_pct=Decimal("1.40"),
        cost_model_id=COST_MODEL_ID,
    )


def test_probability_sum_is_rejected_before_database_access() -> None:
    conn = MagicMock(spec=psycopg.Connection[Any])
    forecast = replace(_forecast(), timeout_probability=Decimal("0.14"))

    with pytest.raises(OpportunityForecastError, match="sum to one"):
        record_opportunity_forecast(conn, forecast)

    conn.execute.assert_not_called()


def test_conservative_expectancy_must_reconcile_before_database_access() -> None:
    conn = MagicMock(spec=psycopg.Connection[Any])
    forecast = replace(_forecast(), conservative_net_expectancy_pct=Decimal("1.41"))

    with pytest.raises(OpportunityForecastError, match="does not reconcile"):
        record_opportunity_forecast(conn, forecast)

    conn.execute.assert_not_called()


def test_barrier_geometry_is_required_before_database_access() -> None:
    conn = MagicMock(spec=psycopg.Connection[Any])
    forecast = replace(_forecast(), stop_barrier_pct=Decimal("0"))

    with pytest.raises(OpportunityForecastError, match="stop_barrier_pct"):
        record_opportunity_forecast(conn, forecast)

    conn.execute.assert_not_called()


def test_calibration_requires_a_meaningful_holdout_sample() -> None:
    conn = MagicMock(spec=psycopg.Connection[Any])
    calibration = ForecastCalibration(
        calibration_id="calibration-v1",
        model_version="model-v1",
        holdout_start=date(2026, 1, 1),
        holdout_end=date(2026, 7, 31),
        sample_size=99,
        brier_score=Decimal("0.18"),
        calibration_error=Decimal("0.04"),
        passed=False,
        evidence_ref="test evidence",
    )

    with pytest.raises(OpportunityForecastError, match="at least 100"):
        register_forecast_calibration(conn, calibration)

    conn.execute.assert_not_called()
