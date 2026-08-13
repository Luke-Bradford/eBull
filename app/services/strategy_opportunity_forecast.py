"""Immutable, decision-time opportunity forecasts for portfolio allocation.

Forecasts may be negative or uncalibrated so they can remain in shadow.  The
executor separately requires current, passed calibration and positive
conservative expectancy before any capital authority is considered.

Version 1 is deliberately long-only.  Persisting the side makes that execution
boundary auditable and leaves a versioned migration path for future short
support without implying that borrow and carry evidence exists today.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg

FORECAST_POLICY_VERSION = "opportunity-forecast-v1"
_RECONCILIATION_TOLERANCE = Decimal("0.000001")


class OpportunityForecastError(ValueError):
    """The proposed immutable forecast is internally inconsistent."""


@dataclass(frozen=True)
class ForecastCalibration:
    calibration_id: str
    model_version: str
    holdout_start: date
    holdout_end: date
    sample_size: int
    brier_score: Decimal
    calibration_error: Decimal
    passed: bool
    evidence_ref: str


@dataclass(frozen=True)
class OpportunityForecast:
    signal_id: int
    decided_at: datetime
    valid_through: datetime
    horizon_market_days: int
    target_barrier_pct: Decimal
    stop_barrier_pct: Decimal
    setup_version: str
    exit_policy_version: str
    calibration_id: str
    target_probability: Decimal
    stop_probability: Decimal
    timeout_probability: Decimal
    target_net_return_pct: Decimal
    stop_net_return_pct: Decimal
    timeout_net_return_pct: Decimal
    expected_duration_hours: Decimal
    uncertainty_penalty_pct: Decimal
    tail_penalty_pct: Decimal
    correlation_penalty_pct: Decimal
    cost_stress_penalty_pct: Decimal
    conservative_net_expectancy_pct: Decimal
    cost_model_id: str
    forecast_policy_version: str = FORECAST_POLICY_VERSION

    def reconciled_expectancy(self) -> Decimal:
        weighted = (
            self.target_probability * self.target_net_return_pct
            + self.stop_probability * self.stop_net_return_pct
            + self.timeout_probability * self.timeout_net_return_pct
        )
        return weighted - (
            self.uncertainty_penalty_pct
            + self.tail_penalty_pct
            + self.correlation_penalty_pct
            + self.cost_stress_penalty_pct
        )


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise OpportunityForecastError(f"{field} must be non-empty")


def register_forecast_calibration(conn: psycopg.Connection[Any], calibration: ForecastCalibration) -> None:
    """Insert one frozen calibration record; an existing id is immutable."""
    for field, value in (
        ("calibration_id", calibration.calibration_id),
        ("model_version", calibration.model_version),
        ("evidence_ref", calibration.evidence_ref),
    ):
        _require_text(value, field)
    if calibration.holdout_end < calibration.holdout_start:
        raise OpportunityForecastError("calibration holdout_end must not precede holdout_start")
    if calibration.sample_size < 100:
        raise OpportunityForecastError("calibration requires at least 100 holdout observations")
    for field, value in (
        ("brier_score", calibration.brier_score),
        ("calibration_error", calibration.calibration_error),
    ):
        if not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
            raise OpportunityForecastError(f"{field} must be finite and between zero and one")
    row = conn.execute(
        """
            INSERT INTO strategy_forecast_calibrations (
                calibration_id,model_version,holdout_start,holdout_end,sample_size,
                brier_score,calibration_error,passed,evidence_ref
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (calibration_id) DO NOTHING
            RETURNING calibration_id
            """,
        (
            calibration.calibration_id,
            calibration.model_version,
            calibration.holdout_start,
            calibration.holdout_end,
            calibration.sample_size,
            calibration.brier_score,
            calibration.calibration_error,
            calibration.passed,
            calibration.evidence_ref,
        ),
    ).fetchone()
    if row is None:
        raise OpportunityForecastError("calibration_id already exists and is immutable")


def record_opportunity_forecast(conn: psycopg.Connection[Any], forecast: OpportunityForecast) -> int:
    """Validate and insert one forecast for an existing fired entry signal."""
    for field, value in (
        ("forecast_policy_version", forecast.forecast_policy_version),
        ("setup_version", forecast.setup_version),
        ("exit_policy_version", forecast.exit_policy_version),
        ("calibration_id", forecast.calibration_id),
        ("cost_model_id", forecast.cost_model_id),
    ):
        _require_text(value, field)
    if forecast.valid_through < forecast.decided_at:
        raise OpportunityForecastError("forecast validity must not end before its decision time")
    if not 0 < forecast.horizon_market_days <= 60:
        raise OpportunityForecastError("forecast horizon must be between one and 60 market days")
    for field, value, upper, inclusive in (
        ("target_barrier_pct", forecast.target_barrier_pct, Decimal("1000"), True),
        ("stop_barrier_pct", forecast.stop_barrier_pct, Decimal("100"), False),
    ):
        upper_ok = value <= upper if inclusive else value < upper
        if not value.is_finite() or value <= 0 or not upper_ok:
            comparator = "at most" if inclusive else "below"
            raise OpportunityForecastError(f"{field} must be finite, positive and {comparator} {upper}")
    probabilities = (
        forecast.target_probability,
        forecast.stop_probability,
        forecast.timeout_probability,
    )
    if any(not value.is_finite() or value < 0 or value > 1 for value in probabilities):
        raise OpportunityForecastError("forecast probabilities must be finite and between zero and one")
    if abs(sum(probabilities, Decimal("0")) - Decimal("1")) > _RECONCILIATION_TOLERANCE:
        raise OpportunityForecastError("forecast probabilities must sum to one")
    numeric_values = (
        forecast.target_net_return_pct,
        forecast.stop_net_return_pct,
        forecast.timeout_net_return_pct,
        forecast.expected_duration_hours,
        forecast.uncertainty_penalty_pct,
        forecast.tail_penalty_pct,
        forecast.correlation_penalty_pct,
        forecast.cost_stress_penalty_pct,
        forecast.conservative_net_expectancy_pct,
    )
    if any(not value.is_finite() for value in numeric_values):
        raise OpportunityForecastError("forecast numeric values must be finite")
    if forecast.target_net_return_pct <= 0 or forecast.stop_net_return_pct >= 0:
        raise OpportunityForecastError("target return must be positive and stop return negative")
    if forecast.expected_duration_hours <= 0:
        raise OpportunityForecastError("expected duration must be positive")
    if any(
        value < 0
        for value in (
            forecast.uncertainty_penalty_pct,
            forecast.tail_penalty_pct,
            forecast.correlation_penalty_pct,
            forecast.cost_stress_penalty_pct,
        )
    ):
        raise OpportunityForecastError("forecast penalties cannot be negative")
    if abs(forecast.conservative_net_expectancy_pct - forecast.reconciled_expectancy()) > _RECONCILIATION_TOLERANCE:
        raise OpportunityForecastError("conservative expectancy does not reconcile")
    knowledge = conn.execute(
        """
        SELECT s.signal_kind,s.verdict,
               (
                   SELECT c.holdout_end
                   FROM strategy_forecast_calibrations c
                   WHERE c.calibration_id=%s
               ) AS calibration_holdout_end
        FROM strategy_signals s
        WHERE s.signal_id=%s
        """,
        (forecast.calibration_id, forecast.signal_id),
    ).fetchone()
    if knowledge is None or knowledge[2] is None:
        raise OpportunityForecastError("signal or calibration evidence is missing")
    if knowledge[0] != "entry" or knowledge[1] != "fired":
        raise OpportunityForecastError("only fired entry signals may have opportunity forecasts")
    if knowledge[2] >= forecast.decided_at.date():
        raise OpportunityForecastError("calibration holdout must end before the forecast decision")
    row = conn.execute(
        """
            INSERT INTO strategy_opportunity_forecasts (
                signal_id,forecast_policy_version,decided_at,valid_through,side,
                horizon_market_days,target_barrier_pct,stop_barrier_pct,
                setup_version,exit_policy_version,calibration_id,
                target_probability,stop_probability,timeout_probability,
                target_net_return_pct,stop_net_return_pct,timeout_net_return_pct,
                expected_duration_hours,uncertainty_penalty_pct,tail_penalty_pct,
                correlation_penalty_pct,cost_stress_penalty_pct,
                conservative_net_expectancy_pct,cost_model_id
            ) VALUES (%s,%s,%s,%s,'long',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (signal_id) DO NOTHING
            RETURNING forecast_id
            """,
        (
            forecast.signal_id,
            forecast.forecast_policy_version,
            forecast.decided_at,
            forecast.valid_through,
            forecast.horizon_market_days,
            forecast.target_barrier_pct,
            forecast.stop_barrier_pct,
            forecast.setup_version,
            forecast.exit_policy_version,
            forecast.calibration_id,
            forecast.target_probability,
            forecast.stop_probability,
            forecast.timeout_probability,
            forecast.target_net_return_pct,
            forecast.stop_net_return_pct,
            forecast.timeout_net_return_pct,
            forecast.expected_duration_hours,
            forecast.uncertainty_penalty_pct,
            forecast.tail_penalty_pct,
            forecast.correlation_penalty_pct,
            forecast.cost_stress_penalty_pct,
            forecast.conservative_net_expectancy_pct,
            forecast.cost_model_id,
        ),
    ).fetchone()
    if row is None:
        raise OpportunityForecastError("signal already has an immutable opportunity forecast")
    return int(row[0])


__all__ = [
    "FORECAST_POLICY_VERSION",
    "ForecastCalibration",
    "OpportunityForecast",
    "OpportunityForecastError",
    "record_opportunity_forecast",
    "register_forecast_calibration",
]
