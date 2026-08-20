"""Recent prospective calibration and drift assessment for opportunity forecasts.

The score is deliberately about forecast honesty, not trading profitability.
Resolved target/stop/timeout paths receive a normalized multiclass Brier score
and a classwise adaptive-bin calibration error. Ambiguous, unresolved and
pending paths remain explicit rates and never acquire invented labels.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Any, Final, Literal

import psycopg

from app.services.price_masked_bars import QUARANTINE_RULE_SET_VERSION
from app.services.strategy_forecast_outcome_resolution import RESOLVER_VERSION
from app.services.strategy_opportunity_forecast import FORECAST_POLICY_VERSION

logger = logging.getLogger(__name__)

OutcomeClass = Literal["target_first", "stop_first", "timeout"]
_CLASSES: Final[tuple[OutcomeClass, ...]] = ("target_first", "stop_first", "timeout")
_METRIC_QUANTUM: Final = Decimal("0.00000001")


class ForecastAssessmentError(ValueError):
    """The proposed policy or evidence is internally inconsistent."""


@dataclass(frozen=True)
class ForecastAssessmentPolicy:
    policy_id: str
    effective_from: datetime
    recent_window_days: int
    minimum_resolved_forecasts: int
    adaptive_calibration_bins: int
    max_normalized_brier_score: Decimal
    min_brier_skill_score: Decimal
    max_classwise_calibration_error: Decimal
    max_ambiguous_rate: Decimal
    max_unresolved_rate: Decimal
    max_pending_rate: Decimal
    max_assessment_age_days: int
    evidence_ref: str


@dataclass(frozen=True)
class ForecastObservation:
    forecast_id: int
    target_probability: Decimal
    stop_probability: Decimal
    timeout_probability: Decimal
    outcome: OutcomeClass | None
    terminal_state: Literal["resolved", "ambiguous", "unresolved", "pending"]
    outcome_id: int | None = None

    def __post_init__(self) -> None:
        probabilities = self.probabilities
        if any(not value.is_finite() or not Decimal("0") <= value <= Decimal("1") for value in probabilities):
            raise ForecastAssessmentError("forecast probabilities must be finite and between zero and one")
        if abs(sum(probabilities, Decimal("0")) - Decimal("1")) > Decimal("0.000001"):
            raise ForecastAssessmentError("forecast probabilities must sum to one")
        if (self.terminal_state == "resolved") != (self.outcome is not None):
            raise ForecastAssessmentError("only resolved observations carry a scored outcome class")

    @property
    def probabilities(self) -> tuple[Decimal, Decimal, Decimal]:
        return self.target_probability, self.stop_probability, self.timeout_probability


@dataclass(frozen=True)
class ForecastScope:
    strategy_id: str
    strategy_version: str
    forecast_policy_version: str
    model_version: str
    calibration_id: str
    setup_version: str
    exit_policy_version: str


@dataclass(frozen=True)
class ForecastAssessment:
    scope: ForecastScope
    window_start: date
    window_end: date
    evidence_hash: str
    total_forecasts: int
    resolved_forecasts: int
    target_first_count: int
    stop_first_count: int
    timeout_count: int
    ambiguous_count: int
    unresolved_count: int
    pending_count: int
    normalized_brier_score: Decimal | None
    baseline_normalized_brier_score: Decimal | None
    brier_skill_score: Decimal | None
    max_classwise_calibration_error: Decimal | None
    ambiguous_rate: Decimal
    unresolved_rate: Decimal
    pending_rate: Decimal
    passed: bool
    reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class ForecastAssessmentRunReport:
    policy_id: str | None
    scopes_selected: int = 0
    evidence_rows_written: int = 0
    current_rows_refreshed: int = 0
    passed_scopes: int = 0


def _validate_policy(policy: ForecastAssessmentPolicy) -> None:
    if not policy.policy_id.strip() or not policy.evidence_ref.strip():
        raise ForecastAssessmentError("policy_id and evidence_ref must be non-empty")
    if policy.effective_from.tzinfo is None:
        raise ForecastAssessmentError("effective_from must be timezone-aware")
    if not 20 <= policy.recent_window_days <= 365:
        raise ForecastAssessmentError("recent_window_days must be between 20 and 365")
    if policy.minimum_resolved_forecasts < 30:
        raise ForecastAssessmentError("minimum_resolved_forecasts must be at least 30")
    if not 2 <= policy.adaptive_calibration_bins <= 20:
        raise ForecastAssessmentError("adaptive_calibration_bins must be between 2 and 20")
    for field, value in (
        ("max_normalized_brier_score", policy.max_normalized_brier_score),
        ("max_classwise_calibration_error", policy.max_classwise_calibration_error),
        ("max_ambiguous_rate", policy.max_ambiguous_rate),
        ("max_unresolved_rate", policy.max_unresolved_rate),
        ("max_pending_rate", policy.max_pending_rate),
    ):
        if not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
            raise ForecastAssessmentError(f"{field} must be finite and between zero and one")
    if not policy.min_brier_skill_score.is_finite() or not Decimal("0") < policy.min_brier_skill_score <= 1:
        raise ForecastAssessmentError("min_brier_skill_score must be finite, positive and at most one")
    if not 1 <= policy.max_assessment_age_days <= 7:
        raise ForecastAssessmentError("max_assessment_age_days must be between one and seven")


def register_assessment_policy(conn: psycopg.Connection[Any], policy: ForecastAssessmentPolicy) -> None:
    """Register immutable thresholds; there is intentionally no seeded default."""
    _validate_policy(policy)
    row = conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_policies (
            policy_id,effective_from,recent_window_days,minimum_resolved_forecasts,
            adaptive_calibration_bins,max_normalized_brier_score,
            min_brier_skill_score,max_classwise_calibration_error,max_ambiguous_rate,max_unresolved_rate,
            max_pending_rate,max_assessment_age_days,evidence_ref
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (policy_id) DO NOTHING RETURNING policy_id
        """,
        (
            policy.policy_id,
            policy.effective_from,
            policy.recent_window_days,
            policy.minimum_resolved_forecasts,
            policy.adaptive_calibration_bins,
            policy.max_normalized_brier_score,
            policy.min_brier_skill_score,
            policy.max_classwise_calibration_error,
            policy.max_ambiguous_rate,
            policy.max_unresolved_rate,
            policy.max_pending_rate,
            policy.max_assessment_age_days,
            policy.evidence_ref,
        ),
    ).fetchone()
    if row is None:
        raise ForecastAssessmentError("policy_id already exists and is immutable")


def _normalized_brier(observations: Sequence[ForecastObservation]) -> Decimal:
    error = Decimal("0")
    for observation in observations:
        assert observation.outcome is not None
        for class_name, probability in zip(_CLASSES, observation.probabilities, strict=True):
            actual = Decimal("1") if class_name == observation.outcome else Decimal("0")
            error += (probability - actual) ** 2
    return error / (Decimal("2") * len(observations))


def _baseline_normalized_brier(observations: Sequence[ForecastObservation]) -> Decimal:
    count = Decimal(len(observations))
    frequencies = tuple(
        Decimal(sum(item.outcome == class_name for item in observations)) / count for class_name in _CLASSES
    )
    error = Decimal("0")
    for observation in observations:
        assert observation.outcome is not None
        for class_name, probability in zip(_CLASSES, frequencies, strict=True):
            actual = Decimal("1") if class_name == observation.outcome else Decimal("0")
            error += (probability - actual) ** 2
    return error / (Decimal("2") * len(observations))


def _adaptive_classwise_calibration_error(observations: Sequence[ForecastObservation], *, bins: int) -> Decimal:
    """Maximum classwise ECE using deterministic equal-frequency bins."""
    class_errors: list[Decimal] = []
    count = len(observations)
    for class_index, class_name in enumerate(_CLASSES):
        ordered = sorted(observations, key=lambda item: (item.probabilities[class_index], item.forecast_id))
        calibration_error = Decimal("0")
        for bin_index in range(min(bins, count)):
            start = bin_index * count // min(bins, count)
            end = (bin_index + 1) * count // min(bins, count)
            bucket = ordered[start:end]
            if not bucket:
                continue
            mean_probability = sum((item.probabilities[class_index] for item in bucket), Decimal("0")) / len(bucket)
            observed_rate = sum((item.outcome == class_name for item in bucket), 0) / Decimal(len(bucket))
            calibration_error += Decimal(len(bucket)) / count * abs(mean_probability - observed_rate)
        class_errors.append(calibration_error)
    return max(class_errors)


def _evidence_hash(observations: Sequence[ForecastObservation]) -> str:
    payload = [
        (
            item.forecast_id,
            item.outcome_id,
            item.terminal_state,
            item.outcome,
            *(str(value) for value in item.probabilities),
        )
        for item in sorted(observations, key=lambda value: value.forecast_id)
    ]
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def calculate_assessment(
    *,
    scope: ForecastScope,
    observations: Sequence[ForecastObservation],
    policy: ForecastAssessmentPolicy,
    window_start: date,
    window_end: date,
) -> ForecastAssessment:
    _validate_policy(policy)
    if window_end < window_start:
        raise ForecastAssessmentError("window_end must not precede window_start")
    resolved = [item for item in observations if item.terminal_state == "resolved"]
    total = len(observations)
    counts = {class_name: sum(item.outcome == class_name for item in resolved) for class_name in _CLASSES}
    ambiguous = sum(item.terminal_state == "ambiguous" for item in observations)
    unresolved = sum(item.terminal_state == "unresolved" for item in observations)
    pending = sum(item.terminal_state == "pending" for item in observations)
    denominator = Decimal(total) if total else Decimal("1")
    ambiguous_rate = (Decimal(ambiguous) / denominator).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN)
    unresolved_rate = (Decimal(unresolved) / denominator).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN)
    pending_rate = (Decimal(pending) / denominator).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN)
    brier = _normalized_brier(resolved).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN) if resolved else None
    baseline_brier = (
        _baseline_normalized_brier(resolved).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN) if resolved else None
    )
    brier_skill = (
        (Decimal("1") - brier / baseline_brier).quantize(_METRIC_QUANTUM, rounding=ROUND_HALF_EVEN)
        if brier is not None and baseline_brier is not None and baseline_brier > 0
        else None
    )
    calibration_error = (
        _adaptive_classwise_calibration_error(resolved, bins=policy.adaptive_calibration_bins).quantize(
            _METRIC_QUANTUM, rounding=ROUND_HALF_EVEN
        )
        if resolved
        else None
    )
    reasons: list[str] = []
    if len(resolved) < policy.minimum_resolved_forecasts:
        reasons.append("insufficient_resolved_forecasts")
    if brier is not None and brier > policy.max_normalized_brier_score:
        reasons.append("normalized_brier_score_high")
    if resolved and brier_skill is None:
        reasons.append("brier_baseline_uninformative")
    elif brier_skill is not None and brier_skill < policy.min_brier_skill_score:
        reasons.append("brier_skill_below_policy")
    if calibration_error is not None and calibration_error > policy.max_classwise_calibration_error:
        reasons.append("classwise_calibration_error_high")
    if ambiguous_rate > policy.max_ambiguous_rate:
        reasons.append("ambiguous_rate_high")
    if unresolved_rate > policy.max_unresolved_rate:
        reasons.append("unresolved_rate_high")
    if pending_rate > policy.max_pending_rate:
        reasons.append("pending_rate_high")
    return ForecastAssessment(
        scope=scope,
        window_start=window_start,
        window_end=window_end,
        evidence_hash=_evidence_hash(observations),
        total_forecasts=total,
        resolved_forecasts=len(resolved),
        target_first_count=counts["target_first"],
        stop_first_count=counts["stop_first"],
        timeout_count=counts["timeout"],
        ambiguous_count=ambiguous,
        unresolved_count=unresolved,
        pending_count=pending,
        normalized_brier_score=brier,
        baseline_normalized_brier_score=baseline_brier,
        brier_skill_score=brier_skill,
        max_classwise_calibration_error=calibration_error,
        ambiguous_rate=ambiguous_rate,
        unresolved_rate=unresolved_rate,
        pending_rate=pending_rate,
        passed=not reasons,
        reason_codes=tuple(reasons),
    )


def _current_policy(conn: psycopg.Connection[Any], *, now: datetime) -> ForecastAssessmentPolicy | None:
    row = conn.execute(
        """
        SELECT policy_id,effective_from,recent_window_days,minimum_resolved_forecasts,
               adaptive_calibration_bins,max_normalized_brier_score,
               min_brier_skill_score,max_classwise_calibration_error,max_ambiguous_rate,max_unresolved_rate,
               max_pending_rate,max_assessment_age_days,evidence_ref
        FROM strategy_forecast_assessment_policies
        WHERE effective_from <= %s
        ORDER BY effective_from DESC LIMIT 1
        """,
        (now,),
    ).fetchone()
    return None if row is None else ForecastAssessmentPolicy(*row)


def _load_scopes(
    conn: psycopg.Connection[Any], *, window_start: date, window_end: date
) -> Mapping[ForecastScope, tuple[ForecastObservation, ...]]:
    rows = conn.execute(
        """
        SELECT f.forecast_id,s.strategy_id,s.strategy_version,f.forecast_policy_version,
               c.model_version,c.calibration_id,f.setup_version,
               f.exit_policy_version,f.target_probability,f.stop_probability,
               f.timeout_probability,o.forecast_outcome_id,o.outcome
        FROM strategy_opportunity_forecasts f
        JOIN strategy_signals s ON s.signal_id=f.signal_id
        JOIN strategy_forecast_calibrations c ON c.calibration_id=f.calibration_id
        LEFT JOIN strategy_opportunity_forecast_outcomes o
          ON o.forecast_id=f.forecast_id
         AND o.resolver_version=%s AND o.input_rule_set_version=%s
        WHERE f.forecast_policy_version=%s
          AND f.decided_at::date BETWEEN %s AND %s
        ORDER BY f.forecast_id
        """,
        (RESOLVER_VERSION, QUARANTINE_RULE_SET_VERSION, FORECAST_POLICY_VERSION, window_start, window_end),
    ).fetchall()
    grouped: dict[ForecastScope, list[ForecastObservation]] = {}
    for row in rows:
        scope = ForecastScope(*(str(row[index]) for index in range(1, 8)))
        stored_outcome = row[12]
        if stored_outcome in _CLASSES:
            terminal_state: Literal["resolved", "ambiguous", "unresolved", "pending"] = "resolved"
            outcome: OutcomeClass | None = stored_outcome
        elif stored_outcome == "ambiguous":
            terminal_state, outcome = "ambiguous", None
        elif stored_outcome == "unresolved":
            terminal_state, outcome = "unresolved", None
        else:
            terminal_state, outcome = "pending", None
        grouped.setdefault(scope, []).append(
            ForecastObservation(
                forecast_id=int(row[0]),
                target_probability=Decimal(row[8]),
                stop_probability=Decimal(row[9]),
                timeout_probability=Decimal(row[10]),
                outcome_id=int(row[11]) if row[11] is not None else None,
                outcome=outcome,
                terminal_state=terminal_state,
            )
        )
    return {scope: tuple(items) for scope, items in grouped.items()}


def _store_assessment(
    conn: psycopg.Connection[Any],
    *,
    policy: ForecastAssessmentPolicy,
    assessment: ForecastAssessment,
    checked_at: datetime,
) -> tuple[int, bool]:
    scope = assessment.scope
    params = (
        policy.policy_id,
        scope.strategy_id,
        scope.strategy_version,
        scope.forecast_policy_version,
        scope.model_version,
        scope.calibration_id,
        scope.setup_version,
        scope.exit_policy_version,
        RESOLVER_VERSION,
        QUARANTINE_RULE_SET_VERSION,
        assessment.window_start,
        assessment.window_end,
        assessment.evidence_hash,
        assessment.total_forecasts,
        assessment.resolved_forecasts,
        assessment.target_first_count,
        assessment.stop_first_count,
        assessment.timeout_count,
        assessment.ambiguous_count,
        assessment.unresolved_count,
        assessment.pending_count,
        assessment.normalized_brier_score,
        assessment.baseline_normalized_brier_score,
        assessment.brier_skill_score,
        assessment.max_classwise_calibration_error,
        assessment.ambiguous_rate,
        assessment.unresolved_rate,
        assessment.pending_rate,
        assessment.passed,
        json.dumps(assessment.reason_codes),
    )
    row = conn.execute(
        """
        INSERT INTO strategy_forecast_assessments (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version,window_start,window_end,evidence_hash,
            total_forecasts,resolved_forecasts,target_first_count,stop_first_count,timeout_count,
            ambiguous_count,unresolved_count,pending_count,normalized_brier_score,
            baseline_normalized_brier_score,brier_skill_score,
            max_classwise_calibration_error,ambiguous_rate,unresolved_rate,pending_rate,passed,reason_codes
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
        ON CONFLICT (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version,evidence_hash
        ) DO NOTHING RETURNING assessment_id
        """,
        params,
    ).fetchone()
    inserted = row is not None
    if row is None:
        row = conn.execute(
            """
            SELECT assessment_id FROM strategy_forecast_assessments
            WHERE policy_id=%s AND strategy_id=%s AND strategy_version=%s
              AND forecast_policy_version=%s AND model_version=%s AND calibration_id=%s
              AND setup_version=%s AND exit_policy_version=%s AND resolver_version=%s
              AND input_rule_set_version=%s AND evidence_hash=%s
            """,
            (
                policy.policy_id,
                scope.strategy_id,
                scope.strategy_version,
                scope.forecast_policy_version,
                scope.model_version,
                scope.calibration_id,
                scope.setup_version,
                scope.exit_policy_version,
                RESOLVER_VERSION,
                QUARANTINE_RULE_SET_VERSION,
                assessment.evidence_hash,
            ),
        ).fetchone()
        assert row is not None
    assessment_id = int(row[0])
    conn.execute(
        """
        INSERT INTO strategy_forecast_assessment_current (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version,assessment_id,checked_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (
            policy_id,strategy_id,strategy_version,forecast_policy_version,model_version,
            calibration_id,setup_version,exit_policy_version,
            resolver_version,input_rule_set_version
        ) DO UPDATE SET assessment_id=EXCLUDED.assessment_id,checked_at=EXCLUDED.checked_at
        """,
        (
            policy.policy_id,
            scope.strategy_id,
            scope.strategy_version,
            scope.forecast_policy_version,
            scope.model_version,
            scope.calibration_id,
            scope.setup_version,
            scope.exit_policy_version,
            RESOLVER_VERSION,
            QUARANTINE_RULE_SET_VERSION,
            assessment_id,
            checked_at,
        ),
    )
    return assessment_id, inserted


def run_forecast_assessments(
    conn: psycopg.Connection[Any], *, now: datetime | None = None
) -> ForecastAssessmentRunReport:
    if not conn.autocommit:
        raise ForecastAssessmentError("run_forecast_assessments needs an autocommit connection")
    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        raise ForecastAssessmentError("assessment time must be timezone-aware")
    policy = _current_policy(conn, now=observed_at)
    if policy is None:
        return ForecastAssessmentRunReport(policy_id=None)
    window_end = observed_at.date()
    window_start = window_end - timedelta(days=policy.recent_window_days - 1)
    grouped = _load_scopes(conn, window_start=window_start, window_end=window_end)
    written = 0
    passed = 0
    with conn.transaction():
        for scope, observations in sorted(
            grouped.items(),
            key=lambda item: (
                item[0].strategy_id,
                item[0].strategy_version,
                item[0].model_version,
                item[0].calibration_id,
                item[0].setup_version,
                item[0].exit_policy_version,
            ),
        ):
            assessment = calculate_assessment(
                scope=scope,
                observations=observations,
                policy=policy,
                window_start=window_start,
                window_end=window_end,
            )
            _, inserted = _store_assessment(conn, policy=policy, assessment=assessment, checked_at=observed_at)
            written += inserted
            passed += assessment.passed
    report = ForecastAssessmentRunReport(
        policy_id=policy.policy_id,
        scopes_selected=len(grouped),
        evidence_rows_written=written,
        current_rows_refreshed=len(grouped),
        passed_scopes=passed,
    )
    logger.info(
        "strategy_forecast_assessment: policy=%s scopes=%d evidence_written=%d refreshed=%d passed=%d",
        report.policy_id,
        report.scopes_selected,
        report.evidence_rows_written,
        report.current_rows_refreshed,
        report.passed_scopes,
    )
    return report


__all__ = [
    "ForecastAssessment",
    "ForecastAssessmentError",
    "ForecastAssessmentPolicy",
    "ForecastAssessmentRunReport",
    "ForecastObservation",
    "ForecastScope",
    "calculate_assessment",
    "register_assessment_policy",
    "run_forecast_assessments",
]
