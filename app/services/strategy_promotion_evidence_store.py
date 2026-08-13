"""Persistence for #2505's single bounded aggregate evidence record."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal, DecimalException
from typing import Any, cast

import psycopg

from app.services.strategy_promotion_evidence import (
    ChallengerEvidence,
    ChallengerRole,
    ContrastDimension,
    CostInput,
    ExpectedValueBucket,
    OutcomeContrast,
    PromotionEvidence,
    RecentYearEvidence,
)

MAX_PAYLOAD_BYTES = 65_536


def _payload(evidence: PromotionEvidence) -> dict[str, object]:
    return {
        "after_cost_expectancy_ci_low_pct": str(evidence.after_cost_expectancy_ci_low_pct),
        "ambiguous_path_count": evidence.ambiguous_path_count,
        "capacity_usd": str(evidence.capacity_usd),
        "causal_observation_rule_version": evidence.causal_observation_rule_version,
        "cost_observed_on": evidence.cost_observed_on.isoformat(),
        "cost_source_version": evidence.cost_source_version,
        "cost_valid_through": evidence.cost_valid_through.isoformat(),
        "challengers": [
            {
                "candidate_minus_challenger_pct": str(item.candidate_minus_challenger_pct),
                "causal_observation_rule_version": item.causal_observation_rule_version,
                "expectancy_pct": str(item.expectancy_pct),
                "fill_rule_version": item.fill_rule_version,
                "observation_count": item.observation_count,
                "role": item.role,
                "same_observations_and_fills": item.same_observations_and_fills,
                "overlap_rule_version": item.overlap_rule_version,
            }
            for item in evidence.challengers
        ],
        "ev_buckets": [
            {
                "forecast_ev_pct": str(item.forecast_ev_pct),
                "observation_count": item.observation_count,
                "rank": item.rank,
                "realised_expectancy_pct": str(item.realised_expectancy_pct),
            }
            for item in evidence.ev_buckets
        ],
        "evidence_version": evidence.evidence_version,
        "excluding_best_1_expectancy_pct": str(evidence.excluding_best_1_expectancy_pct),
        "expected_shortfall_5_pct": str(evidence.expected_shortfall_5_pct),
        "financing_bps_per_day": str(evidence.financing_bps_per_day),
        "flat_outcome_count": evidence.flat_outcome_count,
        "fill_rule_version": evidence.fill_rule_version,
        "fx_bps": str(evidence.fx_bps),
        "max_concurrency": evidence.max_concurrency,
        "max_drawdown_pct": str(evidence.max_drawdown_pct),
        "max_date_contribution_pct": str(evidence.max_date_contribution_pct),
        "max_name_contribution_pct": str(evidence.max_name_contribution_pct),
        "max_sector_contribution_pct": str(evidence.max_sector_contribution_pct),
        "observed_cost_inputs": sorted(evidence.observed_cost_inputs),
        "outcome_count": evidence.outcome_count,
        "overlap_rule_version": evidence.overlap_rule_version,
        "outcome_contrasts": [
            {
                "dimension": item.dimension,
                "losing_count": item.losing_count,
                "losing_mean": str(item.losing_mean),
                "profitable_count": item.profitable_count,
                "profitable_mean": str(item.profitable_mean),
                "profitable_minus_losing": str(item.profitable_minus_losing),
            }
            for item in evidence.outcome_contrasts
        ],
        "path_diagnostics_complete": evidence.path_diagnostics_complete,
        "broker_eligible": evidence.broker_eligible,
        "profitable_outcome_count": evidence.profitable_outcome_count,
        "losing_outcome_count": evidence.losing_outcome_count,
        "probability_calibration_passed": evidence.probability_calibration_passed,
        "recent_year_stable": evidence.recent_year_stable,
        "recent_year_evidence": [
            {
                "after_cost_expectancy_pct": str(item.after_cost_expectancy_pct),
                "expectancy_ci_low_pct": str(item.expectancy_ci_low_pct),
                "observation_count": item.observation_count,
                "risk_limits_passed": item.risk_limits_passed,
                "year": item.year,
            }
            for item in evidence.recent_year_evidence
        ],
        "recent_years_evaluated": evidence.recent_years_evaluated,
        "risk_limits_passed": evidence.risk_limits_passed,
        "risk_limits_version": evidence.risk_limits_version,
        "slippage_bps": str(evidence.slippage_bps),
        "spread_bps": str(evidence.spread_bps),
        "stop_first_count": evidence.stop_first_count,
        "target_first_count": evidence.target_first_count,
        "timeout_count": evidence.timeout_count,
        "worst_gap_pct": str(evidence.worst_gap_pct),
    }


def _canonical(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _jsonb_text_size(payload: dict[str, object]) -> int:
    """Bytes PostgreSQL's ``jsonb::text`` representation will occupy.

    JSONB adds one space after each comma and colon and emits UTF-8 rather than
    ASCII escape sequences. Key order cannot alter the byte count. Keeping this
    separate from the compact canonical hash prevents the writer accepting a
    payload which the database's identical 64 KiB policy then rejects.
    """
    return len(json.dumps(payload, sort_keys=True, separators=(", ", ": "), ensure_ascii=False).encode())


def evidence_sha256(evidence: PromotionEvidence) -> str:
    return hashlib.sha256(_canonical(_payload(evidence))).hexdigest()


def store_promotion_evidence(
    conn: psycopg.Connection[Any],
    *,
    result_id: int,
    evidence: PromotionEvidence,
) -> None:
    """Append one record; duplicates and later mutations are refused by SQL."""
    if result_id < 1:
        raise ValueError("result_id must be positive")
    payload = _payload(evidence)
    encoded = _canonical(payload)
    if _jsonb_text_size(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(f"promotion evidence payload exceeds {MAX_PAYLOAD_BYTES} bytes")
    conn.execute(
        """
        INSERT INTO strategy_promotion_evidence (
            result_id, evidence_version, payload_sha256, evidence_payload
        ) VALUES (%s, %s, %s, %s::jsonb)
        """,
        (result_id, evidence.evidence_version, hashlib.sha256(encoded).hexdigest(), encoded.decode()),
    )


def _text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise ValueError(f"promotion evidence {key} must be a string")
    return value


def _boolean(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if type(value) is not bool:
        raise ValueError(f"promotion evidence {key} must be a boolean")
    return value


def _integer(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if type(value) is not int:
        raise ValueError(f"promotion evidence {key} must be an integer")
    return value


def _object_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"promotion evidence {key} must be an array of objects")
    return value


def _string_list(payload: dict[str, Any], key: str) -> list[str]:
    value = payload.get(key)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"promotion evidence {key} must be an array of strings")
    return value


def _evidence_from_payload(payload: dict[str, Any]) -> PromotionEvidence:
    challengers = _object_list(payload, "challengers")
    ev_buckets = _object_list(payload, "ev_buckets")
    outcome_contrasts = _object_list(payload, "outcome_contrasts")
    recent_year_evidence = _object_list(payload, "recent_year_evidence")
    return PromotionEvidence(
        evidence_version=_text(payload, "evidence_version"),
        causal_observation_rule_version=_text(payload, "causal_observation_rule_version"),
        fill_rule_version=_text(payload, "fill_rule_version"),
        overlap_rule_version=_text(payload, "overlap_rule_version"),
        after_cost_expectancy_ci_low_pct=Decimal(_text(payload, "after_cost_expectancy_ci_low_pct")),
        max_drawdown_pct=Decimal(_text(payload, "max_drawdown_pct")),
        expected_shortfall_5_pct=Decimal(_text(payload, "expected_shortfall_5_pct")),
        worst_gap_pct=Decimal(_text(payload, "worst_gap_pct")),
        excluding_best_1_expectancy_pct=Decimal(_text(payload, "excluding_best_1_expectancy_pct")),
        recent_year_stable=_boolean(payload, "recent_year_stable"),
        recent_years_evaluated=_integer(payload, "recent_years_evaluated"),
        recent_year_evidence=tuple(
            RecentYearEvidence(
                year=_integer(item, "year"),
                observation_count=_integer(item, "observation_count"),
                after_cost_expectancy_pct=Decimal(_text(item, "after_cost_expectancy_pct")),
                expectancy_ci_low_pct=Decimal(_text(item, "expectancy_ci_low_pct")),
                risk_limits_passed=_boolean(item, "risk_limits_passed"),
            )
            for item in recent_year_evidence
        ),
        max_date_contribution_pct=Decimal(_text(payload, "max_date_contribution_pct")),
        max_name_contribution_pct=Decimal(_text(payload, "max_name_contribution_pct")),
        max_sector_contribution_pct=Decimal(_text(payload, "max_sector_contribution_pct")),
        max_concurrency=_integer(payload, "max_concurrency"),
        capacity_usd=Decimal(_text(payload, "capacity_usd")),
        risk_limits_version=_text(payload, "risk_limits_version"),
        risk_limits_passed=_boolean(payload, "risk_limits_passed"),
        probability_calibration_passed=_boolean(payload, "probability_calibration_passed"),
        path_diagnostics_complete=_boolean(payload, "path_diagnostics_complete"),
        outcome_count=_integer(payload, "outcome_count"),
        profitable_outcome_count=_integer(payload, "profitable_outcome_count"),
        losing_outcome_count=_integer(payload, "losing_outcome_count"),
        flat_outcome_count=_integer(payload, "flat_outcome_count"),
        target_first_count=_integer(payload, "target_first_count"),
        stop_first_count=_integer(payload, "stop_first_count"),
        timeout_count=_integer(payload, "timeout_count"),
        ambiguous_path_count=_integer(payload, "ambiguous_path_count"),
        observed_cost_inputs=frozenset(cast(CostInput, item) for item in _string_list(payload, "observed_cost_inputs")),
        cost_observed_on=date.fromisoformat(_text(payload, "cost_observed_on")),
        cost_valid_through=date.fromisoformat(_text(payload, "cost_valid_through")),
        cost_source_version=_text(payload, "cost_source_version"),
        spread_bps=Decimal(_text(payload, "spread_bps")),
        slippage_bps=Decimal(_text(payload, "slippage_bps")),
        financing_bps_per_day=Decimal(_text(payload, "financing_bps_per_day")),
        fx_bps=Decimal(_text(payload, "fx_bps")),
        broker_eligible=_boolean(payload, "broker_eligible"),
        challengers=tuple(
            ChallengerEvidence(
                role=cast(ChallengerRole, _text(item, "role")),
                observation_count=_integer(item, "observation_count"),
                expectancy_pct=Decimal(_text(item, "expectancy_pct")),
                candidate_minus_challenger_pct=Decimal(_text(item, "candidate_minus_challenger_pct")),
                same_observations_and_fills=_boolean(item, "same_observations_and_fills"),
                causal_observation_rule_version=_text(item, "causal_observation_rule_version"),
                fill_rule_version=_text(item, "fill_rule_version"),
                overlap_rule_version=_text(item, "overlap_rule_version"),
            )
            for item in challengers
        ),
        ev_buckets=tuple(
            ExpectedValueBucket(
                rank=_integer(item, "rank"),
                observation_count=_integer(item, "observation_count"),
                forecast_ev_pct=Decimal(_text(item, "forecast_ev_pct")),
                realised_expectancy_pct=Decimal(_text(item, "realised_expectancy_pct")),
            )
            for item in ev_buckets
        ),
        outcome_contrasts=tuple(
            OutcomeContrast(
                dimension=cast(ContrastDimension, _text(item, "dimension")),
                profitable_count=_integer(item, "profitable_count"),
                losing_count=_integer(item, "losing_count"),
                profitable_mean=Decimal(_text(item, "profitable_mean")),
                losing_mean=Decimal(_text(item, "losing_mean")),
                profitable_minus_losing=Decimal(_text(item, "profitable_minus_losing")),
            )
            for item in outcome_contrasts
        ),
    )


def load_promotion_evidence(conn: psycopg.Connection[Any], result_id: int) -> PromotionEvidence | None:
    row = conn.execute(
        """
        SELECT evidence_version, payload_sha256, evidence_payload
        FROM strategy_promotion_evidence
        WHERE result_id = %s
        """,
        (result_id,),
    ).fetchone()
    if row is None:
        return None
    payload = row[2]
    if not isinstance(payload, dict):
        raise RuntimeError(f"promotion evidence for result {result_id} is not an object")
    encoded = _canonical(payload)
    actual_hash = hashlib.sha256(encoded).hexdigest()
    if actual_hash != str(row[1]):
        raise RuntimeError(f"promotion evidence hash mismatch for result {result_id}")
    try:
        evidence = _evidence_from_payload(payload)
    except (DecimalException, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"promotion evidence payload is invalid for result {result_id}: {exc}") from exc
    if evidence.evidence_version != str(row[0]):
        raise RuntimeError(f"promotion evidence version mismatch for result {result_id}")
    return evidence


__all__ = [
    "MAX_PAYLOAD_BYTES",
    "evidence_sha256",
    "load_promotion_evidence",
    "store_promotion_evidence",
]
