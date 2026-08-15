"""Audit one atomic MT-1 invocation without selecting a performance value."""

from __future__ import annotations

import hashlib
import json
import math
import sys
from collections.abc import Mapping
from datetime import date
from typing import cast

import psycopg

from app.config import settings
from app.services.strategy_mt1_read_model import load_mt1_controlled_trial_state

_ARM_PREFIXES = {"mt1_scaled", "mt1_unscaled", "s8_scaled", "s8_unscaled"}
_RISK_KEYS = {"certainty_equivalent", "expected_shortfall_5", "maximum_drawdown"}
_RESULT_KEYS = {
    "bootstrap_block_length",
    "bootstrap_resamples",
    "bootstrap_seed",
    "common_months",
    "evaluator_version",
    "excluded_months_by_arm",
    "historical_statistical_conjuncts_pass",
    "mt1_delta_cer",
    "mt1_delta_interval",
    "mt1_drawdown_improved",
    "mt1_expected_shortfall_improved",
    "mt1_lower_bound_positive",
    "mt1_scaled",
    "mt1_unscaled",
    "primary_difference_in_differences",
    "primary_interval",
    "primary_lower_bound_positive",
    "s8_delta_cer",
    "s8_scaled",
    "s8_unscaled",
}


def _digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    ).hexdigest()


def _finite_number_string(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return math.isfinite(float(value))
    except ValueError:
        return False


def _valid_result_payload(payload: object) -> bool:
    if not isinstance(payload, dict) or set(payload) != _RESULT_KEYS:
        return False
    for role in _ARM_PREFIXES:
        risk = payload.get(role)
        if not isinstance(risk, dict) or set(risk) != _RISK_KEYS:
            return False
        if not all(_finite_number_string(value) for value in risk.values()):
            return False
    intervals = (payload.get("mt1_delta_interval"), payload.get("primary_interval"))
    if any(
        not isinstance(interval, dict)
        or set(interval) != {"low", "high"}
        or not all(_finite_number_string(value) for value in interval.values())
        for interval in intervals
    ):
        return False
    scalars = ("mt1_delta_cer", "s8_delta_cer", "primary_difference_in_differences")
    if not all(_finite_number_string(payload.get(field)) for field in scalars):
        return False
    if any(
        not isinstance(payload.get(field), bool)
        for field in (
            "historical_statistical_conjuncts_pass",
            "mt1_drawdown_improved",
            "mt1_expected_shortfall_improved",
            "mt1_lower_bound_positive",
            "primary_lower_bound_positive",
        )
    ):
        return False
    if any(
        not isinstance(payload.get(field), int) or isinstance(payload.get(field), bool) or int(payload[field]) < 1
        for field in ("bootstrap_block_length", "bootstrap_resamples")
    ):
        return False
    if not isinstance(payload.get("bootstrap_seed"), int) or isinstance(payload.get("bootstrap_seed"), bool):
        return False
    if not isinstance(payload.get("evaluator_version"), str) or not str(payload["evaluator_version"]).strip():
        return False
    exclusions = payload.get("excluded_months_by_arm")
    if (
        not isinstance(exclusions, list)
        or len(exclusions) != 4
        or any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in exclusions)
    ):
        return False
    raw_months = payload.get("common_months")
    if (
        not isinstance(raw_months, list)
        or len(raw_months) < 120
        or not all(isinstance(item, str) for item in raw_months)
    ):
        return False
    try:
        months = tuple(date.fromisoformat(item) for item in raw_months)
    except ValueError:
        return False
    return all(item.day == 1 for item in months) and all(
        current > previous for previous, current in zip(months, months[1:])
    )


def main() -> int:
    with psycopg.connect(settings.database_url) as conn:
        state = load_mt1_controlled_trial_state(conn)
        counts = conn.execute(
            """
            SELECT
                (SELECT count(*) FROM strategy_mt1_structural_attempts
                  WHERE mt1_strategy_version = %s AND s8_strategy_version = %s),
                (SELECT count(*) FROM strategy_mt1_trial_results r
                  JOIN strategy_mt1_structural_attempts a USING (structural_attempt_id)
                 WHERE a.mt1_strategy_version = %s AND a.s8_strategy_version = %s),
                (SELECT count(*) FROM strategy_mt1_trial_result_cells c
                  JOIN strategy_mt1_trial_results r USING (mt1_trial_result_id)
                  JOIN strategy_mt1_structural_attempts a USING (structural_attempt_id)
                 WHERE a.mt1_strategy_version = %s AND a.s8_strategy_version = %s)
            """,
            (
                state.strategy_version,
                state.negative_control_version,
                state.strategy_version,
                state.negative_control_version,
                state.strategy_version,
                state.negative_control_version,
            ),
        ).fetchone()
        assert counts is not None
        expected_role_columns = {f"{prefix}_certainty_equivalent": prefix for prefix in _ARM_PREFIXES}
        role_columns = {
            expected_role_columns[str(row[0])]
            for row in conn.execute(
                """
                SELECT column_name FROM information_schema.columns
                 WHERE table_schema = 'public'
                   AND table_name = 'strategy_mt1_trial_result_cells'
                   AND column_name = ANY(%s)
                """,
                (sorted(expected_role_columns),),
            ).fetchall()
        }
        result_header = None
        result_evidence: list[tuple[object, ...]] = []
        if state.trial_result_id is not None:
            result_header = conn.execute(
                """SELECT historical_conjuncts_pass, evidence_sha256, evidence_json
                     FROM strategy_mt1_trial_results WHERE mt1_trial_result_id = %s""",
                (state.trial_result_id,),
            ).fetchone()
            result_evidence = conn.execute(
                """SELECT ambiguity_arm, quarantine_arm, historical_conjuncts_pass,
                          evidence_sha256, evidence_json
                     FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = %s
                     ORDER BY ambiguity_arm, quarantine_arm""",
                (state.trial_result_id,),
            ).fetchall()

    attempt_count, result_count, result_cell_count = (int(value) for value in counts)
    differences = list(state.integrity_refusals)
    if attempt_count != 1:
        differences.append(f"structural_attempt_count={attempt_count}")
    if state.state == "structural_refused":
        if result_count or result_cell_count:
            differences.append("structural_refusal_exposed_results")
    else:
        if result_count != 1:
            differences.append(f"trial_result_count={result_count}")
        if result_cell_count != 4:
            differences.append(f"result_cell_count={result_cell_count}")
        if role_columns != _ARM_PREFIXES:
            differences.append("four_arm_role_schema_incomplete")
        if result_header is None:
            differences.append("result_header_missing")
        else:
            declared_pass, header_sha, raw_header = result_header
            header = cast(dict[str, object], raw_header) if isinstance(raw_header, dict) else None
            if header is None:
                differences.append("result_header_shape_invalid")
            elif _digest(header) != str(header_sha):
                differences.append("result_header_digest_mismatch")
            if header is not None and set(header) != {"historical_conjuncts_pass", "result_cell_digests"}:
                differences.append("result_header_shape_invalid")
            if header is not None and header.get("historical_conjuncts_pass") is not bool(declared_pass):
                differences.append("result_header_conjunction_mismatch")
            cell_digests = [str(row[3]) for row in result_evidence]
            if header is not None and header.get("result_cell_digests") != cell_digests:
                differences.append("result_header_child_digest_chain_mismatch")
        for ambiguity, quarantine, passed, evidence_sha, raw_payload in result_evidence:
            label = f"{ambiguity}:{quarantine}"
            if not isinstance(raw_payload, dict):
                differences.append(f"result_cell_shape_invalid:{label}")
                continue
            payload = cast(dict[str, object], raw_payload)
            if _digest(payload) != str(evidence_sha):
                differences.append(f"result_cell_digest_mismatch:{label}")
            if not _valid_result_payload(payload):
                differences.append(f"result_cell_shape_invalid:{label}")
            if payload.get("historical_statistical_conjuncts_pass") is not bool(passed):
                differences.append(f"result_cell_conjunction_mismatch:{label}")
    if state.holdout_evaluations:
        differences.append(f"holdout_evaluations={state.holdout_evaluations}")
    if state.holdout_accesses:
        differences.append(f"holdout_accesses={state.holdout_accesses}")
    if state.state in {"not_run", "structural_passed_outcomes_pending", "evidence_inconsistent"}:
        differences.append(f"terminal_state={state.state}")
    output = {
        "arm_roles": result_cell_count * len(role_columns),
        "differences": sorted(set(differences)),
        "holdout_accesses": state.holdout_accesses,
        "holdout_evaluations": state.holdout_evaluations,
        "result_cells": [
            {
                "ambiguity_arm": cell.ambiguity_arm,
                "historical_conjuncts_pass": cell.historical_conjuncts_pass,
                "quarantine_arm": cell.quarantine_arm,
            }
            for cell in state.result_cells
        ],
        "state": state.state,
        "status": "verified" if not differences else "refused",
        "structural_attempts": attempt_count,
        "structural_cells": state.structural_cells,
        "trial_results": result_count,
    }
    (sys.stdout if not differences else sys.stderr).write(json.dumps(output, sort_keys=True) + "\n")
    return 0 if not differences else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
