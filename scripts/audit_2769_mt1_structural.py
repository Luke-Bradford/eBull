"""Emit the complete MT-1 structural fan without querying a performance table."""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Mapping

import psycopg
import psycopg.rows

from app.config import settings
from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity

_EXPECTED_FAN = {
    ("best_case", "masked"),
    ("best_case", "admitted"),
    ("worst_case", "masked"),
    ("worst_case", "admitted"),
}


def _digest(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()
    ).hexdigest()


def main() -> int:
    mt1 = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            attempt = cur.execute(
                """
                SELECT structural_attempt_id, passed, refusal_code, refusal_detail, assessed_at,
                       metric_axis_rule_version, cardinality(metric_axis_dates) AS metric_axis_dates,
                       metric_axis_start, metric_axis_end, metric_axis_digest,
                       opportunity_set_digest, corpus_version, cost_model_id,
                       trial_register_version, trial_contract_version, book_rule_version,
                       evaluator_version, structural_evidence_sha256, structural_evidence_json
                  FROM strategy_mt1_structural_attempts
                 WHERE mt1_strategy_version = %s AND s8_strategy_version = %s
                """,
                (mt1.version, s8.version),
            ).fetchone()
            if attempt is None:
                sys.stderr.write(json.dumps({"status": "refused", "reason": "structural_attempt_missing"}) + "\n")
                return 2
            cells = list(
                cur.execute(
                    """
                    SELECT ambiguity_arm, quarantine_arm,
                           cardinality(mt1_decision_dates) AS mt1_decision_dates,
                           cardinality(s8_decision_dates) AS s8_decision_dates,
                           mt1_annualised_turnover, s8_annualised_turnover,
                           mt1_traded_notional, s8_traded_notional, exposure_reconciled,
                           evidence_sha256, evidence_json
                      FROM strategy_mt1_structural_cells
                     WHERE structural_attempt_id = %s
                     ORDER BY ambiguity_arm, quarantine_arm
                    """,
                    (attempt["structural_attempt_id"],),
                ).fetchall()
            )

    differences: list[str] = []
    keys = {(str(row["ambiguity_arm"]), str(row["quarantine_arm"])) for row in cells}
    if bool(attempt["passed"]) and keys != _EXPECTED_FAN:
        differences.append("structural_fan_incomplete")
    if not bool(attempt["passed"]) and cells:
        differences.append("refused_attempt_has_cells")
    if _digest(attempt["structural_evidence_json"]) != attempt["structural_evidence_sha256"]:
        differences.append("structural_header_digest_mismatch")
    for row in cells:
        if _digest(row["evidence_json"]) != row["evidence_sha256"]:
            differences.append(f"cell_digest_mismatch:{row['ambiguity_arm']}:{row['quarantine_arm']}")
    output = {
        "differences": sorted(differences),
        "status": "verified" if not differences else "refused",
        "structural_attempt": {
            key: value.isoformat() if hasattr(value, "isoformat") else value
            for key, value in attempt.items()
            if key != "structural_evidence_json"
        },
        "structural_cells": [{key: value for key, value in row.items() if key != "evidence_json"} for row in cells],
    }
    (sys.stdout if not differences else sys.stderr).write(json.dumps(output, sort_keys=True, default=str) + "\n")
    return 0 if not differences else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
