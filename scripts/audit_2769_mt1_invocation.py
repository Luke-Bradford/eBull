"""Audit one atomic MT-1 invocation without selecting a performance value."""

from __future__ import annotations

import json
import sys

import psycopg

from app.config import settings
from app.services.strategy_mt1_read_model import load_mt1_controlled_trial_state

_ARM_PREFIXES = {"mt1_scaled", "mt1_unscaled", "s8_scaled", "s8_unscaled"}


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
