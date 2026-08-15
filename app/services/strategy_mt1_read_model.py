"""Outcome-minimal operator read model for the MT-1 controlled trial (#2769)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

import psycopg
import psycopg.rows

from app.services.backtest_run import BACKTEST_UNIVERSE
from app.services.cost_model import COST_MODEL_ID
from app.services.research_price_structure_store import QuarantineArm
from app.services.result_ledger import holdout_access_counts
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_result import AmbiguityArm

MT1ControlledTrialState = Literal[
    "not_run",
    "structural_refused",
    "structural_passed_outcomes_pending",
    "historical_conjuncts_failed",
    "historical_conjuncts_passed",
    "evidence_inconsistent",
]

_EXPECTED_FAN = {
    ("best_case", "masked"),
    ("best_case", "admitted"),
    ("worst_case", "masked"),
    ("worst_case", "admitted"),
}


@dataclass(frozen=True)
class MT1ControlledTrialCellState:
    ambiguity_arm: AmbiguityArm
    quarantine_arm: QuarantineArm
    historical_conjuncts_pass: bool


@dataclass(frozen=True)
class MT1ControlledTrialReadModel:
    trial_id: str
    strategy_version: str
    negative_control_id: str
    negative_control_version: str
    state: MT1ControlledTrialState
    structural_attempt_id: int | None
    trial_result_id: int | None
    structural_assessed_at: datetime | None
    evaluated_at: datetime | None
    structural_cells: int
    result_cells: tuple[MT1ControlledTrialCellState, ...]
    historical_conjuncts_pass: bool | None
    refusal_code: str | None
    refusal_detail: str | None
    integrity_refusals: tuple[str, ...]
    holdout_evaluations: int
    holdout_accesses: int
    promotion_authority: Literal[False] = False
    paper_activation_reachable: Literal[False] = False
    live_activation_reachable: Literal[False] = False


def load_mt1_controlled_trial_state(conn: psycopg.Connection[Any]) -> MT1ControlledTrialReadModel:
    """Read state/verdicts only; never expose return values or make mutation reachable."""
    mt1 = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    mt1_exposure = holdout_access_counts(conn, mt1.strategy_id, mt1.version)
    s8_exposure = holdout_access_counts(conn, s8.strategy_id, s8.version)
    holdout_evaluations = mt1_exposure.holdout_evaluations + s8_exposure.holdout_evaluations
    holdout_accesses = mt1_exposure.recorded_accesses + s8_exposure.recorded_accesses

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT a.structural_attempt_id, a.passed, a.refusal_code, a.refusal_detail,
                   a.assessed_at, count(sc.structural_attempt_id) AS structural_cells,
                   r.mt1_trial_result_id, r.historical_conjuncts_pass, r.evaluated_at
              FROM strategy_mt1_structural_attempts a
              LEFT JOIN strategy_mt1_structural_cells sc
                ON sc.structural_attempt_id = a.structural_attempt_id
              LEFT JOIN strategy_mt1_trial_results r
                ON r.structural_attempt_id = a.structural_attempt_id
             WHERE a.mt1_strategy_version = %s AND a.s8_strategy_version = %s
             GROUP BY a.structural_attempt_id, r.mt1_trial_result_id
            """,
            (mt1.version, s8.version),
        )
        attempt = cur.fetchone()
        result_rows: list[dict[str, object]] = []
        if attempt is not None and attempt["mt1_trial_result_id"] is not None:
            cur.execute(
                """
                SELECT ambiguity_arm, quarantine_arm, historical_conjuncts_pass
                  FROM strategy_mt1_trial_result_cells
                 WHERE mt1_trial_result_id = %s
                 ORDER BY ambiguity_arm, quarantine_arm
                """,
                (attempt["mt1_trial_result_id"],),
            )
            result_rows = list(cur.fetchall())

    integrity: list[str] = []
    if holdout_evaluations:
        integrity.append("holdout_evaluations_exist")
    if holdout_accesses:
        integrity.append("holdout_accesses_exist")
    if attempt is None:
        return MT1ControlledTrialReadModel(
            trial_id=mt1.strategy_id,
            strategy_version=mt1.version,
            negative_control_id=s8.strategy_id,
            negative_control_version=s8.version,
            state="evidence_inconsistent" if integrity else "not_run",
            structural_attempt_id=None,
            trial_result_id=None,
            structural_assessed_at=None,
            evaluated_at=None,
            structural_cells=0,
            result_cells=(),
            historical_conjuncts_pass=None,
            refusal_code=None,
            refusal_detail=None,
            integrity_refusals=tuple(integrity),
            holdout_evaluations=holdout_evaluations,
            holdout_accesses=holdout_accesses,
        )

    structural_passed = bool(attempt["passed"])
    structural_cells = int(attempt["structural_cells"])
    result_id = cast(int | None, attempt["mt1_trial_result_id"])
    cells = tuple(
        MT1ControlledTrialCellState(
            ambiguity_arm=cast(AmbiguityArm, row["ambiguity_arm"]),
            quarantine_arm=cast(QuarantineArm, row["quarantine_arm"]),
            historical_conjuncts_pass=bool(row["historical_conjuncts_pass"]),
        )
        for row in result_rows
    )
    keys = {(cell.ambiguity_arm, cell.quarantine_arm) for cell in cells}
    if structural_passed and structural_cells != 4:
        integrity.append("structural_fan_incomplete")
    if not structural_passed and structural_cells:
        integrity.append("refused_attempt_has_structural_cells")
    if result_id is not None and (not structural_passed or keys != _EXPECTED_FAN):
        integrity.append("result_fan_inconsistent")
    declared_conjunction = cast(bool | None, attempt["historical_conjuncts_pass"])
    if result_id is not None and declared_conjunction is not all(cell.historical_conjuncts_pass for cell in cells):
        integrity.append("result_conjunction_inconsistent")

    if integrity:
        state: MT1ControlledTrialState = "evidence_inconsistent"
    elif not structural_passed:
        state = "structural_refused"
    elif result_id is None:
        state = "structural_passed_outcomes_pending"
    elif declared_conjunction:
        state = "historical_conjuncts_passed"
    else:
        state = "historical_conjuncts_failed"
    return MT1ControlledTrialReadModel(
        trial_id=mt1.strategy_id,
        strategy_version=mt1.version,
        negative_control_id=s8.strategy_id,
        negative_control_version=s8.version,
        state=state,
        structural_attempt_id=int(attempt["structural_attempt_id"]),
        trial_result_id=result_id,
        structural_assessed_at=cast(datetime, attempt["assessed_at"]),
        evaluated_at=cast(datetime | None, attempt["evaluated_at"]),
        structural_cells=structural_cells,
        result_cells=cells,
        historical_conjuncts_pass=declared_conjunction,
        refusal_code=cast(str | None, attempt["refusal_code"]),
        refusal_detail=cast(str | None, attempt["refusal_detail"]),
        integrity_refusals=tuple(integrity),
        holdout_evaluations=holdout_evaluations,
        holdout_accesses=holdout_accesses,
    )


__all__ = [
    "MT1ControlledTrialCellState",
    "MT1ControlledTrialReadModel",
    "MT1ControlledTrialState",
    "load_mt1_controlled_trial_state",
]
