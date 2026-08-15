"""Evidence-bound operator transitions from research registration to paper.

The browser names an action, never a destination stage, result id, assessment
id, or verdict.  This module resolves the complete authoritative evidence set
under the strategy lock and gives ``promote_strategy`` only server-selected
immutable identities.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal, cast

import psycopg
import psycopg.rows

from app.services.backtest_run import BACKTEST_UNIVERSE, RESULT_SCOPE, corpus_version_for
from app.services.cost_model import COST_MODEL_ID
from app.services.equity_curve import BENCHMARK_RULE_ID, SIZING_RULE_ID
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.prereg_contract import declaration_refusals
from app.services.research_price_structure_store import QUARANTINE_RULE_SET_VERSION
from app.services.result_ledger import load_preregistration
from app.services.strategy_ambiguity_policy import AMBIGUITY_RULE_VERSION
from app.services.strategy_control_plane import (
    Promotion,
    Stage,
    StrategyControlError,
    current_stage,
    lock_strategy_control,
    promote_strategy,
    registered_strategy_purpose,
)
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import TOTAL_RETURN_BASIS

PromotionAction = Literal[
    "register_candidate",
    "validate_historical",
    "start_forward_observation",
    "approve_paper",
]

_TARGET_BY_ACTION: Final[dict[PromotionAction, Stage]] = {
    "register_candidate": "research_candidate",
    "validate_historical": "historical_validated",
    "start_forward_observation": "forward_observation",
    "approve_paper": "paper_enabled",
}
_ACTION_BY_STAGE: Final[dict[str | None, PromotionAction]] = {
    None: "register_candidate",
    "research_candidate": "validate_historical",
    "historical_validated": "start_forward_observation",
    "forward_observation": "approve_paper",
}
_ARMS: Final = (
    ("best_case", "masked"),
    ("best_case", "admitted"),
    ("worst_case", "masked"),
    ("worst_case", "admitted"),
)


@dataclass(frozen=True)
class OperatorPromotion:
    promotion: Promotion
    evidence_ref: str | None
    created: bool


@dataclass(frozen=True)
class ProspectiveEvidence:
    assessment_id: int
    evidence_ref: str


@dataclass(frozen=True)
class ForwardFloorEvidence:
    declaration_id: int
    resolved_signals: int
    decision_dates: int
    elapsed_days: int
    assessed_at: datetime
    evidence_ref: str


class OperatorPromotionRefusal(StrategyControlError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def next_promotion_action(stage: str | None) -> PromotionAction | None:
    return _ACTION_BY_STAGE.get(stage)


def _canonical_ref(kind: str, payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return f"{kind}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _load_complete_result_bundle(
    conn: psycopg.Connection[Any], *, strategy_id: str, strategy_version: str
) -> tuple[tuple[int, ...], str]:
    window_starts = [item.window.start for item in RECENT_EVIDENCE_WINDOWS.values()]
    window_ends = [item.window.end for item in RECENT_EVIDENCE_WINDOWS.values()]
    rows = conn.execute(
        """
        SELECT result_id,result_version,window_start,window_end,evidence_window_id,
               ambiguity_arm,quarantine_arm
        FROM strategy_results_store
        WHERE strategy_id=%(strategy_id)s AND strategy_version=%(strategy_version)s
          AND result_scope=%(result_scope)s
          AND namespace='hold_out'
          AND corpus_version=%(corpus_version)s
          AND cost_model_id=%(cost_model_id)s
          AND sizing_rule=%(sizing_rule)s
          AND benchmark_rule=%(benchmark_rule)s
          AND return_basis=%(return_basis)s
          AND ambiguity_rule_version=%(ambiguity_rule_version)s
          AND position_rule_set_version=%(position_version)s
          AND outcome_rule_set_version=%(outcome_version)s
          AND input_rule_set_version=%(input_version)s
          AND (window_start,window_end) IN (
              SELECT * FROM unnest(%(window_starts)s::date[], %(window_ends)s::date[])
          )
        ORDER BY window_start,window_end,evidence_window_id,ambiguity_arm,quarantine_arm,result_id
        """,
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "result_scope": RESULT_SCOPE,
            "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
            "cost_model_id": COST_MODEL_ID,
            "sizing_rule": SIZING_RULE_ID,
            "benchmark_rule": BENCHMARK_RULE_ID,
            "return_basis": TOTAL_RETURN_BASIS,
            "ambiguity_rule_version": AMBIGUITY_RULE_VERSION,
            "position_version": POSITION_RULE_SET_VERSION,
            "outcome_version": OUTCOME_RULE_SET_VERSION,
            "input_version": QUARANTINE_RULE_SET_VERSION,
            "window_starts": window_starts,
            "window_ends": window_ends,
        },
    ).fetchall()
    expected = {
        (item.window.start, item.window.end, item.window_id, ambiguity, quarantine)
        for item in RECENT_EVIDENCE_WINDOWS.values()
        for ambiguity, quarantine in _ARMS
    }
    observed = [(row[2], row[3], row[4], str(row[5]), str(row[6])) for row in rows]
    counts = {key: observed.count(key) for key in set(observed)}
    missing = sorted(expected - set(observed), key=str)
    duplicate = sorted((key for key, count in counts.items() if count != 1), key=str)
    unexpected = sorted(set(observed) - expected, key=str)
    if missing or duplicate or unexpected or len(rows) != len(expected):
        raise StrategyControlError(
            "authoritative recent evidence denominator is incomplete or ambiguous "
            f"(expected={len(expected)}, found={len(rows)}, missing={len(missing)}, "
            f"duplicate={len(duplicate)}, unexpected={len(unexpected)})"
        )
    payload = [
        {
            "result_id": int(row[0]),
            "result_version": str(row[1]),
            "result_scope": RESULT_SCOPE,
            "window_start": row[2].isoformat(),
            "window_end": row[3].isoformat(),
            "evidence_window_id": str(row[4]),
            "ambiguity_arm": str(row[5]),
            "quarantine_arm": str(row[6]),
        }
        for row in rows
    ]
    return tuple(item["result_id"] for item in payload), _canonical_ref("strategy-result-bundle", payload)


def _require_preserved_result_bundle(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    stage: Literal["historical_validated", "forward_observation", "paper_enabled"],
    result_ids: tuple[int, ...],
    evidence_ref: str,
) -> None:
    """Prove a later transition still names the exact earlier denominator."""
    rows = conn.execute(
        """
        SELECT p.promotion_id,p.evidence_ref,pr.result_id
        FROM strategy_promotions p
        LEFT JOIN strategy_promotion_results pr ON pr.promotion_id=p.promotion_id
        WHERE p.strategy_id=%s AND p.strategy_version=%s AND p.to_stage=%s
        ORDER BY p.promotion_id,pr.result_id
        """,
        (strategy_id, strategy_version, stage),
    ).fetchall()
    if not rows:
        raise StrategyControlError(f"{stage} evidence event is missing")
    promotion_ids = {int(row[0]) for row in rows}
    if len(promotion_ids) != 1:
        raise StrategyControlError(f"{stage} evidence event is ambiguous")
    pinned_ids = tuple(int(row[2]) for row in rows if row[2] is not None)
    if pinned_ids != tuple(sorted(result_ids)):
        raise StrategyControlError(f"{stage} does not preserve the exact current historical evidence bundle")
    # Paper approval has a composite evidence reference that includes the
    # forward floor and prospective assessment. Earlier stages must retain the
    # canonical result-bundle reference itself.
    if stage != "paper_enabled" and str(rows[0][1]) != evidence_ref:
        raise StrategyControlError(f"{stage} evidence reference does not match its pinned result bundle")


def load_forward_floor_evidence(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    now: datetime,
) -> ForwardFloorEvidence:
    frozen = load_preregistration(conn, strategy_id, strategy_version)
    if frozen is None:
        raise OperatorPromotionRefusal(
            "preregistration_declaration_missing",
            "paper approval requires a frozen preregistration declaration",
        )
    if not frozen.digest_intact:
        raise OperatorPromotionRefusal(
            "declaration_digest_mismatch",
            "paper approval declaration digest does not match its stored payload",
        )
    refusals = declaration_refusals(frozen.declaration)
    if refusals:
        raise OperatorPromotionRefusal(
            "declaration_no_longer_coherent",
            f"paper approval declaration is incoherent: {', '.join(refusals)}",
        )
    if frozen.declaration.prereg_purpose != "capital_candidate":
        raise OperatorPromotionRefusal(
            "declaration_not_capital_candidate",
            "paper approval requires a capital-candidate declaration",
        )
    stage_row = conn.execute(
        """
        SELECT promoted_at
        FROM strategy_promotions
        WHERE strategy_id=%s AND strategy_version=%s AND to_stage='forward_observation'
        """,
        (strategy_id, strategy_version),
    ).fetchone()
    if stage_row is None:
        raise OperatorPromotionRefusal("forward_observation_missing", "forward-observation audit event is missing")
    forward_started_at = cast(datetime, stage_row[0])
    observed_at = now.astimezone(UTC)
    if forward_started_at > observed_at:
        raise OperatorPromotionRefusal(
            "forward_observation_future_dated",
            "forward-observation audit event is future-dated",
        )
    evidence_row = conn.execute(
        """
        SELECT count(*) AS resolved_signals,
               count(DISTINCT s.signal_bar_date) AS decision_dates
        FROM strategy_signals s
        JOIN strategy_outcomes o
          ON o.signal_id=s.signal_id
         AND o.rule_set_version=%(outcome_version)s
         AND o.input_rule_set_version=%(input_version)s
        WHERE s.strategy_id=%(strategy_id)s AND s.strategy_version=%(strategy_version)s
          AND s.signal_kind='entry' AND s.verdict='fired'
          AND o.gross_return_pct IS NOT NULL
          AND s.created_at >= %(forward_started_at)s
          AND s.created_at <= %(observed_at)s
          AND s.signal_bar_date >= %(forward_date)s
        """,
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "outcome_version": OUTCOME_RULE_SET_VERSION,
            "input_version": QUARANTINE_RULE_SET_VERSION,
            "forward_started_at": forward_started_at,
            "observed_at": observed_at,
            "forward_date": forward_started_at.date(),
        },
    ).fetchone()
    assert evidence_row is not None
    resolved_signals = int(evidence_row[0])
    decision_dates = int(evidence_row[1])
    elapsed_days = max((observed_at - forward_started_at).days, 0)
    floor = frozen.declaration.forward_shadow
    if decision_dates < floor.min_independent_decision_dates:
        raise OperatorPromotionRefusal(
            "forward_decision_dates_insufficient",
            "forward independent decision-date floor is not satisfied",
        )
    if elapsed_days < 7 * floor.min_calendar_weeks:
        raise OperatorPromotionRefusal(
            "forward_calendar_weeks_insufficient",
            "forward calendar-week floor is not satisfied",
        )
    payload = {
        "declaration_id": frozen.declaration_id,
        "declaration_sha256": frozen.declaration_sha256,
        "resolved_signals": resolved_signals,
        "decision_dates": decision_dates,
        "elapsed_days": elapsed_days,
        "assessed_at": observed_at.isoformat(),
    }
    return ForwardFloorEvidence(
        declaration_id=frozen.declaration_id,
        resolved_signals=resolved_signals,
        decision_dates=decision_dates,
        elapsed_days=elapsed_days,
        assessed_at=observed_at,
        evidence_ref=_canonical_ref("strategy-forward-floor", payload),
    )


def _load_current_prospective_evidence(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    now: datetime,
) -> ProspectiveEvidence:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT a.assessment_id,a.evidence_hash,a.passed,a.reason_codes,
                   a.window_start,a.window_end,c.checked_at,p.policy_id,p.evidence_ref,
                   p.max_assessment_age_days,forward_stage.promoted_at AS forward_started_at
            FROM strategy_forecast_assessment_policies p
            JOIN strategy_forecast_assessment_current c ON c.policy_id=p.policy_id
            JOIN strategy_forecast_assessments a
              ON a.assessment_id=c.assessment_id
             AND a.policy_id=c.policy_id
             AND a.strategy_id=c.strategy_id
             AND a.strategy_version=c.strategy_version
             AND a.forecast_policy_version=c.forecast_policy_version
             AND a.model_version=c.model_version
             AND a.calibration_id=c.calibration_id
             AND a.setup_version=c.setup_version
             AND a.exit_policy_version=c.exit_policy_version
             AND a.resolver_version=c.resolver_version
             AND a.input_rule_set_version=c.input_rule_set_version
            JOIN strategy_promotions forward_stage
              ON forward_stage.strategy_id=c.strategy_id
             AND forward_stage.strategy_version=c.strategy_version
             AND forward_stage.to_stage='forward_observation'
            WHERE p.policy_id=(
                SELECT policy_id FROM strategy_forecast_assessment_policies
                WHERE effective_from <= %(now)s ORDER BY effective_from DESC LIMIT 1
            )
              AND c.strategy_id=%(strategy_id)s AND c.strategy_version=%(strategy_version)s
            ORDER BY a.assessment_id
            """,
            {"now": now, "strategy_id": strategy_id, "strategy_version": strategy_version},
        )
        rows = list(cur.fetchall())
    if not rows:
        raise StrategyControlError("current prospective assessment is missing")
    if len(rows) != 1:
        raise StrategyControlError("current prospective assessment scope is ambiguous")
    row = rows[0]
    if not bool(row["passed"]):
        raise StrategyControlError("current prospective assessment did not pass")
    checked_at = cast(datetime, row["checked_at"])
    forward_started_at = cast(datetime, row["forward_started_at"])
    if cast(date, row["window_start"]) <= forward_started_at.date():
        raise StrategyControlError("current prospective assessment includes pre-forward observations")
    if cast(date, row["window_end"]) > checked_at.date():
        raise StrategyControlError("current prospective assessment window ends after it was checked")
    if checked_at <= forward_started_at:
        raise StrategyControlError("current prospective assessment predates forward observation")
    if checked_at > now + timedelta(seconds=5):
        raise StrategyControlError("current prospective assessment is future-dated")
    if checked_at < now - timedelta(days=int(row["max_assessment_age_days"])):
        raise StrategyControlError("current prospective assessment is stale")
    assessment_id = int(row["assessment_id"])
    return ProspectiveEvidence(
        assessment_id=assessment_id,
        evidence_ref=_canonical_ref(
            "strategy-prospective-assessment",
            {
                "assessment_id": assessment_id,
                "evidence_hash": str(row["evidence_hash"]),
                "policy_id": str(row["policy_id"]),
                "policy_evidence_ref": str(row["evidence_ref"]),
            },
        ),
    )


def advance_strategy_for_operator(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str,
    strategy_version: str,
    action: PromotionAction,
    promoted_by: str,
    reason: str,
    now: datetime | None = None,
) -> OperatorPromotion:
    """Apply one explicit positive transition using only authoritative evidence."""
    for value, field in (
        (strategy_id, "strategy_id"),
        (strategy_version, "strategy_version"),
        (promoted_by, "promoted_by"),
        (reason, "reason"),
    ):
        if not value.strip():
            raise StrategyControlError(f"{field} must be non-empty")
    if registered_strategy_purpose(strategy_id) != "capital_candidate":
        raise StrategyControlError("only registered capital-candidate strategies can advance")
    target = _TARGET_BY_ACTION[action]
    lock_strategy_control(conn, strategy_id, strategy_version)
    stage = current_stage(conn, strategy_id, strategy_version)
    if stage == target:
        current_ids: tuple[int, ...] = ()
        current_ref = ""
        if target in {"historical_validated", "forward_observation", "paper_enabled"}:
            current_ids, current_ref = _load_complete_result_bundle(
                conn, strategy_id=strategy_id, strategy_version=strategy_version
            )
            _require_preserved_result_bundle(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                stage=cast(
                    Literal["historical_validated", "forward_observation", "paper_enabled"],
                    target,
                ),
                result_ids=current_ids,
                evidence_ref=current_ref,
            )
        if target in {"forward_observation", "paper_enabled"}:
            _require_preserved_result_bundle(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                stage="historical_validated",
                result_ids=current_ids,
                evidence_ref=current_ref,
            )
        if target == "paper_enabled":
            _require_preserved_result_bundle(
                conn,
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                stage="forward_observation",
                result_ids=current_ids,
                evidence_ref=current_ref,
            )
        row = conn.execute(
            """
            SELECT promotion_id,from_stage,evidence_ref
            FROM strategy_promotions
            WHERE strategy_id=%s AND strategy_version=%s AND to_stage=%s
            ORDER BY promotion_id DESC LIMIT 1
            """,
            (strategy_id, strategy_version, target),
        ).fetchone()
        assert row is not None
        if target == "paper_enabled":
            link = conn.execute(
                "SELECT assessment_id FROM strategy_promotion_forward_evidence WHERE promotion_id=%s",
                (int(row[0]),),
            ).fetchone()
            if link is None:
                raise StrategyControlError("paper promotion prospective evidence link is missing")
        promotion = Promotion(int(row[0]), strategy_id, strategy_version, cast(Stage | None, row[1]), target)
        return OperatorPromotion(promotion, None if row[2] is None else str(row[2]), False)
    if next_promotion_action(stage) != action:
        raise StrategyControlError(f"action {action!r} is not allowed from stage {stage!r}")

    result_ids: tuple[int, ...] = ()
    result_ref = ""
    evidence_ref: str | None = None
    prospective: ProspectiveEvidence | None = None
    forward_floor: ForwardFloorEvidence | None = None
    replay = False
    if action in {"validate_historical", "start_forward_observation", "approve_paper"}:
        result_ids, result_ref = _load_complete_result_bundle(
            conn, strategy_id=strategy_id, strategy_version=strategy_version
        )
        evidence_ref = result_ref
    if action in {"start_forward_observation", "approve_paper"}:
        _require_preserved_result_bundle(
            conn,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            stage="historical_validated",
            result_ids=result_ids,
            evidence_ref=result_ref,
        )
    if action == "approve_paper":
        _require_preserved_result_bundle(
            conn,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            stage="forward_observation",
            result_ids=result_ids,
            evidence_ref=result_ref,
        )
        observed_at = (now or datetime.now(UTC)).astimezone(UTC)
        forward_floor = load_forward_floor_evidence(
            conn,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            now=observed_at,
        )
        prospective = _load_current_prospective_evidence(
            conn,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            now=observed_at,
        )
        evidence_ref = _canonical_ref(
            "strategy-paper-approval",
            {
                "historical_bundle": evidence_ref,
                "forward_floor": forward_floor.evidence_ref,
                "prospective_assessment": prospective.evidence_ref,
            },
        )
        replay = True
    promotion = promote_strategy(
        conn,
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        to_stage=target,
        promoted_by=promoted_by,
        reason=reason,
        evidence_ref=evidence_ref,
        result_ids=result_ids,
        replay_result_evidence=replay,
    )
    if prospective is not None and forward_floor is not None:
        conn.execute(
            """
            INSERT INTO strategy_promotion_forward_evidence (
                promotion_id,strategy_id,strategy_version,declaration_id,assessment_id,forward_resolved_signals,
                forward_decision_dates,forward_elapsed_days,assessed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                promotion.promotion_id,
                strategy_id,
                strategy_version,
                forward_floor.declaration_id,
                prospective.assessment_id,
                forward_floor.resolved_signals,
                forward_floor.decision_dates,
                forward_floor.elapsed_days,
                forward_floor.assessed_at,
            ),
        )
    return OperatorPromotion(promotion, evidence_ref, True)


__all__ = [
    "OperatorPromotion",
    "ForwardFloorEvidence",
    "OperatorPromotionRefusal",
    "ProspectiveEvidence",
    "PromotionAction",
    "advance_strategy_for_operator",
    "load_forward_floor_evidence",
    "next_promotion_action",
]
