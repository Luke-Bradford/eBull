"""Two-phase durable store for the sealed MT-1 in-sample trial (#2769)."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_mt1_books import BOOK_RULE_VERSION
from app.services.strategy_mt1_runner import (
    MT1HistoricalBundle,
    MT1InSampleEvaluation,
    MT1InSamplePreparation,
    MT1InSampleStructuralRefusal,
    MT1InSampleStructuralRefused,
    MT1PreparedCell,
    ProgressCallback,
    evaluate_mt1_prepared_bundle,
    prepare_mt1_in_sample_evaluation,
)
from app.services.strategy_mt1_trial import TRIAL_CONTRACT_VERSION, TRIAL_EVALUATOR_VERSION, MT1TrialResult
from app.services.strategy_result import METRIC_AXIS_RULE_VERSION, metric_axis_sha256
from app.services.strategy_result_universe import record_sha256
from app.services.trial_register import TRIAL_REGISTER_VERSION


class MT1EvidenceConflict(RuntimeError):
    """An immutable version already owns different controlled-trial evidence."""


@dataclass(frozen=True)
class MT1StoredEvaluation:
    structural_attempt_id: int
    trial_result_id: int
    evaluation: MT1InSampleEvaluation


@dataclass(frozen=True)
class MT1StoredRefusal:
    structural_attempt_id: int
    detail: str


def _canonical(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _number(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("MT-1 evidence contains a non-finite number")
    return repr(value)


def _structural_cell_payload(cell: MT1PreparedCell) -> dict[str, object]:
    mt1 = cell.books.mt1.structural
    s8 = cell.books.s8.structural
    return {
        "ambiguity_arm": cell.ambiguity_arm,
        "book_rule_version": cell.books.rule_version,
        "mt1_annualised_turnover": _number(mt1.annualised_turnover),
        "mt1_decision_dates": [item.isoformat() for item in mt1.decision_dates],
        "mt1_traded_notional": _number(mt1.traded_notional),
        "quarantine_arm": cell.quarantine_arm,
        "s8_annualised_turnover": _number(s8.annualised_turnover),
        "s8_decision_dates": [item.isoformat() for item in s8.decision_dates],
        "s8_traded_notional": _number(s8.traded_notional),
        "exposure_reconciled": mt1.exposure_reconciled and s8.exposure_reconciled,
    }


def _structural_header_payload(preparation: MT1InSamplePreparation) -> dict[str, object]:
    cell_digests = [_digest(_structural_cell_payload(cell)) for cell in preparation.prepared.cells]
    return {
        "book_rule_version": BOOK_RULE_VERSION,
        "corpus_version": preparation.corpus_version,
        "cost_model_id": COST_MODEL_ID,
        "declarations": [
            {
                "declaration_id": authority.declaration_id,
                "declaration_sha256": authority.declaration_sha256,
                "strategy_id": authority.strategy_id,
                "strategy_version": authority.strategy_version,
            }
            for authority in preparation.authorities
        ],
        "evaluator_version": TRIAL_EVALUATOR_VERSION,
        "metric_axis_digest": metric_axis_sha256(preparation.prepared.axis_dates),
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "mt1_source_strategy_version": preparation.mt1_source_strategy_version,
        "mt1_strategy_version": preparation.mt1_strategy_version,
        "opportunity_set_digest": record_sha256(preparation.prepared.opportunity_record),
        "passed": True,
        "s8_control_strategy_version": preparation.s8_control_strategy_version,
        "s8_source_strategy_version": preparation.s8_source_strategy_version,
        "structural_cell_digests": cell_digests,
        "trial_contract_version": TRIAL_CONTRACT_VERSION,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "universe_basis": "survivorship_free",
    }


def _existing_structural_attempt(
    conn: psycopg.Connection[Any],
    preparation: MT1InSamplePreparation,
    *,
    expected_sha: str,
) -> int | None:
    existing = conn.execute(
        """
        SELECT structural_attempt_id, structural_evidence_sha256, structural_evidence_json
          FROM strategy_mt1_structural_attempts
         WHERE mt1_strategy_version = %s AND s8_strategy_version = %s
        """,
        (preparation.mt1_strategy_version, preparation.s8_control_strategy_version),
    ).fetchone()
    if existing is None:
        return None
    stored_sha = str(existing[1])
    stored_payload = dict(existing[2])
    expected_cells = [
        (cell.ambiguity_arm, cell.quarantine_arm, _digest(_structural_cell_payload(cell)))
        for cell in preparation.prepared.cells
    ]
    stored_cells = conn.execute(
        """
        SELECT ambiguity_arm, quarantine_arm, evidence_sha256
          FROM strategy_mt1_structural_cells WHERE structural_attempt_id = %s
         ORDER BY ambiguity_arm, quarantine_arm
        """,
        (int(existing[0]),),
    ).fetchall()
    if (
        stored_sha != expected_sha
        or _digest(stored_payload) != stored_sha
        or [(str(row[0]), str(row[1]), str(row[2])) for row in stored_cells] != expected_cells
    ):
        raise MT1EvidenceConflict("the MT-1 strategy versions already own different structural evidence")
    return int(existing[0])


def store_mt1_structural_preparation(conn: psycopg.Connection[Any], preparation: MT1InSamplePreparation) -> int:
    """Atomically commit the complete structural fan before outcome evaluation."""
    payload = _structural_header_payload(preparation)
    evidence_sha = _digest(payload)
    existing_id = _existing_structural_attempt(conn, preparation, expected_sha=evidence_sha)
    if existing_id is not None:
        conn.commit()
        return existing_id

    mt1_authority, s8_authority = preparation.authorities
    axis = preparation.prepared.axis_dates
    row = conn.execute(
        """
        INSERT INTO strategy_mt1_structural_attempts (
            mt1_declaration_id, s8_declaration_id, mt1_strategy_id, mt1_strategy_version,
            s8_strategy_id, s8_strategy_version, mt1_source_strategy_version,
            s8_source_strategy_version, universe_basis, corpus_version, cost_model_id,
            trial_register_version, trial_contract_version, book_rule_version,
            evaluator_version, metric_axis_rule_version, metric_axis_dates,
            metric_axis_start, metric_axis_end, metric_axis_digest,
            opportunity_set_digest, passed, structural_evidence_sha256, structural_evidence_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'survivorship_free', %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s
        ) RETURNING structural_attempt_id
        """,
        (
            mt1_authority.declaration_id,
            s8_authority.declaration_id,
            mt1_authority.strategy_id,
            preparation.mt1_strategy_version,
            s8_authority.strategy_id,
            preparation.s8_control_strategy_version,
            preparation.mt1_source_strategy_version,
            preparation.s8_source_strategy_version,
            preparation.corpus_version,
            COST_MODEL_ID,
            TRIAL_REGISTER_VERSION,
            TRIAL_CONTRACT_VERSION,
            BOOK_RULE_VERSION,
            TRIAL_EVALUATOR_VERSION,
            METRIC_AXIS_RULE_VERSION,
            list(axis),
            axis[0],
            axis[-1],
            metric_axis_sha256(axis),
            record_sha256(preparation.prepared.opportunity_record),
            evidence_sha,
            Jsonb(payload),
        ),
    ).fetchone()
    assert row is not None
    attempt_id = int(row[0])
    for cell in preparation.prepared.cells:
        cell_payload = _structural_cell_payload(cell)
        mt1 = cell.books.mt1.structural
        s8 = cell.books.s8.structural
        conn.execute(
            """
            INSERT INTO strategy_mt1_structural_cells (
                structural_attempt_id, ambiguity_arm, quarantine_arm, mt1_decision_dates,
                s8_decision_dates, mt1_annualised_turnover, s8_annualised_turnover,
                mt1_traded_notional, s8_traded_notional, exposure_reconciled,
                evidence_sha256, evidence_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s, %s)
            """,
            (
                attempt_id,
                cell.ambiguity_arm,
                cell.quarantine_arm,
                list(mt1.decision_dates),
                list(s8.decision_dates),
                mt1.annualised_turnover,
                s8.annualised_turnover,
                mt1.traded_notional,
                s8.traded_notional,
                _digest(cell_payload),
                Jsonb(cell_payload),
            ),
        )
    conn.commit()
    readback_id = _existing_structural_attempt(conn, preparation, expected_sha=evidence_sha)
    if readback_id != attempt_id:  # pragma: no cover - the committed row was just returned
        raise MT1EvidenceConflict("the committed MT-1 structural evidence did not read back exactly")
    return attempt_id


def store_mt1_structural_refusal(conn: psycopg.Connection[Any], refusal: MT1InSampleStructuralRefusal) -> int:
    """Commit one outcome-free structural refusal with no child cells."""
    if not refusal.detail.strip() or len(refusal.detail) > 2000:
        raise ValueError("MT-1 structural refusal detail must contain 1..2000 characters")
    payload: dict[str, object] = {
        "book_rule_version": BOOK_RULE_VERSION,
        "corpus_version": refusal.corpus_version,
        "cost_model_id": COST_MODEL_ID,
        "declarations": [
            {
                "declaration_id": authority.declaration_id,
                "declaration_sha256": authority.declaration_sha256,
                "strategy_id": authority.strategy_id,
                "strategy_version": authority.strategy_version,
            }
            for authority in refusal.authorities
        ],
        "detail": refusal.detail,
        "evaluator_version": TRIAL_EVALUATOR_VERSION,
        "metric_axis_digest": metric_axis_sha256(refusal.axis_dates),
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "mt1_source_strategy_version": refusal.mt1_source_strategy_version,
        "mt1_strategy_version": refusal.mt1_strategy_version,
        "opportunity_set_digest": record_sha256(refusal.opportunity_record),
        "passed": False,
        "refusal_code": "structural_gate_refused",
        "s8_control_strategy_version": refusal.s8_control_strategy_version,
        "s8_source_strategy_version": refusal.s8_source_strategy_version,
        "structural_cell_digests": [],
        "trial_contract_version": TRIAL_CONTRACT_VERSION,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "universe_basis": "survivorship_free",
    }
    evidence_sha = _digest(payload)
    existing = conn.execute(
        """
        SELECT a.structural_attempt_id, a.structural_evidence_sha256,
               a.structural_evidence_json, a.passed, count(c.structural_attempt_id)
          FROM strategy_mt1_structural_attempts a
          LEFT JOIN strategy_mt1_structural_cells c
            ON c.structural_attempt_id = a.structural_attempt_id
         WHERE mt1_strategy_version = %s AND s8_strategy_version = %s
         GROUP BY a.structural_attempt_id
        """,
        (refusal.mt1_strategy_version, refusal.s8_control_strategy_version),
    ).fetchone()
    if existing is not None:
        if (
            str(existing[1]) != evidence_sha
            or _digest(dict(existing[2])) != evidence_sha
            or bool(existing[3])
            or int(existing[4]) != 0
        ):
            raise MT1EvidenceConflict("the MT-1 strategy versions already own different structural evidence")
        conn.commit()
        return int(existing[0])

    mt1_authority, s8_authority = refusal.authorities
    axis = refusal.axis_dates
    row = conn.execute(
        """
        INSERT INTO strategy_mt1_structural_attempts (
            mt1_declaration_id, s8_declaration_id, mt1_strategy_id, mt1_strategy_version,
            s8_strategy_id, s8_strategy_version, mt1_source_strategy_version,
            s8_source_strategy_version, universe_basis, corpus_version, cost_model_id,
            trial_register_version, trial_contract_version, book_rule_version,
            evaluator_version, metric_axis_rule_version, metric_axis_dates,
            metric_axis_start, metric_axis_end, metric_axis_digest,
            opportunity_set_digest, passed, refusal_code, refusal_detail,
            structural_evidence_sha256, structural_evidence_json
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 'survivorship_free', %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE,
            'structural_gate_refused', %s, %s, %s
        ) RETURNING structural_attempt_id
        """,
        (
            mt1_authority.declaration_id,
            s8_authority.declaration_id,
            mt1_authority.strategy_id,
            refusal.mt1_strategy_version,
            s8_authority.strategy_id,
            refusal.s8_control_strategy_version,
            refusal.mt1_source_strategy_version,
            refusal.s8_source_strategy_version,
            refusal.corpus_version,
            COST_MODEL_ID,
            TRIAL_REGISTER_VERSION,
            TRIAL_CONTRACT_VERSION,
            BOOK_RULE_VERSION,
            TRIAL_EVALUATOR_VERSION,
            METRIC_AXIS_RULE_VERSION,
            list(axis),
            axis[0],
            axis[-1],
            metric_axis_sha256(axis),
            record_sha256(refusal.opportunity_record),
            refusal.detail,
            evidence_sha,
            Jsonb(payload),
        ),
    ).fetchone()
    assert row is not None
    attempt_id = int(row[0])
    conn.commit()
    readback_id = store_mt1_structural_refusal(conn, refusal)
    if readback_id != attempt_id:  # pragma: no cover - the committed row was just returned
        raise MT1EvidenceConflict("the committed MT-1 structural refusal did not read back exactly")
    return attempt_id


def _risk_payload(result: MT1TrialResult) -> dict[str, object]:
    def risk(report: Any) -> dict[str, str]:
        return {
            "certainty_equivalent": _number(report.certainty_equivalent),
            "expected_shortfall_5": _number(report.expected_shortfall_5),
            "maximum_drawdown": _number(report.maximum_drawdown),
        }

    return {
        "bootstrap_block_length": result.bootstrap_block_length,
        "bootstrap_resamples": result.bootstrap_resamples,
        "bootstrap_seed": result.bootstrap_seed,
        "common_months": [item.isoformat() for item in result.common_months],
        "evaluator_version": result.evaluator_version,
        "excluded_months_by_arm": list(result.excluded_months_by_arm),
        "historical_statistical_conjuncts_pass": result.historical_statistical_conjuncts_pass,
        "mt1_delta_cer": _number(result.mt1_delta_cer),
        "mt1_delta_interval": {
            "high": _number(result.mt1_delta_interval.high),
            "low": _number(result.mt1_delta_interval.low),
        },
        "mt1_drawdown_improved": result.mt1_drawdown_improved,
        "mt1_expected_shortfall_improved": result.mt1_expected_shortfall_improved,
        "mt1_lower_bound_positive": result.mt1_lower_bound_positive,
        "mt1_scaled": risk(result.mt1_scaled),
        "mt1_unscaled": risk(result.mt1_unscaled),
        "primary_difference_in_differences": _number(result.primary_difference_in_differences),
        "primary_interval": {
            "high": _number(result.primary_interval.high),
            "low": _number(result.primary_interval.low),
        },
        "primary_lower_bound_positive": result.primary_lower_bound_positive,
        "s8_delta_cer": _number(result.s8_delta_cer),
        "s8_scaled": risk(result.s8_scaled),
        "s8_unscaled": risk(result.s8_unscaled),
    }


def store_mt1_trial_bundle(
    conn: psycopg.Connection[Any], *, structural_attempt_id: int, bundle: MT1HistoricalBundle
) -> int:
    """Atomically commit all four outcome cells and their conjunction header."""
    cell_payloads = [_risk_payload(cell.result) for cell in bundle.cells]
    header_payload: dict[str, object] = {
        "historical_conjuncts_pass": bundle.historical_statistical_conjuncts_pass,
        "result_cell_digests": [_digest(payload) for payload in cell_payloads],
    }
    header_sha = _digest(header_payload)
    existing = conn.execute(
        """
        SELECT mt1_trial_result_id, evidence_sha256, evidence_json
          FROM strategy_mt1_trial_results WHERE structural_attempt_id = %s
        """,
        (structural_attempt_id,),
    ).fetchone()
    if existing is not None:
        stored_cells = conn.execute(
            """
            SELECT ambiguity_arm, quarantine_arm, evidence_sha256
              FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = %s
             ORDER BY ambiguity_arm, quarantine_arm
            """,
            (int(existing[0]),),
        ).fetchall()
        expected_cells = [
            (cell.ambiguity_arm, cell.quarantine_arm, _digest(payload))
            for cell, payload in zip(bundle.cells, cell_payloads, strict=True)
        ]
        if (
            str(existing[1]) != header_sha
            or _digest(dict(existing[2])) != str(existing[1])
            or [(str(row[0]), str(row[1]), str(row[2])) for row in stored_cells] != expected_cells
        ):
            raise MT1EvidenceConflict("the structural attempt already owns different MT-1 outcome evidence")
        conn.commit()
        return int(existing[0])

    row = conn.execute(
        """
        INSERT INTO strategy_mt1_trial_results (
            structural_attempt_id, historical_conjuncts_pass, evidence_sha256, evidence_json
        ) VALUES (%s, %s, %s, %s) RETURNING mt1_trial_result_id
        """,
        (structural_attempt_id, bundle.historical_statistical_conjuncts_pass, header_sha, Jsonb(header_payload)),
    ).fetchone()
    assert row is not None
    result_id = int(row[0])
    for cell, payload in zip(bundle.cells, cell_payloads, strict=True):
        result = cell.result
        conn.execute(
            """
            INSERT INTO strategy_mt1_trial_result_cells (
                mt1_trial_result_id, ambiguity_arm, quarantine_arm, common_months,
                excluded_months_by_arm, mt1_scaled_certainty_equivalent,
                mt1_scaled_maximum_drawdown, mt1_scaled_expected_shortfall_5,
                mt1_unscaled_certainty_equivalent, mt1_unscaled_maximum_drawdown,
                mt1_unscaled_expected_shortfall_5, s8_scaled_certainty_equivalent,
                s8_scaled_maximum_drawdown, s8_scaled_expected_shortfall_5,
                s8_unscaled_certainty_equivalent, s8_unscaled_maximum_drawdown,
                s8_unscaled_expected_shortfall_5, mt1_delta_cer, s8_delta_cer,
                primary_difference_in_differences, mt1_interval_low, mt1_interval_high,
                primary_interval_low, primary_interval_high, primary_lower_bound_positive,
                mt1_lower_bound_positive, mt1_drawdown_improved,
                mt1_expected_shortfall_improved, historical_conjuncts_pass,
                evidence_sha256, evidence_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                result_id,
                cell.ambiguity_arm,
                cell.quarantine_arm,
                list(result.common_months),
                list(result.excluded_months_by_arm),
                result.mt1_scaled.certainty_equivalent,
                result.mt1_scaled.maximum_drawdown,
                result.mt1_scaled.expected_shortfall_5,
                result.mt1_unscaled.certainty_equivalent,
                result.mt1_unscaled.maximum_drawdown,
                result.mt1_unscaled.expected_shortfall_5,
                result.s8_scaled.certainty_equivalent,
                result.s8_scaled.maximum_drawdown,
                result.s8_scaled.expected_shortfall_5,
                result.s8_unscaled.certainty_equivalent,
                result.s8_unscaled.maximum_drawdown,
                result.s8_unscaled.expected_shortfall_5,
                result.mt1_delta_cer,
                result.s8_delta_cer,
                result.primary_difference_in_differences,
                result.mt1_delta_interval.low,
                result.mt1_delta_interval.high,
                result.primary_interval.low,
                result.primary_interval.high,
                result.primary_lower_bound_positive,
                result.mt1_lower_bound_positive,
                result.mt1_drawdown_improved,
                result.mt1_expected_shortfall_improved,
                result.historical_statistical_conjuncts_pass,
                _digest(payload),
                Jsonb(payload),
            ),
        )
    conn.commit()
    readback = conn.execute(
        "SELECT evidence_sha256, evidence_json FROM strategy_mt1_trial_results WHERE mt1_trial_result_id = %s",
        (result_id,),
    ).fetchone()
    readback_cells = conn.execute(
        """
        SELECT ambiguity_arm, quarantine_arm, evidence_sha256
          FROM strategy_mt1_trial_result_cells WHERE mt1_trial_result_id = %s
         ORDER BY ambiguity_arm, quarantine_arm
        """,
        (result_id,),
    ).fetchall()
    expected_cells = [
        (cell.ambiguity_arm, cell.quarantine_arm, _digest(payload))
        for cell, payload in zip(bundle.cells, cell_payloads, strict=True)
    ]
    if (
        readback is None
        or str(readback[0]) != header_sha
        or _digest(dict(readback[1])) != header_sha
        or [(str(row[0]), str(row[1]), str(row[2])) for row in readback_cells] != expected_cells
    ):
        raise MT1EvidenceConflict("the committed MT-1 outcome evidence did not read back exactly")
    return result_id


def run_and_store_mt1_in_sample_evaluation(
    conn: psycopg.Connection[Any], *, progress: ProgressCallback | None = None
) -> MT1StoredEvaluation | MT1StoredRefusal:
    """Paved runner: structural commit, outcome calculation, atomic result commit."""
    try:
        preparation = prepare_mt1_in_sample_evaluation(conn, progress=progress)
    except MT1InSampleStructuralRefused as exc:
        attempt_id = store_mt1_structural_refusal(conn, exc.evidence)
        return MT1StoredRefusal(structural_attempt_id=attempt_id, detail=exc.evidence.detail)
    structural_attempt_id = store_mt1_structural_preparation(conn, preparation)
    bundle = evaluate_mt1_prepared_bundle(preparation.prepared)
    trial_result_id = store_mt1_trial_bundle(
        conn,
        structural_attempt_id=structural_attempt_id,
        bundle=bundle,
    )
    evaluation = MT1InSampleEvaluation(
        authorities=preparation.authorities,
        bundle=bundle,
        mt1_strategy_version=preparation.mt1_strategy_version,
        s8_control_strategy_version=preparation.s8_control_strategy_version,
        mt1_source_strategy_version=preparation.mt1_source_strategy_version,
        s8_source_strategy_version=preparation.s8_source_strategy_version,
        corpus_version=preparation.corpus_version,
    )
    return MT1StoredEvaluation(structural_attempt_id, trial_result_id, evaluation)


__all__ = [
    "MT1EvidenceConflict",
    "MT1StoredEvaluation",
    "MT1StoredRefusal",
    "run_and_store_mt1_in_sample_evaluation",
    "store_mt1_structural_preparation",
    "store_mt1_structural_refusal",
    "store_mt1_trial_bundle",
]
