"""Replay MT-1 persisted input provenance without reading an outcome value."""

from __future__ import annotations

import hashlib
import json
import sys
from collections.abc import Mapping
from typing import Any

import psycopg
import psycopg.rows

from app.config import settings
from app.services.backtest_run import BACKTEST_UNIVERSE, corpus_version_for, load_corpus
from app.services.cost_model import COST_MODEL_ID
from app.services.strategy_manifest import STRATEGY_MANIFEST
from app.services.strategy_mt1_books import BOOK_RULE_VERSION
from app.services.strategy_mt1_identity import mt1_identity, s8_control_identity
from app.services.strategy_mt1_runner import (
    MT1_IN_SAMPLE_WINDOW,
    MT1_SOURCE_STRATEGY_ID,
    S8_SOURCE_STRATEGY_ID,
)
from app.services.strategy_mt1_trial import TRIAL_CONTRACT_VERSION, TRIAL_EVALUATOR_VERSION
from app.services.strategy_result import METRIC_AXIS_RULE_VERSION, metric_axis_sha256
from app.services.strategy_result_universe import ResultUniverseRecord, record_sha256
from app.services.trial_register import TRIAL_REGISTER_VERSION


def _canonical_digest(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def verify_persisted_inputs(
    row: Mapping[str, object],
    *,
    derived_axis: tuple[Any, ...],
    derived_opportunity: ResultUniverseRecord,
) -> list[str]:
    """Return exact provenance differences; the caller decides the exit code."""
    mt1_source = STRATEGY_MANIFEST[MT1_SOURCE_STRATEGY_ID].identity(
        universe=BACKTEST_UNIVERSE,
        cost_model_id=COST_MODEL_ID,
    )
    s8_source = STRATEGY_MANIFEST[S8_SOURCE_STRATEGY_ID].identity(
        universe=BACKTEST_UNIVERSE,
        cost_model_id=COST_MODEL_ID,
    )
    mt1 = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    expected: dict[str, object] = {
        "book_rule_version": BOOK_RULE_VERSION,
        "corpus_version": corpus_version_for(BACKTEST_UNIVERSE),
        "cost_model_id": COST_MODEL_ID,
        "evaluator_version": TRIAL_EVALUATOR_VERSION,
        "metric_axis_dates": list(derived_axis),
        "metric_axis_digest": metric_axis_sha256(derived_axis),
        "metric_axis_rule_version": METRIC_AXIS_RULE_VERSION,
        "mt1_source_strategy_version": mt1_source.version,
        "mt1_strategy_version": mt1.version,
        "opportunity_set_digest": record_sha256(derived_opportunity),
        "s8_source_strategy_version": s8_source.version,
        "s8_strategy_version": s8.version,
        "trial_contract_version": TRIAL_CONTRACT_VERSION,
        "trial_register_version": TRIAL_REGISTER_VERSION,
        "universe_basis": "survivorship_free",
    }
    differences = [name for name, value in expected.items() if row.get(name) != value]
    payload = row.get("structural_evidence_json")
    if not isinstance(payload, dict) or _canonical_digest(payload) != row.get("structural_evidence_sha256"):
        differences.append("structural_evidence_digest")
    return sorted(differences)


def main() -> int:
    mt1 = mt1_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    s8 = s8_control_identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            row = cur.execute(
                """
                SELECT mt1_strategy_version, s8_strategy_version, mt1_source_strategy_version,
                       s8_source_strategy_version, universe_basis, corpus_version, cost_model_id,
                       trial_register_version, trial_contract_version, book_rule_version,
                       evaluator_version, metric_axis_rule_version, metric_axis_dates,
                       metric_axis_digest, opportunity_set_digest, structural_evidence_sha256,
                       structural_evidence_json
                  FROM strategy_mt1_structural_attempts
                 WHERE mt1_strategy_version = %s AND s8_strategy_version = %s
                """,
                (mt1.version, s8.version),
            ).fetchone()
        if row is None:
            sys.stderr.write(json.dumps({"outcome": "refused", "reason": "structural_attempt_missing"}) + "\n")
            return 2
        corpus = load_corpus(
            conn,
            universe_basis=BACKTEST_UNIVERSE,
            evaluation_window=MT1_IN_SAMPLE_WINDOW,
        )
        differences = verify_persisted_inputs(
            row,
            derived_axis=corpus.in_sample_axis,
            derived_opportunity=corpus.opportunity_records["in_sample"],
        )
    output = {
        "differences": differences,
        "metric_axis_dates": len(corpus.in_sample_axis),
        "outcome": "verified" if not differences else "refused",
        "opportunity_set_digest": record_sha256(corpus.opportunity_records["in_sample"]),
        "window_end": MT1_IN_SAMPLE_WINDOW.end.isoformat(),
    }
    (sys.stdout if not differences else sys.stderr).write(json.dumps(output, sort_keys=True) + "\n")
    return 0 if not differences else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main", "verify_persisted_inputs"]
