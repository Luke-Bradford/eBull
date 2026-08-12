"""DB boundary for #2505's one-row-per-result aggregate evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.result_ledger import store_in_sample_result
from app.services.strategy_promotion_evidence import (
    EVIDENCE_VERSION,
    REQUIRED_CHALLENGERS,
    REQUIRED_CONTRASTS,
    REQUIRED_COST_INPUTS,
    ChallengerEvidence,
    ExpectedValueBucket,
    OutcomeContrast,
    PromotionEvidence,
    RecentYearEvidence,
)
from app.services.strategy_promotion_evidence_store import (
    MAX_PAYLOAD_BYTES,
    _jsonb_text_size,
    _payload,
    load_promotion_evidence,
    store_promotion_evidence,
)
from tests.test_result_ledger import build_result


def _evidence() -> PromotionEvidence:
    return PromotionEvidence(
        evidence_version=EVIDENCE_VERSION,
        causal_observation_rule_version="causal-v1",
        fill_rule_version="fills-v1",
        overlap_rule_version="overlap-v1",
        after_cost_expectancy_ci_low_pct=Decimal("0.123456789"),
        max_drawdown_pct=Decimal("-8.5"),
        expected_shortfall_5_pct=Decimal("-3.25"),
        worst_gap_pct=Decimal("-5.75"),
        excluding_best_1_expectancy_pct=Decimal("0.44"),
        recent_year_stable=True,
        recent_years_evaluated=3,
        recent_year_evidence=(
            RecentYearEvidence(2024, 40, Decimal("0.2"), Decimal("-0.1"), True),
            RecentYearEvidence(2025, 40, Decimal("0.3"), Decimal("0.0"), True),
            RecentYearEvidence(2026, 40, Decimal("0.4"), Decimal("0.1"), True),
        ),
        max_date_contribution_pct=Decimal("8.1"),
        max_name_contribution_pct=Decimal("7.2"),
        max_sector_contribution_pct=Decimal("19.3"),
        max_concurrency=12,
        capacity_usd=Decimal("123456.78"),
        risk_limits_version="risk-v1+abc",
        risk_limits_passed=True,
        probability_calibration_passed=True,
        path_diagnostics_complete=True,
        outcome_count=120,
        profitable_outcome_count=70,
        losing_outcome_count=50,
        flat_outcome_count=0,
        target_first_count=48,
        stop_first_count=36,
        timeout_count=36,
        ambiguous_path_count=2,
        observed_cost_inputs=REQUIRED_COST_INPUTS,
        cost_observed_on=date(2026, 8, 11),
        cost_valid_through=date(2026, 8, 13),
        cost_source_version="etoro-quote-v1",
        spread_bps=Decimal("8"),
        slippage_bps=Decimal("5"),
        financing_bps_per_day=Decimal("1"),
        fx_bps=Decimal("2"),
        broker_eligible=True,
        challengers=tuple(
            ChallengerEvidence(
                role,
                120,
                Decimal("0.1"),
                Decimal("0.2"),
                True,
                "causal-v1",
                "fills-v1",
                "overlap-v1",
            )
            for role in sorted(REQUIRED_CHALLENGERS)
        ),
        ev_buckets=(
            ExpectedValueBucket(1, 40, Decimal("-0.2"), Decimal("-0.1")),
            ExpectedValueBucket(2, 40, Decimal("0.1"), Decimal("0.2")),
            ExpectedValueBucket(3, 40, Decimal("0.4"), Decimal("0.5")),
        ),
        outcome_contrasts=tuple(
            OutcomeContrast(role, 70, 50, Decimal("1"), Decimal("0"), Decimal("1"))
            for role in sorted(REQUIRED_CONTRASTS)
        ),
    )


def _result_id(conn: psycopg.Connection[tuple]) -> int:
    return store_in_sample_result(conn, build_result(namespace="in_sample"))


def test_evidence_round_trips_exactly_and_absence_is_explicit(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    assert load_promotion_evidence(ebull_test_conn, 999_999_999) is None
    result_id = _result_id(ebull_test_conn)
    evidence = _evidence()
    store_promotion_evidence(ebull_test_conn, result_id=result_id, evidence=evidence)
    assert load_promotion_evidence(ebull_test_conn, result_id) == evidence


def test_one_result_cannot_accumulate_evidence_rewrites(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    result_id = _result_id(ebull_test_conn)
    store_promotion_evidence(ebull_test_conn, result_id=result_id, evidence=_evidence())
    with pytest.raises(psycopg.errors.UniqueViolation):
        store_promotion_evidence(
            ebull_test_conn,
            result_id=result_id,
            evidence=replace(_evidence(), after_cost_expectancy_ci_low_pct=Decimal("99")),
        )


@pytest.mark.parametrize("verb", ["UPDATE", "DELETE"])
def test_evidence_is_immutable_in_the_database(ebull_test_conn: psycopg.Connection[tuple], verb: str) -> None:
    result_id = _result_id(ebull_test_conn)
    store_promotion_evidence(ebull_test_conn, result_id=result_id, evidence=_evidence())
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        if verb == "UPDATE":
            ebull_test_conn.execute(
                "UPDATE strategy_promotion_evidence SET evidence_version='rewrite' WHERE result_id=%s",
                (result_id,),
            )
        else:
            ebull_test_conn.execute("DELETE FROM strategy_promotion_evidence WHERE result_id=%s", (result_id,))


def test_payload_has_a_database_and_writer_size_ceiling(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    result_id = _result_id(ebull_test_conn)
    huge_version = "v" * MAX_PAYLOAD_BYTES
    with pytest.raises(ValueError, match="exceeds"):
        store_promotion_evidence(
            ebull_test_conn,
            result_id=result_id,
            evidence=replace(_evidence(), risk_limits_version=huge_version),
        )


def test_database_also_refuses_an_oversized_raw_payload(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    result_id = _result_id(ebull_test_conn)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            """
            INSERT INTO strategy_promotion_evidence (
                result_id, evidence_version, payload_sha256, evidence_payload
            ) VALUES (%s, %s, %s, %s::jsonb)
            """,
            (result_id, EVIDENCE_VERSION, "0" * 64, json.dumps({"padding": "x" * MAX_PAYLOAD_BYTES})),
        )


def test_writer_size_measure_matches_postgres_jsonb_text(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    payload = _payload(replace(_evidence(), risk_limits_version="risk-£-é-v1"))
    compact = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    row = ebull_test_conn.execute(
        "SELECT octet_length((%s::jsonb)::text)",
        (compact,),
    ).fetchone()
    assert row is not None
    assert _jsonb_text_size(payload) == int(row[0])


def test_loader_fails_closed_on_a_raw_malformed_payload(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    result_id = _result_id(ebull_test_conn)
    payload = {"evidence_version": EVIDENCE_VERSION, "recent_year_stable": "false"}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ebull_test_conn.execute(
        """
        INSERT INTO strategy_promotion_evidence (
            result_id, evidence_version, payload_sha256, evidence_payload
        ) VALUES (%s, %s, %s, %s::jsonb)
        """,
        (result_id, EVIDENCE_VERSION, hashlib.sha256(encoded).hexdigest(), encoded.decode()),
    )
    with pytest.raises(RuntimeError, match="payload is invalid"):
        load_promotion_evidence(ebull_test_conn, result_id)


def test_no_secondary_index_is_created_for_the_one_to_one_lookup(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    indexes = ebull_test_conn.execute(
        "SELECT indexname FROM pg_indexes WHERE tablename='strategy_promotion_evidence' ORDER BY indexname"
    ).fetchall()
    assert indexes == [("strategy_promotion_evidence_pkey",)]
