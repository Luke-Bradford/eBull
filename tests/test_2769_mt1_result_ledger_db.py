"""Database invariants for the sealed MT-1 controlled-trial ledger (#2769)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.result_ledger import freeze_preregistration
from app.services.strategy_mt1_preregistration import build_declarations

_FAN = (("best_case", "masked"), ("best_case", "admitted"), ("worst_case", "masked"), ("worst_case", "admitted"))
_SHA = "0" * 64
_RUNNER_HEAD = "a" * 40
_DECISION_DATES = [date(2020, 1, 2), date(2020, 2, 3)]


def _months(count: int = 120) -> list[date]:
    return [date(2010 + offset // 12, offset % 12 + 1, 1) for offset in range(count)]


def _freeze_pair(conn: psycopg.Connection[tuple]) -> tuple[int, int, str, str]:
    mt1, s8 = build_declarations()
    mt1_id = freeze_preregistration(conn, mt1)
    s8_id = freeze_preregistration(conn, s8)
    conn.commit()
    return mt1_id, s8_id, mt1.strategy_version, s8.strategy_version


def _insert_attempt(conn: psycopg.Connection[tuple], *, passed: bool) -> int:
    mt1_id, s8_id, mt1_version, s8_version = _freeze_pair(conn)
    row = conn.execute(
        """
        INSERT INTO strategy_mt1_structural_attempts (
            mt1_declaration_id, s8_declaration_id, mt1_strategy_id, mt1_strategy_version,
            s8_strategy_id, s8_strategy_version, mt1_source_strategy_version,
            s8_source_strategy_version, universe_basis, corpus_version, cost_model_id,
            trial_register_version, trial_contract_version, book_rule_version,
            evaluator_version, metric_axis_rule_version, metric_axis_dates,
            metric_axis_start, metric_axis_end, metric_axis_digest,
            opportunity_set_digest, runner_source_head, passed, refusal_code, refusal_detail,
            structural_evidence_sha256, structural_evidence_json
        ) VALUES (
            %s, %s, 'mt1-capped-volatility-managed-relative-strength-v1', %s,
            'mt1-s8-capped-volatility-negative-control-v1', %s, 'source-mt1', 'source-s8',
            'survivorship_free', 'corpus-v1', 'cost-v1', 'register-v1', 'contract-v1',
            'book-v1', 'evaluator-v1', 'full-namespace-panel-v1', %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, '{}'::jsonb
        ) RETURNING structural_attempt_id
        """,
        (
            mt1_id,
            s8_id,
            mt1_version,
            s8_version,
            [*_months(121), *_DECISION_DATES],
            _months()[0],
            _DECISION_DATES[-1],
            _SHA,
            _SHA,
            _RUNNER_HEAD,
            passed,
            None if passed else "structural_gate_refused",
            None if passed else "synthetic structural refusal",
            _SHA,
        ),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_structural_cells(
    conn: psycopg.Connection[tuple], attempt_id: int, *, decision_dates: list[date] | None = None
) -> None:
    clock = _DECISION_DATES if decision_dates is None else decision_dates
    for ambiguity_arm, quarantine_arm in _FAN:
        conn.execute(
            """
            INSERT INTO strategy_mt1_structural_cells (
                structural_attempt_id, ambiguity_arm, quarantine_arm, mt1_decision_dates,
                s8_decision_dates, mt1_annualised_turnover, s8_annualised_turnover,
                mt1_traded_notional, s8_traded_notional, exposure_reconciled,
                evidence_sha256, evidence_json
            ) VALUES (%s, %s, %s, %s, %s, 1, 1, 100, 100, TRUE, %s, '{}'::jsonb)
            """,
            (attempt_id, ambiguity_arm, quarantine_arm, clock, clock, _SHA),
        )


def _complete_structural_attempt(conn: psycopg.Connection[tuple]) -> int:
    attempt_id = _insert_attempt(conn, passed=True)
    _insert_structural_cells(conn, attempt_id)
    conn.commit()
    return attempt_id


def _insert_result_header(conn: psycopg.Connection[tuple], attempt_id: int, *, passed: bool) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_mt1_trial_results (
            structural_attempt_id, historical_conjuncts_pass, evidence_sha256, evidence_json
        ) VALUES (%s, %s, %s, '{}'::jsonb) RETURNING mt1_trial_result_id
        """,
        (attempt_id, passed, _SHA),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _insert_result_cell(
    conn: psycopg.Connection[tuple],
    result_id: int,
    arms: tuple[str, str],
    *,
    passed: bool,
    cer: object = 0,
    common_months: list[date] | None = None,
) -> None:
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
            %s, %s, %s, %s, ARRAY[0,0,0,0], %s, 0.1, -0.1, 0, 0.1, -0.1,
            0, 0.1, -0.1, 0, 0.1, -0.1, 0, 0, 0, -0.1, 0.1, -0.1, 0.1,
            %s, %s, %s, %s, %s, %s, '{}'::jsonb
        )
        """,
        (
            result_id,
            arms[0],
            arms[1],
            _months() if common_months is None else common_months,
            cer,
            passed,
            passed,
            passed,
            passed,
            passed,
            _SHA,
        ),
    )


def test_passed_structural_attempt_cannot_commit_without_complete_fan(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _insert_attempt(ebull_test_conn, passed=True)
    with pytest.raises(psycopg.errors.RaiseException, match="requires exactly 4 cells"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_mt1_structural_attempts").fetchone() == (0,)


def test_duplicate_structural_fan_cell_is_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _insert_attempt(ebull_test_conn, passed=True)
    arms = _FAN[0]
    for _ in range(2):
        try:
            ebull_test_conn.execute(
                """
                INSERT INTO strategy_mt1_structural_cells (
                    structural_attempt_id, ambiguity_arm, quarantine_arm, mt1_decision_dates,
                    s8_decision_dates, mt1_annualised_turnover, s8_annualised_turnover,
                    mt1_traded_notional, s8_traded_notional, exposure_reconciled,
                    evidence_sha256, evidence_json
                ) VALUES (%s, %s, %s, %s, %s, 1, 1, 100, 100, TRUE, %s, '{}'::jsonb)
                """,
                (attempt_id, arms[0], arms[1], _DECISION_DATES, _DECISION_DATES, _SHA),
            )
        except psycopg.errors.UniqueViolation:
            break
    else:  # pragma: no cover - the primary key must reject the second row
        raise AssertionError("duplicate fan cell was accepted")
    ebull_test_conn.rollback()


def test_refused_structural_attempt_has_zero_cells_and_cannot_own_results(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _insert_attempt(ebull_test_conn, passed=False)
    ebull_test_conn.commit()
    _insert_result_header(ebull_test_conn, attempt_id, passed=False)
    with pytest.raises(psycopg.errors.RaiseException, match="passed four-cell structural attempt"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()


def test_partial_result_fan_cannot_commit(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=False)
    for arms in _FAN[:3]:
        _insert_result_cell(ebull_test_conn, result_id, arms, passed=False)
    with pytest.raises(psycopg.errors.RaiseException, match="four result cells"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_mt1_trial_results").fetchone() == (0,)


def test_partial_four_arm_result_cell_is_refused(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=False)
    with pytest.raises(psycopg.errors.NotNullViolation):
        _insert_result_cell(ebull_test_conn, result_id, _FAN[0], passed=False, cer=None)
    ebull_test_conn.rollback()


def test_structural_decisions_must_belong_to_the_parent_metric_axis(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _insert_attempt(ebull_test_conn, passed=True)
    _insert_structural_cells(
        ebull_test_conn,
        attempt_id,
        decision_dates=[date(2020, 1, 2), date(2020, 3, 2)],
    )
    with pytest.raises(psycopg.errors.RaiseException, match="decision outside its frozen metric axis"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()


def test_all_result_cells_must_share_one_parent_backed_month_axis(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=False)
    for index, arms in enumerate(_FAN):
        months = _months(121)[1:] if index == 0 else _months()
        _insert_result_cell(ebull_test_conn, result_id, arms, passed=False, common_months=months)
    with pytest.raises(psycopg.errors.RaiseException, match="common-month axes differ"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()


def test_header_is_the_conjunction_of_all_four_cells(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=True)
    for index, arms in enumerate(_FAN):
        _insert_result_cell(ebull_test_conn, result_id, arms, passed=index != 0)
    with pytest.raises(psycopg.errors.RaiseException, match="four-cell conjunction is f"):
        ebull_test_conn.commit()
    ebull_test_conn.rollback()


@pytest.mark.parametrize("non_finite", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_result_cells_refuse_non_finite_metrics(
    ebull_test_conn: psycopg.Connection[tuple], non_finite: Decimal
) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=False)
    with pytest.raises(psycopg.errors.CheckViolation, match="metrics_finite"):
        _insert_result_cell(ebull_test_conn, result_id, _FAN[0], passed=False, cer=non_finite)
    ebull_test_conn.rollback()


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE strategy_mt1_structural_attempts SET corpus_version = 'changed'",
        "DELETE FROM strategy_mt1_structural_cells",
        "UPDATE strategy_mt1_trial_results SET historical_conjuncts_pass = FALSE",
        "DELETE FROM strategy_mt1_trial_result_cells",
    ],
)
def test_committed_trial_evidence_is_immutable(ebull_test_conn: psycopg.Connection[tuple], statement: str) -> None:
    attempt_id = _complete_structural_attempt(ebull_test_conn)
    result_id = _insert_result_header(ebull_test_conn, attempt_id, passed=True)
    for arms in _FAN:
        _insert_result_cell(ebull_test_conn, result_id, arms, passed=True)
    ebull_test_conn.commit()
    with pytest.raises(psycopg.errors.RaiseException, match="evidence is immutable"):
        ebull_test_conn.execute(statement)  # type: ignore[arg-type] - fixed test-only SQL cases above
    ebull_test_conn.rollback()
