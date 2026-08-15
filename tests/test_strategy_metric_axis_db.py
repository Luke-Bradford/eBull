"""#2697 — durable metric-axis shape and legacy separation (DB tier)."""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
import psycopg.sql
import pytest

from app.services.result_ledger import store_in_sample_result
from app.services.strategy_recent_evidence import RECENT_EVIDENCE_WINDOWS
from app.services.strategy_result import (
    METRIC_AXIS_RULE_VERSION,
    TOTAL_RETURN_BASIS,
    ResultIdentity,
    StrategyResult,
    metric_axis_sha256,
)
from app.services.strategy_statistics import StrategyMetrics, periods_per_year
from tests.test_result_ledger import build_result

pytestmark = pytest.mark.integration


def _current_result() -> StrategyResult:
    axis = (date(2020, 1, 2), date(2020, 1, 3))
    metrics = StrategyMetrics(
        expectancy_per_trade_pct=0.0,
        profit_factor=None,
        cagr_pct=0.0,
        annualised_volatility_pct=0.0,
        sharpe=0.0,
        sortino=None,
        max_drawdown_pct=0.0,
        exposure_time_pct=0.0,
        turnover_annualised=0.0,
        trade_count=0,
        effective_sample_size=None,
        return_vs_buy_and_hold_pct=0.0,
        losing_trade_count=0,
        losing_period_count=0,
        open_trade_count=0,
        unpriced_trade_count=0,
        periods_per_year=periods_per_year(axis),
        total_return_pct=0.0,
        buy_and_hold_return_pct=0.0,
    )
    return StrategyResult(
        identity=ResultIdentity(
            strategy_id="S-AXIS",
            strategy_version="strategy-v1+axis",
            result_scope="sleeve",
            namespace="in_sample",
            ambiguity_arm="worst_case",
            quarantine_arm="masked",
            sizing_rule="equal_weight_concurrent_v1",
            benchmark_rule="equal_weight_buy_and_hold_v1",
            cost_model_id="cost-v1",
            corpus_version="corpus-v1",
            window_start=date(2020, 1, 1),
            window_end=date(2020, 1, 4),
            position_rule_set_version="positions-v1",
            outcome_rule_set_version="outcomes-v1",
            input_rule_set_version="inputs-v1",
            return_basis=TOTAL_RETURN_BASIS,
            metric_axis_rule_version=METRIC_AXIS_RULE_VERSION,
            metric_axis_dates=axis,
            metric_axis_start=axis[0],
            metric_axis_end=axis[-1],
            metric_axis_digest=metric_axis_sha256(axis),
            opportunity_set_digest="b" * 64,
        ),
        purpose="capital_candidate",
        metrics=metrics,
        universe_basis="survivorship_free",
        carry_unmodelled=False,
        fx_unmodelled=False,
        evaluated_instrument_count=1,
    )


def test_legacy_rows_remain_all_null(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, build_result(namespace="in_sample"))
    row = ebull_test_conn.execute(
        """
        SELECT metric_axis_rule_version, metric_axis_dates, metric_axis_start,
               metric_axis_end, metric_axis_digest, opportunity_set_digest, evidence_window_id
          FROM strategy_results_store WHERE result_id = %s
        """,
        (result_id,),
    ).fetchone()
    assert row == (None, None, None, None, None, None, None)


def test_current_axis_round_trips_through_the_view(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result = _current_result()
    result_id = store_in_sample_result(ebull_test_conn, result)
    row = ebull_test_conn.execute(
        "SELECT metric_axis_dates, metric_axis_digest FROM strategy_results WHERE result_id = %s",
        (result_id,),
    ).fetchone()
    assert row == (list(result.identity.metric_axis_dates or ()), result.identity.metric_axis_digest)


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("metric_axis_dates", [date(2020, 1, 2), date(2020, 1, 2)]),
        ("metric_axis_rule_version", "invented"),
        ("metric_axis_digest", None),
    ),
)
def test_malformed_direct_writes_are_refused(
    ebull_test_conn: psycopg.Connection[Any], column: str, value: object
) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _current_result())
    with pytest.raises(psycopg.errors.CheckViolation):
        with ebull_test_conn.transaction():
            ebull_test_conn.execute(
                psycopg.sql.SQL("UPDATE strategy_results_store SET {} = %s WHERE result_id = %s").format(
                    psycopg.sql.Identifier(column)
                ),
                (value, result_id),
            )


def test_sql_recent_window_mirror_is_closed(ebull_test_conn: psycopg.Connection[Any]) -> None:
    for window_id, item in RECENT_EVIDENCE_WINDOWS.items():
        assert ebull_test_conn.execute(
            "SELECT strategy_evidence_window_is_registered(%s, %s, %s)",
            (window_id, item.window.start, item.window.end),
        ).fetchone() == (True,)
    assert ebull_test_conn.execute(
        "SELECT strategy_evidence_window_is_registered(%s, %s, %s)",
        ("searched", date(2022, 1, 1), date(2024, 9, 27)),
    ).fetchone() == (False,)
