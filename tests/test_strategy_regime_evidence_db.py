from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg
import pytest

from app.services.market_regime import Regime
from app.services.result_ledger import store_in_sample_result
from app.services.strategy_regime_evidence import (
    RegimeTradeObservation,
    build_regime_cohorts,
    store_result_regime_cohorts,
)
from tests.test_result_ledger import build_metrics, build_result

pytestmark = pytest.mark.integration


def _cohorts():
    start = date(2024, 1, 1)
    observations = [
        RegimeTradeObservation(
            instrument_key=index % 3 + 1,
            signal_date=start + timedelta(days=index),
            net_return_pct=1.0 if index % 2 else -0.5,
            regime=Regime.BULL_QUIET if index < 10 else Regime.BEAR_QUIET,
        )
        for index in range(20)
    ]
    return build_regime_cohorts(observations, root_seed=17)


def _result(*, trade_count: int = 20):
    empty_overrides = (
        {
            "profit_factor": None,
            "hold_days_p25": None,
            "median_hold_days": None,
            "hold_days_p75": None,
        }
        if trade_count == 0
        else {}
    )
    return build_result(
        namespace="in_sample",
        metrics=build_metrics(
            trade_count=trade_count,
            losing_trade_count=min(10, trade_count),
            open_trade_count=0,
            **empty_overrides,
        ),
    )


def test_store_round_trips_and_reconciles(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _result())
    cohorts = _cohorts()
    store_result_regime_cohorts(
        ebull_test_conn,
        result_id=result_id,
        cohorts=cohorts,
        expected_trade_count=20,
    )
    assert ebull_test_conn.execute(
        "SELECT regime, trade_count FROM strategy_result_regime_cohorts WHERE result_id=%s ORDER BY regime",
        (result_id,),
    ).fetchall() == [("bear_quiet", 10), ("bull_quiet", 10)]


def test_parent_count_mismatch_refuses_before_insert(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _result())
    with pytest.raises(ValueError, match="do not reconcile"):
        store_result_regime_cohorts(
            ebull_test_conn,
            result_id=result_id,
            cohorts=_cohorts(),
            expected_trade_count=19,
        )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_result_regime_cohorts WHERE result_id=%s",
        (result_id,),
    ).fetchone() == (0,)


def test_actual_parent_count_mismatch_refuses_before_insert(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _result(trade_count=21))
    with pytest.raises(ValueError, match="does not match parent result 21"):
        store_result_regime_cohorts(
            ebull_test_conn,
            result_id=result_id,
            cohorts=_cohorts(),
            expected_trade_count=20,
        )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_result_regime_cohorts WHERE result_id=%s",
        (result_id,),
    ).fetchone() == (0,)


def test_zero_trade_parent_stores_no_empty_placeholder_cohort(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _result(trade_count=0))
    store_result_regime_cohorts(
        ebull_test_conn,
        result_id=result_id,
        cohorts=(),
        expected_trade_count=0,
    )
    assert ebull_test_conn.execute(
        "SELECT count(*) FROM strategy_result_regime_cohorts WHERE result_id=%s",
        (result_id,),
    ).fetchone() == (0,)


def test_cohorts_are_immutable(ebull_test_conn: psycopg.Connection[Any]) -> None:
    result_id = store_in_sample_result(ebull_test_conn, _result())
    store_result_regime_cohorts(
        ebull_test_conn,
        result_id=result_id,
        cohorts=_cohorts(),
        expected_trade_count=20,
    )
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        with ebull_test_conn.transaction():
            ebull_test_conn.execute(
                "UPDATE strategy_result_regime_cohorts SET trade_count=9 WHERE result_id=%s",
                (result_id,),
            )
