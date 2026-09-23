"""#3007 part 1 / #2942 slice C prerequisite: the claim INSERT records the submission
context (``sql/409``) and the pool's average cost with an EXIT lot (``sql/413``), and
both read back exactly.

One DB file for the one new SQL mechanism (the columns and their CHECK). The mock-level
proof that ``execute_order`` hands the resolved lot to the claim is
``tests/test_order_client.py::test_live_exit_calls_broker_close_position``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import OrderParams
from app.services.order_client import (
    ExitLot,
    _persist_submitted_intent,
    load_recommendation_submission_context,
)

INSTRUMENT_ID = 993_007
_NOW = datetime(2026, 9, 22, 19, 0, tzinfo=UTC)


def _seed(conn: psycopg.Connection[Any], *, action: str) -> tuple[int, int]:
    """``is_tradable`` listed explicitly per #1233 §6.2 (chokepoint lint)."""
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CTX.3007','Submission Context Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    rec = conn.execute(
        "INSERT INTO trade_recommendations (instrument_id, action, rationale, status) "
        "VALUES (%s,%s,'context test','approved') RETURNING recommendation_id",
        (INSTRUMENT_ID, action),
    ).fetchone()
    decision = conn.execute(
        "INSERT INTO decision_audit (decision_time, instrument_id, stage, pass_fail, explanation) "
        "VALUES (%s,%s,'execution','PASS','context test') RETURNING decision_id",
        (_NOW, INSTRUMENT_ID),
    ).fetchone()
    assert rec is not None and decision is not None
    return int(rec[0]), int(decision[0])


def _claim(
    conn: psycopg.Connection[Any],
    *,
    action: str,
    order_params: OrderParams | None,
    exit_lot: ExitLot | None,
) -> int:
    rec, decision = _seed(conn, action=action)
    order_id, _request_id = _persist_submitted_intent(
        conn,
        instrument_id=INSTRUMENT_ID,
        recommendation_id=rec,
        decision_id=decision,
        action=action,
        requested_amount=None,
        requested_units=None,
        broker_env="demo",
        order_params=order_params,
        exit_lot=exit_lot,
        now=_NOW,
    )
    conn.commit()
    return order_id


def test_an_exit_claim_records_the_exact_lot_at_broker_scale(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Eight decimals survive. ``orders.requested_units`` is numeric(18,6) and would
    round this lot (#3006), which is why the lot has its own numeric(20,8) column."""
    lot = ExitLot(position_id=3_601_264_304, units=Decimal("1305.05709612"))
    order_id = _claim(ebull_test_conn, action="EXIT", order_params=None, exit_lot=lot)

    context = load_recommendation_submission_context(ebull_test_conn, order_id=order_id)

    assert context.recorded is True
    assert context.exit_lot == lot
    assert context.order_params is None


def test_a_buy_claim_records_the_params_sent_exactly(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    params = OrderParams(
        stop_loss_rate=Decimal("123.456789"),
        take_profit_rate=None,
        is_tsl_enabled=True,
        leverage=1,
    )
    order_id = _claim(ebull_test_conn, action="BUY", order_params=params, exit_lot=None)

    context = load_recommendation_submission_context(ebull_test_conn, order_id=order_id)

    assert context.recorded is True
    assert context.order_params == params
    assert context.exit_lot is None


def test_a_row_the_claim_did_not_write_reads_as_not_recorded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Every pre-409 row, and the demo `_persist_order` path. The booking slice must
    refuse these rather than re-select a lot, so the loader must not invent one."""
    rec, _decision = _seed(ebull_test_conn, action="EXIT")
    row = ebull_test_conn.execute(
        "INSERT INTO orders (instrument_id, recommendation_id, action, order_type, status, "
        "raw_payload_json, created_at, recommendation_request_id) "
        "VALUES (%s,%s,'EXIT','market','pending','{}'::jsonb,%s,%s) RETURNING order_id",
        (INSTRUMENT_ID, rec, _NOW, uuid4()),
    ).fetchone()
    assert row is not None
    ebull_test_conn.commit()

    context = load_recommendation_submission_context(ebull_test_conn, order_id=int(row[0]))

    assert context.recorded is False
    assert context.exit_lot is None
    assert context.order_params is None


_CTX = Jsonb({"order_params": None})


@pytest.mark.parametrize(
    ("action", "position_id", "units", "context"),
    [
        # A lot on a non-EXIT order has no meaning.
        ("BUY", 42, Decimal("1"), _CTX),
        # A lot without its context is half a record.
        ("EXIT", 42, Decimal("1"), None),
        # Half a lot, each way. Without explicit IS NOT NULL the arm is UNKNOWN and
        # Postgres accepts it.
        ("EXIT", 42, None, _CTX),
        ("EXIT", None, Decimal("1"), _CTX),
        # `'NaN'::numeric > 0` is TRUE in Postgres.
        ("EXIT", 42, Decimal("NaN"), _CTX),
        ("EXIT", 42, Decimal("0"), _CTX),
        ("EXIT", -42, Decimal("1"), _CTX),
    ],
)
def test_the_check_refuses_a_lot_that_is_not_one_whole_exit_record(
    ebull_test_conn: psycopg.Connection[tuple],
    action: str,
    position_id: int | None,
    units: Decimal | None,
    context: Jsonb | None,
) -> None:
    rec, _decision = _seed(ebull_test_conn, action=action)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO orders (instrument_id, recommendation_id, action, order_type, status, "
            "raw_payload_json, created_at, recommendation_exit_position_id, recommendation_exit_units, "
            "recommendation_submission_context) "
            "VALUES (%s,%s,%s,'market','submitted','{}'::jsonb,%s,%s,%s,%s)",
            (INSTRUMENT_ID, rec, action, _NOW, position_id, units, context),
        )
    ebull_test_conn.rollback()


def test_an_unknown_order_is_a_caller_defect_not_unrecorded(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    with pytest.raises(LookupError):
        load_recommendation_submission_context(ebull_test_conn, order_id=-1)


def _seed_position(conn: psycopg.Connection[Any], *, avg_cost: Decimal | None) -> None:
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,is_tradable) "
        "VALUES (%s,'CTX.3007','Submission Context Test',TRUE) ON CONFLICT DO NOTHING",
        (INSTRUMENT_ID,),
    )
    conn.execute(
        "INSERT INTO positions (instrument_id, current_units, cost_basis, avg_cost, open_date, source, updated_at) "
        "VALUES (%s, 10, %s, %s, %s, 'broker_sync', %s) "
        "ON CONFLICT (instrument_id) DO UPDATE SET avg_cost = EXCLUDED.avg_cost",
        (
            INSTRUMENT_ID,
            Decimal(0) if avg_cost is None or not avg_cost.is_finite() else avg_cost * 10,
            avg_cost,
            _NOW.date(),
            _NOW,
        ),
    )


_LOT = ExitLot(position_id=3_601_264_304, units=Decimal("10"))


def test_an_exit_claim_records_the_pool_cost_the_lot_is_disposed_against(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """sql/413: a late fill books realized P&L after the pool may have moved, so the
    cost is taken at claim time and survives a later change to ``positions``."""
    _seed_position(ebull_test_conn, avg_cost=Decimal("123.456789"))
    order_id = _claim(ebull_test_conn, action="EXIT", order_params=None, exit_lot=_LOT)
    _seed_position(ebull_test_conn, avg_cost=Decimal("999"))
    ebull_test_conn.commit()

    context = load_recommendation_submission_context(ebull_test_conn, order_id=order_id)

    assert context.exit_avg_cost == Decimal("123.456789")


@pytest.mark.parametrize("avg_cost", [None, Decimal("0"), Decimal("-1"), Decimal("NaN")])
def test_an_unusable_pool_cost_records_null_and_never_refuses_the_exit(
    ebull_test_conn: psycopg.Connection[tuple],
    avg_cost: Decimal | None,
) -> None:
    """EXIT is never blocked (execution-guard). The claim still lands; the booking
    slice refuses the NULL instead."""
    _seed_position(ebull_test_conn, avg_cost=avg_cost)
    order_id = _claim(ebull_test_conn, action="EXIT", order_params=None, exit_lot=_LOT)

    context = load_recommendation_submission_context(ebull_test_conn, order_id=order_id)

    assert context.exit_lot == _LOT
    assert context.exit_avg_cost is None


def test_an_exit_with_no_position_row_records_null(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    order_id = _claim(ebull_test_conn, action="EXIT", order_params=None, exit_lot=_LOT)

    assert load_recommendation_submission_context(ebull_test_conn, order_id=order_id).exit_avg_cost is None


def test_a_claim_without_a_lot_records_no_cost(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Only a lot is disposed against a pool. A BUY must not carry one, and the CHECK
    would refuse it if the INSERT tried."""
    _seed_position(ebull_test_conn, avg_cost=Decimal("50"))
    order_id = _claim(ebull_test_conn, action="BUY", order_params=None, exit_lot=None)

    assert load_recommendation_submission_context(ebull_test_conn, order_id=order_id).exit_avg_cost is None


@pytest.mark.parametrize(
    ("action", "position_id", "avg_cost"),
    [
        # A cost without a lot has nothing to be the cost of.
        ("EXIT", None, Decimal("1")),
        ("BUY", None, Decimal("1")),
        # `'NaN'::numeric > 0` is TRUE in Postgres.
        ("EXIT", 42, Decimal("NaN")),
        ("EXIT", 42, Decimal("0")),
        ("EXIT", 42, Decimal("-1")),
    ],
)
def test_the_check_refuses_a_cost_that_is_not_a_lot_s_usable_cost(
    ebull_test_conn: psycopg.Connection[tuple],
    action: str,
    position_id: int | None,
    avg_cost: Decimal,
) -> None:
    rec, _decision = _seed(ebull_test_conn, action=action)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "INSERT INTO orders (instrument_id, recommendation_id, action, order_type, status, "
            "raw_payload_json, created_at, recommendation_exit_position_id, recommendation_exit_units, "
            "recommendation_submission_context, recommendation_exit_avg_cost) "
            "VALUES (%s,%s,%s,'market','submitted','{}'::jsonb,%s,%s,%s,%s,%s)",
            (
                INSTRUMENT_ID,
                rec,
                action,
                _NOW,
                position_id,
                None if position_id is None else Decimal("1"),
                _CTX if position_id is not None else None,
                avg_cost,
            ),
        )
    ebull_test_conn.rollback()
