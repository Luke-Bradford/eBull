"""A broker-side whole close (SL/TP fired) releases core ownership — the SQL + writes (#2965).

One DB test for the new mechanism, per the lean-tests rule; the verdict's shapes are
table-tested in ``tests/test_2965_whole_close_release.py``.  The world is #2949's: a real
entry run to one active core ownership, then the fake broker closes the position
WITHOUT any engine close request -- exactly what a broker-side stop does.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerProvider
from app.services.strategy_engine_capital import load_engine_capital_authority
from app.services.strategy_order_reconciliation import reconcile_backlog
from app.services.strategy_position_manager import manage_owned_position
from tests.fixtures.core_restart import (
    CLOCK,
    CORE_INSTRUMENT_ID,
    FileBackedFakeBroker,
    close_state_report,
    core_ownership_coordinates,
    core_state_report,
    record_whole_close_witness,
    run_engine_until_fault,
    seed_core_execution_world,
    select_core_instrument,
)
from tests.fixtures.ebull_test_db import test_database_url

UNITS = "0.500000"
DOLLARS = "250.00"


@pytest.fixture
def core_world(ebull_test_conn: psycopg.Connection[Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_OUTCOME", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    return tmp_path


def _owned_then_stopped_out(conn: psycopg.Connection[Any], workdir: Path) -> tuple[FileBackedFakeBroker, int, int, int]:
    process = run_engine_until_fault(database_url=test_database_url(), workdir=workdir, fault="none")
    assert process.returncode == 0, process.stderr
    broker = FileBackedFakeBroker(workdir / "broker.json")
    assert [r.state for r in reconcile_backlog(conn, broker=cast(BrokerProvider, broker), limit=20)] == ["resolved"]
    trade_id, position_id = core_ownership_coordinates(conn)
    ref = conn.execute(
        """
        SELECT o.broker_order_ref FROM strategy_trade_orders link
        JOIN orders o ON o.order_id=link.order_id
        WHERE link.strategy_trade_id=%s AND link.purpose='entry'
        """,
        (trade_id,),
    ).fetchone()
    conn.commit()
    assert ref is not None
    state = broker.read()
    for record in state["orders"]:
        if int(record["position_id"]) == position_id:
            record["closed"] = True  # the broker's own stop: no engine close request
    broker._write(state)
    return broker, trade_id, position_id, int(ref[0])


def _record_events(conn: psycopg.Connection[Any], *, position_id: int, order_ref: int, close_units: str) -> None:
    record_whole_close_witness(
        conn, position_id=position_id, order_ref=order_ref, close_units=close_units, units=UNITS, dollars=DOLLARS
    )


def _manage(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker, trade_id: int, position_id: int) -> Any:
    return manage_owned_position(
        conn,
        broker=cast(BrokerProvider, broker),
        strategy_trade_id=trade_id,
        broker_position_id=position_id,
        now=CLOCK,
    )


def test_a_broker_side_whole_close_releases_ownership_and_closes_the_trade(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    broker, trade_id, position_id, order_ref = _owned_then_stopped_out(ebull_test_conn, core_world)

    # Before the history row lands: today's behaviour, unchanged.
    waiting = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (waiting.state, waiting.reason_code) == ("reconcile_required", "owned_position_missing")

    _record_events(ebull_test_conn, position_id=position_id, order_ref=order_ref, close_units=UNITS)
    released = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (released.state, released.reason_code) == ("applied", "broker_closed_externally")

    report = close_state_report(ebull_test_conn)
    assert report["active_ownership"] == 0
    assert report["released_ownership"] == 1
    assert core_state_report(ebull_test_conn)["trade_statuses"] == ["closed"]
    row = ebull_test_conn.execute(
        "SELECT released_at, release_reason FROM strategy_position_ownership WHERE broker_position_id=%s",
        (position_id,),
    ).fetchone()
    ebull_test_conn.commit()
    assert row == (CLOCK - timedelta(hours=1), "broker_closed_externally")
    assert broker.read().get("close_calls", 0) == 0

    # The wedge this ticket exists for: the capital reader no longer iterates the id.
    authority = load_engine_capital_authority(ebull_test_conn)
    ebull_test_conn.rollback()
    assert authority is not None
    assert position_id not in authority.core_active_position_ids


def test_a_partial_close_under_the_live_id_stays_wedged(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    broker, trade_id, position_id, order_ref = _owned_then_stopped_out(ebull_test_conn, core_world)
    _record_events(ebull_test_conn, position_id=position_id, order_ref=order_ref, close_units="0.250000")

    refused = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (refused.state, refused.reason_code) == ("reconcile_required", "owned_position_missing")
    assert close_state_report(ebull_test_conn)["active_ownership"] == 1


def test_an_unowned_slice_under_the_entry_order_is_a_partial_close_and_refuses(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    """The attended 2026-09-23 shape: the partial slice is booked under a NEW position id."""
    broker, trade_id, position_id, order_ref = _owned_then_stopped_out(ebull_test_conn, core_world)
    _record_events(ebull_test_conn, position_id=position_id, order_ref=order_ref, close_units=UNITS)
    ebull_test_conn.execute(
        """
        INSERT INTO trade_events (position_id,etoro_instrument_id,instrument_id,event_kind,side,units,
                                  executed_at,realized_pnl_usd,order_id,source,raw_payload)
        VALUES (%s,%s,%s,'close','sell',0.1,%s,0,%s,'etoro_history',%s)
        """,
        (
            position_id + 1,
            CORE_INSTRUMENT_ID,
            CORE_INSTRUMENT_ID,
            CLOCK - timedelta(hours=2),
            order_ref,
            Jsonb({"orderId": order_ref, "positionId": position_id + 1}),
        ),
    )
    ebull_test_conn.commit()

    refused = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (refused.state, refused.reason_code) == ("reconcile_required", "owned_position_missing")
    assert close_state_report(ebull_test_conn)["active_ownership"] == 1
