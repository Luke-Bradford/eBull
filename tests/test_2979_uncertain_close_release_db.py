"""An uncertain engine close no longer blocks the whole-close release (#2979 half a).

Spec: ``docs/proposals/execution/2026-09-23-uncertain-close-witness-release.md``.  The
inverted scenario 7b in ``tests/test_2949_core_close_recovery_db.py`` is the end-to-end
crash; this file pins the edges -- which ops the witness may settle, the CHECK, the
rollback guard, ``_finish_close``'s stamp and the two core readers.  Uncertain ops are
seeded directly, in exactly the shape ``strategy_position_manager`` writes them.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerProvider
from app.services import strategy_position_manager
from app.services.strategy_core_executor import _core_sell_target
from app.services.strategy_core_preflight import _PREFLIGHT_SQL
from app.services.strategy_order_reconciliation import reconcile_backlog
from app.services.strategy_position_manager import StrategyPositionManagerError, manage_owned_position
from tests.fixtures.core_restart import (
    CLOCK,
    CORE_INSTRUMENT_ID,
    FileBackedFakeBroker,
    close_state_report,
    core_ownership_coordinates,
    entry_broker_order_ref,
    record_whole_close_witness,
    run_engine_until_fault,
    seed_core_execution_world,
    select_core_instrument,
)
from tests.fixtures.ebull_test_db import test_database_url

CRASH = "crash_before_submission_identity"
UNCERTAIN = "broker_close_uncertain"


@pytest.fixture
def core_world(ebull_test_conn: psycopg.Connection[Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_OUTCOME", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    return tmp_path


def _owned(conn: psycopg.Connection[Any], workdir: Path) -> tuple[FileBackedFakeBroker, int, int]:
    process = run_engine_until_fault(database_url=test_database_url(), workdir=workdir, fault="none")
    assert process.returncode == 0, process.stderr
    broker = FileBackedFakeBroker(workdir / "broker.json")
    assert [r.state for r in reconcile_backlog(conn, broker=cast(BrokerProvider, broker), limit=20)] == ["resolved"]
    trade_id, position_id = core_ownership_coordinates(conn)
    return broker, trade_id, position_id


def _closed_at_broker(broker: FileBackedFakeBroker, position_id: int) -> None:
    state = broker.read()
    for record in state["orders"]:
        if int(record["position_id"]) == position_id:
            record["closed"] = True
    broker._write(state)


def _seed_uncertain_close(
    conn: psycopg.Connection[Any],
    *,
    position_id: int,
    code: str | None,
    response: dict[str, Any] | None = None,
) -> int:
    """One terminal close op + its EXIT order, as ``_submit_close``/resume leave them."""
    request_id = uuid4()
    order = conn.execute(
        """
        INSERT INTO orders (instrument_id, action, order_type, status, raw_payload_json,
                            execution_origin, strategy_request_id)
        VALUES (%s,'EXIT','MARKET','submitted',%s,'strategy',%s)
        RETURNING order_id
        """,
        (CORE_INSTRUMENT_ID, None if response is None else Jsonb(response), request_id),
    ).fetchone()
    assert order is not None
    op = conn.execute(
        """
        INSERT INTO strategy_position_operations (
            ownership_id, order_id, operation_type, trigger_code, request_id, status,
            last_error_code, resolved_at
        )
        SELECT ownership_id, %s, 'close', 'strategy_exit', %s, 'reconcile_required', %s, now()
        FROM strategy_position_ownership WHERE broker_position_id=%s
        RETURNING position_operation_id
        """,
        (int(order[0]), request_id, code, position_id),
    ).fetchone()
    assert op is not None
    conn.commit()
    return int(op[0])


def _manage(conn: psycopg.Connection[Any], broker: FileBackedFakeBroker, trade_id: int, position_id: int) -> Any:
    return manage_owned_position(
        conn,
        broker=cast(BrokerProvider, broker),
        strategy_trade_id=trade_id,
        broker_position_id=position_id,
        now=CLOCK,
    )


def _stamps(conn: psycopg.Connection[Any]) -> list[bool]:
    rows = conn.execute(
        "SELECT broker_close_witnessed_at IS NOT NULL FROM strategy_position_operations ORDER BY position_operation_id"
    ).fetchall()
    conn.commit()
    return [bool(row[0]) for row in rows]


def _preflight_outstanding(conn: psycopg.Connection[Any]) -> bool:
    row = conn.execute(_PREFLIGHT_SQL, {"core_instrument_id": CORE_INSTRUMENT_ID}).fetchone()
    conn.commit()
    assert row is not None
    return bool(row[-1])


def test_both_uncertain_codes_release_are_stamped_and_clear_the_preflight(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    broker, trade_id, position_id = _owned(ebull_test_conn, core_world)
    _closed_at_broker(broker, position_id)
    _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=CRASH)
    # A response that names OUR position is not a divergence.
    _seed_uncertain_close(
        ebull_test_conn,
        position_id=position_id,
        code=UNCERTAIN,
        response={"orderForClose": {"positionID": position_id}},
    )
    assert _preflight_outstanding(ebull_test_conn) is True

    record_whole_close_witness(
        ebull_test_conn, position_id=position_id, order_ref=entry_broker_order_ref(ebull_test_conn, trade_id)
    )
    released = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (released.state, released.reason_code) == ("applied", "broker_closed_after_uncertain_close")

    report = close_state_report(ebull_test_conn)
    assert report["active_ownership"] == 0
    assert report["close_statuses"] == ["reconcile_required", "reconcile_required"]
    assert report["exit_order_statuses"] == ["submitted"]
    assert _stamps(ebull_test_conn) == [True, True]
    # §4 keeps "active or released"; the op stops counting because it is witnessed.
    assert _preflight_outstanding(ebull_test_conn) is False


@pytest.mark.parametrize(
    ("code", "response"),
    [
        pytest.param(UNCERTAIN, {"orderForClose": {"positionID": 1, "orderID": 9}}, id="response-names-another"),
        pytest.param(UNCERTAIN, {"orderForClose": {"positionID": "0"}}, id="response-non-canonical"),
        pytest.param("close_order_did_not_affect_exact_position", None, id="divergent-code"),
        pytest.param(None, None, id="null-code"),
    ],
)
def test_a_close_the_witness_cannot_settle_keeps_blocking(
    ebull_test_conn: psycopg.Connection[Any],
    core_world: Path,
    code: str | None,
    response: dict[str, Any] | None,
) -> None:
    broker, trade_id, position_id = _owned(ebull_test_conn, core_world)
    _closed_at_broker(broker, position_id)
    _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=code, response=response)
    record_whole_close_witness(
        ebull_test_conn, position_id=position_id, order_ref=entry_broker_order_ref(ebull_test_conn, trade_id)
    )

    refused = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (refused.state, refused.reason_code) == ("reconcile_required", "owned_position_missing")
    assert close_state_report(ebull_test_conn)["active_ownership"] == 1
    assert _stamps(ebull_test_conn) == [False]
    assert _preflight_outstanding(ebull_test_conn) is True


def test_a_stamp_set_that_changed_rolls_the_whole_release_back(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker, trade_id, position_id = _owned(ebull_test_conn, core_world)
    _closed_at_broker(broker, position_id)
    _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=CRASH)
    record_whole_close_witness(
        ebull_test_conn, position_id=position_id, order_ref=entry_broker_order_ref(ebull_test_conn, trade_id)
    )
    monkeypatch.setattr(strategy_position_manager, "stamp_uncertain_closes", lambda *a, **k: 0)

    with pytest.raises(StrategyPositionManagerError, match="uncertain close set changed"):
        _manage(ebull_test_conn, broker, trade_id, position_id)
    ebull_test_conn.rollback()
    report = close_state_report(ebull_test_conn)
    assert report["active_ownership"] == 1
    assert report["released_ownership"] == 0


@pytest.mark.parametrize("code", ["close_order_did_not_affect_exact_position", None])
def test_the_check_refuses_a_stamp_on_a_close_it_does_not_cover(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path, code: str | None
) -> None:
    _, _, position_id = _owned(ebull_test_conn, core_world)
    op_id = _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=code)
    with pytest.raises(psycopg.errors.CheckViolation):
        ebull_test_conn.execute(
            "UPDATE strategy_position_operations SET broker_close_witnessed_at=now() WHERE position_operation_id=%s",
            (op_id,),
        )
    ebull_test_conn.rollback()


def test_a_later_filled_close_stamps_an_earlier_uncertain_one(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    """§4b: without the stamp the earlier op would hold the core preflight for ever."""
    broker, trade_id, position_id = _owned(ebull_test_conn, core_world)
    earlier = _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=UNCERTAIN)
    divergent = _seed_uncertain_close(
        ebull_test_conn, position_id=position_id, code="close_order_did_not_affect_exact_position"
    )

    submitted = manage_owned_position(
        ebull_test_conn,
        broker=cast(BrokerProvider, broker),
        strategy_trade_id=trade_id,
        broker_position_id=position_id,
        close_reason="operator_close",
        now=CLOCK,
    )
    assert submitted.state == "submitted"
    finished = _manage(ebull_test_conn, broker, trade_id, position_id)
    assert (finished.state, finished.reason_code) == ("applied", "exact_position_closed")

    rows = dict(
        ebull_test_conn.execute(
            "SELECT position_operation_id, broker_close_witnessed_at IS NOT NULL FROM strategy_position_operations"
        ).fetchall()
    )
    ebull_test_conn.commit()
    assert rows[earlier] is True
    assert rows[divergent] is False
    # The divergent op still holds the preflight: only witnessed ops stop counting.
    assert _preflight_outstanding(ebull_test_conn) is True


def test_the_core_sell_target_skips_only_a_witnessed_close(
    ebull_test_conn: psycopg.Connection[Any], core_world: Path
) -> None:
    _, _, position_id = _owned(ebull_test_conn, core_world)
    op_id = _seed_uncertain_close(ebull_test_conn, position_id=position_id, code=CRASH)
    assert _core_sell_target(ebull_test_conn) == "core_operation_outstanding"

    ebull_test_conn.execute(
        "UPDATE strategy_position_operations SET broker_close_witnessed_at=now() WHERE position_operation_id=%s",
        (op_id,),
    )
    ebull_test_conn.commit()
    assert _core_sell_target(ebull_test_conn) != "core_operation_outstanding"
