"""#2961 window B — the attended release against a real database and a real dead sender.

The stranded row is produced the way production produces one: the #2949 harness
child runs ``execute_core_rebalance``, commits ``mark_core_submission_entered`` and is
SIGKILLed inside the provider call (``after_commit_before_submit``). Its pid is
therefore genuinely dead on this host, so the liveness probe is the real ``os.kill``.

The witness broker and the clock are doubles: the broker answers only the one
informational read, and the clock's ``sleep`` advances time instead of waiting
``WINDOW_B_VISIBILITY_WAIT``.
"""

from __future__ import annotations

import json
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
import pytest

from app.services import strategy_core_window_b_release as window_b
from app.services.strategy_core_executor import load_core_resume_authority
from app.services.strategy_core_submission_gate import CORE_SUBMISSION_ADVISORY_LOCK
from app.services.strategy_core_window_b_release import (
    WINDOW_B_RELEASE_ERROR_CODE,
    WINDOW_B_VISIBILITY_WAIT,
    release_window_b_core_entry,
)
from tests.fixtures.core_restart import (
    API_CREDENTIAL_ID,
    OPERATOR_ID,
    SIGKILL_RETURNCODE,
    USER_CREDENTIAL_ID,
    core_state_report,
    run_engine_until_fault,
    seed_core_execution_world,
    select_core_instrument,
)
from tests.fixtures.ebull_test_db import test_database_url


@pytest.fixture
def stranded_order(
    ebull_test_conn: psycopg.Connection[Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> int:
    """A window-B row left by a sender that really died after the marker."""
    from app.services import strategy_core_selection

    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_OUTCOME", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_INSTRUMENT_ID", None, raising=False)
    monkeypatch.setattr(strategy_core_selection, "SELECTED_CORE_EVIDENCE_REF", None, raising=False)
    select_core_instrument()
    seed_core_execution_world(ebull_test_conn)
    process = run_engine_until_fault(
        database_url=test_database_url(), workdir=tmp_path, fault="after_commit_before_submit"
    )
    assert process.returncode == SIGKILL_RETURNCODE
    assert core_state_report(ebull_test_conn)["submission_phases"] == ["broker_verb_entered"]
    assert json.loads((tmp_path / "broker.json").read_text())["mutation_calls"] == 0
    authority = load_core_resume_authority(ebull_test_conn)
    assert authority is not None
    return authority.order_id


@dataclass
class _FakeClock:
    mono: float = 1000.0
    offset: timedelta = timedelta(0)
    slept: list[float] = field(default_factory=list)

    def monotonic(self) -> float:
        return self.mono

    def now(self) -> datetime:
        return datetime.now(UTC) + self.offset

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.mono += seconds
        self.offset += timedelta(seconds=seconds)


@dataclass
class _Snapshot:
    raw_payload: dict[str, Any]
    observed_at: datetime


@dataclass
class _WitnessBroker:
    """Only the informational read exists, so any mutation would be an AttributeError."""

    clock: _FakeClock
    payload: dict[str, Any]
    reads: int = 0
    bound_to: list[tuple[UUID, UUID, UUID]] = field(default_factory=list)

    def get_account_risk_snapshot(self) -> _Snapshot:
        self.reads += 1
        return _Snapshot(self.payload, self.clock.now())

    @contextmanager
    def factory(self, operator_id: UUID, api_key_id: UUID, user_key_id: UUID) -> Iterator[Any]:
        self.bound_to.append((operator_id, api_key_id, user_key_id))
        yield self


def _empty_account() -> dict[str, Any]:
    return {"clientPortfolio": {"positions": [], "ordersForOpen": [], "orders": []}}


def _release(conn: psycopg.Connection[Any], order_id: int, broker: _WitnessBroker) -> Any:
    return release_window_b_core_entry(
        conn,
        order_id=order_id,
        operator_id="attending-operator",
        attestation="  checked the demo account by hand  ",
        broker_factory=broker.factory,
        environment="demo",
        clock=broker.clock,
        attendance=lambda: None,
    )


def _row_state(conn: psycopg.Connection[Any], order_id: int) -> tuple[Any, ...]:
    row = conn.execute(
        """
        SELECT state.state, state.last_error_code, o.status, trade.status
        FROM strategy_order_reconciliation_state state
        JOIN orders o ON o.order_id = state.order_id
        JOIN strategy_trade_orders link ON link.order_id = o.order_id
        JOIN strategy_trades trade ON trade.strategy_trade_id = link.strategy_trade_id
        WHERE state.order_id=%s
        """,
        (order_id,),
    ).fetchone()
    conn.commit()
    assert row is not None
    return tuple(row)


def _audits(conn: psycopg.Connection[Any]) -> list[tuple[str, str, dict[str, Any]]]:
    rows = conn.execute(
        "SELECT pass_fail, model_version, evidence_json FROM decision_audit "
        "WHERE stage='core_window_b_release' ORDER BY decision_id"
    ).fetchall()
    conn.commit()
    return [(str(r[0]), str(r[1]), dict(r[2])) for r in rows]


def test_the_marker_records_the_sender_identity(ebull_test_conn: psycopg.Connection[Any], stranded_order: int) -> None:
    row = ebull_test_conn.execute(
        "SELECT submission_entered_at, submission_entered_pid, submission_entered_host "
        "FROM strategy_order_reconciliation_state WHERE order_id=%s",
        (stranded_order,),
    ).fetchone()
    ebull_test_conn.commit()
    assert row is not None
    entered_at, pid, host = row
    assert entered_at is not None
    assert isinstance(pid, int) and pid > 0
    assert host == socket.gethostname()


def test_a_dead_sender_and_a_clean_witness_release_the_authority(
    ebull_test_conn: psycopg.Connection[Any], stranded_order: int
) -> None:
    clock = _FakeClock()
    broker = _WitnessBroker(clock, _empty_account())

    result = _release(ebull_test_conn, stranded_order, broker)

    assert (result.passed, result.slug) == (True, WINDOW_B_RELEASE_ERROR_CODE)
    assert _row_state(ebull_test_conn, stranded_order) == (
        "rejected",
        WINDOW_B_RELEASE_ERROR_CODE,
        "rejected",
        "failed",
    )
    # The wait was applied, in full, before the one read.
    assert sum(clock.slept) >= WINDOW_B_VISIBILITY_WAIT.total_seconds() - 1
    assert broker.reads == 1
    assert broker.bound_to == [(OPERATOR_ID, API_CREDENTIAL_ID, USER_CREDENTIAL_ID)]
    audits = _audits(ebull_test_conn)
    assert [(pass_fail, version) for pass_fail, version, _ in audits] == [("PASS", "core-window-b-v2")]
    evidence = audits[0][2]
    assert evidence["attestation"] == "checked the demo account by hand"
    assert evidence["operator_id"] == "attending-operator"
    assert evidence["witness"]["total_positions"] == 0
    assert result.decision_id is not None
    # The released row no longer holds the arm: nothing non-terminal remains.
    assert load_core_resume_authority(ebull_test_conn) is None


def test_a_pending_order_on_the_instrument_refuses_and_is_audited(
    ebull_test_conn: psycopg.Connection[Any], stranded_order: int
) -> None:
    before = _row_state(ebull_test_conn, stranded_order)
    instrument_id = int(
        ebull_test_conn.execute("SELECT instrument_id FROM orders WHERE order_id=%s", (stranded_order,)).fetchone()[0]  # type: ignore[index]
    )
    ebull_test_conn.commit()
    payload = _empty_account()
    payload["clientPortfolio"]["ordersForOpen"] = [{"instrumentID": instrument_id, "mirrorID": 0, "amount": 100}]

    result = _release(ebull_test_conn, stranded_order, _WitnessBroker(_FakeClock(), payload))

    assert (result.passed, result.slug) == (False, "window_b_witness_pending_order")
    assert _row_state(ebull_test_conn, stranded_order) == before
    audits = _audits(ebull_test_conn)
    assert [a[0] for a in audits] == ["FAIL"]
    assert audits[0][2]["refusal"] == "window_b_witness_pending_order"
    assert audits[0][2]["witness"]["orders_for_open_on_instrument"] == payload["clientPortfolio"]["ordersForOpen"]


def test_a_core_lock_held_elsewhere_refuses_and_is_audited(
    ebull_test_conn: psycopg.Connection[Any], stranded_order: int
) -> None:
    before = _row_state(ebull_test_conn, stranded_order)
    broker = _WitnessBroker(_FakeClock(), _empty_account())
    with psycopg.connect(test_database_url()) as submitter:
        submitter.execute("SELECT pg_advisory_lock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)
        submitter.commit()
        result = _release(ebull_test_conn, stranded_order, broker)
        submitter.execute("SELECT pg_advisory_unlock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)
        submitter.commit()
    assert (result.passed, result.slug) == (False, "window_b_core_lock_busy")
    assert _row_state(ebull_test_conn, stranded_order) == before
    assert broker.reads == 0
    assert [a[0] for a in _audits(ebull_test_conn)] == ["FAIL"]


def test_a_caller_already_holding_the_core_key_refuses(
    ebull_test_conn: psycopg.Connection[Any], stranded_order: int
) -> None:
    broker = _WitnessBroker(_FakeClock(), _empty_account())
    ebull_test_conn.execute("SELECT pg_advisory_lock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)
    ebull_test_conn.commit()
    try:
        result = _release(ebull_test_conn, stranded_order, broker)
    finally:
        ebull_test_conn.execute("SELECT pg_advisory_unlock(%s, %s)", CORE_SUBMISSION_ADVISORY_LOCK)
        ebull_test_conn.commit()
    assert (result.passed, result.slug) == (False, "window_b_caller_holds_core_key")
    assert broker.reads == 0


def test_candidacy_changed_before_the_locks_refuses(
    ebull_test_conn: psycopg.Connection[Any],
    stranded_order: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The row moves between the unlocked read and the locked re-read."""
    real_holds = window_b._holds_core_key

    def holds_and_move_the_row(conn: psycopg.Connection[Any]) -> bool:
        with psycopg.connect(test_database_url()) as other:
            other.execute(
                "UPDATE strategy_order_reconciliation_state SET submission_entered_pid = submission_entered_pid + 1 "
                "WHERE order_id=%s",
                (stranded_order,),
            )
            other.commit()
        return real_holds(conn)

    monkeypatch.setattr(window_b, "_holds_core_key", holds_and_move_the_row)
    broker = _WitnessBroker(_FakeClock(), _empty_account())
    result = _release(ebull_test_conn, stranded_order, broker)
    assert (result.passed, result.slug) == (False, "window_b_candidate_changed")
    assert broker.reads == 0
    assert _row_state(ebull_test_conn, stranded_order)[0] != "rejected"


def test_an_unrecorded_sender_refuses(ebull_test_conn: psycopg.Connection[Any], stranded_order: int) -> None:
    """A row marked before ``sql/410`` carries no identity and can never be released."""
    ebull_test_conn.execute(
        "UPDATE strategy_order_reconciliation_state SET submission_entered_pid = NULL WHERE order_id=%s",
        (stranded_order,),
    )
    ebull_test_conn.commit()
    broker = _WitnessBroker(_FakeClock(), _empty_account())
    result = _release(ebull_test_conn, stranded_order, broker)
    assert (result.passed, result.slug) == (False, "window_b_sender_identity_unrecorded")
    assert broker.reads == 0
