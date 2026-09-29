"""#3471 §9 halts against real Postgres: the loss halt reads a leg's close slices and its open
remainder at the manager's quote; the harm stop writes the look that fired. Both are terminal."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services.ai_trial_halts import HaltCheck, enforce_trial_halts
from app.services.ai_trial_readout import HarmLook
from tests.test_ai_trial_deadline_db import _POSITION_ID, _opened_arm_leg
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW

Conn = psycopg.Connection[Any]


def _states(conn: Conn) -> list[tuple[str, str, str]]:
    rows = conn.execute("SELECT to_state, actor, reason FROM ai_trial_state_events ORDER BY event_id").fetchall()
    conn.commit()
    return rows


def _enforce(conn: Conn) -> list[HaltCheck]:
    checks = enforce_trial_halts(conn, now=NOW + timedelta(days=1))
    conn.commit()
    return checks


def test_a_leg_loss_at_the_limit_halts_the_trial_once(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    row = conn.execute("SELECT declaration_id FROM ai_trial_declarations").fetchone()
    conn.commit()
    assert row is not None
    declaration_id = int(row[0])

    # 1.25 units opened at 100, bid 99: −1.25 unrealised. Nothing fires.
    assert _enforce(conn) == [HaltCheck(declaration_id, None, 0)]
    assert _states(conn)[-1][0] == "active"

    # Two partial-close slices of a quarter each, each under a new, unowned position id
    # sharing the entry orderId (eToro's booking), realise −198.75; the open half is −0.625 at the bid. −199.375.
    def _slice(position_id: int, pnl: str) -> None:
        conn.execute(
            """
            INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units,
                                      price, executed_at, fees_usd, realized_pnl_usd, investment_usd, order_id,
                                      source, raw_payload, recorded_at)
            VALUES (%s, %s, %s, 'close', 'sell', 0.3125, 90, %s, 0, %s, 31.25, 3471001, 'etoro_history', %s, %s)
            """,
            (position_id, ARM_INSTRUMENT, ARM_INSTRUMENT, NOW, pnl, Jsonb({}), NOW),
        )
        conn.commit()

    _slice(_POSITION_ID + 1, "-198.75")
    _slice(_POSITION_ID + 2, "0")
    assert _enforce(conn) == [HaltCheck(declaration_id, None, 0)]

    # The bid falls to 96: the open half is 0.625 × −4 = −2.5, total −201.25 ≥ 200.
    conn.execute("UPDATE quotes SET bid = 96 WHERE instrument_id = %s", (ARM_INSTRUMENT,))
    conn.commit()
    assert _enforce(conn) == [HaltCheck(declaration_id, "halted_loss", 0)]
    to_state, actor, reason = _states(conn)[-1]
    assert (to_state, actor) == ("halted_loss", "engine")
    assert reason.startswith("loss_halt:leg=arm:loss_usd=201.25:limit_usd=200.00")

    # Terminal: the trial is no longer checked, and nothing stacks.
    assert _enforce(conn) == []
    assert len(_states(conn)) == 2


def test_a_harm_look_that_halts_writes_halted_harm(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    looks = [
        HarmLook(1, 10, 8, 0.2, 0.025, False, flows_final=True),
        # Would halt, but a unit's flow window is still open: provisional, not acted on.
        HarmLook(2, 20, 9, 0.001, 0.0125, True, flows_final=False),
    ]
    monkeypatch.setattr(
        "app.services.ai_trial_halts.compute_readout", lambda *_, **__: SimpleNamespace(harm_looks=looks)
    )

    assert [check.halted for check in _enforce(conn)] == [None]
    looks[1] = HarmLook(2, 20, 9, 0.004, 0.0125, True, flows_final=True)
    (check,) = _enforce(conn)
    assert check.halted == "halted_harm"
    assert _states(conn)[-1][:2] == ("halted_harm", "engine")
    assert _states(conn)[-1][2] == "harm_look:k=2:units=20:clusters=9:p=0.004<0.0125"


def test_a_check_that_cannot_run_fails_closed_to_a_resumable_halt(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    row = conn.execute("SELECT declaration_id FROM ai_trial_declarations").fetchone()
    conn.commit()
    assert row is not None
    looks = [HarmLook(1, 10, 8, 0.001, 0.025, True, flows_final=True)]
    calls: list[str] = []

    def readout(_conn: Conn, *, strategy_version: str, as_of: Any) -> SimpleNamespace:
        calls.append(strategy_version)
        # The halt is written first, then this declaration's own check raises: its savepoint
        # takes the halt with it, and the pass reports it failed instead of raising.
        _conn.execute(
            "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
            "VALUES (%s, 'active', 'halted_operator', 'probe', 'engine')",
            (int(row[0]),),
        )
        raise RuntimeError("bo\x00om")

    monkeypatch.setattr("app.services.ai_trial_halts.compute_readout", readout)
    (check,) = _enforce(conn)
    assert (check.failed, check.halted) == (True, "halted_operator")
    # The probe's write went with the savepoint; the fail-closed halt is the only event.
    assert [(state, actor, reason) for state, actor, reason in _states(conn)][1:] == [
        # The NUL makes the detailed reason unwritable; the class-only reason still halts.
        ("halted_operator", "engine", "halt_check_failed:RuntimeError")
    ]
    assert calls == ["v1"]

    # The supervisor resumes; a clean pass then halts on the harm look.
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'halted_operator', 'active', 'resume', 'supervisor')",
        (int(row[0]),),
    )
    conn.commit()
    monkeypatch.setattr(
        "app.services.ai_trial_halts.compute_readout", lambda *_, **__: SimpleNamespace(harm_looks=looks)
    )
    assert [check.halted for check in _enforce(conn)] == ["halted_harm"]
