"""#3515 §0 — the v1 wind-down reader against real Postgres: each count reads the population the
live v1 code still acts on, and a terminal state alone does not end v1."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.ai_trial_wind_down import read_v1_declarations, wind_down_refusal
from tests.test_ai_trial_deadline_db import _opened_arm_leg
from tests.test_ai_trial_intent_db import SESSION, _declare

Conn = psycopg.Connection[Any]


def _halt(conn: Conn, declaration_id: int, to_state: str) -> None:
    conn.execute(
        "INSERT INTO ai_trial_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'active', %s, 'test', 'engine')",
        (declaration_id, to_state),
    )
    conn.commit()


def test_a_terminal_trial_with_an_open_leg_and_a_queued_leg_is_not_wound_down(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    # One published pair: the arm leg is submitted, filled and owned; the control leg is queued.
    _opened_arm_leg(conn, monkeypatch)
    [declaration] = read_v1_declarations(conn)
    assert (declaration.state, declaration.claimed_runs) == ("active", 0)
    assert (declaration.undispatched_legs, declaration.open_trades, declaration.active_ownerships) == (1, 1, 1)
    assert declaration.unfinished_pairs == 1
    assert declaration.outstanding() == (
        "trial_active",
        "undispatched_leg",
        "open_trade",
        "active_ownership",
        "unfinished_pair",
    )

    _halt(conn, declaration.declaration_id, "halted_harm")
    [declaration] = read_v1_declarations(conn)
    assert declaration.outstanding() == ("undispatched_leg", "open_trade", "active_ownership", "unfinished_pair")
    assert wind_down_refusal([declaration]) == "v1_not_wound_down:undispatched_leg"


def test_a_claimed_run_holds_the_wind_down_and_a_bare_terminal_trial_passes(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    assert wind_down_refusal(read_v1_declarations(conn)) == "v1_not_wound_down:declaration_missing"

    declaration_id, _ = _declare(conn, "v1", tamper=False)
    conn.commit()
    _halt(conn, declaration_id, "halted_loss")
    [declaration] = read_v1_declarations(conn)
    assert declaration.policy_hash is None  # the fixture document carries none
    assert wind_down_refusal([declaration]) is None

    conn.execute(
        "INSERT INTO ai_trial_runs (declaration_id, session_date, as_of, lease_until) "
        "SELECT %s, %s, now(), now() + interval '15 minutes'",
        (declaration_id, SESSION),
    )
    conn.commit()
    assert wind_down_refusal(read_v1_declarations(conn)) == "v1_not_wound_down:claimed_run"
