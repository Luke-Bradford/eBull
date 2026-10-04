"""#2842 slice 4b-i — ``sql/446`` rebalance attempts against real Postgres (spec §4).

One decided-or-skipped row per (declaration, month); outcome ↔ refusal ↔ snapshot coherence; append-only; no attempt
once the trial winds down. Then the writers and ``read_history`` / ``load_declaration`` over those rows.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_rebalance as rb
from tests.fixtures.ebull_test_db import test_database_url
from tests.test_ranking_pot_schema_db import _frozen, _move

# #3610: freezes a claim with no TrialDesign while testing something else (tests/conftest.py).
pytestmark = pytest.mark.usefixtures("assume_trial_powered")

Conn = psycopg.Connection[Any]

OCT, NOV, DEC = date(2026, 10, 1), date(2026, 11, 1), date(2026, 12, 1)
FIRE = datetime(2026, 10, 1, 23, 30, tzinfo=UTC)


def _insert(conn: Conn, decl: int, **over: Any) -> None:
    row: dict[str, Any] = {
        "declaration_id": decl,
        "fired_at": FIRE,
        "target_session": date(2026, 10, 2),
        "month": OCT,
        "outcome": "refused",
        "refusal": "price_daily_stale",
        "scored_at": None,
        "policy_hash": "a" * 64,
        "detail": Jsonb({}),
        "snapshot": None,
        "snapshot_sha256": None,
    }
    row.update(over)
    try:
        conn.execute(
            "INSERT INTO ranking_pot_rebalance_attempts "
            "(declaration_id, fired_at, target_session, month, outcome, refusal, scored_at, policy_hash, detail, "
            " snapshot, snapshot_sha256) "
            "VALUES (%(declaration_id)s, %(fired_at)s, %(target_session)s, %(month)s, %(outcome)s, %(refusal)s, "
            " %(scored_at)s, %(policy_hash)s, %(detail)s, %(snapshot)s, %(snapshot_sha256)s)",
            row,
        )
        conn.commit()
    except psycopg.Error:
        conn.rollback()
        raise


def _decided(**over: Any) -> dict[str, Any]:
    return {
        "outcome": "decided",
        "refusal": None,
        "scored_at": FIRE,
        "snapshot": Jsonb({"kind": rb.SNAPSHOT_KIND}),
        "snapshot_sha256": "b" * 64,
        "detail": Jsonb({"r_count": 1650}),
    } | over


def test_row_coherence_constraints(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    bad: list[dict[str, Any]] = [
        _decided(snapshot=None, snapshot_sha256=None),  # decided needs its snapshot
        _decided(refusal="price_daily_stale"),  # decided carries no refusal
        _decided(scored_at=None),  # decided names its scores run
        _decided(snapshot_sha256=None),  # snapshot and sha together
        {"refusal": None},  # refused needs its code
        {"refusal": "not_attempted"},  # only a skip is not_attempted
        {"refusal": "made_up"},
        {"snapshot": Jsonb({}), "snapshot_sha256": "b" * 64},  # a refusal stores no snapshot
        {"month": date(2026, 10, 2)},  # month is a first-of-month
        {"month": NOV},  # refused/decided rows are about the target session's month
        {"outcome": "skipped", "refusal": "not_attempted", "month": NOV},  # a skip cannot close a future month
    ]
    for over in bad:
        with pytest.raises(psycopg.errors.CheckViolation):
            _insert(conn, decl, **over)
    _insert(conn, decl)  # the default refused row is valid
    _insert(conn, decl, outcome="skipped", refusal="not_attempted", month=date(2026, 9, 1))


def test_one_decided_or_skipped_row_per_month(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    _insert(conn, decl)
    _insert(conn, decl)  # refusals repeat freely
    _insert(conn, decl, **_decided())
    for over in (_decided(), {"outcome": "skipped", "refusal": "price_daily_stale"}):
        with pytest.raises(psycopg.errors.UniqueViolation):
            _insert(conn, decl, **over)
    _insert(conn, decl)  # a late refused row is still only history


def test_append_only_and_no_attempt_after_wind_down(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _frozen(conn)
    _insert(conn, decl)
    for sql in (
        "UPDATE ranking_pot_rebalance_attempts SET refusal = 'ranking_drift' WHERE declaration_id = %s",
        "DELETE FROM ranking_pot_rebalance_attempts WHERE declaration_id = %s",
    ):
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute(sql, (decl,))
        conn.rollback()
    _move(conn, decl, "shadow_only", "winding_down", "operator", "operator")
    with pytest.raises(psycopg.errors.RaiseException, match="no rebalance runs"):
        _insert(conn, decl)


def test_writers_and_history_round_trip(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    decl = rb.load_declaration(conn)
    conn.commit()
    assert decl is not None and decl.declaration_id == decl_id and decl.state == "shadow_only"
    assert decl.s0_ids == (2842, 2843, 2844)

    oct_fire = datetime(2026, 10, 2, 23, 30, tzinfo=UTC)
    due = rb.plan(oct_fire, first=OCT, resolved=frozenset())
    rb.record_refused(conn, decl, due, rb.Refused("universe_collapse", {"r_count": 1}), as_of=oct_fire, scored_at=FIRE)
    conn.commit()
    history = rb.read_history(conn, decl_id)
    assert history.last_refusal == {OCT: "universe_collapse"} and history.resolved == frozenset()

    # Downtime to Dec 10: October (last refusal universe_collapse), November (never fired) and December
    # (past its sixth session) all close in one fire.
    late = datetime(2026, 12, 10, 23, 30, tzinfo=UTC)
    due = rb.plan(late, first=OCT, resolved=history.resolved)
    assert due.skips == (OCT, NOV, DEC)
    assert rb.record_skips(conn, decl, due, history, as_of=late) == 3
    conn.commit()
    rows = conn.execute(
        "SELECT month, refusal FROM ranking_pot_rebalance_attempts WHERE outcome = 'skipped' ORDER BY month"
    ).fetchall()
    conn.commit()
    assert rows == [(OCT, "universe_collapse"), (NOV, "not_attempted"), (DEC, "not_attempted")]
    history = rb.read_history(conn, decl_id)
    assert history.resolved == {OCT, NOV, DEC} and history.last_refusal == {}
    assert (history.previous_r_count, history.consecutive_collapses) == (None, 0)

    _insert(conn, decl_id, **_decided(month=date(2027, 1, 1), target_session=date(2027, 1, 4)))
    history = rb.read_history(conn, decl_id)
    assert history.previous_r_count == 1650


def test_repeatable_read_attempts_need_the_state_lock_taken_before_the_snapshot(ebull_test_conn: Conn) -> None:
    """Codex ckpt-2: under REPEATABLE READ a row lock cannot refresh the trigger's view of the state."""
    conn = ebull_test_conn
    decl = _frozen(conn)
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        with pytest.raises(psycopg.errors.RaiseException, match="must hold SHARE"):
            _insert_raw(conn, decl)
    with conn.transaction():
        rb.begin_rebalance(conn)
        _insert_raw(conn, decl)  # holds the lock: admitted
    with conn.transaction():
        conn.execute("SELECT 1")
        with pytest.raises(psycopg.errors.ActiveSqlTransaction):
            rb.begin_rebalance(conn)  # a late call cannot pass: a snapshot may already exist

    # An uncommitted wind-down blocks the rebalance from taking its snapshot; once it commits, the rebalance
    # sees it and the attempt is refused.
    with psycopg.connect(test_database_url()) as other:
        other.execute(
            "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, wind_down_reason, reason, "
            "actor) VALUES (%s, 'shadow_only', 'winding_down', 'operator', 't', 'operator')",
            (decl,),
        )
        conn.execute("SET lock_timeout = '200ms'")
        conn.commit()
        with pytest.raises(psycopg.errors.LockNotAvailable), conn.transaction():
            rb.begin_rebalance(conn)
        other.commit()
    conn.execute("SET lock_timeout = 0")
    conn.commit()
    with conn.transaction(), pytest.raises(psycopg.errors.RaiseException, match="no rebalance runs"):
        rb.begin_rebalance(conn)
        _insert_raw(conn, decl)


def _insert_raw(conn: Conn, decl: int) -> None:
    conn.execute(
        "INSERT INTO ranking_pot_rebalance_attempts "
        "(declaration_id, fired_at, target_session, month, outcome, refusal, policy_hash) "
        "VALUES (%s, %s, %s, %s, 'refused', 'price_daily_stale', %s)",
        (decl, FIRE, date(2026, 10, 2), OCT, "a" * 64),
    )
