"""#2842 slice 6a — the step job's SQL mechanisms (``sql/447``): the first step creates every book's checkpoint, a
failed gate records a refusal, a stale session is stepped ``forced``, publication is fenced against stepping, and a
checkpoint only moves forward. Bars and the snapshot's universes are synthetic (``tests/test_ranking_pot_step.py``
covers the decisions); the database parts are real."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st
from app.services.ai_trial_pack import canonical_sha256
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_rebalance_db import _decided, _insert
from tests.test_ranking_pot_schema_db import _frozen
from tests.test_ranking_pot_step import FLAT, _rebalance

Conn = psycopg.Connection[Any]
SPY = sim.Bar(Decimal(500), Decimal(501), Decimal(499), Decimal(500))


def _at(session: date) -> datetime:
    return datetime.combine(session + timedelta(days=1), time(9, 10), UTC)


def _steps(conn: Conn) -> list[tuple[date, int | None, bool]]:
    rows = conn.execute("SELECT session, applied_attempt_id, forced FROM ranking_pot_steps ORDER BY session").fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def test_the_step_job_writes_books_refuses_forces_and_fences(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)  # S₀ = 2842, 2843, 2844
    d, decl = _first_window(conn)
    target = sim.next_session(d)
    snapshot = {"kind": rb.SNAPSHOT_KIND, "target_session": target.isoformat(), "last_session": d.isoformat()}
    _insert(
        conn,
        decl_id,
        target_session=target,
        month=target.replace(day=1),
        **_decided(snapshot=Jsonb(snapshot), snapshot_sha256=canonical_sha256(snapshot)),
    )
    attempt = conn.execute("SELECT attempt_id FROM ranking_pot_rebalance_attempts").fetchone()
    assert attempt is not None
    conn.commit()
    conn.autocommit = True

    r = {2842: "0.9", 2843: "0.8", 2844: "0.7"}
    monkeypatch.setattr(st, "book_terms", lambda _decl: (2, 3))
    monkeypatch.setattr(rb, "decode_snapshot", lambda doc: doc)
    monkeypatch.setattr(
        st.Rebalance, "of", classmethod(lambda cls, aid, _doc: replace_id(_rebalance(target, d, r, set(r)), aid))
    )
    spy: dict[str, sim.Bar | None] = {"bar": SPY}

    def bars(_conn: Conn, ids: Any, session: date) -> st.SessionBars:
        prev = {iid: {s: Decimal(100) for s in (d, target, session - timedelta(days=1))} for iid in r}
        return st.SessionBars(session, {iid: FLAT for iid in ids}, prev, spy["bar"])

    monkeypatch.setattr(st, "read_session_bars", bars)

    # Not due before 09:00 UTC on the day after the target session.
    early = st.run_step_job(conn, now=lambda: datetime.combine(target + timedelta(days=1), time(8), UTC))
    assert early.stepped == 0 and _steps(conn) == []

    first = st.run_step_job(conn, now=lambda: _at(target))
    assert first.stepped == 1 and _steps(conn) == [(target, attempt[0], False)]
    books = conn.execute(
        "SELECT book, last_session, instrument_ids FROM ranking_pot_book_checkpoints ORDER BY book"
    ).fetchall()
    assert [b[0] for b in books] == [0, 1, 2, 3] and {b[1] for b in books} == {target}
    assert sorted(books[0][2]) == [2842, 2843]  # the shadow entered the two best of F
    shadow = conn.execute("SELECT shadow, controls FROM ranking_pot_steps").fetchone()
    assert shadow is not None
    assert shadow[0]["decision"]["entries"] == [2842, 2843]
    assert len(shadow[1]["records"]) == 3 and len(shadow[1]["decision"]["missing_donors"]) == 3

    # Exactly once: the same fire again steps nothing.
    assert st.run_step_job(conn, now=lambda: _at(target)).stepped == 0

    # A failed gate (SPY missing) records a refusal and steps nothing.
    nxt = sim.next_session(target)
    spy["bar"] = None
    refused = st.run_step_job(conn, now=lambda: _at(nxt))
    assert refused.stepped == 0 and "bar_gate" in refused.note
    assert conn.execute("SELECT reason FROM ranking_pot_step_refusals").fetchall() == [("bar_gate",)]

    # Ten later sessions completed: the stale session is stepped forced; the loop stops at the first that is not.
    later = nxt
    for _ in range(st.FORCE_AFTER):
        later = sim.next_session(later)
    forced = st.run_step_job(conn, now=lambda: _at(later))
    steps = _steps(conn)
    assert forced.stepped >= 1 and steps[1] == (nxt, None, True)
    assert all(s[2] for s in steps[1:])

    # Policy drift: recorded, then the job fails visibly.
    monkeypatch.setattr(rb, "_policy_ok", lambda _decl: False)
    with pytest.raises(RuntimeError, match="policy_drift"):
        st.run_step_job(conn, now=lambda: _at(later))
    reasons = [r[0] for r in conn.execute("SELECT reason FROM ranking_pot_step_refusals ORDER BY refusal_id")]
    assert reasons[-1] == "policy_drift"

    # Publication fence, rebalance side: no decided row for a session already stepped.
    with pytest.raises(psycopg.errors.RaiseException, match="already stepped"):
        conn.execute(
            "INSERT INTO ranking_pot_rebalance_attempts (declaration_id, fired_at, target_session, month, outcome, "
            "scored_at, policy_hash, detail, snapshot, snapshot_sha256) "
            "VALUES (%s, now(), %s, %s, 'decided', now(), %s, '{}', '{}', %s)",
            (decl_id, nxt, nxt.replace(day=1), "a" * 64, "b" * 64),
        )
    # Publication fence, step side: a step for the decided session must apply it.
    with pytest.raises(psycopg.errors.RaiseException, match="must apply decided attempt"):
        conn.execute(
            "INSERT INTO ranking_pot_steps (declaration_id, session, stepped_at, policy_hash, forced, inputs, "
            "inputs_sha256, shadow, controls, checkpoint_sha256) "
            "VALUES (%s, %s, now(), %s, FALSE, '{}', %s, '{}', '{}', %s)",
            (decl_id, target, "a" * 64, "b" * 64, "c" * 64),
        )
    # Checkpoints only move forward; step rows are append-only.
    with pytest.raises(psycopg.errors.RaiseException, match="only moves forward"):
        conn.execute("UPDATE ranking_pot_book_checkpoints SET last_session = %s WHERE book = 0", (target,))
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("DELETE FROM ranking_pot_steps")
    assert decl.declaration_id == decl_id


def replace_id(rebalance: st.Rebalance, attempt_id: int) -> st.Rebalance:
    from dataclasses import replace

    return replace(rebalance, attempt_id=attempt_id)
