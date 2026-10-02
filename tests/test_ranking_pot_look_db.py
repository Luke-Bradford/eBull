"""#2842 slice 6b — the looks' SQL mechanisms (``sql/448``): a look is stored once its endpoint is stepped, exactly
once; ``look_pending`` clears with it; the last look winds the trial down through the state writer; the activation
and completion fences. Endpoints are pulled in to the next sessions and bars are synthetic
(``tests/test_ranking_pot_look.py`` covers the arithmetic); the database parts are real."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_sim as sim
from app.services import ranking_pot_step as st
from app.services.ai_trial_pack import canonical_sha256
from app.services.market_calendar import us_market_status
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_rebalance_db import _decided, _insert
from tests.test_ranking_pot_schema_db import _frozen, _move
from tests.test_ranking_pot_step import FLAT, _rebalance
from tests.test_ranking_pot_step_db import SPY, _at, replace_id

Conn = psycopg.Connection[Any]


def _decide(conn: Conn, decl_id: int, target: date, last: date) -> int:
    snapshot = {
        "kind": rb.SNAPSHOT_KIND,
        "target_session": target.isoformat(),
        "last_session": last.isoformat(),
        "spy": {"bid": "499.9", "ask": "500.1"},
    }
    _insert(
        conn,
        decl_id,
        target_session=target,
        month=target.replace(day=1),
        **_decided(snapshot=Jsonb(snapshot), snapshot_sha256=canonical_sha256(snapshot)),
    )
    row = conn.execute("SELECT max(attempt_id) FROM ranking_pot_rebalance_attempts").fetchone()
    assert row is not None
    return int(row[0])


def _state(conn: Conn, decl_id: int) -> tuple[str, str | None]:
    row = conn.execute(
        "SELECT to_state, wind_down_reason FROM ranking_pot_state_events WHERE declaration_id = %s "
        "ORDER BY event_id DESC LIMIT 1",
        (decl_id,),
    ).fetchone()
    assert row is not None
    return row[0], row[1]


def _stepped_pot(conn: Conn, monkeypatch: pytest.MonkeyPatch) -> tuple[int, date, date, date]:
    """A frozen pot with one decided snapshot at T₀, E_12 / E_24 pulled in to the first / second session after T₀,
    N = 2, K = 3 and flat synthetic bars. Returns (declaration_id, T₀, E_12, E_24); ``conn`` is left autocommit."""
    decl_id = _frozen(conn)
    d, _ = _first_window(conn)
    t0 = sim.next_session(d)
    _decide(conn, decl_id, t0, d)
    conn.commit()
    conn.autocommit = True

    # E_12 / E_24 pulled in to the first / second session after T₀.
    e12 = sim.next_session(t0)
    e24 = sim.next_session(e12)
    monkeypatch.setattr(look, "endpoint", lambda first, months: {12: e12, 24: e24}[months] if first == t0 else None)
    r = {2842: "0.9", 2843: "0.8", 2844: "0.7"}
    monkeypatch.setattr(st, "book_terms", lambda _decl: (2, 3))
    real_terms = look.terms_of
    monkeypatch.setattr(look, "terms_of", lambda decl: replace(real_terms(decl), n=2, k=3))
    monkeypatch.setattr(rb, "decode_snapshot", lambda doc: doc)
    monkeypatch.setattr(
        st.Rebalance, "of", classmethod(lambda cls, aid, _doc: replace_id(_rebalance(t0, d, r, set(r)), aid))
    )

    def bars(_conn: Conn, ids: Any, session: date) -> st.SessionBars:
        prev = {iid: {s: Decimal(100) for s in (d, t0, e12, session - timedelta(days=1))} for iid in r}
        return st.SessionBars(session, {iid: FLAT for iid in ids}, prev, SPY)

    monkeypatch.setattr(st, "read_session_bars", bars)
    return decl_id, t0, e12, e24


def test_looks_are_stored_once_clear_look_pending_and_wind_the_trial_down(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id, t0, e12, e24 = _stepped_pot(conn, monkeypatch)

    assert st.run_step_job(conn, now=lambda: _at(t0)).stepped == 1
    assert look.look_pending(conn, decl_id, sim.next_session(e12)) == {"look_months": 12, "endpoint": e12.isoformat()}
    assert look.look_pending(conn, decl_id, e12) is None  # a rebalance targeting E itself is not refused

    # Stepping E_12 stores the 12-month look in the same fire; K = 3 and N = 2 cannot meet the evidence minimum.
    fire = st.run_step_job(conn, now=lambda: _at(e12))
    assert fire.stepped == 1 and "look 12m" in fire.note
    rows = conn.execute(
        "SELECT look_months, endpoint_session, kind, verdict, harm, reasons, detail FROM ranking_pot_looks"
    ).fetchall()
    assert [(x[0], x[1], x[2], x[3], x[4]) for x in rows] == [(12, e12, "result", "unevaluable", False)]
    assert "lifecycles_below_minimum" in rows[0][5]
    assert rows[0][6]["t0"] == t0.isoformat() and rows[0][6]["controls"] == 3
    assert look.look_pending(conn, decl_id, sim.next_session(e12)) is None
    assert _state(conn, decl_id) == ("shadow_only", None)

    # Exactly one authoritative result per look; rows are append-only.
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(
            "INSERT INTO ranking_pot_looks (declaration_id, look_months, endpoint_session, kind, verdict, harm, "
            "policy_hash, computed_at) VALUES (%s, 12, %s, 'result', 'not_passed', FALSE, %s, now())",
            (decl_id, e12, "a" * 64),
        )
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE ranking_pot_looks SET harm = TRUE")
    assert st.run_step_job(conn, now=lambda: _at(e12)).stepped == 0  # nothing recomputed

    # The 24-month look, any verdict, winds the trial down (the engine, `completed_window`).
    fire = st.run_step_job(conn, now=lambda: _at(e24))
    assert "look 24m" in fire.note and "winding_down (completed_window)" in fire.note
    assert _state(conn, decl_id) == ("winding_down", "completed_window")
    assert look.reconcile_state(conn, decl_id) is None  # idempotent
    # The completion fence: the declaration froze K = 9,999 but this test stepped 3 controls, so the books are
    # incomplete (and two of them still hold positions, and no step has applied the wind-down).
    with pytest.raises(psycopg.errors.RaiseException, match="books_incomplete"):
        _move(conn, decl_id, "winding_down", "completed", "engine")


def test_policy_drift_with_a_look_due_still_records_the_steps_refusal(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id, t0, e12, e24 = _stepped_pot(conn, monkeypatch)
    with monkeypatch.context() as m:  # step T₀ and E_12 without computing the 12-month look
        m.setattr(look, "compute_due_looks", lambda *_a, **_k: [])
        assert st.run_step_job(conn, now=lambda: _at(t0)).stepped == 1
        assert st.run_step_job(conn, now=lambda: _at(e12)).stepped == 1

    # The look is due under drifted code: it is not computed, and the start-of-fire look pass does not pre-empt the
    # step's own `policy_drift` refusal, which is recorded before the run fails.
    monkeypatch.setattr(rb, "_policy_ok", lambda _decl: False)
    # No step due yet (E_24 has not closed): nothing to refuse, but the skipped look still fails the run.
    with pytest.raises(RuntimeError, match="look 12m not computed: policy drift"):
        st.run_step_job(conn, now=lambda: _at(e12))
    with pytest.raises(RuntimeError, match="policy_drift"):
        st.run_step_job(conn, now=lambda: _at(e24))
    refusals = conn.execute(
        "SELECT session, reason FROM ranking_pot_step_refusals WHERE declaration_id = %s", (decl_id,)
    ).fetchall()
    assert refusals == [(e24, "policy_drift")]
    assert conn.execute("SELECT count(*) FROM ranking_pot_looks").fetchone() == (0,)


def test_a_harm_look_winds_down_and_a_missing_event_holds_look_pending(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    d, _ = _first_window(conn)
    t0 = sim.next_session(d)
    _decide(conn, decl_id, t0, d)
    e12 = sim.next_session(t0)
    conn.execute(
        "INSERT INTO ranking_pot_looks (declaration_id, look_months, endpoint_session, kind, verdict, harm, reasons, "
        "policy_hash, computed_at) VALUES (%s, 12, %s, 'result', 'unevaluable', TRUE, '{endpoint_forced}', %s, now())",
        (decl_id, e12, "a" * 64),
    )
    conn.commit()
    conn.autocommit = True
    monkeypatch.setattr(look, "endpoint", lambda first, months: {12: e12, 24: e12 + timedelta(days=400)}[months])

    # Stored, but the wind-down event is not yet written (a crash between the two transactions): still pending.
    assert look.look_pending(conn, decl_id, sim.next_session(e12)) == {"wind_down_unwritten": "harm"}
    assert look.reconcile_state(conn, decl_id) == "winding_down (harm)"
    assert _state(conn, decl_id) == ("winding_down", "harm")
    assert look.look_pending(conn, decl_id, sim.next_session(e12)) is None


def test_activation_is_refused_on_or_after_the_12_month_anniversary(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    today = datetime.now(UTC).date()
    old = today.replace(year=today.year - 1) - timedelta(days=1)
    while us_market_status(old) == "closed":
        old -= timedelta(days=1)
    _decide(conn, decl_id, old, old - timedelta(days=1))
    with pytest.raises(psycopg.errors.RaiseException, match="look_window"):
        _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    assert _state(conn, decl_id) == ("shadow_only", None)
