"""#3592 slice 4b-i — v2's looks against real Postgres (spec §7; Appendix A R2-16–20): the step job computes each
look once its endpoint is stepped, reading the reference column beside the shadow; one ``result`` row per look under
v2's hash with ``detail.v2``; the last look winds the seat down; a month with no decided attempt is counted, not
averaged.

Endpoints are pulled in to the next sessions and bars are synthetic (``tests/test_ranking_pot_v2_look.py`` covers the
arithmetic); the database parts are real."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ranking_pot_look as look
from app.services import ranking_pot_rebalance as rb
from app.services import ranking_pot_v2 as v2
from app.services import ranking_pot_v2_look as v2look
from app.services import ranking_pot_v2_step as st
from app.services.ai_trial_pack import canonical_sha256
from app.services.ranking_pot_v2_policy import HARM_ALPHA, RANKING_POT_V2_POLICY_HASH, TURNOVER_BAR
from tests.test_ranking_pot_rebalance_db import _decided, _insert
from tests.test_ranking_pot_step import FLAT
from tests.test_ranking_pot_v2_step import R5
from tests.test_ranking_pot_v2_step_db import FRI, MON, SPY, THU, _at, _declare, _patch

Conn = psycopg.Connection[Any]
K = 2
TUE = date(2026, 10, 6)


def _decide(conn: Conn, decl: int) -> None:
    snapshot = {
        "kind": rb.SNAPSHOT_KIND,
        "strategy_id": v2.STRATEGY_ID,
        "target_session": FRI.isoformat(),
        "last_session": THU.isoformat(),
        "spy": {"bid": "499.9", "ask": "500.1"},
    }
    _insert(
        conn,
        decl,
        target_session=FRI,
        month=FRI.replace(day=1),
        **_decided(snapshot=Jsonb(snapshot), snapshot_sha256=canonical_sha256(snapshot)),
    )
    # A refusal in T₀'s own month: never a skipped month.
    _insert(conn, decl, target_session=TUE, month=TUE.replace(day=1))
    conn.commit()


def _looks(conn: Conn, decl: int) -> list[tuple[Any, ...]]:
    return conn.execute(
        "SELECT look_months, endpoint_session, verdict, harm, reasons, detail, policy_hash FROM ranking_pot_looks "
        "WHERE declaration_id = %s ORDER BY look_months",
        (decl,),
    ).fetchall()


def test_v2_looks_are_stored_once_under_v2s_hash_and_wind_the_seat_down(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    terms = {
        "n": 2,
        "k_controls": K,
        "book_count": K + 3,
        "execution": "none",
        "per_look_alpha": "1/40",
        "harm_alpha": str(HARM_ALPHA),
        "turnover_bar": str(TURNOVER_BAR),
    }
    decl = _declare(conn, terms=terms)
    _decide(conn, decl)
    conn.autocommit = True
    _patch(monkeypatch, terms_calls=[])
    monkeypatch.setattr(v2look, "policy_ok", lambda _decl: True)
    monkeypatch.setattr(look, "endpoint", lambda first, months: {12: MON, 24: TUE}[months] if first == FRI else None)

    def bars(_conn: Conn, ids: Any, session: date) -> st.SessionBars:
        closes = {iid: {s: Decimal(100) for s in (THU, FRI, MON, TUE)} for iid in R5}
        return st.SessionBars(session, {iid: FLAT for iid in ids}, closes, SPY)

    monkeypatch.setattr(st, "read_session_bars", bars)

    first = st.run_step_job(conn, now=lambda: _at(MON))
    assert first.stepped == 2 and "look 12m at 2026-10-05: unevaluable" in first.note, first.note
    [(months, end, verdict, harm, reasons, detail, policy_hash)] = _looks(conn, decl)
    assert (months, end, policy_hash, harm) == (12, MON, RANKING_POT_V2_POLICY_HASH, False)
    # One representation: v1's verdict and reasons in the row, conditions 6–7 and v2_verdict in detail.v2.
    assert verdict == "unevaluable" and "lifecycles_below_minimum" in reasons
    decoded = v2look.v2_detail_of(detail)
    assert decoded.v2_verdict == "unevaluable" and decoded.reasons == ("reference_below_minimum",)
    ref = detail["v2"]["reference"]
    assert ref["sessions"] == 2 and ref["lifecycles"] == 2  # the reference's own two names, read from its column
    assert detail["v2"]["turnover"] | {"bar": None} == {
        "mean": None,
        "bar": None,
        "rebalances": 0,  # the only applied rebalance is the initial fill
        "entered": [],
        "skipped_months": 0,
    }
    assert look.look_pending(conn, decl, TUE) is None

    second = st.run_step_job(conn, now=lambda: _at(TUE))
    assert second.stepped == 1 and "winding_down (completed_window)" in second.note, second.note
    looks = _looks(conn, decl)
    assert [r[0] for r in looks] == [12, 24]
    assert looks[1][5]["v2"]["turnover"]["skipped_months"] == 0
    state = conn.execute(
        "SELECT to_state, wind_down_reason FROM ranking_pot_state_events WHERE declaration_id = %s "
        "ORDER BY event_id DESC LIMIT 1",
        (decl,),
    ).fetchone()
    assert state == ("winding_down", "completed_window")

    # Exactly once: a later fire computes nothing new.
    st.run_step_job(conn, now=lambda: _at(TUE) + timedelta(hours=1))
    assert len(_looks(conn, decl)) == 2


def test_a_drifted_v2_look_is_not_computed(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    terms = {"n": 2, "k_controls": K, "book_count": K + 3, "execution": "none", "turnover_bar": "1/2"}
    decl = _declare(conn, terms=terms)
    _decide(conn, decl)
    conn.autocommit = True
    _patch(monkeypatch, terms_calls=[])
    monkeypatch.setattr(look, "endpoint", lambda first, months: {12: FRI, 24: MON}[months] if first == FRI else None)
    # The step's own check passes (patched); the look's fails: the look is skipped and noted, nothing is stored.
    monkeypatch.setattr(v2look, "policy_ok", lambda _decl: False)
    out = st.run_step_job(conn, now=lambda: _at(FRI))
    assert out.stepped == 1 and "look 12m not computed: policy drift" in out.note, out.note
    assert _looks(conn, decl) == []


def test_skipped_months_are_keyed_on_month_not_target(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    decl = _declare(conn)
    _decide(conn, decl)  # T₀ = FRI; plus a refusal in T₀'s own month
    attempts: list[dict[str, Any]] = [
        # November refused twice (a retry): one month.
        {"target_session": date(2026, 11, 2), "month": date(2026, 11, 1)},
        {"target_session": date(2026, 11, 2), "month": date(2026, 11, 1)},
        # December refused on one target, decided on a later one: not skipped.
        {"target_session": date(2026, 12, 1), "month": date(2026, 12, 1)},
        _decided(target_session=date(2026, 12, 2), month=date(2026, 12, 1)),
        # January skipped by catch-up against February's target, which is decided: January counts, February not.
        {
            "target_session": date(2027, 2, 1),
            "month": date(2027, 1, 1),
            "outcome": "skipped",
            "refusal": "not_attempted",
        },
        _decided(target_session=date(2027, 2, 1), month=date(2027, 2, 1)),
    ]
    for over in attempts:
        _insert(conn, decl, **over)
    conn.commit()
    assert v2look._skipped_months(conn, decl, t0=FRI, end=date(2026, 11, 30)) == 1
    assert v2look._skipped_months(conn, decl, t0=FRI, end=date(2026, 12, 31)) == 1  # by target: 2 (12-01)
    assert v2look._skipped_months(conn, decl, t0=FRI, end=date(2027, 2, 26)) == 2  # November and January
