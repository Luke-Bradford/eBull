"""#2842 slice 5a — the executed book's decision against real Postgres (spec §4, ``sql/449``)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import Any, cast

import psycopg
import pytest

from app.services import ranking_pot as pot
from app.services import ranking_pot_exec as ex
from app.services import ranking_pot_rebalance as rb
from app.services.ai_trial_pack import canonical_sha256
from app.services.strategy_control_plane import configure_deployment
from tests.test_ranking_pot_job_db import _first_window
from tests.test_ranking_pot_schema_db import POT, SCORED_AT, _frozen, _move

Conn = psycopg.Connection[Any]
S0: tuple[int, ...] = (2842, 2843, 2844)


def _facts(iid: int, last: date) -> pot.NameFacts:
    bar = {"open": Decimal(100), "high": Decimal(101), "low": Decimal(99), "close": Decimal(100), "volume": 1000}
    return pot.NameFacts(
        instrument_id=iid,
        symbol=f"POT{iid}",
        is_tradable=True,
        asset_class="us_equity",
        total_score=Decimal("0.5"),
        completeness_tier="full",
        filings_status="analysable",
        market_cap_usd=Decimal(10**11),
        bid=Decimal("99.95"),
        ask=Decimal("100.05"),
        quoted_at=datetime.combine(last, time(21), UTC),
        bar_dates=(last,),
        bar_rows=(bar,),
    )


def _universes(scores: dict[int, str], f: set[int]) -> pot.Universes:
    r = frozenset(scores)
    return pot.Universes(
        breakpoint=Decimal(1),
        max_cut=Fraction(1, 10),
        r_ids=r,
        f_ids=frozenset(f),
        hold_failure=dict.fromkeys(frozenset(S0) - r, cast(pot.HoldRule, "not_tradable")),
        entry_failure=dict.fromkeys(r - f, cast(pot.EntryRule, "quote_ineligible")),
        own_score={i: Decimal(s) for i, s in scores.items()},
        max_return={i: Fraction(1, 100) for i in r},
        atr={i: Fraction(2) for i in r},
        max_population=len(r),
        nyse_cap_population=len(r),
    )


def _decide_month(
    conn: Conn,
    monkeypatch: pytest.MonkeyPatch,
    d: date,
    universes: pot.Universes,
    *,
    state: str = "executing",
    policy_hash: str = "e" * 64,
) -> tuple[int, ex.ExecutedResult]:
    """One decided attempt for the month after completed session ``d``, with its executed decision."""
    as_of = datetime.combine(d, time(23, 40), UTC)
    decl = rb.load_declaration(conn)
    assert decl is not None
    due = rb.plan(
        as_of, first=rb.first_month(decl.frozen_at), resolved=rb.read_history(conn, decl.declaration_id).resolved
    )
    assert due.due
    inputs = rb.SnapshotInputs(
        declaration_id=decl.declaration_id,
        declaration_sha256=decl.doc_sha256,
        policy_hash=policy_hash,
        as_of=as_of,
        last_session=d,
        target_session=due.target_session,
        scored_at=SCORED_AT,
        facts=tuple(_facts(i, d) for i in S0),
        scores={
            i: pot.ScoreBreakdown("v1.5-balanced", Decimal("0.5"), Decimal("0.5"), {"quality": Decimal("0.5")}, ())
            for i in S0
        },
        nyse_caps=(),
        spy=rb.SpyInputs(None, None, None, None),
        theses={},
    )
    monkeypatch.setattr(rb, "decode_snapshot", lambda _doc: inputs)
    snapshot = {"kind": rb.SNAPSHOT_KIND, "target_session": due.target_session.isoformat(), "names": []}
    prepared = rb.Prepared(snapshot, canonical_sha256(snapshot), universes, {"r_count": len(universes.r_ids)})
    with conn.transaction():
        rb.begin_rebalance(conn)
        attempt = rb.record_decided(conn, decl, due, prepared, as_of=as_of, scored_at=SCORED_AT)
        result = ex.record_executed(conn, decl, attempt, prepared, state=state)
    return attempt, result


def _rows(conn: Conn, attempt: int) -> dict[int, tuple[str, str | None, int | None]]:
    return {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT instrument_id, action, reason, lifecycle_id FROM ranking_pot_exec_decisions WHERE attempt_id = %s",
            (attempt,),
        ).fetchall()
    }


def _lifecycles(conn: Conn) -> list[tuple[int, int, int, int | None]]:
    return [
        (r[0], r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT lifecycle_id, instrument_id, slot, replaces_lifecycle_id FROM ranking_pot_exec_lifecycles "
            "ORDER BY lifecycle_id"
        ).fetchall()
    ]


def _fund_open(conn: Conn, lifecycle_id: int, deployment_id: int) -> None:
    """The executor's (5b) effect, simulated: an allocated funding decision and an open trade."""
    row = conn.execute(
        "INSERT INTO strategy_funding_decisions (signal_id, deployment_id, verdict, amount, reason_code) "
        "SELECT signal_id, %s, 'allocated', 100, 't' FROM ranking_pot_exec_lifecycles WHERE lifecycle_id = %s "
        "RETURNING funding_decision_id",
        (deployment_id, lifecycle_id),
    ).fetchone()
    assert row is not None
    conn.execute(
        "INSERT INTO strategy_trades (funding_decision_id, instrument_id, status) "
        "SELECT %s, instrument_id, 'open' FROM ranking_pot_exec_lifecycles WHERE lifecycle_id = %s",
        (row[0], lifecycle_id),
    )


def test_executed_book_decides_enters_stamps_and_reuses_slots(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    deployment = configure_deployment(
        conn,
        strategy_id=POT,
        strategy_version="v1",
        mode="paper",
        capital_limit=Decimal(1000),
        enabled=True,
        changed_by="operator",
        reason="#2842 test",
    ).deployment_id
    conn.commit()
    conn.autocommit = True
    d, _ = _first_window(conn)

    # Month 1: 2842 and 2843 enter (F = {2842, 2843}); 2844 is in R but infeasible.
    a1, r1 = _decide_month(conn, monkeypatch, d, _universes({2842: "0.9", 2843: "0.8", 2844: "0.7"}, {2842, 2843}))
    assert (r1.entries_allowed, r1.v1_active, r1.entries, r1.exits) == (True, False, 2, 0)
    lcs = _lifecycles(conn)
    assert [(iid, slot, rep) for _, iid, slot, rep in lcs] == [(2842, 1, None), (2843, 2, None)]
    rows = _rows(conn, a1)
    assert rows[2844] == ("not_selected", "infeasible:quote_ineligible", None)
    assert rows[2842] == ("enter", None, lcs[0][0])
    signal = conn.execute(
        "SELECT s.strategy_id, s.fill_bar_date, s.fill_price, l.ticket->>'book', l.ticket_sha256 "
        "FROM ranking_pot_exec_lifecycles l JOIN strategy_signals s USING (signal_id) WHERE l.lifecycle_id = %s",
        (lcs[0][0],),
    ).fetchone()
    assert signal is not None and signal[:4] == (
        POT,
        rb.target_session(datetime.combine(d, time(23, 40), UTC)),
        Decimal(100),
        "executed",
    )

    # The executor fills 2842; 2843 is never submitted (expired by month 2).
    _fund_open(conn, lcs[0][0], deployment)

    # Month 2: 2842 leaves R (exit, stamped); 2843 (its old lifecycle expired) and 2844 enter.
    d2 = _next_month_session(d)
    a2, r2 = _decide_month(conn, monkeypatch, d2, _universes({2843: "0.8", 2844: "0.7"}, {2843, 2844}))
    assert (r2.held, r2.exits, r2.entries) == (1, 1, 2)
    stamp = conn.execute("SELECT lifecycle_id, attempt_id, reason FROM ranking_pot_exec_exit_stamps").fetchone()
    assert stamp == (lcs[0][0], a2, "ineligible:not_tradable")
    new = _lifecycles(conn)[2:]
    # Free slots in ascending index (2: the expired 2843 lifecycle released it); slot 1 is still 2842's (r3-99).
    assert [(iid, slot, rep) for _, iid, slot, rep in new] == [(2843, 2, None), (2844, 3, None)]

    # A halt withholds entries; the stamped lifecycle stays exit_pending and keeps slot 1.
    conn.execute(
        "INSERT INTO ranking_pot_state_events (declaration_id, from_state, to_state, reason, actor) "
        "VALUES (%s, 'executing', 'halted_operator', 't', 'operator')",
        (decl_id,),
    )
    d3 = _next_month_session(d2)
    a3, r3 = _decide_month(
        conn, monkeypatch, d3, _universes({2843: "0.8", 2844: "0.7"}, {2843, 2844}), state="halted_operator"
    )
    assert (r3.entries_allowed, r3.entries) == (False, 0)
    assert _rows(conn, a3)[2842][0] == "exit_pending"


def test_executed_rows_are_written_only_by_the_deciding_transaction(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    conn.autocommit = True
    d, _ = _first_window(conn)
    a1, _ = _decide_month(conn, monkeypatch, d, _universes({2842: "0.9"}, {2842}))

    # A later transaction, even holding the lock, cannot add rows to that attempt.
    with pytest.raises(psycopg.errors.RaiseException, match="only by the transaction that decided it"):
        with conn.transaction():
            rb.begin_rebalance(conn)
            conn.execute(
                "INSERT INTO ranking_pot_exec_decisions (attempt_id, instrument_id, action, reason) "
                "VALUES (%s, 2843, 'not_selected', 'band_full')",
                (a1,),
            )
    # Append-only.
    with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
        conn.execute("UPDATE ranking_pot_exec_lifecycles SET slot = 2")

    # In the wrong state the header is refused (the decision read `executing`, the book is `halted_operator`).
    _move(conn, decl_id, "executing", "halted_operator", "operator")
    with pytest.raises(psycopg.errors.RaiseException, match="no executed-book decision"):
        _decide_month(
            conn,
            monkeypatch,
            _next_month_session(d),
            _universes({2842: "0.9"}, {2842}),
            state="executing",
        )


def _next_month_session(d: date) -> date:
    d2 = d + timedelta(days=31)
    while rb.target_session(datetime.combine(d2, time(23, 40), UTC)).day > 7:
        d2 += timedelta(days=1)
    return d2


def test_no_entry_while_an_ai_trial_is_active(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    decl_id = _frozen(conn)
    _move(conn, decl_id, "shadow_only", "executing", "supervisor")
    conn.autocommit = True
    d, _ = _first_window(conn)
    monkeypatch.setattr(ex, "V1_ACTIVE_SQL", "SELECT TRUE")
    attempt, result = _decide_month(conn, monkeypatch, d, _universes({2842: "0.9"}, {2842}))
    assert (result.v1_active, result.entries_allowed, result.entries) == (True, False, 0)
    assert _rows(conn, attempt)[2842] == ("not_selected", "entries_halted", None)
